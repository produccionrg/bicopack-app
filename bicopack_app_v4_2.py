import streamlit as st
import pandas as pd
import uuid
from datetime import datetime, date, timedelta, time
import os
import json
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Bicopack – Registro", layout="centered")

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
EN_CURSO_PATH = os.path.join(BASE_PATH, "bobinas_en_curso.csv")
EVENTOS_PATH = os.path.join(BASE_PATH, "eventos.csv")


# --------------------
# CSV helpers
# --------------------
def load_csv(path: str, columns: list[str]) -> pd.DataFrame:
    if os.path.exists(path):
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame(columns=columns)
    return pd.DataFrame(columns=columns)


def save_csv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False)


# --------------------
# Time helpers (manual input)
# --------------------
def parse_hhmm(value: str) -> time:
    """
    Parse time string 'HH:MM' into datetime.time.
    Raises ValueError if invalid.
    """
    if value is None:
        raise ValueError("Hora vacía.")
    s = str(value).strip()
    if not s:
        raise ValueError("Hora vacía.")
    try:
        dt = datetime.strptime(s, "%H:%M")
        return dt.time()
    except Exception:
        raise ValueError("Formato inválido. Usa HH:MM (ej: 07:05, 14:30).")


def safe_int(x, default=None):
    try:
        if pd.isna(x):
            return default
        return int(x)
    except Exception:
        try:
            return int(str(x).strip())
        except Exception:
            return default


# --------------------
# Google Sheets helpers
# --------------------
def _gs_client():
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT", "")
    if not sa_json:
        raise RuntimeError("Falta GOOGLE_SERVICE_ACCOUNT en Render.")

    info = json.loads(sa_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def gs_append_row(worksheet_name: str, row: list):
    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "")
    if not sheet_id:
        raise RuntimeError("Falta GOOGLE_SHEET_ID en Render.")
    gc = _gs_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)
    ws.append_row(row, value_input_option="USER_ENTERED")


def gs_get_all(worksheet_name: str) -> pd.DataFrame:
    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "")
    if not sheet_id:
        raise RuntimeError("Falta GOOGLE_SHEET_ID en Render.")
    gc = _gs_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)
    data = ws.get_all_records()
    return pd.DataFrame(data)


def gs_delete_row_by_bobina(bobina_id):
    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "")
    if not sheet_id:
        raise RuntimeError("Falta GOOGLE_SHEET_ID en Render.")
    gc = _gs_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet("EN_CURSO")

    data = ws.get_all_values()

    for i, row in enumerate(data):
        if row and row[0] == str(bobina_id):
            ws.delete_rows(i + 1)
            break


# --------------------
# UI
# --------------------
st.title("Bicopack – Registro de producción")

tabs = st.tabs(["Inicio de bobina", "Fin de bobina", "Tareas / Incidencias"])


# =========================
# INICIO DE BOBINA
# =========================
with tabs[0]:
    st.subheader("Inicio de bobina")

    with st.form("inicio_bobina"):
        fecha = st.date_input("Fecha", value=date.today())
        turno = st.selectbox("Turno", ["1 (mañana)", "2 (tarde)", "3 (noche)"])
        maquina = st.number_input("Número de máquina", min_value=1, step=1)
        lote_mp = st.text_input("Lote de materia prima")
        lote_of = st.text_input("Lote de orden de fabricación (OF)")
        operario_inicio = st.text_input("Nombre del operario")

        # Hora manual (HH:MM)
        hora_inicio_txt = st.text_input(
            "Hora de inicio (HH:MM)",
            value=datetime.now().strftime("%H:%M"),
            help="Escribe la hora en formato HH:MM (ej: 07:05, 14:30)."
        )

        observaciones_inicio = st.text_area("Observaciones inicio")

        guardar_inicio = st.form_submit_button("Guardar inicio")

        if guardar_inicio:
            if not lote_mp or not lote_of or not operario_inicio:
                st.error("Faltan campos obligatorios.")
                st.stop()

            # validar hora
            try:
                hora_inicio = parse_hhmm(hora_inicio_txt)
            except ValueError as e:
                st.error(str(e))
                st.stop()

            columnas_en_curso = [
                "bobina_id", "fecha", "turno", "maquina",
                "lote_materia_prima", "lote_of",
                "hora_inicio", "operario_inicio",
                "observaciones_inicio"
            ]

            # 1) Bloqueo: no permitir iniciar si ya hay bobina abierta en esa máquina
            try:
                df_en_curso_check = gs_get_all("EN_CURSO")
            except Exception as e:
                st.warning(f"No se pudo leer EN_CURSO para validar bobinas abiertas: {e}")
                df_en_curso_check = pd.DataFrame(columns=columnas_en_curso)

            if not df_en_curso_check.empty and "maquina" in df_en_curso_check.columns:
                df_en_curso_check["maquina_norm"] = df_en_curso_check["maquina"].apply(lambda x: safe_int(x, default=-999))
                if int(maquina) in df_en_curso_check["maquina_norm"].tolist():
                    st.error("⚠️ Ya hay una bobina abierta en esta máquina. Primero debes cerrar la bobina en 'Fin de bobina'.")
                    st.stop()

            bobina_id = str(uuid.uuid4())

            new_row = [
                bobina_id,
                fecha.isoformat(),
                turno,
                int(maquina),
                lote_mp,
                lote_of,
                hora_inicio.strftime("%H:%M"),
                operario_inicio,
                observaciones_inicio
            ]

            # guardar en Google
            try:
                gs_append_row("EN_CURSO", new_row)
            except Exception as e:
                st.warning(f"No se pudo enviar a Google Sheets: {e}")

            # backup CSV
            df_backup = load_csv(EN_CURSO_PATH, columnas_en_curso)
            df_backup = pd.concat(
                [df_backup, pd.DataFrame([new_row], columns=columnas_en_curso)],
                ignore_index=True
            )
            save_csv(df_backup, EN_CURSO_PATH)

            st.success("✅ Inicio registrado")


# =========================
# FIN DE BOBINA
# =========================
with tabs[1]:
    st.subheader("Fin de bobina")

    columnas_en_curso = [
        "bobina_id", "fecha", "turno", "maquina",
        "lote_materia_prima", "lote_of",
        "hora_inicio", "operario_inicio",
        "observaciones_inicio"
    ]

    try:
        df_en_curso = gs_get_all("EN_CURSO")
    except Exception:
        df_en_curso = pd.DataFrame(columns=columnas_en_curso)

    if df_en_curso.empty:
        st.info("No hay bobinas en curso.")
    else:
        opciones = df_en_curso.copy()

        # normalizar máquina por si viene como texto
        if "maquina" in opciones.columns:
            opciones["maquina"] = opciones["maquina"].apply(lambda x: safe_int(x, default=x))

        opciones["label"] = opciones.apply(
            lambda r: f"Máquina {r.get('maquina','?')} – OF {r.get('lote_of','')} – inicio {r.get('hora_inicio','')}",
            axis=1
        )

        seleccion = st.selectbox("Selecciona la bobina a cerrar", opciones["label"])

        fila = opciones[opciones["label"] == seleccion].iloc[0]

        # fecha_inicio viene de EN_CURSO
        try:
            fecha_inicio = datetime.strptime(str(fila["fecha"]), "%Y-%m-%d").date()
        except Exception:
            fecha_inicio = date.today()

        with st.form("fin_bobina"):
            # Hora manual (HH:MM)
            hora_fin_txt = st.text_input(
                "Hora de fin (HH:MM)",
                value=datetime.now().strftime("%H:%M"),
                help="Escribe la hora en formato HH:MM (ej: 07:05, 14:30)."
            )

            operario_fin = st.text_input("Operario que finaliza")
            peso = st.number_input("Peso (kg)", min_value=0.0, step=0.1)
            taras = st.number_input("Taras", min_value=0, step=1)
            observaciones_fin = st.text_area("Observaciones fin")

            guardar_fin = st.form_submit_button("Guardar fin")

            if guardar_fin:
                if not operario_fin:
                    st.error("Debes indicar el operario.")
                    st.stop()

                # validar hora fin
                try:
                    hora_fin = parse_hhmm(hora_fin_txt)
                except ValueError as e:
                    st.error(str(e))
                    st.stop()

                # calcular fecha_fin automática (si pasa de medianoche)
                try:
                    hora_ini_obj = parse_hhmm(str(fila["hora_inicio"]))
                except Exception:
                    # si por algún motivo no se puede parsear la hora inicio, asumimos mismo día
                    hora_ini_obj = None

                fecha_fin = fecha_inicio
                if hora_ini_obj is not None:
                    if datetime.combine(fecha_inicio, hora_fin) < datetime.combine(fecha_inicio, hora_ini_obj):
                        fecha_fin = fecha_inicio + timedelta(days=1)

                # ⚠️ NUEVO FORMATO BOBINAS: fecha_inicio y fecha_fin
                # Asegúrate de tener estas columnas en Google Sheets -> pestaña BOBINAS
                fila_bobinas = [
                    str(fecha_inicio.isoformat()),   # fecha_inicio
                    str(fecha_fin.isoformat()),      # fecha_fin
                    str(fila.get("turno", "")),
                    safe_int(fila.get("maquina", ""), default=""),
                    str(fila.get("lote_materia_prima", "")),
                    str(fila.get("lote_of", "")),
                    str(fila.get("hora_inicio", "")),
                    str(fila.get("operario_inicio", "")),
                    hora_fin.strftime("%H:%M"),
                    str(operario_fin),
                    float(peso),
                    int(taras),
                    str(observaciones_fin)
                ]

                fila_bobinas = [None if pd.isna(x) else x for x in fila_bobinas]

                try:
                    gs_append_row("BOBINAS", fila_bobinas)

                    # eliminar de EN_CURSO en Google
                    gs_delete_row_by_bobina(fila["bobina_id"])

                except Exception as e:
                    st.warning(f"No se pudo enviar a Google Sheets: {e}")

                st.success("✅ Bobina cerrada")


# =========================
# EVENTOS
# =========================
with tabs[2]:
    st.subheader("Registro de tareas / incidencias")

    columnas_eventos = [
        "fecha", "turno", "maquina", "lote_of",
        "tipo", "hora_inicio", "hora_fin",
        "minutos", "operario", "descripcion"
    ]

    df_eventos = load_csv(EVENTOS_PATH, columnas_eventos)

    try:
        df_en_curso = gs_get_all("EN_CURSO")
    except Exception:
        df_en_curso = pd.DataFrame()

    with st.form("evento"):
        tipo = st.selectbox("Tipo", ["Incidencia", "Tarea/Limpieza"])
        fecha = st.date_input("Fecha", value=date.today())
        maquina = st.number_input("Máquina", min_value=1, step=1)

        # Horas manuales
        hora_inicio_txt = st.text_input(
            "Hora inicio (HH:MM)",
            value=datetime.now().strftime("%H:%M")
        )
        hora_fin_txt = st.text_input(
            "Hora fin (HH:MM)",
            value=datetime.now().strftime("%H:%M")
        )

        operario = st.text_input("Operario")
        descripcion = st.text_area("Descripción")

        guardar_evento = st.form_submit_button("Guardar evento")

        if guardar_evento:
            if not operario or not descripcion:
                st.error("Faltan campos obligatorios.")
                st.stop()

            # validar horas
            try:
                hora_inicio = parse_hhmm(hora_inicio_txt)
                hora_fin = parse_hhmm(hora_fin_txt)
            except ValueError as e:
                st.error(str(e))
                st.stop()

            turno = ""
            lote_of = ""

            if not df_en_curso.empty and "maquina" in df_en_curso.columns:
                df_en_curso["maquina_norm"] = df_en_curso["maquina"].apply(lambda x: safe_int(x, default=-999))
                bobina_activa = df_en_curso[df_en_curso["maquina_norm"] == int(maquina)]
            else:
                bobina_activa = pd.DataFrame()

            if not bobina_activa.empty:
                turno = bobina_activa.iloc[0].get("turno", "")
                lote_of = bobina_activa.iloc[0].get("lote_of", "")

            elif tipo == "Incidencia":
                st.error("⚠️ No hay OF activa en esta máquina.")
                st.stop()

            start_dt = datetime.combine(fecha, hora_inicio)
            end_dt = datetime.combine(fecha, hora_fin)

            if end_dt < start_dt:
                end_dt += timedelta(days=1)

            minutos = int((end_dt - start_dt).total_seconds() // 60)

            new_event = [
                fecha.isoformat(),
                str(turno),
                int(maquina),
                str(lote_of),
                str(tipo),
                hora_inicio.strftime("%H:%M"),
                hora_fin.strftime("%H:%M"),
                int(minutos),
                str(operario),
                str(descripcion)
            ]

            try:
                gs_append_row("EVENTOS", new_event)
            except Exception as e:
                st.warning(f"No se pudo enviar a Google Sheets: {e}")

            st.success("✅ Evento guardado")
