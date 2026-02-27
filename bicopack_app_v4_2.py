import streamlit as st
import pandas as pd
import uuid
from datetime import datetime, date, timedelta
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

        hora_inicio = st.time_input(
            "Hora de inicio",
            value=datetime.now().time().replace(second=0, microsecond=0),
            step=60
        )

        observaciones_inicio = st.text_area("Observaciones inicio")

        guardar_inicio = st.form_submit_button("Guardar inicio")

        if guardar_inicio:
            if not lote_mp or not lote_of or not operario_inicio:
                st.error("Faltan campos obligatorios.")
            else:
                columnas_en_curso = [
                    "bobina_id", "fecha", "turno", "maquina",
                    "lote_materia_prima", "lote_of",
                    "hora_inicio", "operario_inicio",
                    "observaciones_inicio"
                ]

                df_en_curso = load_csv(EN_CURSO_PATH, columnas_en_curso)

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

                df_en_curso = pd.concat(
                    [df_en_curso, pd.DataFrame([new_row], columns=columnas_en_curso)],
                    ignore_index=True
                )

                save_csv(df_en_curso, EN_CURSO_PATH)

                try:
                    gs_append_row("EN_CURSO", new_row)
                except Exception as e:
                    st.warning(f"No se pudo enviar a Google Sheets: {e}")

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

    df_en_curso = load_csv(EN_CURSO_PATH, columnas_en_curso)

    if df_en_curso.empty:
        st.info("No hay bobinas en curso.")
    else:
        opciones = df_en_curso.copy()
        opciones["label"] = opciones.apply(
            lambda r: f"Máquina {r['maquina']} – OF {r['lote_of']} – inicio {r['hora_inicio']}",
            axis=1
        )

        seleccion = st.selectbox("Selecciona la bobina a cerrar", opciones["label"])
        fila = opciones[opciones["label"] == seleccion].iloc[0]

        with st.form("fin_bobina"):
            hora_fin = st.time_input("Hora de fin", step=60)
            operario_fin = st.text_input("Operario que finaliza")
            peso = st.number_input("Peso (kg)", min_value=0.0, step=0.1)
            taras = st.number_input("Taras", min_value=0, step=1)
            observaciones_fin = st.text_area("Observaciones fin")

            guardar_fin = st.form_submit_button("Guardar fin")

            if guardar_fin:
                if not operario_fin:
                    st.error("Debes indicar el operario.")
                else:
                    fila_bobinas = [
                        fila["fecha"],
                        fila["turno"],
                        int(fila["maquina"]),
                        fila["lote_materia_prima"],
                        fila["lote_of"],
                        fila["hora_inicio"],
                        fila["operario_inicio"],
                        hora_fin.strftime("%H:%M"),
                        operario_fin,
                        float(peso),
                        int(taras),
                        observaciones_fin
                    ]

                    try:
                        gs_append_row("BOBINAS", fila_bobinas)
                    except Exception as e:
                        st.warning(f"No se pudo enviar a Google Sheets: {e}")

                    df_en_curso = df_en_curso[df_en_curso["bobina_id"] != fila["bobina_id"]]
                    save_csv(df_en_curso, EN_CURSO_PATH)

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

    df_en_curso = load_csv(EN_CURSO_PATH, columnas_en_curso)

    with st.form("evento"):
        tipo = st.selectbox("Tipo", ["Incidencia", "Tarea/Limpieza"])
        fecha = st.date_input("Fecha", value=date.today())
        maquina = st.number_input("Máquina", min_value=1, step=1)
        hora_inicio = st.time_input("Hora inicio", step=60)
        hora_fin = st.time_input("Hora fin", step=60)
        operario = st.text_input("Operario")
        descripcion = st.text_area("Descripción")

        guardar_evento = st.form_submit_button("Guardar evento")

        if guardar_evento:
            if not operario or not descripcion:
                st.error("Faltan campos obligatorios.")
            else:
                bobina_activa = df_en_curso[df_en_curso["maquina"] == int(maquina)]

                turno = ""
                lote_of = ""

                if not bobina_activa.empty:
                    turno = bobina_activa.iloc[0]["turno"]
                    lote_of = bobina_activa.iloc[0]["lote_of"]
                elif tipo == "Incidencia":
                    st.error("⚠️ No hay OF activa en esta máquina.")
                    st.stop()

                start_dt = datetime.combine(fecha, hora_inicio)
                end_dt = datetime.combine(fecha, hora_fin)

                if end_dt < start_dt:
                    end_dt = end_dt + timedelta(days=1)

                minutos = int((end_dt - start_dt).total_seconds() / 60)

                new_event = [
                    fecha.isoformat(),
                    turno,
                    int(maquina),
                    lote_of,
                    tipo,
                    hora_inicio.strftime("%H:%M"),
                    hora_fin.strftime("%H:%M"),
                    minutos,
                    operario,
                    descripcion
                ]

                df_eventos = pd.concat(
                    [df_eventos, pd.DataFrame([new_event], columns=columnas_eventos)],
                    ignore_index=True
                )

                save_csv(df_eventos, EVENTOS_PATH)

                try:
                    gs_append_row("EVENTOS", new_event)
                except Exception as e:
                    st.warning(f"No se pudo enviar a Google Sheets: {e}")

                st.success("✅ Evento guardado")
