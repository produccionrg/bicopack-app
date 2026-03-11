import streamlit as st
import pandas as pd
import uuid
from datetime import datetime, date, timedelta
import os
import json
import gspread
from google.oauth2.service_account import Credentials
import pytz

st.set_page_config(page_title="Bicopack – Registro", layout="centered")

tz = pytz.timezone("Europe/Madrid")

# --------------------
# Helpers
# --------------------

def parse_hhmm(value: str):
    return datetime.strptime(value.strip(), "%H:%M").time()


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


def clean_row(row):
    clean = []

    for x in row:
        if pd.isna(x):
            clean.append("")
        elif hasattr(x, "item"):
            clean.append(x.item())
        else:
            clean.append(x)

    return clean


def ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)

    for col in columns:
        if col not in df.columns:
            df[col] = ""

    return df


def get_first_existing_value(row, possible_cols, default=""):
    for col in possible_cols:
        if col in row and pd.notna(row[col]) and str(row[col]).strip() != "":
            return row[col]
    return default


# --------------------
# Google Sheets
# --------------------

@st.cache_resource
def _gs_client():
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT", "")
    info = json.loads(sa_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


@st.cache_resource
def _get_spreadsheet():
    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "")
    gc = _gs_client()
    return gc.open_by_key(sheet_id)


@st.cache_resource
def _get_ws(sheet_name: str):
    sh = _get_spreadsheet()
    return sh.worksheet(sheet_name)


def gs_append_row(sheet_name: str, row: list):
    ws = _get_ws(sheet_name)
    row = clean_row(row)
    ws.append_row(row, value_input_option="RAW")
    gs_get_all.clear()


@st.cache_data(ttl=60)
def gs_get_all(sheet_name: str):
    ws = _get_ws(sheet_name)
    data = ws.get_all_records()
    return pd.DataFrame(data)


def gs_delete_row_by_bobina(bobina_id):
    ws = _get_ws("EN_CURSO")
    data = ws.get_all_values()

    for i, row in enumerate(data):
        if row and str(row[0]).strip() == str(bobina_id).strip():
            ws.delete_rows(i + 1)
            break

    gs_get_all.clear()


# --------------------
# UI
# --------------------

st.title("Bicopack – Registro de producción")

tabs = st.tabs([
    "Panel producción",
    "Inicio bobina",
    "Fin bobina",
    "Tareas / Incidencias"
])

# --------------------
# Carga única de EN_CURSO
# --------------------

EN_CURSO_COLS = [
    "bobina_id",
    "fecha",
    "turno",
    "maquina",
    "lote_mp",
    "lote_of",
    "hora_inicio",
    "operario_inicio",
    "observaciones_inicio",
]

with st.spinner("Cargando datos..."):
    try:
        df_en_curso = gs_get_all("EN_CURSO")
    except Exception:
        df_en_curso = pd.DataFrame(columns=EN_CURSO_COLS)

df_en_curso = ensure_columns(df_en_curso, EN_CURSO_COLS)

if not df_en_curso.empty:
    df_en_curso["maquina_norm"] = df_en_curso["maquina"].apply(lambda x: safe_int(x, -999))


# =========================
# PANEL PRODUCCIÓN
# =========================

with tabs[0]:
    st.subheader("Producción en curso")

    df = df_en_curso.copy()

    if df.empty:
        st.info("No hay bobinas en producción")
    else:
        def tiempo(row):
            try:
                hora_inicio = parse_hhmm(str(row["hora_inicio"]))
                fecha_inicio = datetime.strptime(str(row["fecha"]), "%Y-%m-%d")
                inicio_naive = datetime.combine(fecha_inicio.date(), hora_inicio)
                inicio = tz.localize(inicio_naive)
                ahora = datetime.now(tz)
                minutos = int((ahora - inicio).total_seconds() / 60)
                if minutos < 0:
                    minutos = 0
                return f"{minutos} min"
            except Exception:
                return "-"

        df["tiempo"] = df.apply(tiempo, axis=1)

        mostrar = df[[
            "maquina",
            "lote_of",
            "hora_inicio",
            "operario_inicio",
            "tiempo"
        ]].copy()

        mostrar.columns = [
            "Máquina",
            "OF",
            "Inicio",
            "Operario",
            "Tiempo produciendo"
        ]

        st.dataframe(mostrar, use_container_width=True, hide_index=True)


# =========================
# INICIO BOBINA
# =========================

with tabs[1]:
    st.subheader("Inicio bobina")

    with st.form("inicio"):
        fecha = st.date_input("Fecha", value=date.today())
        turno = st.selectbox("Turno", ["1", "2", "3"])
        maquina = st.number_input("Máquina", min_value=1, step=1)
        lote_mp = st.text_input("Lote materia prima")
        lote_of = st.text_input("OF")
        operario_inicio = st.text_input("Operario")
        hora_inicio_txt = st.text_input("Hora inicio (HH:MM)", placeholder="ej: 14:30")
        observaciones_inicio = st.text_area("Observaciones")

        guardar = st.form_submit_button("Guardar inicio")

        if guardar:
            if not hora_inicio_txt.strip():
                st.error("Debes introducir hora inicio")
                st.stop()

            try:
                hora_inicio = parse_hhmm(hora_inicio_txt)
            except Exception:
                st.error("La hora inicio debe tener formato HH:MM")
                st.stop()

            df = df_en_curso.copy()

            maquina_ocupada = False
            bobina_abierta = None

            if not df.empty:
                bobina = df[df["maquina_norm"] == int(maquina)]

                if not bobina.empty:
                    maquina_ocupada = True
                    bobina_abierta = bobina.iloc[0]

            if maquina_ocupada:
                st.warning("⚠️ Ya hay una bobina abierta")
                st.info(
                    f"""
OF: {bobina_abierta.get('lote_of', '')}
Inicio: {bobina_abierta.get('hora_inicio', '')}
Operario: {bobina_abierta.get('operario_inicio', '')}
"""
                )

                continuar = st.checkbox("Iniciar igualmente")

                if not continuar:
                    st.stop()

            bobina_id = str(uuid.uuid4())

            row = [
                bobina_id,
                fecha.isoformat(),
                str(turno),
                int(maquina),
                lote_mp,
                lote_of,
                hora_inicio.strftime("%H:%M"),
                operario_inicio,
                observaciones_inicio
            ]

            try:
                gs_append_row("EN_CURSO", row)
                st.success("Bobina iniciada")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo guardar el inicio: {e}")


# =========================
# FIN BOBINA
# =========================

with tabs[2]:
    st.subheader("Fin bobina")

    df = df_en_curso.copy()

    if df.empty:
        st.info("No hay bobinas abiertas")
    else:
        df["label"] = df.apply(
            lambda r: f"Máquina {r['maquina']} – OF {r['lote_of']} – inicio {r['hora_inicio']}",
            axis=1
        )

        seleccion = st.selectbox("Selecciona bobina", df["label"])
        fila = df[df["label"] == seleccion].iloc[0]

        try:
            fecha_inicio = datetime.strptime(str(fila["fecha"]), "%Y-%m-%d")
        except Exception:
            st.error("La fecha de inicio de la bobina no es válida")
            st.stop()

        with st.form("fin"):
            hora_fin_txt = st.text_input("Hora fin (HH:MM)", placeholder="ej: 15:10")
            operario_fin = st.text_input("Operario")
            peso = st.number_input("Peso", min_value=0.0)
            taras = st.number_input("Taras", min_value=0, step=1)
            observaciones_fin = st.text_area("Observaciones")

            guardar = st.form_submit_button("Guardar")

            if guardar:
                if not hora_fin_txt.strip():
                    st.error("Debes introducir hora fin")
                    st.stop()

                try:
                    hora_fin = parse_hhmm(hora_fin_txt)
                except Exception:
                    st.error("La hora fin debe tener formato HH:MM")
                    st.stop()

                try:
                    hora_ini = parse_hhmm(str(fila["hora_inicio"]))
                except Exception:
                    st.error("La hora de inicio guardada no es válida")
                    st.stop()

                fecha_fin = fecha_inicio

                if datetime.combine(fecha_inicio.date(), hora_fin) < datetime.combine(fecha_inicio.date(), hora_ini):
                    fecha_fin = fecha_inicio + timedelta(days=1)

                lote_mp_val = get_first_existing_value(
                    fila,
                    ["lote_mp", "lote_materia_prima"],
                    default=""
                )

                row = [
                    fecha_inicio.date().isoformat(),
                    fecha_fin.date().isoformat(),
                    fila["turno"],
                    int(safe_int(fila["maquina"], 0)),
                    lote_mp_val,
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
                    gs_append_row("BOBINAS", row)
                    gs_delete_row_by_bobina(fila["bobina_id"])
                    st.success("Bobina cerrada")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo cerrar la bobina: {e}")


# =========================
# EVENTOS
# =========================

with tabs[3]:
    st.subheader("Incidencias / Tareas")

    df = df_en_curso.copy()

    with st.form("evento"):
        tipo = st.selectbox("Tipo", ["Incidencia", "Tarea/Limpieza"])
        fecha = st.date_input("Fecha", value=date.today())
        maquina = st.number_input("Máquina", min_value=1, step=1, key="maquina_evento")
        hora_inicio_txt = st.text_input("Hora inicio (HH:MM)", placeholder="ej: 10:20")
        hora_fin_txt = st.text_input("Hora fin (HH:MM)", placeholder="ej: 10:40")
        operario = st.text_input("Operario")
        descripcion = st.text_area("Descripción")

        guardar = st.form_submit_button("Guardar")

        if guardar:
            if not hora_inicio_txt.strip() or not hora_fin_txt.strip():
                st.error("Debes introducir hora inicio y fin")
                st.stop()

            try:
                hora_inicio = parse_hhmm(hora_inicio_txt)
                hora_fin = parse_hhmm(hora_fin_txt)
            except Exception:
                st.error("Las horas deben tener formato HH:MM")
                st.stop()

            turno = ""
            lote_of = ""

            if not df.empty:
                bobina = df[df["maquina_norm"] == int(maquina)]

                if not bobina.empty:
                    turno = bobina.iloc[0].get("turno", "")
                    lote_of = bobina.iloc[0].get("lote_of", "")

            start = datetime.combine(fecha, hora_inicio)
            end = datetime.combine(fecha, hora_fin)

            if end < start:
                end += timedelta(days=1)

            minutos = int((end - start).total_seconds() / 60)

            row = [
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

            try:
                gs_append_row("EVENTOS", row)
                st.success("Evento guardado")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo guardar el evento: {e}")
