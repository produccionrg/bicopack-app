import os
import json
import uuid
from datetime import datetime, timedelta
import pytz

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


# -----------------------------------------------------------------------------
# Bicopack – Registro de producción
#
# This file implements a Streamlit application for registering production at
# Bicopack. The app allows users to:
#   • view ongoing production and recent events;
#   • start and end production runs;
#   • log incidents, tasks and cleaning activities;
#   • record flat bobbin production per shift.
# Data is stored in Google Sheets via the gspread API. To avoid issues when
# comparing machine identifiers from Google Sheets (which may come in as
# strings, floats or ints), the code normalises the ``maquina`` column across
# all dataframes. Reading from the sheets is cached for 60 seconds to improve
# responsiveness.
#
# Environment variables used:
#   GOOGLE_SERVICE_ACCOUNT   JSON string for the service account.
#   GOOGLE_SHEET_ID          ID of the primary spreadsheet (PRODUCCIÓN).
#   GOOGLE_SHEET_ID_MAQUINAS ID of the auxiliary machines spreadsheet.
#
# Author: ChatGPT
# Date: 2026-03-12

# -----------------------------------------------------------------------------
# Streamlit configuration
# -----------------------------------------------------------------------------
st.set_page_config(page_title="Bicopack – Registro", layout="centered")

# Timezone for all date/time operations
tz = pytz.timezone("Europe/Madrid")


# -----------------------------------------------------------------------------
# Sheet names and constant definitions
# -----------------------------------------------------------------------------
SHEET_EN_CURSO = "EN_CURSO"
SHEET_PRODUCCION = "PRODUCCION"
SHEET_EVENTOS = "EVENTOS"
SHEET_PLANAS_TURNO = "PLANAS_TURNO"

SHEET_MAQUINAS = "MAQUINAS"

MAX_MAQUINA = 21

EN_CURSO_COLS = [
    "bobina_id",
    "fecha",
    "turno",
    "maquina",
    "tipo_produccion",
    "lote_mp",
    "lote_of",
    "hora_inicio",
    "operario_inicio",
    "observaciones",
]

PRODUCCION_COLS = [
    "fecha_inicio",
    "fecha_fin",
    "turno",
    "maquina",
    "tipo_produccion",
    "lote_mp",
    "lote_of",
    "hora_inicio",
    "operario_inicio",
    "hora_fin",
    "operario_fin",
    "peso",
    "taras",
    "observaciones",
]

EVENTOS_COLS = [
    "fecha",
    "turno",
    "maquina",
    "lote_of",
    "tipo",
    "hora_inicio",
    "hora_fin",
    "minutos",
    "operario",
    "descripcion",
]

# Columnas para la producción de bobina plana reprocesada. Ahora aceptamos
# varios lotes y órdenes de trabajo y un único campo de cantidad total,
# por lo que se han eliminado las columnas de máquinas individuales.
PLANAS_TURNO_COLS = [
    "fecha",
    "turno",
    "lotes",
    "ordenes_trabajo",
    "operario_1",
    "operario_2",
    "operario_3",
    "operario_4",
    "operario_5",
    "cantidad_reprocesadas",
]

MAQUINAS_COLS = [
    "maquina",
    "tipo_produccion",
    "lote_of",
    "lote_mp",
]

# Opciones de tipo de producción. Se ha sustituido "Bobina plana" por
# "Bobina plana reprocesada" para reflejar las nuevas necesidades.
TIPOS_PRODUCCION = ["Bobina cruzada", "Bobina plana reprocesada", "Saco"]
TIPOS_EVENTO = ["Incidencia", "Tarea - cambio de agujas", "Limpieza"]


# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------
def parse_hhmm(value: str):
    """Parse a string in HH:MM format into a ``datetime.time`` object."""
    return datetime.strptime(str(value).strip(), "%H:%M").time()


def safe_int(x, default=None):
    """Safely convert a value to an integer, returning ``default`` if not possible."""
    try:
        if pd.isna(x):
            return default
        return int(x)
    except Exception:
        try:
            return int(str(x).strip())
        except Exception:
            return default


def safe_float(x, default=None):
    """Safely convert a value to a float, returning ``default`` if not possible."""
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        try:
            return float(str(x).strip().replace(",", "."))
        except Exception:
            return default


def clean_row(row):
    """
    Ensure that values in a row are JSON serialisable before appending to a sheet.
    ``gspread`` does not handle NumPy types or NaNs well, so convert them to
    plain Python types or empty strings.
    """
    clean = []
    for x in row:
        try:
            if pd.isna(x):
                clean.append("")
            elif hasattr(x, "item"):
                clean.append(x.item())
            else:
                clean.append(x)
        except Exception:
            clean.append(x)
    return clean


def ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    Ensure that a DataFrame has at least the specified columns. If the
    DataFrame is empty or None, a new empty DataFrame with those columns is
    returned. Any missing columns are added with empty strings.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)
    df = df.copy()
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    return df


def get_first_existing_value(row, possible_cols, default=""):
    """
    Given a row and a list of possible column names, return the first
    non-empty value found. If none are present, return ``default``.
    """
    for col in possible_cols:
        if col in row and pd.notna(row[col]) and str(row[col]).strip() != "":
            return row[col]
    return default


def normalize_name(value: str) -> str:
    """Normalise a name to lower-case and trimmed."""
    return str(value).strip().lower()


def current_date_madrid():
    """Return the current date in the Europe/Madrid timezone."""
    return datetime.now(tz).date()


def current_time_madrid_str():
    """Return the current time as HH:MM string in the Europe/Madrid timezone."""
    return datetime.now(tz).strftime("%H:%M")


def compute_minutes(fecha_obj, hora_inicio_str, hora_fin_str):
    """
    Compute the difference in minutes between two HH:MM strings on a given date.
    If times are missing or cannot be parsed, return an empty string. Handles
    overnight spans by rolling the end time to the next day.
    """
    if not str(hora_inicio_str).strip() or not str(hora_fin_str).strip():
        return ""
    try:
        h_ini = parse_hhmm(hora_inicio_str)
        h_fin = parse_hhmm(hora_fin_str)
        start = datetime.combine(fecha_obj, h_ini)
        end = datetime.combine(fecha_obj, h_fin)
        if end < start:
            end += timedelta(days=1)
        return int((end - start).total_seconds() / 60)
    except Exception:
        return ""


def event_datetime_from_row(row, prefer_end=False):
    """
    Construct a timezone-aware datetime from a row of the EVENTOS sheet. If
    ``prefer_end`` is True, the end time is used; otherwise the start time.
    Returns None if the date or time cannot be parsed.
    """
    try:
        fecha_txt = str(row.get("fecha", "")).strip()
        if not fecha_txt:
            return None
        fecha_obj = datetime.strptime(fecha_txt, "%Y-%m-%d").date()
        hora_key = "hora_fin" if prefer_end else "hora_inicio"
        hora_txt = str(row.get(hora_key, "")).strip() or "00:00"
        hora_obj = parse_hhmm(hora_txt)
        dt_naive = datetime.combine(fecha_obj, hora_obj)
        return tz.localize(dt_naive)
    except Exception:
        return None


def filter_last_hours_events(df: pd.DataFrame, hours: int = 24) -> pd.DataFrame:
    """
    Filter events DataFrame to only include rows whose start time falls within
    the last ``hours`` hours. Returns an empty DataFrame if none match.
    """
    if df.empty:
        return df.copy()
    now = datetime.now(tz)
    threshold = now - timedelta(hours=hours)
    keep_idx = []
    for idx, row in df.iterrows():
        dt = event_datetime_from_row(row, prefer_end=False)
        if dt is not None and dt >= threshold:
            keep_idx.append(idx)
    if not keep_idx:
        return pd.DataFrame(columns=df.columns)
    return df.loc[keep_idx].copy()


# -----------------------------------------------------------------------------
# UI callbacks
# -----------------------------------------------------------------------------

# Ya no se utiliza un callback para cambiar de máquina en el formulario de
# inicio de producción, ya que la selección de máquina se sitúa fuera del
# formulario y el autocompletado se actualiza automáticamente cuando la
# página se vuelve a ejecutar tras modificar el valor.


# -----------------------------------------------------------------------------
# Google Sheets access
# -----------------------------------------------------------------------------

@st.cache_resource
def _gs_client():
    """Create and cache a gspread client using a service account JSON string."""
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT", "")
    if not sa_json:
        raise ValueError("Falta la variable de entorno GOOGLE_SERVICE_ACCOUNT")
    info = json.loads(sa_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


@st.cache_resource
def _get_spreadsheet():
    """Open and cache the main production spreadsheet by ID."""
    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "")
    if not sheet_id:
        raise ValueError("Falta la variable de entorno GOOGLE_SHEET_ID")
    gc = _gs_client()
    return gc.open_by_key(sheet_id)


@st.cache_resource
def _get_ws(sheet_name: str):
    """Get and cache a worksheet from the main spreadsheet."""
    sh = _get_spreadsheet()
    return sh.worksheet(sheet_name)


@st.cache_resource
def _get_spreadsheet_maquinas():
    """Open and cache the machines spreadsheet by ID."""
    sheet_id = os.environ.get("GOOGLE_SHEET_ID_MAQUINAS", "")
    if not sheet_id:
        raise ValueError("Falta la variable de entorno GOOGLE_SHEET_ID_MAQUINAS")
    gc = _gs_client()
    return gc.open_by_key(sheet_id)


@st.cache_resource
def _get_ws_maquinas(sheet_name: str):
    """Get and cache a worksheet from the machines spreadsheet."""
    sh = _get_spreadsheet_maquinas()
    return sh.worksheet(sheet_name)


def gs_append_row(sheet_name: str, row: list):
    """
    Append a row to a sheet in the main spreadsheet. The row is cleaned to
    ensure values are JSON serialisable. After appending, clear the cached
    ``gs_get_all`` so subsequent reads reflect the new data.
    """
    ws = _get_ws(sheet_name)
    row = clean_row(row)
    ws.append_row(row, value_input_option="RAW")
    gs_get_all.clear()


@st.cache_data(ttl=60)
def gs_get_all(sheet_name: str):
    """
    Retrieve all records from a sheet in the main spreadsheet as a DataFrame.
    Caches results for 60 seconds to reduce API calls.
    """
    ws = _get_ws(sheet_name)
    data = ws.get_all_records()
    return pd.DataFrame(data)


@st.cache_data(ttl=60)
def gs_get_maquinas():
    """Retrieve all machine records from the machines spreadsheet."""
    ws = _get_ws_maquinas(SHEET_MAQUINAS)
    data = ws.get_all_records()
    return pd.DataFrame(data)


def gs_delete_row_by_bobina(bobina_id):
    """
    Delete the row in the EN_CURSO sheet whose first column (bobina_id) matches
    ``bobina_id``. After deletion, clear the cached ``gs_get_all``.
    """
    ws = _get_ws(SHEET_EN_CURSO)
    data = ws.get_all_values()
    for i, row in enumerate(data):
        if row and str(row[0]).strip() == str(bobina_id).strip():
            ws.delete_rows(i + 1)
            break
    gs_get_all.clear()


# -----------------------------------------------------------------------------
# Data loading
# -----------------------------------------------------------------------------
st.title("Bicopack – Registro de producción")

with st.spinner("Cargando datos..."):
    try:
        df_en_curso = gs_get_all(SHEET_EN_CURSO)
    except Exception:
        df_en_curso = pd.DataFrame(columns=EN_CURSO_COLS)
    try:
        df_eventos = gs_get_all(SHEET_EVENTOS)
    except Exception:
        df_eventos = pd.DataFrame(columns=EVENTOS_COLS)
    try:
        df_maquinas = gs_get_maquinas()
    except Exception:
        df_maquinas = pd.DataFrame(columns=MAQUINAS_COLS)

# Ensure required columns exist even when dataframes are empty
df_en_curso = ensure_columns(df_en_curso, EN_CURSO_COLS)
df_eventos = ensure_columns(df_eventos, EVENTOS_COLS)
df_maquinas = ensure_columns(df_maquinas, MAQUINAS_COLS)

# Normalise machine identifiers in all dataframes
if not df_en_curso.empty:
    df_en_curso["maquina_norm"] = df_en_curso["maquina"].apply(lambda x: safe_int(x, -999))
else:
    df_en_curso["maquina_norm"] = pd.Series(dtype="int64")

if not df_eventos.empty:
    df_eventos["maquina_norm"] = df_eventos["maquina"].apply(lambda x: safe_int(x, -999))
else:
    df_eventos["maquina_norm"] = pd.Series(dtype="int64")

if not df_maquinas.empty:
    # Ensure 'maquina' is numeric and create a normalised column
    df_maquinas["maquina"] = df_maquinas["maquina"].apply(lambda x: safe_int(x, -999))
    df_maquinas["maquina_norm"] = df_maquinas["maquina"].apply(lambda x: safe_int(x, -999))
    # Strip whitespace from string columns
    df_maquinas["tipo_produccion"] = df_maquinas["tipo_produccion"].astype(str).str.strip()
    df_maquinas["lote_of"] = df_maquinas["lote_of"].astype(str).str.strip()
    df_maquinas["lote_mp"] = df_maquinas["lote_mp"].astype(str).str.strip()
else:
    df_maquinas["maquina_norm"] = pd.Series(dtype="int64")


# -----------------------------------------------------------------------------
# Streamlit tabs
# -----------------------------------------------------------------------------
# Define tabs for the different panels.  A seventh tab is added for
# configuring machine data directly from the app and an eighth tab for
# revisiting recently closed productions.  The new panel allows
# supervisors to review productions closed in the last 24 hours and
# optionally reopen them if they were closed by mistake.
tabs = st.tabs([
    "Panel producción",
    "Panel incidencias",
    "Inicio producción",
    "Fin producción",
    "Incidencias / tareas",
    "Producción bobina plana reprocesada",
    "Configuración máquinas",
    "Cierres últimas 24h",
    "Estado de máquinas",
])


# =========================
# PANEL PRODUCCIÓN
# =========================
with tabs[0]:
    st.subheader("Producción en curso")
    df = df_en_curso.copy()
    if df.empty:
        st.info("No hay producciones en curso")
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
            "tipo_produccion",
            "lote_of",
            "lote_mp",
            "hora_inicio",
            "operario_inicio",
            "tiempo",
        ]].copy()
        mostrar.columns = [
            "Máquina",
            "Tipo",
            "OF",
            "Lote MP",
            "Inicio",
            "Operario",
            "Tiempo produciendo",
        ]
        # Sort by machine (numeric) and start time
        mostrar["Máquina_sort"] = mostrar["Máquina"].apply(lambda x: safe_int(x, 999999))
        mostrar = mostrar.sort_values(
            by=["Máquina_sort", "Inicio"],
            ascending=[True, True]
        ).drop(columns=["Máquina_sort"])
        st.dataframe(mostrar, use_container_width=True, hide_index=True)


# =========================
# PANEL INCIDENCIAS
# =========================
with tabs[1]:
    st.subheader("Incidencias / tareas últimas 24 horas")
    df = df_eventos.copy()
    if df.empty:
        st.info("No hay incidencias o tareas registradas")
    else:
        df = filter_last_hours_events(df, hours=24)
        if df.empty:
            st.info("No hay incidencias o tareas en las últimas 24 horas")
        else:
            mostrar = df[[
                "maquina",
                "tipo",
                "hora_inicio",
                "hora_fin",
                "operario",
                "lote_of",
                "descripcion",
            ]].copy()
            mostrar.columns = [
                "Máquina",
                "Tipo",
                "Hora inicio",
                "Hora fin",
                "Operario",
                "OF",
                "Motivo",
            ]
            mostrar["Máquina_sort"] = mostrar["Máquina"].apply(lambda x: safe_int(x, 999999))
            mostrar = mostrar.sort_values(
                by=["Máquina_sort", "Hora inicio"],
                ascending=[True, False]
            ).drop(columns=["Máquina_sort"])
            st.dataframe(mostrar, use_container_width=True, hide_index=True)


# =========================
# INICIO PRODUCCIÓN
# =========================
with tabs[2]:
    st.subheader("Inicio producción")
    # Inputs para fecha, turno y máquina fuera del formulario para permitir que
    # la página se recargue automáticamente cuando cambie la máquina (sin
    # necesidad de callbacks dentro de un formulario, que Streamlit no permite).
    # Solicitar la fecha y el turno sin valores por defecto, de forma que
    # los operarios deban seleccionarlos manualmente. Cuando ``value`` es
    # ``None`` o ``index`` es ``None``, Streamlit muestra un campo vacío y
    # devuelve ``None`` hasta que el usuario elige un valor.
    fecha = st.date_input(
        "Fecha",
        value=None,
        key="inicio_fecha",
    )
    turno = st.selectbox(
        "Turno",
        ["1", "2", "3"],
        index=None,
        placeholder="Selecciona turno",
        key="inicio_turno",
    )
    maquina = st.number_input("Máquina", min_value=1, max_value=MAX_MAQUINA, step=1)

    # Autocompletar datos en función de la máquina seleccionada
    tipo_auto = ""
    lote_of_auto = ""
    lote_mp_auto = ""
    machine_int = safe_int(maquina, -999)
    if not df_maquinas.empty:
        for _, m_row in df_maquinas.iterrows():
            row_machine_int = safe_int(m_row.get("maquina"), -999)
            if row_machine_int == machine_int:
                tipo_auto = str(m_row.get("tipo_produccion", "")).strip()
                lote_of_auto = str(m_row.get("lote_of", "")).strip()
                lote_mp_auto = str(m_row.get("lote_mp", "")).strip()
                break

    # Seleccionar índice inicial para el tipo de producción
    index_tipo = 0
    if tipo_auto and tipo_auto in TIPOS_PRODUCCION:
        index_tipo = TIPOS_PRODUCCION.index(tipo_auto)

    # Formulario para el resto de datos de inicio de producción
    with st.form("inicio_produccion"):
        tipo_produccion = st.selectbox("Tipo producción", TIPOS_PRODUCCION, index=index_tipo)
        lote_mp = st.text_input("Lote materia prima", value=lote_mp_auto)
        lote_of = st.text_input("OF", value=lote_of_auto)
        # Mensaje si no hay datos precargados
        if not tipo_auto and not lote_of_auto and not lote_mp_auto:
            st.caption("Esta máquina no tiene datos cargados o está parada.")
        operario_inicio = st.text_input("Operario")
        hora_inicio_txt = st.text_input("Hora inicio (HH:MM)", placeholder="ej: 14:30")
        observaciones_inicio = st.text_area("Observaciones")
        guardar = st.form_submit_button("Guardar inicio")
        if guardar:
            # Verificar que se haya seleccionado una fecha y un turno
            if fecha is None:
                st.error("Debes seleccionar la fecha")
                st.stop()
            if turno is None or str(turno).strip() == "":
                st.error("Debes seleccionar el turno")
                st.stop()
            if not hora_inicio_txt.strip():
                st.error("Debes introducir hora inicio")
                st.stop()
            # Validar formato de la hora
            try:
                hora_inicio = parse_hhmm(hora_inicio_txt)
            except Exception:
                st.error("La hora inicio debe tener formato HH:MM")
                st.stop()
            # Comprobar si ya hay una producción abierta en la máquina
            df = df_en_curso.copy()
            maquina_ocupada = False
            registro_abierto = None
            if not df.empty:
                abierta = df[df["maquina_norm"] == machine_int]
                if not abierta.empty:
                    maquina_ocupada = True
                    registro_abierto = abierta.iloc[0]
            if maquina_ocupada:
                st.warning("⚠️ Ya hay una producción abierta en esa máquina")
                st.info(
                    f"Tipo: {registro_abierto.get('tipo_produccion', '')}\n"
                    f"OF: {registro_abierto.get('lote_of', '')}\n"
                    f"Lote MP: {registro_abierto.get('lote_mp', '')}\n"
                    f"Inicio: {registro_abierto.get('hora_inicio', '')}\n"
                    f"Operario: {registro_abierto.get('operario_inicio', '')}"
                )
                st.stop()
            # Crear un nuevo registro de producción
            produccion_id = str(uuid.uuid4())
            row = [
                produccion_id,
                fecha.isoformat(),
                str(turno),
                int(machine_int),
                tipo_produccion,
                lote_mp,
                lote_of,
                hora_inicio.strftime("%H:%M"),
                operario_inicio,
                observaciones_inicio,
            ]
            try:
                gs_append_row(SHEET_EN_CURSO, row)
                st.success("Producción iniciada")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo guardar el inicio: {e}")


# =========================
# FIN PRODUCCIÓN
# =========================
with tabs[3]:
    st.subheader("Fin producción")
    df = df_en_curso.copy()
    if df.empty:
        st.info("No hay producciones abiertas")
    else:
        # Build a human-friendly label for each open production
        df["label"] = df.apply(
            lambda r: f"Máquina {r['maquina']} – {r.get('tipo_produccion', '')} – OF {r['lote_of']} – inicio {r['hora_inicio']}",
            axis=1
        )
        seleccion = st.selectbox("Selecciona producción", df["label"])
        fila = df[df["label"] == seleccion].iloc[0]
        # Parse stored start date
        try:
            fecha_inicio = datetime.strptime(str(fila["fecha"]), "%Y-%m-%d")
        except Exception:
            st.error("La fecha de inicio guardada no es válida")
            st.stop()
        with st.form("fin_produccion"):
            hora_fin_txt = st.text_input("Hora fin (HH:MM)", placeholder="ej: 15:10")
            operario_fin = st.text_input("Operario")
            peso = st.number_input("Peso", min_value=0.0)
            taras = st.number_input("Taras", min_value=0, step=1)
            observaciones_fin = st.text_area("Observaciones")
            guardar = st.form_submit_button("Guardar fin")
            if guardar:
                if not hora_fin_txt.strip():
                    st.error("Debes introducir hora fin")
                    st.stop()
                try:
                    hora_fin = parse_hhmm(hora_fin_txt)
                except Exception:
                    st.error("La hora fin debe tener formato HH:MM")
                    st.stop()
                # Validate stored start time
                try:
                    hora_ini = parse_hhmm(str(fila["hora_inicio"]))
                except Exception:
                    st.error("La hora de inicio guardada no es válida")
                    st.stop()
                # Determine if end date is next day
                fecha_fin = fecha_inicio
                if datetime.combine(fecha_inicio.date(), hora_fin) < datetime.combine(fecha_inicio.date(), hora_ini):
                    fecha_fin = fecha_inicio + timedelta(days=1)
                # Some fields may have alternate column names (lote_materia_prima)
                lote_mp_val = get_first_existing_value(
                    fila,
                    ["lote_mp", "lote_materia_prima"],
                    default=""
                )
                row = [
                    fecha_inicio.date().isoformat(),
                    fecha_fin.date().isoformat(),
                    str(fila["turno"]),
                    int(safe_int(fila["maquina"], 0)),
                    str(fila.get("tipo_produccion", "")),
                    lote_mp_val,
                    str(fila["lote_of"]),
                    str(fila["hora_inicio"]),
                    str(fila["operario_inicio"]),
                    hora_fin.strftime("%H:%M"),
                    operario_fin,
                    float(safe_float(peso, 0.0)),
                    int(safe_int(taras, 0)),
                    observaciones_fin,
                ]
                try:
                    gs_append_row(SHEET_PRODUCCION, row)
                    gs_delete_row_by_bobina(fila["bobina_id"])
                    st.success("Producción cerrada")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo cerrar la producción: {e}")


# =========================
# INCIDENCIAS / TAREAS
# =========================
with tabs[4]:
    st.subheader("Registrar incidencia / tarea / limpieza")
    # Seleccionar el tipo de evento fuera del formulario para que, al cambiar
    # su valor, la página se vuelva a ejecutar y se actualicen los campos
    tipo_evento = st.selectbox("Tipo", TIPOS_EVENTO, key="evento_tipo")
    # Para limpieza, mostrar la opción de carga de filetas fuera del formulario
    carga_filetas = False
    if tipo_evento == "Limpieza":
        carga_filetas = st.checkbox(
            "¿Carga de filetas?",
            key="limpieza_carga_filetas",
        )

    with st.form("evento_form"):
        # Fecha sin autocompletado
        fecha = st.date_input(
            "Fecha",
            value=None,
            key="evento_fecha",
        )
        # Selección de máquina
        maquina = st.number_input(
            "Máquina",
            min_value=1,
            max_value=MAX_MAQUINA,
            step=1,
            key="maquina_evento",
        )
        # Auto-detect current shift and OF for the selected machine
        df_maquina = df_en_curso.copy()
        turno_auto = ""
        lote_of_auto = ""
        machine_int = safe_int(maquina, -999)
        if not df_maquina.empty:
            match = df_maquina[df_maquina["maquina_norm"] == machine_int]
            if not match.empty:
                turno_auto = str(match.iloc[0].get("turno", "")).strip()
                lote_of_auto = str(match.iloc[0].get("lote_of", "")).strip()
        if lote_of_auto:
            st.caption(f"OF detectada automáticamente para esa máquina: {lote_of_auto}")
        # Descripción común
        descripcion = st.text_area("Descripción")
        # Inicializar variables comunes
        hora_inicio_txt = ""
        hora_fin_txt = ""
        minutos = ""
        # Campos específicos según el tipo de evento seleccionado
        if tipo_evento == "Incidencia":
            operario = st.text_input("Operario")
            hora_inicio_txt = st.text_input(
                "Hora inicio (HH:MM)",
                placeholder="ej: 10:20",
                key="inc_hora_ini",
            )
            hora_fin_txt = st.text_input(
                "Hora fin (HH:MM)",
                placeholder="ej: 10:40",
                key="inc_hora_fin",
            )
        elif tipo_evento == "Tarea - cambio de agujas":
            operario = st.selectbox("Operario", ["John", "Rafa"])
            hora_inicio_txt = st.text_input(
                "Hora inicio (HH:MM)",
                placeholder="ej: 10:20",
                key="agujas_ini",
            )
            hora_fin_txt = st.text_input(
                "Hora fin (HH:MM)",
                placeholder="ej: 10:40",
                key="agujas_fin",
            )
        elif tipo_evento == "Limpieza":
            # Si es carga de filetas (marcada fuera del formulario), no pedir operario ni horas
            if carga_filetas:
                operario = ""
                hora_inicio_txt = ""
                hora_fin_txt = ""
            else:
                operario = st.text_input("Operario")
                hora_inicio_txt = st.text_input(
                    "Hora inicio (HH:MM)",
                    placeholder="ej: 10:20",
                    key="limp_hora_ini",
                )
                hora_fin_txt = st.text_input(
                    "Hora fin (HH:MM)",
                    placeholder="ej: 10:40",
                    key="limp_hora_fin",
                )
        # Botón de guardar
        guardar = st.form_submit_button("Guardar evento")
        if guardar:
            # Verificar fecha
            if fecha is None:
                st.error("Debes seleccionar la fecha")
                st.stop()
            operario_norm = normalize_name(operario)
            if tipo_evento == "Incidencia":
                if not hora_inicio_txt.strip() or not hora_fin_txt.strip():
                    st.error(
                        "En incidencias debes introducir hora inicio y hora fin"
                    )
                    st.stop()
                try:
                    parse_hhmm(hora_inicio_txt)
                    parse_hhmm(hora_fin_txt)
                except Exception:
                    st.error("Las horas deben tener formato HH:MM")
                    st.stop()
                minutos = compute_minutes(fecha, hora_inicio_txt, hora_fin_txt)
            elif tipo_evento == "Tarea - cambio de agujas":
                if not hora_inicio_txt.strip() or not hora_fin_txt.strip():
                    st.error("Debes introducir hora inicio y hora fin")
                    st.stop()
                try:
                    parse_hhmm(hora_inicio_txt)
                    parse_hhmm(hora_fin_txt)
                except Exception:
                    st.error("Las horas deben tener formato HH:MM")
                    st.stop()
                minutos = compute_minutes(fecha, hora_inicio_txt, hora_fin_txt)
            elif tipo_evento == "Limpieza":
                # Si es una carga de filetas, no se registran horas ni operario
                if carga_filetas:
                    hora_inicio_txt = ""
                    hora_fin_txt = ""
                    minutos = ""
                    # Marcar en la descripción que es carga de filetas
                    if descripcion.strip():
                        descripcion = f"Carga de filetas: {descripcion.strip()}"
                    else:
                        descripcion = "Carga de filetas"
                else:
                    # Para limpieza normal se requieren horas de inicio y fin
                    if not hora_inicio_txt.strip() or not hora_fin_txt.strip():
                        st.error(
                            "En limpieza debes introducir hora inicio y hora fin"
                        )
                        st.stop()
                    try:
                        parse_hhmm(hora_inicio_txt)
                        parse_hhmm(hora_fin_txt)
                    except Exception:
                        st.error("Las horas deben tener formato HH:MM")
                        st.stop()
                    minutos = compute_minutes(fecha, hora_inicio_txt, hora_fin_txt)
            # Construir y guardar la fila
            row = [
                fecha.isoformat(),
                turno_auto,
                int(machine_int),
                lote_of_auto,
                tipo_evento,
                hora_inicio_txt,
                hora_fin_txt,
                minutos,
                operario,
                descripcion,
            ]
            try:
                gs_append_row(SHEET_EVENTOS, row)
                st.success("Evento guardado")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo guardar el evento: {e}")


# =========================
# PRODUCCIÓN BOBINA PLANA
# =========================
with tabs[5]:
    st.subheader("Producción bobina plana reprocesada (1 registro por turno)")
    with st.form("planas_turno_form"):
        # Solicitar fecha y turno sin valores por defecto. Se pide al usuario
        # seleccionarlos manualmente.
        fecha = st.date_input(
            "Fecha",
            value=None,
            key="planas_fecha",
        )
        turno = st.selectbox(
            "Turno",
            ["1", "2", "3"],
            index=None,
            placeholder="Selecciona turno",
            key="planas_turno",
        )
        # Permitir ingresar varios lotes y órdenes de trabajo
        lotes = st.text_input("Lotes")
        ordenes_trabajo = st.text_input("Ordenes de trabajo", placeholder="024-1234")
        st.markdown("**Operarios del turno**")
        operario_1 = st.text_input("Operario 1")
        operario_2 = st.text_input("Operario 2")
        operario_3 = st.text_input("Operario 3")
        operario_4 = st.text_input("Operario 4")
        operario_5 = st.text_input("Operario 5")
        st.markdown("**Cantidad de bobinas planas reprocesadas**")
        cantidad_reprocesadas = st.number_input(
            "Cantidad de bobinas planas reprocesadas",
            min_value=0,
            step=1,
        )
        guardar = st.form_submit_button("Guardar producción bobina plana reprocesada")
        if guardar:
            # Verificar que se haya seleccionado una fecha y un turno
            if fecha is None:
                st.error("Debes seleccionar la fecha")
                st.stop()
            if turno is None or str(turno).strip() == "":
                st.error("Debes seleccionar el turno")
                st.stop()
            # Validar que se ha introducido la orden de trabajo
            if not ordenes_trabajo.strip():
                st.error("Debes introducir la orden de trabajo")
                st.stop()
            # Mantener la misma regla de prefijo para las órdenes de trabajo
            if not ordenes_trabajo.strip().startswith("024-"):
                st.error("La orden de trabajo debe empezar por 024-")
                st.stop()
            row = [
                fecha.isoformat(),
                str(turno),
                lotes,
                ordenes_trabajo.strip(),
                operario_1,
                operario_2,
                operario_3,
                operario_4,
                operario_5,
                int(cantidad_reprocesadas),
            ]
            try:
                gs_append_row(SHEET_PLANAS_TURNO, row)
                st.success("Producción bobina plana reprocesada guardada")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo guardar la producción de bobina plana reprocesada: {e}")

# =========================
# CONFIGURACIÓN MÁQUINAS
# =========================
with tabs[6]:
    """
    Panel para editar los datos de las máquinas (tipo de producción, lote OF y
    lote de materia prima) desde la propia aplicación. Estos valores se
    utilizan para autocompletar los formularios de inicio de producción e
    incidencias.
    """
    st.subheader("Configuración de máquinas")
    # Mostrar siempre los 21 números de máquina para permitir crear o editar datos
    machine_options = list(range(1, MAX_MAQUINA + 1))
    selected_machine = st.selectbox("Selecciona máquina", machine_options)
    # Buscar los datos actuales de la máquina seleccionada en df_maquinas (si existen)
    machine_row = df_maquinas[df_maquinas["maquina"] == selected_machine]
    if not machine_row.empty:
        m_row = machine_row.iloc[0]
        current_tipo = str(m_row.get("tipo_produccion", "")).strip()
        current_lote_of = str(m_row.get("lote_of", "")).strip()
        current_lote_mp = str(m_row.get("lote_mp", "")).strip()
    else:
        current_tipo = ""
        current_lote_of = ""
        current_lote_mp = ""
    # Seleccionar tipo de producción
    index_tipo_machine = 0
    if current_tipo and current_tipo in TIPOS_PRODUCCION:
        index_tipo_machine = TIPOS_PRODUCCION.index(current_tipo)
    new_tipo = st.selectbox("Tipo de producción", TIPOS_PRODUCCION, index=index_tipo_machine, key="tipo_maquina")
    new_lote_of = st.text_input("Lote OF", value=current_lote_of, key="lote_of_maquina")
    new_lote_mp = st.text_input("Lote materia prima", value=current_lote_mp, key="lote_mp_maquina")
    if st.button("Guardar cambios", key="guardar_maquina"):
        # Conectar a la hoja de máquinas y actualizar o insertar los datos
        try:
            ws_m = _get_ws_maquinas(SHEET_MAQUINAS)
        except Exception as e:
            st.error(f"No se pudo acceder a la hoja de máquinas: {e}")
        else:
            try:
                data = ws_m.get_all_values()
                # Intentar encontrar la fila de la máquina existente
                row_index = None
                # data[0] contiene encabezados
                for idx, row in enumerate(data):
                    if idx == 0:
                        continue  # saltar encabezado
                    if safe_int(row[0], -1) == selected_machine:
                        row_index = idx + 1  # ajustar a índice base 1 de Sheets
                        break
                if row_index is not None:
                    # Actualizar valores existentes
                    ws_m.update_cell(row_index, 2, new_tipo)
                    ws_m.update_cell(row_index, 3, new_lote_of)
                    ws_m.update_cell(row_index, 4, new_lote_mp)
                else:
                    # No existe, insertar una nueva fila al final
                    new_row = [selected_machine, new_tipo, new_lote_of, new_lote_mp]
                    ws_m.append_row(new_row, value_input_option="RAW")
                # Limpiar la caché para que la app recupere los nuevos datos
                gs_get_maquinas.clear()
                st.success("Datos de la máquina guardados correctamente")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudieron guardar los cambios: {e}")

# =========================
# CIERRES ÚLTIMAS 24 HORAS
# =========================
with tabs[7]:
    """
    Panel para revisar producciones que se han cerrado recientemente.
    Muestra las producciones cerradas en las últimas 24 horas para que los
    supervisores puedan comprobar si el cierre fue correcto y, en caso
    necesario, reabrir la producción. Al reabrir una producción se crea
    un nuevo registro en la hoja EN_CURSO con los datos originales, con
    un nuevo identificador único. El registro en la hoja PRODUCCION no
    se elimina, de modo que quede constancia del cierre anterior.
    """
    st.subheader("Producciones cerradas últimas 24 horas")
    # Cargar la hoja de producciones cerradas
    try:
        df_produccion = gs_get_all(SHEET_PRODUCCION)
    except Exception:
        df_produccion = pd.DataFrame(columns=PRODUCCION_COLS)
    # Asegurar columnas
    df_produccion = ensure_columns(df_produccion, PRODUCCION_COLS)
    # Filtrar las producciones cerradas en las últimas 24 horas
    if not df_produccion.empty:
        now = datetime.now(tz)
        threshold = now - timedelta(hours=24)
        # Calcular un datetime de cierre para cada fila
        def _parse_end(row):
            fecha_txt = str(row.get("fecha_fin", "")).strip()
            if not fecha_txt:
                return None
            hora_txt = str(row.get("hora_fin", "")).strip()
            if not hora_txt:
                hora_txt = "00:00"
            try:
                fecha_dt = datetime.strptime(fecha_txt, "%Y-%m-%d").date()
                hora_dt = parse_hhmm(hora_txt)
                dt_naive = datetime.combine(fecha_dt, hora_dt)
                return tz.localize(dt_naive)
            except Exception:
                return None
        df_produccion["dt_end"] = df_produccion.apply(_parse_end, axis=1)
        df_recent = df_produccion[
            df_produccion["dt_end"].notna() & (df_produccion["dt_end"] >= threshold)
        ].copy()
    else:
        df_recent = pd.DataFrame(columns=df_produccion.columns)
    if df_recent.empty:
        st.info("No hay producciones cerradas en las últimas 24 horas")
    else:
        # Construir etiquetas legibles para cada producción
        def _build_label(r):
            return (
                f"Máquina {r.get('maquina', '')} – {r.get('tipo_produccion', '')} "
                f"– OF {r.get('lote_of', '')} – fin {r.get('hora_fin', '')}"
            )
        df_recent["label"] = df_recent.apply(_build_label, axis=1)
        seleccion = st.selectbox("Selecciona producción cerrada", df_recent["label"])
        fila = df_recent[df_recent["label"] == seleccion].iloc[0]
        # Mostrar detalles básicos de la producción cerrada
        st.markdown("**Detalles de la producción cerrada:**")
        st.write(f"Fecha inicio: {fila.get('fecha_inicio', '')}")
        st.write(f"Fecha fin: {fila.get('fecha_fin', '')}")
        st.write(f"Turno: {fila.get('turno', '')}")
        st.write(f"Máquina: {fila.get('maquina', '')}")
        st.write(f"Tipo de producción: {fila.get('tipo_produccion', '')}")
        st.write(f"Lote MP: {fila.get('lote_mp', '')}")
        st.write(f"OF: {fila.get('lote_of', '')}")
        st.write(f"Hora inicio: {fila.get('hora_inicio', '')}")
        st.write(f"Operario inicio: {fila.get('operario_inicio', '')}")
        st.write(f"Hora fin: {fila.get('hora_fin', '')}")
        st.write(f"Operario fin: {fila.get('operario_fin', '')}")
        st.write(f"Observaciones: {fila.get('observaciones', '')}")
        # Botón para reabrir la producción
        if st.button("Volver a abrir esta producción"):
            # Construir un nuevo registro para EN_CURSO a partir de los datos de la producción cerrada
            new_bobina_id = str(uuid.uuid4())
            # Usar fecha de inicio original para la reapertura
            nueva_fila = [
                new_bobina_id,
                fila.get("fecha_inicio", ""),
                str(fila.get("turno", "")),
                int(safe_int(fila.get("maquina", 0), 0)),
                str(fila.get("tipo_produccion", "")),
                str(fila.get("lote_mp", "")),
                str(fila.get("lote_of", "")),
                str(fila.get("hora_inicio", "")),
                str(fila.get("operario_inicio", "")),
                str(fila.get("observaciones", "")),
            ]
            try:
                gs_append_row(SHEET_EN_CURSO, nueva_fila)
                st.success("Producción reabierta correctamente")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo reabrir la producción: {e}")

# =========================
# ESTADO DE MÁQUINAS
# =========================
with tabs[8]:
    """
    Panel de estado de máquinas.
    Muestra todas las máquinas (1 a MAX_MAQUINA) con su configuración actual
    (tipo de producción, lotes OF y MP) y el estado de cada máquina. Si la
    máquina está en producción, también se muestran los datos de la
    producción abierta (tipo actual, OF actual, lote MP actual, hora de
    inicio y operario).
    """
    st.subheader("Estado de máquinas")
    # Preparar una lista con la información de cada máquina
    status_rows = []
    for m in range(1, MAX_MAQUINA + 1):
        # Datos de configuración de la hoja MAQUINAS
        conf_row = df_maquinas[df_maquinas["maquina"] == m]
        if not conf_row.empty:
            conf_tipo = str(conf_row.iloc[0].get("tipo_produccion", "")).strip()
            conf_of = str(conf_row.iloc[0].get("lote_of", "")).strip()
            conf_mp = str(conf_row.iloc[0].get("lote_mp", "")).strip()
        else:
            conf_tipo = ""
            conf_of = ""
            conf_mp = ""
        # Datos de producción en curso, si existen
        open_row = df_en_curso[df_en_curso["maquina_norm"] == m]
        if not open_row.empty:
            estado = "En producción"
            current_tipo = str(open_row.iloc[0].get("tipo_produccion", ""))
            current_of = str(open_row.iloc[0].get("lote_of", ""))
            current_mp = str(open_row.iloc[0].get("lote_mp", ""))
            current_inicio = str(open_row.iloc[0].get("hora_inicio", ""))
            current_operario = str(open_row.iloc[0].get("operario_inicio", ""))
        else:
            estado = "Libre"
            current_tipo = ""
            current_of = ""
            current_mp = ""
            current_inicio = ""
            current_operario = ""
        status_rows.append({
            "Máquina": m,
            "Tipo config": conf_tipo,
            "OF config": conf_of,
            "Lote MP config": conf_mp,
            "Estado": estado,
            "Tipo actual": current_tipo,
            "OF actual": current_of,
            "Lote MP actual": current_mp,
            "Inicio actual": current_inicio,
            "Operario actual": current_operario,
        })
    # Convertir a DataFrame para mostrar en la tabla
    df_status = pd.DataFrame(status_rows)
    # Ordenar por número de máquina
    df_status = df_status.sort_values(by="Máquina")
    st.dataframe(df_status, use_container_width=True, hide_index=True)
