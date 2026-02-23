import streamlit as st
import pandas as pd
import uuid
from datetime import datetime, date
import os
import json
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Bicopack – Registro", layout="centered")

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
EN_CURSO_PATH = os.path.join(BASE_PATH, "bobinas_en_curso.csv")
TERMINADAS_PATH = os.path.join(BASE_PATH, "bobinas_terminadas.csv")
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
        raise RuntimeError("Falta GOOGLE_SERVICE_ACCOUNT en Render (Environment).")

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
        raise RuntimeError("Falta GOOGLE_SHEET_ID en Render (Environment).")

    gc = _gs_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)
    ws.append_row(row, value_input_option="USER_ENTERED")


# --------------------
# UI
# --------------------
st.title("Bicopack – Registro de producción")

tabs = st.tabs(["Inicio de bobina", "Fin de bobina", "Tareas / Incidencias"])


# --------------------
# INICIO DE BOBINA
# --------------------
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
            value=datetime.now().time().replace(second=0, microsecond=0)
        )

        guardar_inicio = st.form_submit_button("Guardar inicio")

        if guardar_inicio:
            if (not lote_of) or (not lote_mp) or (not operario_inicio):
                st.error("Faltan campos obligatorios (lote MP, lote OF, operario).")
            else:
                df_en_curso = load_csv(
                    EN_CURSO_PATH,
                    [
                        "bobina_id", "fecha", "turno", "maquina",
                        "lote_materia_prima", "lote_of",
                        "hora_inicio", "operario_inicio"
                    ]
                )

                bobina_id = str(uuid.uuid4())
                new_row = {
                    "bobina_id": bobina_id,
                    "fecha": fecha.isoformat(),
                    "turno": turno,
                    "maquina": int(maquina),
                    "lote_materia_prima": lote_mp,
                    "lote_of": lote_of,
                    "hora_inicio": hora_inicio.strftime("%H:%M"),
                    "operario_inicio": operario_inicio,
                }

                df_en_curso = pd.concat([df_en_curso, pd.DataFrame([new_row])], ignore_index=True)
                save_csv(df_en_curso, EN_CURSO_PATH)

                # Enviar también a Google Sheets (EN_CURSO)
                try:
                    gs_append_row("EN_CURSO", [
                        bobina_id,
                        fecha.isoformat(),
                        turno,
                        int(maquina),
                        lote_mp,
                        lote_of,
                        hora_inicio.strftime("%H:%M"),
                        operario_inicio,
                    ])
                except Exception as e:
                    st.warning(f"⚠️ Guardado local OK, pero no se pudo enviar a Google Sheets: {e}")

                st.success("✅ Inicio registrado")


# --------------------
# FIN DE BOBINA
# --------------------
with tabs[1]:
    st.subheader("Fin de bobina")

    df_en_curso = load_csv(
        EN_CURSO_PATH,
        [
            "bobina_id", "fecha", "turno", "maquina",
            "lote_materia_prima", "lote_of",
            "hora_inicio", "operario_inicio"
        ]
    )

    if df_en_curso.empty:
        st.info("No hay bobinas en curso.")
    else:
        opciones = df_en_curso.copy()
        opciones["label"] = opciones.apply(
            lambda r: f"Máquina {r['maquina']} – OF {r['lote_of']} – inicio {r['hora_inicio']} (op: {r['operario_inicio']})",
            axis=1
        )

        seleccion = st.selectbox("Selecciona la bobina a cerrar", opciones["label"].tolist())
        fila = opciones[opciones["label"] == seleccion].iloc[0]

        with st.form("fin_bobina"):
            hora_fin = st.time_input(
                "Hora de fin",
                value=datetime.now().time().replace(second=0, microsecond=0)
            )
            operario_fin = st.text_input("Operario que finaliza")

            peso = st.number_input("Peso de la bobina (kg)", min_value=0.0, max_value=20.0, step=0.1)
            taras = st.number_input("Número de taras", min_value=0, max_value=20, step=1)
            observaciones = st.text_area("Observaciones")

            guardar_fin = st.form_submit_button("Guardar fin")

            if guardar_fin:
                if not operario_fin:
                    st.error("Debes indicar el operario que finaliza.")
                else:
                    df_terminadas = load_csv(
                        TERMINADAS_PATH,
                        [
                            "bobina_id", "fecha", "turno", "maquina",
                            "lote_materia_prima", "lote_of",
                            "hora_inicio", "operario_inicio",
                            "hora_fin", "operario_fin",
                            "peso", "taras", "observaciones"
                        ]
                    )

                    new_row = {
                        "bobina_id": fila["bobina_id"],
                        "fecha": fila["fecha"],
                        "turno": fila["turno"],
                        "maquina": int(fila["maquina"]),
                        "lote_materia_prima": fila["lote_materia_prima"],
                        "lote_of": fila["lote_of"],
                        "hora_inicio": fila["hora_inicio"],
                        "operario_inicio": fila["operario_inicio"],
                        "hora_fin": hora_fin.strftime("%H:%M"),
                        "operario_fin": operario_fin,
                        "peso": float(peso),
                        "taras": int(taras),
                        "observaciones": observaciones,
                    }

                    df_terminadas = pd.concat([df_terminadas, pd.DataFrame([new_row])], ignore_index=True)
                    save_csv(df_terminadas, TERMINADAS_PATH)

                    # Enviar también a Google Sheets (BOBINAS) - ORDEN SEGÚN TU SHEET
                    try:
                       gs_append_row("BOBINAS", [
    fila["bobina_id"],                 # A
    fila["fecha"],                     # B
    fila["turno"],                     # C
    int(fila["maquina"]),              # D
    fila["lote_materia_prima"],        # E
    fila["lote_of"],                   # F
    fila["hora_inicio"],               # G
    fila["operario_inicio"],           # H
    hora_fin.strftime("%H:%M"),        # I
    operario_fin,                      # J
    float(peso),                       # K
    int(taras),                        # L
    observaciones,                     # M
])
                    except Exception as e:
                        st.warning(f"⚠️ Guardado local OK, pero no se pudo enviar a Google Sheets: {e}")

                    # Quitar de en curso
                    df_en_curso = df_en_curso[df_en_curso["bobina_id"] != fila["bobina_id"]]
                    save_csv(df_en_curso, EN_CURSO_PATH)

                    st.success("✅ Bobina cerrada")


# --------------------
# TAREAS / INCIDENCIAS
# --------------------
with tabs[2]:
    st.subheader("Registro de tareas / incidencias")

    st.caption(
        "Usa esta pestaña para anotar paradas, roturas, limpiezas, cambios de material/color, etc. "
        "Cada envío crea un registro independiente."
    )

    with st.form("evento"):
        tipo = st.selectbox("Tipo", ["Incidencia", "Tarea/Limpieza"])
        fecha = st.date_input("Fecha del evento", value=date.today(), key="ev_fecha")
        maquina = st.number_input("Número de máquina", min_value=1, step=1, key="ev_maquina")
        hora_inicio = st.time_input(
            "Hora inicio",
            value=datetime.now().time().replace(second=0, microsecond=0),
            key="ev_hini"
        )
        hora_fin = st.time_input(
            "Hora fin",
            value=datetime.now().time().replace(second=0, microsecond=0),
            key="ev_hfin"
        )
        operario = st.text_input("Operario", key="ev_op")
        motivo = st.text_area("Descripción / motivo", key="ev_desc")
        metros_paro = st.text_input("Metro en el que se para (opcional)", key="ev_m")

        guardar_evento = st.form_submit_button("Guardar evento")

        if guardar_evento:
            if not operario or not motivo:
                st.error("Faltan campos obligatorios (operario y descripción).")
            else:
                df_eventos = load_csv(
                    EVENTOS_PATH,
                    [
                        "evento_id", "tipo", "fecha", "maquina",
                        "hora_inicio", "hora_fin", "operario",
                        "descripcion", "metro_paro"
                    ]
                )

                evento_id = str(uuid.uuid4())
                new_row = {
                    "evento_id": evento_id,
                    "tipo": tipo,
                    "fecha": fecha.isoformat(),
                    "maquina": int(maquina),
                    "hora_inicio": hora_inicio.strftime("%H:%M"),
                    "hora_fin": hora_fin.strftime("%H:%M"),
                    "operario": operario,
                    "descripcion": motivo,
                    "metro_paro": metros_paro,
                }

                df_eventos = pd.concat([df_eventos, pd.DataFrame([new_row])], ignore_index=True)
                save_csv(df_eventos, EVENTOS_PATH)

                # Calcular minutos (si la hora_fin es menor, asumimos que cruza medianoche)
                start_dt = datetime.combine(date.today(), hora_inicio)
                end_dt = datetime.combine(date.today(), hora_fin)
                if end_dt < start_dt:
                    end_dt = end_dt.replace(day=end_dt.day + 1)
                minutos = int((end_dt - start_dt).total_seconds() / 60)

                # Enviar también a Google Sheets (EVENTOS) - ORDEN SEGÚN TU SHEET
                try:
                    gs_append_row("EVENTOS", [
                        fecha.isoformat(),
                        "",          # turno (no se pide en este formulario)
                        int(maquina),
                        "",          # lote_of (no se pide en este formulario)
                        tipo,
                        hora_inicio.strftime("%H:%M"),
                        hora_fin.strftime("%H:%M"),
                        minutos,
                        operario,
                        motivo,
                    ])
                except Exception as e:
                    st.warning(f"⚠️ Guardado local OK, pero no se pudo enviar a Google Sheets: {e}")

                st.success("✅ Evento guardado")
