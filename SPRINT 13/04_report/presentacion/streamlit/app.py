from pathlib import Path
import base64
import math
import importlib

import pandas as pd
import streamlit as st
import textwrap
import data
import charts

st.set_page_config(
    page_title="Delitos de odio y discurso público",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="collapsed"
)


# ============================================================
# RECARGA DE MÓDULOS
# ============================================================

importlib.invalidate_caches()

data = importlib.reload(data)
charts = importlib.reload(charts)


# ============================================================
# FUNCIONES DE CARGA
# ============================================================

cargar_contexto_nacional = (
    data.cargar_contexto_nacional
)

cargar_datos_territoriales = (
    data.cargar_datos_territoriales
)

cargar_evolucion_electoral = (
    data.cargar_evolucion_electoral
)

cargar_evolucion_bloques = (
    data.cargar_evolucion_bloques
)

cargar_bertopic_prensa = (
    data.cargar_bertopic_prensa
)

cargar_rrss_master = (
    data.cargar_rrss_master
)

cargar_prensa_nlp = (
    data.cargar_prensa_nlp
)

cargar_prensa_nlp_final = (
    data.cargar_prensa_nlp_final
)

cargar_ejemplos_hover = (
    data.cargar_ejemplos_hover
)

ejemplos_hover = cargar_ejemplos_hover()

# ============================================================
# FUNCIONES DE GRÁFICOS
# ============================================================

crear_evolucion_delitos = (
    charts.crear_evolucion_delitos
)

crear_scatter_factores = (
    charts.crear_scatter_factores
)

crear_evolucion_electoral = (
    charts.crear_evolucion_electoral
)

crear_evolucion_negatividad_rrss = (
    charts.crear_evolucion_negatividad_rrss
)

crear_evolucion_temas_prensa = (
    charts.crear_evolucion_temas_prensa
)

crear_sentimiento_plataforma = (
    charts.crear_sentimiento_plataforma
)

crear_matriz_factores = (
    charts.crear_matriz_factores
)

crear_evolucion_socioeconomica = (
    charts.crear_evolucion_socioeconomica
)

crear_recuadro_medios = (
    charts.crear_recuadro_medios
)

# ============================================================
# CONFIGURACIÓN
# ============================================================


TOTAL_PAGINAS = 13

if "pagina" not in st.session_state:
    st.session_state.pagina = 1


# ============================================================
# CARGA DE DATOS
# ============================================================

contexto_nacional = cargar_contexto_nacional()
datos_territoriales = cargar_datos_territoriales()
datos_electorales = cargar_evolucion_electoral()
datos_bloques = cargar_evolucion_bloques()


# ============================================================
# ESTILO
# ============================================================

st.markdown(
    """
    <style>
    .stApp {
        background-color: #F3EBDD;
        color: #181818;
    }

    header[data-testid="stHeader"] {
        display: none;
    }

    [data-testid="stSidebar"] {
        display: none;
    }

    .block-container {
        max-width: 1500px;
        padding-top: 0.3rem;
        padding-bottom: 0.5rem;
    }

    .cabecera {
        text-align: center;
        border-top: 4px solid #181818;
        border-bottom: 1px solid #181818;
        padding: 6px 0 7px 0;
        margin-bottom: 6px;
    }

    .nombre-periodico {
        font-family: Georgia, serif;
        font-size: 35px;
        font-weight: 700;
        letter-spacing: -1px;
        line-height: 1;
    }

    .titular {
        max-width: 1250px;
        font-family: Georgia, serif;
        font-size: 30px;
        font-weight: 700;
        line-height: 1.02;
        letter-spacing: -1px;
        margin-top: 4px;
    }

    .entradilla {
        max-width: 1200px;
        font-family: Georgia, serif;
        font-size: 15px;
        line-height: 1.25;
        color: #48443E;
        margin-top: 7px;
        margin-bottom: 7px;
    }

    .linea-roja {
        width: 65px;
        height: 4px;
        background-color: #A51C30;
        margin-top: 8px;
        margin-bottom: 8px;
    }

    .cuerpo {
        font-family: Georgia, serif;
        font-size: 15px;
        line-height: 1.35;
        color: #302E2A;
        margin-bottom: 7px;
    }

    .destacado {
        border-left: 5px solid #A51C30;
        padding: 12px 20px;
        margin: 15px 0;
        background-color: #fffdf7;
        font-family: Georgia, serif;
        font-size: 18px;
        font-style: italic;
    }

    .zona-grafico {
        min-height: 230px;
        border-top: 1px solid #77716A;
        border-bottom: 1px solid #77716A;
        margin-top: 15px;
        padding: 45px 20px;
        text-align: center;
        font-family: Arial, sans-serif;
        color: #6B6863;
        letter-spacing: 1px;
    }

    .hipotesis {
        min-height: 165px;
        border-top: 4px solid #A51C30;
        background-color: #fffdf7;;
        padding: 18px;
        font-family: Georgia, serif;
        font-size: 16px;
        line-height: 1.4;
    }

    .hipotesis strong {
        display: block;
        margin-bottom: 10px;
        font-family: Arial, sans-serif;
        font-size: 12px;
        letter-spacing: 2px;
        text-transform: uppercase;
        color: #A51C30;
    }

    .indicador {
        height: 315px;
        display: flex;
        flex-direction: column;
        align-items: flex-start;
        justify-content: center;
        gap: 14px;
        margin: 0;
        padding: 18px;
        border-top: 4px solid #A51C30;
        border-bottom: 1px solid #B8AEA0;
        background-color: #fffdf7;;
    }

    .indicador-valor {
        font-family: Georgia, serif;
        font-size: 46px;
        line-height: 1;
        font-weight: bold;
    }

    .indicador-texto {
        font-family: Arial, sans-serif;
        color: #68635D;
    }

    .indicador-titulo {
        font-size: 17px;
        font-weight: bold;
        color: #181818;
        margin-bottom: 8px;
    }

    .indicador-detalle {
        font-size: 13px;
        line-height: 1.4;
        margin-top: 4px;
    }

    .correlacion {
        min-height: 205px;
        margin-top: 12px;
        padding: 18px;
        border-top: 4px solid #A51C30;
        border-bottom: 1px solid #B8AEA0;
        background-color: #fffdf7;;
        font-family: Arial, sans-serif;
    }

    .correlacion-etiqueta {
        color: #68635D;
        font-size: 11px;
        font-weight: bold;
        letter-spacing: 2px;
        text-transform: uppercase;
    }

    .correlacion-valor {
        margin-top: 8px;
        font-family: Georgia, serif;
        font-size: 46px;
        font-weight: bold;
        line-height: 1;
        color: #A51C30;
    }

    .correlacion-lectura {
        margin-top: 12px;
        color: #181818;
        font-size: 15px;
        font-weight: bold;
        line-height: 1.3;
    }

    .correlacion-nota {
        margin-top: 10px;
        color: #68635D;
        font-size: 12px;
        line-height: 1.35;
    }

    .metodologia-grid {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 18px;
        margin-top: 20px;
    }

    .metodo {
        min-height: 245px;
        padding: 20px;
        border-top: 4px solid #A51C30;
        border-bottom: 1px solid #B8AEA0;
        background-color: #fffdf7;;
    }

    .metodo-numero {
        font-family: Arial, sans-serif;
        color: #A51C30;
        font-size: 12px;
        font-weight: bold;
        letter-spacing: 2px;
        text-transform: uppercase;
    }

    .metodo-titulo {
        margin-top: 10px;
        font-family: Georgia, serif;
        font-size: 25px;
        font-weight: bold;
        color: #181818;
    }

    .metodo-pregunta {
        margin-top: 12px;
        font-family: Georgia, serif;
        font-size: 16px;
        font-style: italic;
        color: #48443E;
    }

    .metodo-explicacion {
        margin-top: 15px;
        font-family: Arial, sans-serif;
        font-size: 13px;
        line-height: 1.45;
        color: #68635D;
    }

    .metodo-resultado {
        display: inline-block;
        margin-top: 16px;
        padding-top: 8px;
        border-top: 1px solid #A51C30;
        font-family: Arial, sans-serif;
        font-size: 12px;
        font-weight: bold;
        color: #181818;
    }

    .metodologia-cierre {
        margin-top: 18px;
        padding: 13px 18px;
        border-left: 5px solid #A51C30;
        background-color: #fffdf7;;
        font-family: Georgia, serif;
        font-size: 17px;
        font-style: italic;
        color: #302E2A;
    }

    .pie {
        border-top: 1px solid #77716A;
        margin-top: 12px;
        padding-top: 7px;
        font-family: Arial, sans-serif;
        font-size: 11px;
        color: #68635D;
    }

    div.stButton > button {
        border: 1px solid #181818;
        border-radius: 0;
        background-color: transparent;
        color: #181818;
        font-family: Arial, sans-serif;
        font-weight: 700;
    }

    div.stButton > button:hover {
        border-color: #A51C30;
        color: #A51C30;
    }

    .plataforma-ficha {
        min-height: 275px;
        padding: 20px;
        border-top: 4px solid #A51C30;
        border-bottom: 1px solid #B8AEA0;
        background-color: #fffdf7;;
    }

    .plataforma-tipo {
        font-family: Arial, sans-serif;
        color: #A51C30;
        font-size: 11px;
        font-weight: bold;
        letter-spacing: 2px;
        text-transform: uppercase;
    }

    .plataforma-nombre {
        margin-top: 8px;
        font-family: Georgia, serif;
        font-size: 30px;
        font-weight: bold;
        color: #181818;
    }

    .plataforma-descripcion {
        margin-top: 12px;
        font-family: Georgia, serif;
        font-size: 15px;
        line-height: 1.4;
        color: #48443E;
    }

    .plataforma-cifra {
        margin-top: 18px;
        font-family: Georgia, serif;
        font-size: 36px;
        font-weight: bold;
        color: #A51C30;
    }

    .plataforma-detalle {
        margin-top: 5px;
        font-family: Arial, sans-serif;
        font-size: 12px;
        line-height: 1.5;
        color: #68635D;
    }

    .nota-sentimiento {
        margin-top: -12px;
        padding: 11px 15px;
        border-left: 4px solid #A51C30;
        background-color: #fffdf7;
        font-family: Arial, sans-serif;
        font-size: 11px;
        line-height: 1.4;
        color: #68635D;
    }

    .nota-sentimiento strong {
        color: #181818;
    }

    .nota-comparacion {
        margin-top: -8px;
        padding: 10px 15px;
        border-left: 4px solid #A51C30;
        background-color: #fffdf7;
        font-family: Arial, sans-serif;
        font-size: 11px;
        line-height: 1.4;
        color: #68635D;
    }

    .nota-comparacion strong {
        color: #181818;
    }

    .nota-epilogo {
        max-width: 1100px;
        margin: 10px auto 0 auto;
        padding: 10px 15px;
        border-left: 4px solid #A51C30;
        background-color: #fffdf7;;
        font-family: Arial, sans-serif;
        font-size: 11px;
        line-height: 1.4;
        color: #68635D;
    }

    .nota-epilogo strong {
        color: #181818;
    }

    .cierre-final {
        min-height: 330px;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        border-top: 4px solid #A51C30;
        border-bottom: 1px solid #B8AEA0;
        background-color: #fffdf7;;
        text-align: center;
    }

    .cierre-preguntas {
        font-family: Georgia, serif;
        font-size: 78px;
        font-weight: bold;
        line-height: 1;
        letter-spacing: -2px;
        color: #181818;
}

    .cierre-texto {
        max-width: 700px;
        margin-top: 18px;
        font-family: Georgia, serif;
        font-size: 18px;
        line-height: 1.45;
        color: #48443E;
    }

    .cierre-periodo {
        margin-top: 24px;
        font-family: Arial, sans-serif;
        font-size: 12px;
        letter-spacing: 2px;
        text-transform: uppercase;
        color: #A51C30;
    }    

    .resultados-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 18px;
    margin-top: 18px;
    }

    .resultado-card {
        min-height: 245px;
        padding: 19px;
        border-top: 4px solid #A51C30;
        border-bottom: 1px solid #B8AEA0;
        background-color: #fffdf7;;
    }

    .resultado-estado {
        font-family: Arial, sans-serif;
        color: #A51C30;
        font-size: 11px;
        font-weight: bold;
        letter-spacing: 2px;
        text-transform: uppercase;
    }

    .resultado-titulo {
        margin-top: 10px;
        font-family: Georgia, serif;
        font-size: 22px;
        font-weight: bold;
        line-height: 1.15;
        color: #181818;
    }

    .resultado-texto {
        margin-top: 14px;
        font-family: Georgia, serif;
        font-size: 15px;
        line-height: 1.4;
        color: #48443E;
    }

    .limitacion-card {
        min-height: 245px;
        padding: 19px;
        border-top: 4px solid #6B6863;
        border-bottom: 1px solid #B8AEA0;
        background-color: #fffdf7;;
    }

    .limitacion-numero {
        font-family: Arial, sans-serif;
        color: #6B6863;
        font-size: 11px;
        font-weight: bold;
        letter-spacing: 2px;
        text-transform: uppercase;
    }

    .portada {
    min-height: 500px;
    margin-top: 12px;
    padding: 25px 28px;
    border-top: 5px solid #A51C30;
    border-bottom: 1px solid #77716A;
    background-color: #fffdf7;;
    }

    .portada-edicion {
        font-family: Arial, sans-serif;
        color: #A51C30;
        font-size: 12px;
        font-weight: bold;
        letter-spacing: 3px;
        text-transform: uppercase;
    }

    .portada-grid {
        display: grid;
        grid-template-columns: 2.3fr 1fr;
        gap: 40px;
        margin-top: 20px;
    }

    .portada-titular {
        max-width: 900px;
        font-family: Georgia, serif;
        font-size: 62px;
        font-weight: bold;
        line-height: 0.98;
        letter-spacing: -3px;
        color: #181818;
    }

    .portada-linea {
        width: 90px;
        height: 6px;
        margin-top: 19px;
        background-color: #A51C30;
    }

    .portada-subtitulo {
        max-width: 820px;
        margin-top: 20px;
        font-family: Georgia, serif;
        font-size: 23px;
        line-height: 1.35;
        color: #48443E;
    }

    .portada-sumario {
        padding-left: 22px;
        border-left: 1px solid #77716A;
    }

    .portada-sumario-titulo {
        font-family: Arial, sans-serif;
        color: #A51C30;
        font-size: 11px;
        font-weight: bold;
        letter-spacing: 2px;
        text-transform: uppercase;
    }

    .portada-noticia {
        padding: 13px 0;
        border-bottom: 1px solid #B8AEA0;
        font-family: Georgia, serif;
        font-size: 16px;
        line-height: 1.3;
        color: #302E2A;
    }

    .portada-noticia strong {
        margin-right: 7px;
        color: #A51C30;
        font-family: Arial, sans-serif;
        font-size: 11px;
    }

    .portada-cifras {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        margin-top: 32px;
        border-top: 1px solid #77716A;
    }

    .portada-cifra {
        padding: 15px 13px 4px 13px;
        border-right: 1px solid #B8AEA0;
    }

    .portada-cifra:last-child {
        border-right: none;
    }

    .portada-valor {
        font-family: Georgia, serif;
        font-size: 28px;
        font-weight: bold;
        color: #A51C30;
    }

    .portada-etiqueta {
        margin-top: 4px;
        font-family: Arial, sans-serif;
        font-size: 11px;
        line-height: 1.3;
        color: #68635D;
    }

    .portada-dos-columnas {
    min-height: 475px;
    display: grid;
    grid-template-columns: 1.08fr 1fr;
    overflow: hidden;
    margin-top: 12px;
    border-top: 5px solid #A51C30;
    border-bottom: 1px solid #77716A;
    background-color: #fffdf7;;
    }

    .portada-imagen {
        min-height: 475px;
        overflow: hidden;
        background-color: #181818;
    }

    .portada-imagen img {
        width: 100%;
        height: 100%;
        min-height: 475px;
        display: block;
        object-fit: cover;
        object-position: center;
        filter: saturate(0.78) contrast(1.08);
    }

    .portada-contenido {
        min-height: 475px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        padding: 38px 42px;
    }

    .portada-kicker {
        font-family: Arial, sans-serif;
        color: #A51C30;
        font-size: 12px;
        font-weight: bold;
        letter-spacing: 3px;
        text-transform: uppercase;
    }

    .portada-titulo-proyecto {
        margin-top: 22px;
        font-family: Georgia, serif;
        font-size: 49px;
        font-weight: bold;
        line-height: 0.98;
        letter-spacing: -2px;
        color: #181818;
    }

    .portada-separador {
        width: 85px;
        height: 6px;
        margin-top: 24px;
        background-color: #A51C30;
    }

    .portada-periodo {
        margin-top: 26px;
        padding-top: 15px;
        border-top: 1px solid #B8AEA0;
        font-family: Arial, sans-serif;
        font-size: 12px;
        line-height: 1.5;
        letter-spacing: 1px;
        color: #68635D;
        text-transform: uppercase;
    }

    div[data-testid="stPlotlyChart"] {
        background: #fffdf7;
        border: 1px solid rgba(75, 68, 58, 0.16);
        border-radius: 2px;
        padding: 0.35rem;
        overflow: hidden;
    }
    </style>
    """,
    unsafe_allow_html=True
)


# ============================================================
# CONTENIDO
# ============================================================

PAGINAS = {
    1: {
        "titular": "Delitos de odio y discurso público en España",
        "entradilla": (
            "Del registro oficial a la conversación social, "
            "2015–2023."
        ),
        "cuerpo": (
            "Una investigación territorial, social, política "
            "y mediática."
        )
    },

    2: {
        "titular": (
            "¿Por qué importa analizar el odio?"
        ),
        "entradilla": (
            "El vídeo introduce el contexto social, político "
            "y mediático del análisis"
        ),
        "cuerpo": ""
    },

    3: {
        "titular": (
            "HIPÓTESIS"
        ),
        "entradilla": (
            "Partimos de tres preguntas: "
            "qué papel juega el contexto, "
            "el canal importa y "
            "todo esto evoluciona a la vez."
        ),
        "cuerpo": ""
    },

    4: {
        "titular": (
            "Los delitos registrados aumentan, "
            "pero no de la misma manera"
        ),
        "entradilla": (
            "El aumento se concentra especialmente en determinados ámbitos, mientras otros disminuyen."
        ),
        "cuerpo": ""
    },

    5: {
        "titular": (
            "2019 transforma el escenario político"
        ),
        "entradilla": (
            "De rozar el 0% a 52 escaños: así entró Vox en el Congreso."
        ),
        "cuerpo": ""
    },

    6: {
        "titular": (
            "Ningún indicador explica por sí solo el fenómeno"
        ),
        "entradilla": (
            "Comparamos cuatro factores sociales y políticos "
            "en los años analizados."
        ),
        "cuerpo": ""
    },

    7: {
        "titular": (
            "Encontrar una palabra no es suficiente "
            "para comprender un texto"
        ),
        "entradilla": (
            "El diccionario permite localizar expresiones, "
            "pero no determina por sí solo la intención del mensaje."
        ),
        "cuerpo": ""
    },

    8: {
        "titular": (
            "Encontrar una palabra no es suficiente para entender un texto"
        ),
        "entradilla": (
            "El diccionario localiza expresiones relevantes; "
            "el análisis de sentimiento ayuda a interpretar el tono."
        ),
        "cuerpo": ""
    },

    9: {
        "titular": (
            "La conversación popular cambia "
            "según el espacio y el tema"
        ),
        "entradilla": (
            "Cuatro comunidades digitales, dos temas, y un tono que cambia poco entre ellos"
        ),
        "cuerpo": ""
    },

    10: {
        "titular": (
            "La negatividad no evoluciona igual "
            "en todas las plataformas"
        ),
        "entradilla": (
            "Comparamos el porcentaje de comentarios negativos "
            "durante los cuatro años electorales."
        ),
        "cuerpo": ""
    },

    11: {
        "titular": (
            "No hay una causa única, ni una única conversación"
        ),
        "entradilla": (
            "Hay respuestas, pero también límites que conviene tener presentes."
        ),
        "cuerpo": ""
    },

    12: {
        "titular": (
            "Lo registrado es solo la parte visible"
        ),
        "entradilla": (
            "Una última cifra obliga a interpretar todos "
            "los resultados con cautela."
        ),
        "cuerpo": ""
    },

    13: {
        "titular": "Gracias",
        "entradilla": "",
        "cuerpo": ""
    }
}

# ============================================================
# FUNCIONES VISUALES
# ============================================================

def mostrar_cabecera():

    st.html(
        """
        <div class="cabecera">
            <div class="nombre-periodico">
                EL OBSERVATORIO DEL ODIO
            </div>
        </div>
        """
    )


def mostrar_titular(datos):

    st.html(
        f"""
        <div class="titular">
            {datos["titular"]}
        </div>

        <div class="linea-roja"></div>

        <div class="entradilla">
            {datos["entradilla"]}
        </div>
        """
    )


def mostrar_cuerpo(texto):

    if texto:

        st.html(
            f"""
            <div class="cuerpo">
                {texto}
            </div>
            """
        )


def seleccionar_tema():

    return st.segmented_control(
        "Vista temática",
        options=[
            "General",
            "Por ámbito",
            "Inmigración",
            "LGTBI"
        ],
        default="General",
        label_visibility="collapsed",
        key=f"tema_pagina_{st.session_state.pagina}"
    )


def mostrar_zona_reservada():

    st.html(
        """
        <div class="zona-grafico">
            ZONA RESERVADA PARA EL GRÁFICO
        </div>
        """
    )


# ============================================================
# VÍDEO
# ============================================================

def mostrar_video():

    ruta_video = (
        Path(__file__).parent
        / "assets"
        / "introduccion.mp4"
    )

    if ruta_video.exists():

        st.video(
            str(ruta_video),
            autoplay=False
        )

    else:

        st.html(
            """
            <div class="zona-grafico">
                ESPACIO RESERVADO PARA EL VÍDEO INTRODUCTORIO
                <br><br>
                assets/introduccion.mp4
            </div>
            """
        )


# ============================================================
# HIPÓTESIS
# ============================================================

def mostrar_hipotesis():

    col1, col2, col3 = st.columns(3)

    with col1:

        st.html(
            """
            <div class="hipotesis" style="position: relative; overflow: hidden;">

                <div style="
                    position: absolute;
                    right: 20px;
                    top: 12px;
                    font-family: Georgia, serif;
                    font-size: 64px;
                    color: rgba(165, 28, 48, 0.13);
                    line-height: 1;
                ">
                    ◎
                </div>

                <strong>01 · Contexto</strong>

                <div style="
                    font-family: Georgia, serif;
                    font-size: 18px;
                    line-height: 1.45;
                    max-width: 85%;
                    margin-top: 18px;
                ">
                    Los cambios sociales, económicos y políticos
                    podrían estar asociados a la evolución de los
                    delitos de odio en España.
                </div>

            </div>
            """
        )

    with col2:

        st.html(
            """
            <div class="hipotesis" style="position: relative; overflow: hidden;">

                <div style="
                    position: absolute;
                    right: 20px;
                    top: 14px;
                    font-family: Arial, sans-serif;
                    font-size: 55px;
                    color: rgba(165, 28, 48, 0.13);
                    line-height: 1;
                ">
                    ↔
                </div>

                <strong>02 · Canal</strong>

                <div style="
                    font-family: Georgia, serif;
                    font-size: 18px;
                    line-height: 1.45;
                    max-width: 85%;
                    margin-top: 18px;
                ">
                    ¿El discurso negativo se expresa de igual
                    forma en todos los medios o existen
                    plataformas con mayor nivel de hostilidad?
                </div>

            </div>
            """
        )

    with col3:

        st.html(
            """
            <div class="hipotesis" style="position: relative; overflow: hidden;">

                <div style="
                    position: absolute;
                    right: 20px;
                    top: 14px;
                    font-family: Arial, sans-serif;
                    font-size: 58px;
                    color: rgba(165, 28, 48, 0.13);
                    line-height: 1;
                ">
                    ↗
                </div>

                <strong>03 · Evolución conjunta</strong>

                <div style="
                    font-family: Georgia, serif;
                    font-size: 18px;
                    line-height: 1.45;
                    max-width: 85%;
                    margin-top: 18px;
                ">
                    El incremento del discurso negativo y los
                    delitos de odio podrían evolucionar de forma
                    paralela.
                </div>

            </div>
            """
        )

    st.html(
        """
        <div style="
            width:68%;
            margin:18px auto 12px auto;
            display:flex;
            align-items:center;
            justify-content:space-between;
            color:#A12F33;
            opacity:0.45;
        ">

            <span style="font-size:30px;">🌍</span>

            <div style="
                flex:1;
                height:2px;
                background:#A12F33;
                margin:0 18px;
            "></div>

            <span style="font-size:30px;">💬</span>

            <div style="
                flex:1;
                height:2px;
                background:#A12F33;
                margin:0 18px;
            "></div>

            <span style="font-size:30px;">📈</span>

        </div>
        """
    )

    st.html(
        """
        <div
            class="destacado"
            style="
                width: 100%;
                max-width: none;
                box-sizing: border-box;
                padding: 16px 24px;
                font-size: 17px;
                line-height: 1.45;
            "
        >
            Estas tres hipótesis permiten analizar si el contexto, el canal
            y la evolución temporal del discurso pueden estar relacionados
            con los cambios observados en los delitos de odio.
        </div>
        """
    )

# ============================================================
# PÁGINA 1 — PORTADA
# ============================================================

def mostrar_portada():

    ruta_imagen = (
        Path(__file__).parent
        / "assets"
        / "descarga.jpg"
    )

    if not ruta_imagen.exists():

        st.warning(
            "No se encuentra assets/descarga.jpg"
        )

        return

    imagen_base64 = base64.b64encode(
        ruta_imagen.read_bytes()
    ).decode("utf-8")

    st.html(
        f"""
        <div class="portada-dos-columnas">

            <div class="portada-imagen">
                <img
                    src="data:image/jpeg;base64,{imagen_base64}"
                    alt="Pintadas de simbología de odio"
                >
            </div>

            <div class="portada-contenido">

                <div class="portada-kicker">
                    Ángel Herrezuelo
                </div>

                <div class="portada-titulo-proyecto">
                    Delitos de odio y discurso público en España
                </div>

                <div class="portada-separador"></div>

                <div class="portada-periodo">
                    Análisis de los años electorales
                    2015 · 2016 · 2019 · 2023
                </div>

            </div>

        </div>
        """
    )

# ============================================================
# PÁGINA 4 — EVOLUCIÓN
# ============================================================

def mostrar_evolucion_delitos(
    tema_seleccionado
):

    # ========================================================
    # PANORAMA COMPARADO DE ÁMBITOS
    # ========================================================

    if tema_seleccionado == "Por ámbito":

        figura = crear_evolucion_delitos(
            contexto_nacional,
            tema_seleccionado
        )

        figura.update_layout(
            height=380,
            margin={
                "l": 20,
                "r": 55,
                "t": 15,
                "b": 30
            }
        )

        columna_dato, columna_grafico = st.columns(
            [1, 2.6],
            gap="large"
        )

        with columna_dato:

            st.html(
                """
                <div style="
                    height: 380px;
                    padding: 26px 28px;
                    box-sizing: border-box;
                    border-top: 6px solid #A12F33;
                    border-bottom: 1px solid #AAA49A;
                    background: rgba(255, 255, 255, 0.30);
                    overflow: hidden;
                ">

                    <div style="
                        color: #A12F33;
                        font-family: Georgia, serif;
                        margin-bottom: 22px;
                    ">

                        <div style="
                            font-size: 30px;
                            font-weight: 700;
                            line-height: 1;
                            margin-bottom: 10px;
                        ">
                            Dos ámbitos
                        </div>

                        <div style="
                            font-size: 21px;
                            font-weight: 700;
                            line-height: 1.2;
                        ">
                            lideran el registro
                        </div>

                    </div>

                    <div style="
                        font-family: Arial, sans-serif;
                        font-size: 14px;
                        line-height: 1.35;
                        color: #625F58;
                    ">

                        <div style="margin-bottom: 12px;">
                            <strong style="color: #181818;">
                                Racismo y xenofobia
                            </strong>
                            <br>
                            505 → 948 casos
                        </div>

                        <div style="margin-bottom: 18px;">
                            <strong style="color: #181818;">
                                Orientación sexual e identidad de género
                            </strong>
                            <br>
                            169 → 529 casos
                        </div>

                        <div>
                            Por su volumen, crecimiento y presencia
                            en prensa y plataformas, el análisis
                            continúa con estos dos ámbitos.
                        </div>

                    </div>

                </div>
                """
            )

        with columna_grafico:

            st.plotly_chart(
                figura,
                width="stretch",
                config={
                    "displayModeBar": False,
                    "scrollZoom": False,
                    "responsive": True
                }
            )

        return

    # ========================================================
    # EVOLUCIÓN TEMPORAL DE CADA TEMA
    # ========================================================

    columnas_por_tema = {
        "General": "delitos_por_100k",
        "Inmigración": "xenofobia_por_100k",
        "LGTBI": "lgtbi_por_100k"
    }

    columna_actual = columnas_por_tema[
        tema_seleccionado
    ]

    datos_ordenados = contexto_nacional.sort_values(
        "anio"
    )

    valor_inicial = datos_ordenados[
        columna_actual
    ].iloc[0]

    valor_final = datos_ordenados[
        columna_actual
    ].iloc[-1]

    crecimiento = (
        (valor_final - valor_inicial)
        / valor_inicial
        * 100
    )

    crecimiento_texto = (
        f"{crecimiento:+.1f}%"
        .replace(".", ",")
    )

    valor_inicial_texto = (
        f"{valor_inicial:.2f}"
        .replace(".", ",")
    )

    valor_final_texto = (
        f"{valor_final:.2f}"
        .replace(".", ",")
    )

    color_indicador = (
        "#A12F33"
        if crecimiento >= 0
        else "#2F6B57"
    )

    figura = crear_evolucion_delitos(
        contexto_nacional,
        tema_seleccionado
    )

    figura.update_layout(
        title_text="",
        height=355,
        margin={
            "l": 35,
            "r": 20,
            "t": 12,
            "b": 25
        }
    )

    columna_dato, columna_grafico = st.columns(
        [1, 2.6],
        gap="large"
    )

    with columna_dato:

        st.html(
            f"""
            <div class="indicador">

                <div
                    class="indicador-valor"
                    style="color: {color_indicador};"
                >
                    {crecimiento_texto}
                </div>

                <div class="indicador-texto">

                    <div class="indicador-titulo">
                        Cambio de la tasa
                    </div>

                    <div class="indicador-detalle">
                        Entre 2015 y 2023
                    </div>

                    <div class="indicador-detalle">
                        De {valor_inicial_texto}
                        a {valor_final_texto}
                        delitos por 100.000 habitantes
                    </div>

                </div>
            </div>
            """
        )

    with columna_grafico:

        st.plotly_chart(
            figura,
            width="stretch",
            config={
                "displayModeBar": False,
                "scrollZoom": False
            }
        )

# ============================================================
# PÁGINA 5 — EVOLUCIÓN ELECTORAL
# ============================================================

def mostrar_evolucion_electoral():

    datos = datos_electorales.copy()

    valor_2016 = datos.loc[
        datos["eleccion"] == "2016",
        "pct_vox"
    ].iloc[0]

    valor_noviembre = datos.loc[
        datos["eleccion"] == "Nov. 2019",
        "pct_vox"
    ].iloc[0]

    variacion = valor_noviembre - valor_2016

    variacion_texto = (
        f"+{variacion:.1f} pp"
        .replace(".", ",")
    )

    figura = crear_evolucion_electoral(
        datos,
        datos_bloques
    )

    columna_dato, columna_grafico = st.columns(
        [1, 2.6],
        gap="large"
    )

    figura.update_layout(
        height=355,
        margin={
            "l": 35,
            "r": 20,
            "t": 8,
            "b": 25
        }
    )

    with columna_dato:

        st.html(
            f"""
            <div class="indicador">
                <div
                    class="indicador-valor"
                    style="color: #A12F33;"
                >
                    {variacion_texto}
                </div>

                <div class="indicador-texto">
                    <div class="indicador-titulo">
                        Punto de inflexión: 2019
                    </div>

                    <div class="indicador-detalle">
                        Abril: 10,34 % y 24 escaños
                    </div>

                    <div class="indicador-detalle">
                        Noviembre: 15,21 % y 52 escaños
                    </div>

                    <div class="indicador-detalle">
                        En 2023 retrocede al 12,48 %,
                        pero conserva 33 escaños.
                    </div>
                </div>
            </div>
            """
        )

    with columna_grafico:

        st.plotly_chart(
            figura,
            width="stretch",
            config={
                "displayModeBar": False,
                "scrollZoom": False,
                "responsive": True
            }
        )

# ============================================================
# PÁGINA 6 — FACTORES SOCIOECONÓMICOS
# ============================================================

def mostrar_factores():

    mostrar_evolucion_socioeconomica()

# ============================================================
# EVOLUCION SOCIOECONOMICAS
# ============================================================

def mostrar_evolucion_socioeconomica():

    st.html(
        """
        <div style="
            display:flex;
            gap:16px;
            margin-bottom:10px;
        ">

            <div style="
                flex:1;
                border-left:5px solid #A12F33;
                background:#FAF8F4;
                padding:12px 16px;
                border-radius:6px;
            ">
                <div style="
                    font-size:18px;
                    font-weight:700;
                    color:#A12F33;
                    margin-bottom:4px;
                ">
                    Índice de Gini
                </div>

                <div style="
                    font-size:14px;
                    line-height:1.35;
                    color:#555;
                ">
                    Mide la desigualdad en la distribución de la renta.
                    <b>0</b> = igualdad · <b>100</b> = máxima desigualdad.
                </div>
            </div>

            <div style="
                flex:1;
                border-left:5px solid #A12F33;
                background:#FAF8F4;
                padding:12px 16px;
                border-radius:6px;
            ">
                <div style="
                    font-size:18px;
                    font-weight:700;
                    color:#A12F33;
                    margin-bottom:4px;
                ">
                    Ratio S80/S20
                </div>

                <div style="
                    font-size:14px;
                    line-height:1.35;
                    color:#555;
                ">
                    Compara la renta del 20 % más rico con la del 20 % más pobre.
                    Un valor mayor indica más desigualdad.
                </div>
            </div>

        </div>
        """
    )
    figura = crear_evolucion_socioeconomica(
        datos_territoriales
    )

    figura.update_layout(
        height=410,
        margin={
            "l": 45,
            "r": 30,
            "t": 35,
            "b": 40
        }
    )

    st.plotly_chart(
        figura,
        width="stretch",
        config={
            "displayModeBar": False,
            "scrollZoom": False,
            "responsive": True
        }
    )


# ============================================================
# PÁGINA 7 — METODOLOGÍA DEL DISCURSO
# ============================================================

def mostrar_metodologia():

    st.html(
        """
        <div class="metodologia-grid">

            <div class="metodo">
                <div class="metodo-numero">
                    01 · Identificar
                </div>

                <div class="metodo-titulo">
                    Diccionario
                </div>

                <div class="metodo-pregunta">
                    ¿Qué expresiones aparecen?
                </div>

                <div class="metodo-explicacion">
                    Un diccionario temático detecta palabras y
                    expresiones relacionadas con xenofobia,
                    LGTBIfobia y otros ámbitos de hostilidad.
                    Permite clasificar el contenido, pero no
                    determina por sí solo el tono del texto.
                </div>

                <div class="metodo-resultado">
                    RESULTADO · Categorías y niveles lingüísticos
                </div>
            </div>

            <div class="metodo">
                <div class="metodo-numero">
                    02 · Interpretar
                </div>

                <div class="metodo-titulo">
                    Análisis del sentimiento
                </div>

                <div class="metodo-pregunta">
                    ¿Con qué tono se habla?
                </div>

                <div class="metodo-explicacion">
                    El análisis de sentimiento evalúa el fragmento completo para estimar polaridad
                    (positiva, negativa, neutral).
                </div>

                <div class="metodo-resultado">
                    RESULTADO · Polaridad del discurso
                </div>
            </div>

            <div class="metodo">
                <div class="metodo-numero">
                    03 · Comparar
                </div>

                <div class="metodo-titulo">
                    RRSS vs prensa
                </div>

                <div class="metodo-pregunta">
                    ¿Cambia el discurso según dónde se publica?
                </div>

                <div class="metodo-explicacion">
                   Permie observar diferencias entre prensa online y plataformas sociales.
                </div>

                <div class="metodo-resultado">
                    RESULTADO · evolución temporal y diferencias por canal
                </div>
            </div>

        </div>

        <div class="metodologia-cierre">
            Una palabra apunta al tema.
            El contexto define el tono.
            La comparación revela cómo cambia el discurso.
        </div>
        """
    )

# ============================================================
# PÁGINA 8 — DICCIONARIO Y SENTIMIENTO
# ============================================================

def mostrar_temas_prensa():

    fuente = st.segmented_control(
        "Tipo de texto",
        options=["Prensa", "Redes sociales"],
        default="Prensa",
        label_visibility="collapsed",
        key="fuente_diccionario"
    )

    # ========================================================
    # CONTENIDO DINÁMICO
    # ========================================================

    if fuente == "Prensa":

        ejemplos = {
            "mencion": "inmigración · refugiados · derechos LGTBI",
            "conflicto": "crisis migratoria · efecto llamada · agenda LGTBI",
            "hostilidad": "nos invaden · fuera de España · aberración",
            "violencia": "ataque xenófobo · agresión homófoba · delito de odio"
        }

        texto_ejemplo = (
            "La policía investiga un ataque xenófobo "
            "contra un joven migrante."
        )

        resultado_diccionario = "Violencia / discriminación"
        resultado_sentimiento = "Tono negativo o neutral"
        resultado_contexto = (
            "La noticia describe una agresión. "
            "La expresión no implica que el medio la promueva."
        )

    else:

        ejemplos = {
            "mencion": "inmigrantes · refugiados · orgullo LGTBI",
            "conflicto": "inmigración ilegal · efecto llamada · agenda woke",
            "hostilidad": "nos invaden · que se vayan a su país · aberración",
            "violencia": "atacar · agresión · delito de odio"
        }

        texto_ejemplo = (
            "No hay que atacar a nadie por ser inmigrante."
        )

        resultado_diccionario = "Violencia / discriminación"
        resultado_sentimiento = "Tono positivo o neutral"
        resultado_contexto = (
            "El comentario rechaza una agresión. "
            "La palabra detectada no expresa hostilidad."
        )

    # ========================================================
    # FILA 1 — NIVELES DEL DICCIONARIO
    # ========================================================

    st.html(
        f"""
        <div style="
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 12px;
            margin-top: 6px;
            margin-bottom: 18px;
        ">

            <div style="
                min-height: 125px;
                padding: 16px;
                background: #fffdf7;
                border-top: 5px solid #7D918C;
                box-sizing: border-box;
            ">
                <div style="
                    display:flex;
                    align-items:center;
                    gap:8px;
                    margin-bottom:10px;
                ">
                    <span style="
                        width:14px;
                        height:14px;
                        background:#7D918C;
                        display:inline-block;
                    "></span>

                    <span style="
                        font-family:Arial,sans-serif;
                        font-size:11px;
                        font-weight:700;
                        letter-spacing:1.4px;
                        text-transform:uppercase;
                        color:#171713;
                    ">
                        01 · Mención temática
                    </span>
                </div>

                <div style="
                    font-family:Georgia,serif;
                    font-size:14px;
                    line-height:1.4;
                    color:#56534d;
                ">
                    {ejemplos["mencion"]}
                </div>
            </div>


            <div style="
                min-height: 125px;
                padding: 16px;
                background: #fffdf7;
                border-top: 5px solid #B49A58;
                box-sizing: border-box;
            ">
                <div style="
                    display:flex;
                    align-items:center;
                    gap:8px;
                    margin-bottom:10px;
                ">
                    <span style="
                        width:14px;
                        height:14px;
                        background:#B49A58;
                        display:inline-block;
                    "></span>

                    <span style="
                        font-family:Arial,sans-serif;
                        font-size:11px;
                        font-weight:700;
                        letter-spacing:1.4px;
                        text-transform:uppercase;
                        color:#171713;
                    ">
                        02 · Marco conflictivo
                    </span>
                </div>

                <div style="
                    font-family:Georgia,serif;
                    font-size:14px;
                    line-height:1.4;
                    color:#56534d;
                ">
                    {ejemplos["conflicto"]}
                </div>
            </div>


            <div style="
                min-height: 125px;
                padding: 16px;
                background: #fffdf7;
                border-top: 5px solid #B86B45;
                box-sizing: border-box;
            ">
                <div style="
                    display:flex;
                    align-items:center;
                    gap:8px;
                    margin-bottom:10px;
                ">
                    <span style="
                        width:14px;
                        height:14px;
                        background:#B86B45;
                        display:inline-block;
                    "></span>

                    <span style="
                        font-family:Arial,sans-serif;
                        font-size:11px;
                        font-weight:700;
                        letter-spacing:1.4px;
                        text-transform:uppercase;
                        color:#171713;
                    ">
                        03 · Hostilidad explícita
                    </span>
                </div>

                <div style="
                    font-family:Georgia,serif;
                    font-size:14px;
                    line-height:1.4;
                    color:#56534d;
                ">
                    {ejemplos["hostilidad"]}
                </div>
            </div>


            <div style="
                min-height: 125px;
                padding: 16px;
                background: #fffdf7;
                border-top: 5px solid #9F3034;
                box-sizing: border-box;
            ">
                <div style="
                    display:flex;
                    align-items:center;
                    gap:8px;
                    margin-bottom:10px;
                ">
                    <span style="
                        width:14px;
                        height:14px;
                        background:#9F3034;
                        display:inline-block;
                        transform:rotate(45deg);
                    "></span>

                    <span style="
                        font-family:Arial,sans-serif;
                        font-size:11px;
                        font-weight:700;
                        letter-spacing:1.4px;
                        text-transform:uppercase;
                        color:#171713;
                    ">
                        04 · Violencia / discriminación
                    </span>
                </div>

                <div style="
                    font-family:Georgia,serif;
                    font-size:14px;
                    line-height:1.4;
                    color:#56534d;
                ">
                    {ejemplos["violencia"]}
                </div>
            </div>

        </div>
        """
    )

    # ========================================================
    # FILA 2 — EJEMPLO
    # ========================================================

    st.html(
        f"""
        <div style="
            padding: 18px 24px;
            margin-bottom: 14px;
            border-left: 6px solid #A51C30;
            background: #fffdf7;
        ">

            <div style="
                font-family:Arial,sans-serif;
                font-size:11px;
                font-weight:700;
                letter-spacing:2px;
                text-transform:uppercase;
                color:#A51C30;
                margin-bottom:8px;
            ">
                Ejemplo · {fuente}
            </div>

            <div style="
                font-family:Georgia,serif;
                font-size:22px;
                line-height:1.35;
                color:#181818;
            ">
                “{texto_ejemplo}”
            </div>

        </div>
        """
    )

    # ========================================================
    # FILA 3 — DICCIONARIO / SENTIMIENTO / CONTEXTO
    # ========================================================

    st.html(
        f"""
        <div style="
            display:grid;
            grid-template-columns:repeat(3, 1fr);
            gap:14px;
        ">

            <div style="
                min-height:120px;
                padding:17px;
                background:#fffdf7;
                border-top:4px solid #9F3034;
            ">
                <div style="
                    font-family:Arial,sans-serif;
                    font-size:11px;
                    font-weight:700;
                    letter-spacing:1.5px;
                    text-transform:uppercase;
                    color:#9F3034;
                ">
                    Diccionario · Qué aparece
                </div>

                <div style="
                    margin-top:10px;
                    font-family:Georgia,serif;
                    font-size:17px;
                    line-height:1.35;
                    color:#302E2A;
                ">
                    {resultado_diccionario}
                </div>
            </div>


            <div style="
                min-height:120px;
                padding:17px;
                background:#fffdf7;
                border-top:4px solid #526F69;
            ">
                <div style="
                    font-family:Arial,sans-serif;
                    font-size:11px;
                    font-weight:700;
                    letter-spacing:1.5px;
                    text-transform:uppercase;
                    color:#526F69;
                ">
                    Sentimiento · Cómo se dice
                </div>

                <div style="
                    margin-top:10px;
                    font-family:Georgia,serif;
                    font-size:17px;
                    line-height:1.35;
                    color:#302E2A;
                ">
                    {resultado_sentimiento}
                </div>
            </div>


            <div style="
                min-height:120px;
                padding:17px;
                background:#fffdf7;
                border-top:4px solid #181818;
            ">
                <div style="
                    font-family:Arial,sans-serif;
                    font-size:11px;
                    font-weight:700;
                    letter-spacing:1.5px;
                    text-transform:uppercase;
                    color:#181818;
                ">
                    Contexto · Qué significa
                </div>

                <div style="
                    margin-top:10px;
                    font-family:Georgia,serif;
                    font-size:16px;
                    line-height:1.35;
                    color:#48443E;
                ">
                    {resultado_contexto}
                </div>
            </div>

        </div>
        """
    )

    # ========================================================
    # MENSAJE FINAL
    # ========================================================

    st.html(
        """
        <div style="
            margin-top:16px;
            padding:13px 20px;
            border-left:6px solid #A51C30;
            background:#fffdf7;
            font-family:Georgia,serif;
            font-size:19px;
            font-style:italic;
            line-height:1.35;
            color:#282620;
        ">
            Detectar una palabra no significa detectar odio.
            El tono y el contexto cambian su significado.
        </div>
        """
    )

# ============================================================
# PÁGINA 9 — PRESENTACIÓN DE PLATAFORMAS
# ============================================================

def mostrar_plataformas():

    rrss_master = cargar_rrss_master()
    prensa_nlp = cargar_prensa_nlp_final()

    plataforma = st.segmented_control(
        "Plataforma",
        options=[
            "Prensa",
            "Menéame",
            "ForoCoches",
            "YouTube"
        ],
        default="Prensa",
        label_visibility="collapsed",
        key="plataforma_pagina_9"
    )

    informacion = {
        "Prensa": {
            "tipo": "Prensa digital",
            "descripcion": (
                "Los artículos publicados por medios digitales "
                "permiten observar cómo se construye informativamente "
                "el discurso sobre inmigración y cuestiones LGTBI."
            )
        },
        "Menéame": {
            "tipo": "Agregador de noticias",
            "descripcion": (
                "La conversación nace alrededor de noticias "
                "votadas y compartidas por la comunidad. "
                "Conecta actualidad informativa y reacción popular."
            )
        },
        "ForoCoches": {
            "tipo": "Foro generalista",
            "descripcion": (
                "Un espacio comunitario con conversaciones "
                "espontáneas, identidad propia y menor dependencia "
                "de la estructura de una noticia."
            )
        },
        "YouTube": {
            "tipo": "Plataforma audiovisual",
            "descripcion": (
                "Los comentarios reaccionan a vídeos de medios, "
                "creadores y canales informativos, combinando "
                "actualidad, opinión y conversación social."
            )
        }
    }

    # ========================================================
    # FILTRADO DE LA PLATAFORMA
    # ========================================================

    if plataforma == "Prensa":

        datos_plataforma = prensa_nlp.copy()

        datos_plataforma["plataforma"] = "Prensa"

        datos_plataforma["sentimiento"] = (
            datos_plataforma["sentimiento"]
            .replace(
                {
                    "Negativo": "NEG",
                    "Neutral": "NEU",
                    "Positivo": "POS"
                }
            )
        )

    else:

        datos_plataforma = rrss_master[
            rrss_master["plataforma"] == plataforma
        ].copy()

    numero_comentarios = len(
        datos_plataforma
    )

    cifra_texto = (
        f"{numero_comentarios:,}"
        .replace(",", ".")
    )

    # ========================================================
    # CÁLCULO DEL PORCENTAJE NEGATIVO POR TEMA
    # ========================================================

    datos_plataforma["_tema_normalizado"] = (
        datos_plataforma["tema"]
        .astype(str)
        .str.lower()
        .str.strip()
    )

    datos_plataforma["_sentimiento_normalizado"] = (
        datos_plataforma["sentimiento"]
        .astype(str)
        .str.lower()
        .str.strip()
    )

    mascara_inmigracion = (
        datos_plataforma["_tema_normalizado"]
        .str.contains(
            "inmigra",
            na=False
        )
    )

    mascara_lgtbi = (
        datos_plataforma["_tema_normalizado"]
        .str.contains(
            "lgtb",
            na=False
        )
    )

    negativos = [
        "negativo",
        "negative",
        "neg",
        "label_0"
    ]

    datos_inmigracion = datos_plataforma[
        mascara_inmigracion
    ]

    datos_lgtbi = datos_plataforma[
        mascara_lgtbi
    ]

    if len(datos_inmigracion) > 0:

        porcentaje_negativo_inmigracion = (
            datos_inmigracion[
                "_sentimiento_normalizado"
            ]
            .isin(negativos)
            .mean()
            * 100
        )

    else:

        porcentaje_negativo_inmigracion = 0

    if len(datos_lgtbi) > 0:

        porcentaje_negativo_lgtbi = (
            datos_lgtbi[
                "_sentimiento_normalizado"
            ]
            .isin(negativos)
            .mean()
            * 100
        )

    else:

        porcentaje_negativo_lgtbi = 0

    diferencia = abs(
        porcentaje_negativo_inmigracion
        - porcentaje_negativo_lgtbi
    )

    # ========================================================
    # TEXTO INTERPRETATIVO AUTOMÁTICO
    # ========================================================

    if diferencia < 1:

        lectura = (
            "La proporción de sentimiento negativo es prácticamente "
            "idéntica en inmigración y LGTBI."
        )

    elif porcentaje_negativo_inmigracion > porcentaje_negativo_lgtbi:

        lectura = (
            "Los artículos sobre inmigración presentan 5,3 puntos "
            "porcentuales más de sentimiento negativo que los artículos sobre LGTBI."
        )

    else:

        lectura = (
            "La conversación sobre LGTBI presenta "
            f"{diferencia:.1f} puntos porcentuales más de "
            "sentimiento negativo que la conversación sobre inmigración."
        )

    lectura = lectura.replace(".", ",", 1) if diferencia >= 1 else lectura

    # ========================================================
    # GRÁFICO
    # ========================================================

    figura = crear_sentimiento_plataforma(
        datos_plataforma,
        plataforma,
        ejemplos_hover
    )

    figura.update_layout(
        height=390,
        margin={
            "l": 25,
            "r": 30,
            "t": 55,
            "b": 45
        }
    )

    # ========================================================
    # DISTRIBUCIÓN DE LA PÁGINA
    # ========================================================

    columna_ficha, columna_grafico = st.columns(
        [0.9, 3.1],
        gap="large"
    )

    if plataforma == "YouTube":

        detalle_muestra = """
            <div class="plataforma-cifra">
                49.287
            </div>

            <div class="plataforma-detalle">
                comentarios
                <br><br>
                <strong>365</strong> videos analizados
                <br><br>
                <strong>229</strong> sobre inmigración
                <br>
                <strong>136</strong> sobre cuestiones LGTBI
                <br>
                
            </div>
        """

    else:

        detalle_muestra = f"""
            <div class="plataforma-cifra">
                {cifra_texto}
            </div>

            <div class="plataforma-detalle">
                comentarios analizados
                <br><br>
                2015 · 2016 · 2019 · 2023
                <br><br>
                Inmigración y LGTBI
            </div>
        """

    with columna_ficha:

        st.html(
            f"""
            <div class="plataforma-ficha">

                <div class="plataforma-tipo">
                    {informacion[plataforma]["tipo"]}
                </div>

                <div class="plataforma-nombre">
                    {plataforma}
                </div>

                <div class="plataforma-descripcion">
                    {informacion[plataforma]["descripcion"]}
                </div>

                {detalle_muestra}

            </div>
            """
        )

    with columna_grafico:

        st.plotly_chart(
            figura,
            width="stretch",
            config={
                "displayModeBar": False,
                "scrollZoom": False,
                "responsive": True
            }
        )

        if plataforma == "Prensa":

            st.html(
                crear_recuadro_medios(
                    datos_plataforma,
                    columna="media_name",
                    n=20
                )
            )

        else:

            st.html(
                f"""
                <div style="
                    padding:13px 17px;
                    border-left:5px solid #526f69;
                    background:#fffdf7;
                    font-family:Arial,sans-serif;
                    font-size:13px;
                    line-height:1.4;
                    color:#56534d;
                ">
                    <strong style="color:#171713;">
                        Cómo se interpreta
                    </strong>
                    <br>
                    El modelo de sentimiento clasifica el fragmento
                    procesado como positivo, neutral o negativo.
                </div>
                """
            )

# ============================================================
# PÁGINA 10 — EVOLUCIÓN COMPARADA
# ============================================================

def mostrar_evolucion_rrss():

    rrss_master = cargar_rrss_master()
    prensa_nlp = cargar_prensa_nlp()

    prensa_nlp = prensa_nlp.copy()

    prensa_nlp["plataforma"] = "Prensa"

    prensa_nlp["sentimiento"] = (
        prensa_nlp["sentimiento"]
        .replace(
            {
                "Negativo": "NEG",
                "Neutral": "NEU",
                "Positivo": "POS"
            }
        )
    )

    tema = st.segmented_control(
        "Tema",
        options=[
            "Inmigración",
            "LGTBI"
        ],
        default="Inmigración",
        label_visibility="collapsed",
        key="tema_pagina_10"
    )

    datos_comparacion = pd.concat(
    [
        rrss_master,
        prensa_nlp[
            [
                "anio",
                "plataforma",
                "tema",
                "sentimiento"
            ]
        ]
    ],
    ignore_index=True
)

    figura = crear_evolucion_negatividad_rrss(
        datos_comparacion,
        tema
    )

    st.plotly_chart(
        figura,
        width="stretch",
        config={
            "displayModeBar": False,
            "scrollZoom": False,
            "responsive": True
        }
    )

    st.html(
        """
        <div class="nota-comparacion">
            <strong>Cómo leer el gráfico:</strong>
            se representa el porcentaje de textos
            clasificados como negativos, no el número absoluto.
            Esto permite comparar fuentes con tamaños de
            corpus diferentes. La polaridad negativa no equivale
            automáticamente a discurso de odio.
        </div>
        """
    )

# ============================================================
# PÁGINA 11 — CONCLUSIONES Y LIMITACIONES
# ============================================================

def mostrar_conclusiones_limitaciones():

    vista = st.segmented_control(
        "Conclusiones o limitaciones",
        options=[
            "Limitaciones",
            "Conclusiones"
            
        ],
        default="Limitaciones",
        label_visibility="collapsed",
        key="vista_pagina_11"
    )

    if vista == "Conclusiones":

        st.html(
            """
            <div class="resultados-grid">

                <div class="resultado-card">
                    <div class="resultado-estado">
                        Delitos de odio
                    </div>

                    <div class="resultado-titulo">
                        Los delitos de odio no siguen un patrón único
                    </div>

                    <div class="resultado-texto">
                        Los indicadores socioeconómicos  
                        no empeoran durante el periodo analizado, 
                        por lo que no se puede atribuir 
                        el aumento de estos delitos a un 
                        deterioro económico.
                    </div>
                </div>

                <div class="resultado-card">
                    <div class="resultado-estado">
                        Política
                    </div>

                    <div class="resultado-titulo">
                        El escenario político cambia de forma notable
                    </div>

                    <div class="resultado-texto">
                       El informe identifica un cambio estructural 
                       en el voto: la extrema derecha pasa de 
                       porcentajes residuales (0,26%) a una 
                       presencia significativa (12,76%).
                    </div>
                </div>

                <div class="resultado-card">
                    <div class="resultado-estado">
                        RRSS y prensa digital
                    </div>

                    <div class="resultado-titulo">
                        El discurso en redes sociales es más polarizado
                    </div>

                    <div class="resultado-texto">
                        No puede afirmarse que el discurso 
                        en RRSS provoque directamente un aumento de 
                        los delitos de odio, aunque sí se 
                        observan asociaciones y cambios 
                        simultáneos.
                    </div>
                </div>

            </div>

            """
        )

    else:

        st.html(
            """
            <div class="resultados-grid">

                <div class="limitacion-card">
                    <div class="limitacion-numero">
                        Limitación 01
                    </div>

                    <div class="resultado-titulo">
                        Las cifras oficiales no recogen
                        todos los hechos
                    </div>

                    <div class="resultado-texto">
                        Los datos representan delitos denunciados
                        o identificados por las autoridades.
                        Los casos no denunciados quedan fuera del
                        registro y pueden hacer que la dimensión
                        real del fenómeno sea mayor.
                    </div>
                </div>

                <div class="limitacion-card">
                    <div class="limitacion-numero">
                        Limitación 02
                    </div>

                    <div class="resultado-titulo">
                        Las fuentes digitales no son homogéneas
                    </div>

                    <div class="resultado-texto">
                        Los medios y las plataformas presentan
                        restricciones de acceso, sistemas de
                        moderación y diferentes niveles de
                        disponibilidad. Los comentarios
                        recopilados no representan la totalidad
                        de las conversaciones existentes.
                    </div>
                </div>

                <div class="limitacion-card">
                    <div class="limitacion-numero">
                        Limitación 03
                    </div>

                    <div class="resultado-titulo">
                        El NLP no comprende toda la intención
                    </div>

                    <div class="resultado-texto">
                        Los modelos permiten analizar grandes
                        cantidades de texto, pero pueden tener
                        dificultades con el contexto, la ironía,
                        el sarcasmo, la ambigüedad y la intención
                        real de quien escribe.
                    </div>
                </div>

            </div>

            """
        )


# ============================================================
# PÁGINA 12 — EPÍLOGO
# ============================================================

def mostrar_noticia_final():

    ruta_noticia = (
        Path(__file__).parent
        / "assets"
        / "noticia_infradenuncia.png"
    )

    if ruta_noticia.exists():

        margen_izquierdo, centro, margen_derecho = (
            st.columns(
                [0.18, 0.64, 0.18]
            )
        )

        with centro:

            st.image(
                str(ruta_noticia),
                width="stretch"
            )

        st.html(
            """
            <div class="nota-epilogo">
                <strong>Epílogo:</strong>
                esta publicación es posterior al periodo
                analizado y no forma parte del dataset.
                Se incorpora como reflexión final: las cifras
                oficiales describen los casos registrados,
                no necesariamente la dimensión completa del
                fenómeno.
            </div>
            """
        )

    else:

        st.warning(
            "No se encuentra "
            "assets/noticia_infradenuncia.png"
        )


# ============================================================
# PÁGINA 13 — CIERRE
# ============================================================

def mostrar_gracias():

    st.html(
        """
        <div class="cierre-final">

            <div class="cierre-preguntas">
                Gracias
            </div>


        </div>
        """
    )

# ============================================================
# NAVEGACIÓN
# ============================================================

def pagina_anterior():

    if st.session_state.pagina > 1:
        st.session_state.pagina -= 1


def pagina_siguiente():

    if st.session_state.pagina < TOTAL_PAGINAS:
        st.session_state.pagina += 1


def mostrar_navegacion():

    col_anterior, col_progreso, col_siguiente = (
        st.columns([1, 5, 1])
    )

    with col_anterior:

        st.button(
            "← Anterior",
            on_click=pagina_anterior,
            disabled=(
                st.session_state.pagina == 1
            )
        )

    with col_progreso:

        st.html(
            f"""
            <div style="
                text-align: center;
                font-family: Arial, sans-serif;
                font-size: 13px;
                letter-spacing: 2px;
                padding-top: 9px;
                color: #68635D;
            ">
                PÁGINA {st.session_state.pagina}
                DE {TOTAL_PAGINAS}
            </div>
            """
        )

    with col_siguiente:

        st.button(
            "Siguiente →",
            on_click=pagina_siguiente,
            disabled=(
                st.session_state.pagina
                == TOTAL_PAGINAS
            )
        )


# ============================================================
# PIE
# ============================================================

def mostrar_pie():

    st.html(
        """
        <div class="pie">
            Proyecto de análisis de delitos de odio en España ·
            2015–2023
        </div>
        """
    )


# ============================================================
# RENDERIZADO PRINCIPAL
# ============================================================

mostrar_cabecera()
mostrar_navegacion()

pagina_actual = st.session_state.pagina
datos_pagina = PAGINAS[pagina_actual]

if pagina_actual not in [1, 13]:

    mostrar_titular(
        datos_pagina
    )


if pagina_actual == 1:

    mostrar_portada()


elif pagina_actual == 2:

    mostrar_video()


elif pagina_actual == 3:

    mostrar_hipotesis()


elif pagina_actual == 4:

    tema_seleccionado = seleccionar_tema()

    mostrar_evolucion_delitos(
        tema_seleccionado
    )


elif pagina_actual == 5:

    mostrar_evolucion_electoral()


elif pagina_actual == 6:

    mostrar_factores()

elif pagina_actual == 7:

    mostrar_metodologia()


elif pagina_actual == 8:

    mostrar_temas_prensa()


elif pagina_actual == 9:

    mostrar_plataformas()


elif pagina_actual == 10:

    mostrar_evolucion_rrss()


elif pagina_actual == 11:

    mostrar_conclusiones_limitaciones()


elif pagina_actual == 12:

    mostrar_noticia_final()


elif pagina_actual == 13:

    mostrar_gracias()


else:

    mostrar_cuerpo(
        datos_pagina["cuerpo"]
    )

mostrar_pie()