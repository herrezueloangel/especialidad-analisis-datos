import streamlit as st


st.set_page_config(
    page_title="El Observatorio",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="collapsed"
)


st.markdown(
    """
    <style>
    .stApp {
        background-color: #F3EBDD;
        color: #181818;
    }

    header[data-testid="stHeader"] {
        background: transparent;
    }

    [data-testid="stSidebar"] {
        display: none;
    }

    .block-container {
        max-width: 1400px;
        padding-top: 1.2rem;
        padding-bottom: 2rem;
    }

    .cabecera {
        text-align: center;
        border-top: 3px solid #181818;
        border-bottom: 1px solid #181818;
        padding: 10px 0 12px 0;
        margin-bottom: 22px;
    }

    .nombre-periodico {
        font-family: Georgia, serif;
        font-size: 54px;
        font-weight: 700;
        letter-spacing: -2px;
        line-height: 1;
    }

    .edicion {
        margin-top: 8px;
        font-family: Arial, sans-serif;
        font-size: 13px;
        letter-spacing: 2px;
        text-transform: uppercase;
    }

    .seccion {
        color: #A51C30;
        font-family: Arial, sans-serif;
        font-size: 14px;
        font-weight: 700;
        letter-spacing: 2px;
        text-transform: uppercase;
        margin-top: 30px;
    }

    .titular {
        max-width: 1050px;
        font-family: Georgia, serif;
        font-size: 55px;
        font-weight: 700;
        line-height: 1.05;
        letter-spacing: -1.5px;
        margin-top: 8px;
    }

    .entradilla {
        max-width: 950px;
        font-family: Georgia, serif;
        font-size: 22px;
        line-height: 1.45;
        color: #48443E;
        margin-top: 18px;
        margin-bottom: 25px;
    }

    .linea-roja {
        width: 90px;
        height: 5px;
        background-color: #A51C30;
        margin-top: 16px;
        margin-bottom: 25px;
    }

    .pie {
        border-top: 1px solid #77716A;
        margin-top: 35px;
        padding-top: 10px;
        font-family: Arial, sans-serif;
        font-size: 12px;
        color: #68635D;
    }
    </style>
    """,
    unsafe_allow_html=True
)


st.markdown(
    """
    <div class="cabecera">
        <div class="nombre-periodico">
            EL OBSERVATORIO
        </div>

        <div class="edicion">
            Datos · Sociedad · Discurso público · Edición especial 2015–2023
        </div>
    </div>
    """,
    unsafe_allow_html=True
)


vista = st.segmented_control(
    "Selecciona una vista",
    options=[
        "General",
        "Inmigración",
        "LGTBI"
    ],
    default="General",
    label_visibility="collapsed"
)


if vista == "General":

    seccion = "Panorama general"

    titular = (
        "Delitos de odio y discurso público "
        "en la España de 2015–2023"
    )

    entradilla = (
        "Una investigación basada en datos territoriales, "
        "indicadores sociales, elecciones, prensa y redes sociales."
    )

elif vista == "Inmigración":

    seccion = "Inmigración"

    titular = (
        "La conversación sobre inmigración cambia "
        "entre territorios, medios y plataformas"
    )

    entradilla = (
        "Analizamos la evolución de la xenofobia registrada "
        "y del discurso sobre inmigración."
    )

else:

    seccion = "LGTBI"

    titular = (
        "Los derechos, la identidad y la hostilidad "
        "compiten por el espacio público"
    )

    entradilla = (
        "La evolución de los delitos y del discurso sobre "
        "el colectivo LGTBI no sigue una única trayectoria."
    )


st.markdown(
    f"""
    <div class="seccion">
        {seccion}
    </div>

    <div class="titular">
        {titular}
    </div>

    <div class="linea-roja"></div>

    <div class="entradilla">
        {entradilla}
    </div>
    """,
    unsafe_allow_html=True
)


st.info(
    "Esta es una primera prueba. "
    "Los gráficos y el vídeo se incorporarán después."
)


st.markdown(
    """
    <div class="pie">
        EL OBSERVATORIO · Proyecto de análisis de delitos de odio en España
    </div>
    """,
    unsafe_allow_html=True
)


# ============================================================
# NAVEGACIÓN PROVISIONAL
# ============================================================

TOTAL_PAGINAS = 10

if "pagina" not in st.session_state:
    st.session_state.pagina = 1


def pagina_anterior():
    if st.session_state.pagina > 1:
        st.session_state.pagina -= 1


def pagina_siguiente():
    if st.session_state.pagina < TOTAL_PAGINAS:
        st.session_state.pagina += 1


st.markdown("<br>", unsafe_allow_html=True)

col_anterior, col_progreso, col_siguiente = st.columns(
    [1, 5, 1]
)

with col_anterior:

    st.button(
        "← Anterior",
        on_click=pagina_anterior,
        disabled=st.session_state.pagina == 1
    )


with col_progreso:

    st.markdown(
        f"""
        <div style="
            text-align: center;
            font-family: Arial, sans-serif;
            font-size: 14px;
            letter-spacing: 2px;
            padding-top: 9px;
            color: #68635D;
        ">
            PÁGINA {st.session_state.pagina}
            DE {TOTAL_PAGINAS}
        </div>
        """,
        unsafe_allow_html=True
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