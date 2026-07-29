from pathlib import Path

import pandas as pd
import streamlit as st
import html
import textwrap


# ============================================================
# RUTAS DEL PROYECTO
# ============================================================

# Carpeta donde se encuentran app.py, data.py y charts.py
BASE = Path(__file__).resolve().parent

# Carpeta que contendrá los datos necesarios para la aplicación
RUTA_DATOS = BASE / "datos"


RUTA_CONTEXTO_NACIONAL = (
    RUTA_DATOS
    / "contexto_nacional_anual.csv"
)

RUTA_DATOS_TERRITORIALES = (
    RUTA_DATOS
    / "datos_final.csv"
)

RUTA_BERTOPIC_INMIGRACION = (
    RUTA_DATOS
    / "bertopic_inmigracion_por_anio.csv"
)

RUTA_BERTOPIC_LGTBI = (
    RUTA_DATOS
    / "bertopic_lgtbi_por_anio.csv"
)

RUTA_RRSS_MASTER = (
    RUTA_DATOS
    / "rrss_master.csv"
)

RUTA_ELECCIONES = (
    RUTA_DATOS
    / "elecciones"
)

RUTA_PRENSA_NLP = (
    RUTA_DATOS
    / "prensa_nlp_comparacion.csv"
)

RUTA_PRENSA_NLP_FINAL = (
    RUTA_DATOS / "prensa_nlp_final.csv"
)

RUTA_EVOLUCION_BLOQUES = (
    RUTA_DATOS
    / "evolucion_bloques.csv"
)

RUTA_EJEMPLOS_HOVER = (
    RUTA_DATOS
    / "ejemplos_hover.csv"
)

# ============================================================
# CONTEXTO NACIONAL ANUAL
# ============================================================

@st.cache_data
def cargar_contexto_nacional():

    datos = pd.read_csv(
        RUTA_CONTEXTO_NACIONAL,
        encoding="utf-8-sig"
    )

    datos["anio"] = pd.to_numeric(
        datos["anio"],
        errors="coerce"
    )

    datos = datos[
        datos["anio"].isin(
            [
                2015,
                2016,
                2019,
                2023
            ]
        )
    ].copy()

    datos["anio"] = datos["anio"].astype(int)

    return datos.sort_values(
        "anio"
    )


# ============================================================
# DATOS TERRITORIALES
# ============================================================

@st.cache_data
def cargar_datos_territoriales():

    datos = pd.read_csv(
        RUTA_DATOS_TERRITORIALES,
        encoding="utf-8-sig"
    )

    datos["año"] = pd.to_numeric(
        datos["año"],
        errors="coerce"
    )

    datos["mes"] = pd.to_numeric(
        datos["mes"],
        errors="coerce"
    )

    datos = datos[
        datos["año"].isin(
            [
                2015,
                2016,
                2019,
                2023
            ]
        )
    ].copy()

    # Una observación anual por comunidad.
    # Para 2019 conserva noviembre, la última elección.
    datos = (
        datos
        .sort_values(
            [
                "comunidad",
                "año",
                "mes"
            ]
        )
        .drop_duplicates(
            subset=[
                "comunidad",
                "año"
            ],
            keep="last"
        )
        .copy()
    )

    columnas_numericas = [
        "delitos_por_100k",
        "pct_inmigrantes",
        "paro_medio",
        "Gini",
        "Desigualdad (S80/S20)",
        "pct_Extrema derecha",
        "pct_Derecha",
        "pct_Izquierda",
        "RACISMO/XENOFOBIA",
        "ORIENTACIÓN SEXUAL E IDENTIDAD DE GÉNERO",
        "poblacion"
    ]

    for columna in columnas_numericas:

        datos[columna] = pd.to_numeric(
            datos[columna],
            errors="coerce"
        )

    # Tasas territoriales comparables.
    datos["xenofobia_por_100k"] = (
        datos["RACISMO/XENOFOBIA"]
        / datos["poblacion"]
        * 100000
    )

    datos["lgtbi_por_100k"] = (
        datos[
            "ORIENTACIÓN SEXUAL E IDENTIDAD DE GÉNERO"
        ]
        / datos["poblacion"]
        * 100000
    )

    datos["año"] = datos["año"].astype(int)

    datos = datos.sort_values(
        [
            "año",
            "comunidad"
        ]
    ).reset_index(
        drop=True
    )

    return datos

@st.cache_data

def cargar_bertopic_prensa(tema):

    rutas = {
        "Inmigración": RUTA_BERTOPIC_INMIGRACION,
        "LGTBI": RUTA_BERTOPIC_LGTBI
    }

    datos = pd.read_csv(
        rutas[tema],
        encoding="utf-8-sig"
    )

    datos["year_dataset"] = pd.to_numeric(
        datos["year_dataset"],
        errors="coerce"
    )

    datos["porcentaje_corpus"] = pd.to_numeric(
        datos["porcentaje_corpus"],
        errors="coerce"
    )

    datos["documentos"] = pd.to_numeric(
        datos["documentos"],
        errors="coerce"
    )

    datos = datos[
        datos["year_dataset"].isin(
            [2015, 2016, 2019, 2023]
        )
    ].dropna(
        subset=[
            "year_dataset",
            "porcentaje_corpus",
            "etiqueta"
        ]
    ).copy()

    datos["year_dataset"] = (
        datos["year_dataset"].astype(int)
    )

    return datos.sort_values(
        ["year_dataset", "Topic"]
    )

@st.cache_data
def cargar_rrss_master():

    datos = pd.read_csv(
        RUTA_RRSS_MASTER,
        encoding="utf-8-sig",
        low_memory=False
    )

    datos["anio"] = pd.to_numeric(
        datos["anio"],
        errors="coerce"
    )

    datos = datos[
        datos["anio"].isin(
            [2015, 2016, 2019, 2023]
        )
    ].copy()

    datos = datos.dropna(
        subset=[
            "anio",
            "plataforma",
            "sentimiento"
        ]
    )

    datos["anio"] = datos["anio"].astype(int)

    datos["plataforma"] = (
        datos["plataforma"]
        .astype(str)
        .str.strip()
    )

    datos["sentimiento"] = (
        datos["sentimiento"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    return datos

@st.cache_data
def cargar_prensa_nlp():

    datos = pd.read_csv(
        RUTA_PRENSA_NLP,
        encoding="utf-8-sig",
        low_memory=False
    )

    datos["anio"] = pd.to_numeric(
        datos["anio"],
        errors="coerce"
    )

    datos = datos[
        datos["anio"].isin(
            [2015, 2016, 2019, 2023]
        )
    ].copy()

    datos = datos.dropna(
        subset=[
            "anio",
            "tema",
            "sentimiento"
        ]
    )

    datos["anio"] = datos["anio"].astype(int)

    datos["tema"] = (
        datos["tema"]
        .astype(str)
        .str.lower()
        .str.strip()
    )

    datos["tema"] = (
    datos["tema"]
    .replace(
        {
            "inmigración": "inmigracion",
            "inmigracion": "inmigracion",
            "lgtbi": "lgtbi"
        }
    )
)

    datos["sentimiento"] = (
        datos["sentimiento"]
        .astype(str)
        .str.strip()
    )

    return datos


def cargar_ejemplos_hover():

    datos = pd.read_csv(
        RUTA_EJEMPLOS_HOVER,
        encoding="utf-8-sig",
        low_memory=False
    )

    datos = datos.dropna(
        subset=[
            "plataforma",
            "tema",
            "sentimiento",
            "texto"
        ]
    ).copy()

    datos["plataforma"] = (
        datos["plataforma"]
        .astype(str)
        .str.strip()
    )

    datos["tema"] = (
        datos["tema"]
        .astype(str)
        .str.lower()
        .str.strip()
        .replace(
            {
                "inmigración": "inmigracion",
                "inmigracion": "inmigracion",
                "lgtbi": "lgtbi"
            }
        )
    )

    datos["sentimiento"] = (
        datos["sentimiento"]
        .astype(str)
        .str.strip()
        .replace(
            {
                "Negativo": "NEG",
                "Neutral": "NEU",
                "Positivo": "POS",
                "NEGATIVO": "NEG",
                "NEUTRAL": "NEU",
                "POSITIVO": "POS"
            }
        )
    )

    datos["texto"] = (
        datos["texto"]
        .astype(str)
        .apply(html.unescape)
        .str.replace(
            r"\s+",
            " ",
            regex=True
        )
        .str.strip()
    )

    return datos

# ============================================================
# EVOLUCIÓN ELECTORAL
# ============================================================

@st.cache_data
def cargar_evolucion_electoral():

    archivos = [
        ("elecciones_2015.csv", "2015", 1),
        ("elecciones_2016.csv", "2016", 2),
        ("elecciones_2019_04.csv", "Abr. 2019", 3),
        ("elecciones_2019_11.csv", "Nov. 2019", 4),
        ("elecciones_2023.csv", "2023", 5)
    ]

    resultados = []

    for nombre_archivo, etiqueta, orden in archivos:

        ruta = RUTA_ELECCIONES / nombre_archivo

        datos = pd.read_csv(
            ruta,
            encoding="utf-8-sig"
        )

        datos["votos"] = pd.to_numeric(
            datos["votos"],
            errors="coerce"
        ).fillna(0)

        datos["diputados"] = pd.to_numeric(
            datos["diputados"],
            errors="coerce"
        ).fillna(0)

        siglas = (
            datos["siglas"]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        total_votos = datos["votos"].sum()

        datos_vox = datos[
            siglas == "VOX"
        ].copy()

        votos_vox = datos_vox["votos"].sum()
        escanos_vox = datos_vox["diputados"].sum()

        porcentaje_vox = (
            votos_vox / total_votos * 100
            if total_votos > 0
            else float("nan")
        )

        resultados.append(
            {
                "eleccion": etiqueta,
                "orden": orden,
                "votos_totales": total_votos,
                "votos_vox": votos_vox,
                "pct_vox": porcentaje_vox,
                "escanos_vox": int(escanos_vox)
            }
        )

    evolucion = pd.DataFrame(resultados)

    return (
        evolucion
        .sort_values("orden")
        .reset_index(drop=True)
    )

@st.cache_data
def cargar_evolucion_bloques():

    datos = pd.read_csv(
        RUTA_EVOLUCION_BLOQUES,
        encoding="utf-8-sig"
    )

    datos["año"] = pd.to_numeric(
        datos["año"],
        errors="coerce"
    )

    datos["mes"] = pd.to_numeric(
        datos["mes"],
        errors="coerce"
    )

    columnas_bloques = [
        "Centro",
        "Derecha",
        "Extrema derecha",
        "Izquierda"
    ]

    for columna in columnas_bloques:
        datos[columna] = pd.to_numeric(
            datos[columna],
            errors="coerce"
        )

    datos = datos[
        datos["año"].isin(
            [2015, 2016, 2019, 2023]
        )
    ].copy()

    return datos.sort_values(
        ["año", "mes"]
    ).reset_index(drop=True)

@st.cache_data
def cargar_prensa_nlp_final():

    return pd.read_csv(
        RUTA_DATOS / "prensa_nlp_final.csv",
        low_memory=False
    )