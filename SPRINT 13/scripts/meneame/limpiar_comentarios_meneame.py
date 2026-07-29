"""Limpieza reproducible del conjunto consolidado de comentarios de Menéame."""

from __future__ import annotations

import html
import re
import unicodedata
from pathlib import Path

import pandas as pd


BASE = Path(__file__).resolve().parent.parent
RUTA_PROCESADOS = BASE / "RRSS" / "MENEAME" / "PROCESADOS"

ARCHIVO_ENTRADA = RUTA_PROCESADOS / "meneame_comentarios.csv"
ARCHIVO_SALIDA = RUTA_PROCESADOS / "meneame_comentarios_limpios.csv"
ARCHIVO_CONTROL = RUTA_PROCESADOS / "meneame_comentarios_control_calidad.csv"


def normalizar_texto(valor: object) -> str | None:
    if pd.isna(valor):
        return None
    texto = html.unescape(str(valor))
    texto = unicodedata.normalize("NFC", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto or None


def quitar_marca_respuesta(texto: object) -> str | None:
    if pd.isna(texto):
        return None
    limpio = re.sub(r"^\s*#\d+\s*[,;:.-]?\s*", "", str(texto)).strip()
    return limpio or None


def main() -> None:
    df = pd.read_csv(
        ARCHIVO_ENTRADA,
        dtype={
            "id_comentario": str,
            "url_meneame": str,
            "tema": str,
            "usuario": str,
        },
    )

    filas_iniciales = len(df)
    df = df.drop_duplicates(
        subset=["url_meneame", "id_comentario"],
        keep="last",
    ).copy()
    duplicados_eliminados = filas_iniciales - len(df)

    for columna in ["tema", "url_meneame", "usuario"]:
        df[columna] = df[columna].astype("string").str.strip()

    df["texto_original"] = df["texto"]
    df["texto"] = df["texto"].apply(normalizar_texto)

    textos_vacios = int(df["texto"].isna().sum())
    df = df.loc[df["texto"].notna()].copy()

    df["texto_analisis"] = df["texto"].apply(quitar_marca_respuesta)
    textos_solo_referencia = int(df["texto_analisis"].isna().sum())
    df = df.loc[df["texto_analisis"].notna()].copy()
    df["texto_minusculas"] = df["texto_analisis"].str.lower()
    df["es_respuesta"] = df["responde_a_numero"].notna()

    df["fecha_comentario_utc"] = pd.to_datetime(
        df["fecha_comentario_utc"],
        utc=True,
        errors="coerce",
    )
    df["fecha"] = df["fecha_comentario_utc"].dt.date
    df["anio"] = df["fecha_comentario_utc"].dt.year.astype("Int64")
    df["mes"] = df["fecha_comentario_utc"].dt.month.astype("Int64")

    columnas_enteras = [
        "id_noticia_meneame",
        "numero_comentario",
        "responde_a_numero",
        "votos",
        "karma",
    ]
    for columna in columnas_enteras:
        df[columna] = pd.to_numeric(df[columna], errors="coerce").astype("Int64")

    df["numero_caracteres"] = df["texto_analisis"].str.len().astype("Int64")
    df["numero_palabras"] = (
        df["texto_analisis"]
        .str.findall(r"\b\w+\b")
        .str.len()
        .astype("Int64")
    )
    df["tema_inmigracion"] = df["tema"].str.contains(
        "inmigracion", regex=False, na=False
    )
    df["tema_lgtbi"] = df["tema"].str.contains(
        "lgtbi", regex=False, na=False
    )

    orden = [
        "tema",
        "tema_inmigracion",
        "tema_lgtbi",
        "url_meneame",
        "id_noticia_meneame",
        "id_comentario",
        "numero_comentario",
        "responde_a_numero",
        "es_respuesta",
        "usuario",
        "fecha_comentario_utc",
        "fecha",
        "anio",
        "mes",
        "votos",
        "karma",
        "numero_caracteres",
        "numero_palabras",
        "texto_original",
        "texto",
        "texto_analisis",
        "texto_minusculas",
    ]
    df = df[orden].sort_values(
        ["fecha_comentario_utc", "url_meneame", "numero_comentario"],
        na_position="last",
    ).reset_index(drop=True)

    df.to_csv(ARCHIVO_SALIDA, index=False, encoding="utf-8-sig")

    control = pd.DataFrame(
        [
            {"indicador": "filas_entrada", "valor": filas_iniciales},
            {"indicador": "duplicados_eliminados", "valor": duplicados_eliminados},
            {"indicador": "textos_vacios_eliminados", "valor": textos_vacios},
            {"indicador": "textos_solo_referencia_eliminados", "valor": textos_solo_referencia},
            {"indicador": "filas_salida", "valor": len(df)},
            {"indicador": "comentarios_inmigracion", "valor": int(df["tema_inmigracion"].sum())},
            {"indicador": "comentarios_lgtbi", "valor": int(df["tema_lgtbi"].sum())},
            {"indicador": "respuestas", "valor": int(df["es_respuesta"].sum())},
            {"indicador": "fechas_invalidas", "valor": int(df["fecha_comentario_utc"].isna().sum())},
        ]
    )
    control.to_csv(ARCHIVO_CONTROL, index=False, encoding="utf-8-sig")

    print(f"Filas de entrada: {filas_iniciales:,}")
    print(f"Duplicados eliminados: {duplicados_eliminados:,}")
    print(f"Textos vacíos eliminados: {textos_vacios:,}")
    print(f"Textos solo con referencia eliminados: {textos_solo_referencia:,}")
    print(f"Filas limpias: {len(df):,}")
    print(f"Archivo generado: {ARCHIVO_SALIDA}")


if __name__ == "__main__":
    main()
