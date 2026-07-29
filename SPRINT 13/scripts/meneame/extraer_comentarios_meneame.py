"""Descarga reanudable de comentarios de Menéame para inmigración y LGTBI."""

from __future__ import annotations

import hashlib
import html
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE = Path(__file__).resolve().parent.parent
RUTA_RAW = BASE / "RRSS" / "MENEAME" / "RAW"
RUTA_PROCESADOS = BASE / "RRSS" / "MENEAME" / "PROCESADOS"
RUTA_POR_PUBLICACION = RUTA_RAW / "COMENTARIOS_POR_PUBLICACION"

ARCHIVO_INMIGRACION = RUTA_RAW / "busqueda_noticias_meneame.csv"
ARCHIVO_LGTBI = RUTA_RAW / "busqueda_noticias_meneame_lgtbi.csv"
ARCHIVO_CATALOGO = RUTA_PROCESADOS / "meneame_publicaciones.csv"
ARCHIVO_PROGRESO = RUTA_RAW / "progreso_comentarios_meneame.csv"
ARCHIVO_COMENTARIOS = RUTA_PROCESADOS / "meneame_comentarios.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/126.0 Safari/537.36"
    )
}

COLUMNAS_COMENTARIOS = [
    "tema",
    "url_meneame",
    "id_noticia_meneame",
    "id_comentario",
    "numero_comentario",
    "responde_a_numero",
    "usuario",
    "fecha_comentario_utc",
    "votos",
    "karma",
    "texto",
]

COLUMNAS_PROGRESO = [
    "url_meneame",
    "tema",
    "estado",
    "id_noticia_meneame",
    "numero_comentarios",
    "intentos",
    "ultimo_error",
    "actualizado_utc",
]


def es_verdadero(serie: pd.Series) -> pd.Series:
    return serie.astype(str).str.strip().str.lower().eq("true")


def unir_valores(valores: pd.Series) -> str:
    limpios = {
        str(valor).strip()
        for valor in valores.dropna()
        if str(valor).strip()
    }
    return "|".join(sorted(limpios))


def cargar_publicaciones() -> pd.DataFrame:
    partes = []
    for tema, archivo in (
        ("inmigracion", ARCHIVO_INMIGRACION),
        ("lgtbi", ARCHIVO_LGTBI),
    ):
        df = pd.read_csv(archivo, dtype={"id_prensa": str})
        df = df.loc[
            es_verdadero(df["encontrada_meneame"])
            & df["url_meneame"].notna()
        ].copy()
        df["tema"] = tema
        partes.append(df)

    publicaciones = pd.concat(partes, ignore_index=True)
    catalogo = (
        publicaciones.groupby("url_meneame", as_index=False)
        .agg(
            tema=("tema", unir_valores),
            id_prensa=("id_prensa", unir_valores),
            titulo_prensa=("titulo_prensa", unir_valores),
            url_original=("url_original", unir_valores),
        )
        .sort_values("url_meneame")
        .reset_index(drop=True)
    )
    return catalogo


def cargar_progreso() -> dict[str, dict]:
    if not ARCHIVO_PROGRESO.exists() or ARCHIVO_PROGRESO.stat().st_size == 0:
        return {}
    progreso = pd.read_csv(ARCHIVO_PROGRESO, dtype=str).fillna("")
    return {
        fila["url_meneame"]: fila
        for fila in progreso.to_dict("records")
        if fila.get("url_meneame")
    }


def guardar_progreso(progreso: dict[str, dict]) -> None:
    filas = list(progreso.values())
    df = pd.DataFrame(filas, columns=COLUMNAS_PROGRESO)
    temporal = ARCHIVO_PROGRESO.with_suffix(".tmp.csv")
    df.to_csv(temporal, index=False, encoding="utf-8-sig")
    temporal.replace(ARCHIVO_PROGRESO)


def extraer_id_noticia(sesion: requests.Session, url_meneame: str) -> int:
    respuesta = sesion.get(url_meneame, timeout=30)
    respuesta.raise_for_status()
    soup = BeautifulSoup(respuesta.text, "html.parser")
    enlace = soup.select_one('a[href^="/comments_rss?id="]')
    href = enlace.get("href", "") if enlace else ""
    coincidencia = re.search(r"[?&]id=(\d+)", href)
    if not coincidencia:
        coincidencia = re.search(r"comments_rss\?id=(\d+)", respuesta.text)
    if not coincidencia:
        raise ValueError("No se encontró el id de la publicación")
    return int(coincidencia.group(1))


def limpiar_texto(valor: object) -> str:
    texto = html.unescape(str(valor or ""))
    if "<" not in texto and ">" not in texto:
        return texto.strip()
    return BeautifulSoup(texto, "html.parser").get_text(" ", strip=True)


def numero_respuesta(texto: str) -> int | None:
    coincidencia = re.match(r"\s*#(\d+)\b", texto)
    return int(coincidencia.group(1)) if coincidencia else None


def descargar_comentarios(
    sesion: requests.Session,
    url_meneame: str,
    tema: str,
) -> tuple[int, pd.DataFrame]:
    id_noticia = extraer_id_noticia(sesion, url_meneame)
    url_api = f"https://www.meneame.net/api/list.php?id={id_noticia}"
    ultimo_error = None
    datos = None
    for intento in range(1, 6):
        try:
            respuesta = sesion.get(url_api, timeout=30)
            respuesta.raise_for_status()
            if not respuesta.text.strip():
                # Menéame responde 200 con cuerpo vacío cuando la
                # publicación no tiene ningún comentario.
                datos = {"objects": []}
            else:
                datos = respuesta.json()
            break
        except (requests.RequestException, ValueError) as error:
            ultimo_error = error
            if intento == 5:
                raise
            time.sleep(2 ** (intento - 1))

    if datos is None:
        raise ValueError(f"No se pudo leer la API: {ultimo_error}")
    objetos = datos.get("objects", [])
    if not isinstance(objetos, list):
        raise ValueError("La API no devolvió una lista de comentarios")

    filas = []
    for objeto in objetos:
        texto = limpiar_texto(objeto.get("content"))
        fecha = pd.to_datetime(
            pd.to_numeric(objeto.get("date"), errors="coerce"),
            unit="s",
            utc=True,
            errors="coerce",
        )
        filas.append(
            {
                "tema": tema,
                "url_meneame": url_meneame,
                "id_noticia_meneame": id_noticia,
                "id_comentario": objeto.get("id"),
                "numero_comentario": objeto.get("order"),
                "responde_a_numero": numero_respuesta(texto),
                "usuario": objeto.get("user"),
                "fecha_comentario_utc": (
                    fecha.isoformat() if not pd.isna(fecha) else None
                ),
                "votos": pd.to_numeric(objeto.get("votes"), errors="coerce"),
                "karma": pd.to_numeric(objeto.get("karma"), errors="coerce"),
                "texto": texto,
            }
        )
    return id_noticia, pd.DataFrame(filas, columns=COLUMNAS_COMENTARIOS)


def archivo_publicacion(url_meneame: str) -> Path:
    nombre = hashlib.sha256(url_meneame.encode("utf-8")).hexdigest()
    return RUTA_POR_PUBLICACION / f"{nombre}.csv"


def guardar_publicacion(url_meneame: str, comentarios: pd.DataFrame) -> None:
    destino = archivo_publicacion(url_meneame)
    temporal = destino.with_suffix(".tmp.csv")
    comentarios.to_csv(temporal, index=False, encoding="utf-8-sig")
    temporal.replace(destino)


def consolidar_comentarios() -> pd.DataFrame:
    partes = []
    for archivo in sorted(RUTA_POR_PUBLICACION.glob("*.csv")):
        try:
            parte = pd.read_csv(archivo, dtype={"id_comentario": str})
        except pd.errors.EmptyDataError:
            continue
        if not parte.empty:
            partes.append(parte)

    if partes:
        comentarios = pd.concat(partes, ignore_index=True)
        comentarios = comentarios.drop_duplicates(
            subset=["url_meneame", "id_comentario"],
            keep="last",
        )
    else:
        comentarios = pd.DataFrame(columns=COLUMNAS_COMENTARIOS)

    comentarios.to_csv(
        ARCHIVO_COMENTARIOS,
        index=False,
        encoding="utf-8-sig",
    )
    return comentarios


def main() -> None:
    RUTA_PROCESADOS.mkdir(parents=True, exist_ok=True)
    RUTA_POR_PUBLICACION.mkdir(parents=True, exist_ok=True)

    catalogo = cargar_publicaciones()
    catalogo.to_csv(ARCHIVO_CATALOGO, index=False, encoding="utf-8-sig")
    progreso = cargar_progreso()
    estados_finales = {"ok", "no_disponible"}
    completadas = {
        url
        for url, fila in progreso.items()
        if fila.get("estado") in estados_finales
    }

    pendientes = catalogo.loc[
        ~catalogo["url_meneame"].isin(completadas)
    ]
    print(f"Publicaciones únicas: {len(catalogo):,}")
    print(f"Ya completadas: {len(completadas):,}")
    print(f"Pendientes: {len(pendientes):,}")

    sesion = requests.Session()
    sesion.headers.update(HEADERS)
    reintentos = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=True,
    )
    adaptador = HTTPAdapter(max_retries=reintentos)
    sesion.mount("https://", adaptador)

    for numero, fila in enumerate(pendientes.itertuples(index=False), start=1):
        anterior = progreso.get(fila.url_meneame, {})
        intentos = int(anterior.get("intentos") or 0) + 1
        try:
            id_noticia, comentarios = descargar_comentarios(
                sesion,
                fila.url_meneame,
                fila.tema,
            )
            guardar_publicacion(fila.url_meneame, comentarios)
            progreso[fila.url_meneame] = {
                "url_meneame": fila.url_meneame,
                "tema": fila.tema,
                "estado": "ok",
                "id_noticia_meneame": id_noticia,
                "numero_comentarios": len(comentarios),
                "intentos": intentos,
                "ultimo_error": "",
                "actualizado_utc": datetime.now(timezone.utc).isoformat(),
            }
        except requests.HTTPError as error:
            codigo = error.response.status_code if error.response is not None else None
            estado = "no_disponible" if codigo == 404 else "error"
            progreso[fila.url_meneame] = {
                "url_meneame": fila.url_meneame,
                "tema": fila.tema,
                "estado": estado,
                "id_noticia_meneame": anterior.get("id_noticia_meneame", ""),
                "numero_comentarios": anterior.get("numero_comentarios", ""),
                "intentos": intentos,
                "ultimo_error": str(error),
                "actualizado_utc": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as error:
            progreso[fila.url_meneame] = {
                "url_meneame": fila.url_meneame,
                "tema": fila.tema,
                "estado": "error",
                "id_noticia_meneame": anterior.get("id_noticia_meneame", ""),
                "numero_comentarios": anterior.get("numero_comentarios", ""),
                "intentos": intentos,
                "ultimo_error": str(error),
                "actualizado_utc": datetime.now(timezone.utc).isoformat(),
            }

        guardar_progreso(progreso)
        estado = progreso[fila.url_meneame]["estado"]
        cantidad = progreso[fila.url_meneame]["numero_comentarios"]
        print(
            f"{numero:,}/{len(pendientes):,} | {estado} | "
            f"comentarios: {cantidad} | {fila.url_meneame}"
        )
        time.sleep(0.5)

    comentarios = consolidar_comentarios()
    completadas_final = sum(
        fila.get("estado") == "ok" for fila in progreso.values()
    )
    errores_finales = sum(
        fila.get("estado") == "error" for fila in progreso.values()
    )
    no_disponibles_finales = sum(
        fila.get("estado") == "no_disponible" for fila in progreso.values()
    )
    print(f"Publicaciones completadas: {completadas_final:,}")
    print(f"Publicaciones con error: {errores_finales:,}")
    print(f"Publicaciones no disponibles (404): {no_disponibles_finales:,}")
    print(f"Comentarios únicos guardados: {len(comentarios):,}")


if __name__ == "__main__":
    main()
