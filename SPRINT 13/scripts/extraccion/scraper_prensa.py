# =============================================================================
# scraper_prensa.py
# Scraper de prensa española para análisis de xenofobia y LGTBIfobia
# 
# LÓGICA CENTRAL: Para cada elección se definen 4 ventanas temporales
# que permiten medir si el lenguaje conflictivo se acelera conforme
# se acercan las elecciones.
#
# Ventanas:
#   - 15 días  → campaña oficial (15 días exactos por ley en España)
#   - 4 semanas → precampaña + campaña
#   - 3 meses  → calentamiento político previo
#   - año completo → línea base para calcular índice de aceleración
#
# Índice de aceleración = pct_conflictivo_15d / pct_conflictivo_anual
#   > 1.0 → más lenguaje conflictivo cerca de las elecciones
#   = 1.0 → sin cambio
#   < 1.0 → menos lenguaje conflictivo (poco probable)
# =============================================================================

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from diccionario_terminos import analizar_articulo_completo, DICCIONARIO

# =============================================================================
# CONFIGURACIÓN GENERAL
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/scraper.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

PAUSA_ENTRE_PETICIONES = 2
MAX_ARTICULOS_POR_BUSQUEDA = 10
TIMEOUT = 15

# =============================================================================
# ELECCIONES Y VENTANAS TEMPORALES
#
# Para cada elección definimos la fecha exacta y calculamos automáticamente
# las 4 ventanas hacia atrás. El año_merge conecta con datos_final.csv.
#
# IMPORTANTE: en 2019 hubo dos elecciones (28A y 10N). Ambas se mapean
# al mismo año_merge=2019 en datos_final.csv. Se guarda cada una por
# separado para poder comparar el efecto acumulativo.
# =============================================================================

ELECCIONES = {

    "20D_2015": {
        "fecha_eleccion": "2015-12-20",
        "descripcion":    "Elecciones Generales Diciembre 2015",
        "año_merge":      2015,   # conecta con datos_final.csv
    },

    "26J_2016": {
        "fecha_eleccion": "2016-06-26",
        "descripcion":    "Elecciones Generales Junio 2016",
        "año_merge":      2016,
    },

    "28A_2019": {
        "fecha_eleccion": "2019-04-28",
        "descripcion":    "Elecciones Generales Abril 2019",
        "año_merge":      2019,
    },

    "10N_2019": {
        "fecha_eleccion": "2019-11-10",
        "descripcion":    "Elecciones Generales Noviembre 2019",
        "año_merge":      2019,
    },

    "23J_2023": {
        "fecha_eleccion": "2023-07-23",
        "descripcion":    "Elecciones Generales Julio 2023",
        "año_merge":      2023,
    },
}


def calcular_ventanas(fecha_eleccion_str):
    """
    Dado el día de las elecciones, calcula las 4 ventanas hacia atrás.
    El día de las elecciones NO se incluye (buscamos lo previo).

    Returns dict con {nombre_ventana: (fecha_ini, fecha_fin)}
    """
    eleccion = datetime.strptime(fecha_eleccion_str, "%Y-%m-%d")
    fin = eleccion - timedelta(days=1)  # día anterior a las elecciones

    return {
        "15d": (
            (fin - timedelta(days=14)).strftime("%Y-%m-%d"),
            fin.strftime("%Y-%m-%d"),
            "Últimos 15 días (campaña oficial)"
        ),
        "4sem": (
            (fin - timedelta(days=27)).strftime("%Y-%m-%d"),
            fin.strftime("%Y-%m-%d"),
            "Últimas 4 semanas (precampaña + campaña)"
        ),
        "3m": (
            (fin - relativedelta(months=3)).strftime("%Y-%m-%d"),
            fin.strftime("%Y-%m-%d"),
            "Últimos 3 meses (calentamiento político)"
        ),
        "anual": (
            fin.replace(month=1, day=1).strftime("%Y-%m-%d"),
            fin.strftime("%Y-%m-%d"),
            "Año completo (línea base)"
        ),
    }


# =============================================================================
# TÉRMINOS DE BÚSQUEDA
# Representativos de cada categoría — el análisis fino lo hace el diccionario
# =============================================================================

TERMINOS_BUSQUEDA = {
    "xenofobia": [
        "inmigración", "migrantes", "refugiados",
        "menas", "pateras", "efecto llamada"
    ],
    "lgtbifobia": [
        "LGTBI", "ley trans", "ideología de género",
        "pin parental", "transexual"
    ]
}

# =============================================================================
# PERIÓDICOS
# Ideología anotada para poder comparar líneas editoriales en el análisis
# =============================================================================

PERIODICOS = {

    "elpais": {
        "nombre_display": "El País",
        "ideologia": "centro-izquierda",
        "url_busqueda": "https://elpais.com/buscador/?q={query}&d1={fecha_ini}&d2={fecha_fin}&s=crono",
        "selector_lista_articulos": "article",
        "selector_link_articulo": "a",
        "selector_titulo": "h1",
        "selector_cuerpo": "div.a_c",
        "selector_fecha": "time",
        "base_url": "https://elpais.com"
    },

    "elmundo": {
        "nombre_display": "El Mundo",
        "ideologia": "centro-derecha",
        "url_busqueda": "https://www.elmundo.es/buscar/noticias.html?q={query}&df={fecha_ini}&dt={fecha_fin}",
        "selector_lista_articulos": "article",
        "selector_link_articulo": "a",
        "selector_titulo": "h1",
        "selector_cuerpo": "div.ue-c-article__body",
        "selector_fecha": "time",
        "base_url": "https://www.elmundo.es"
    },

    "abc": {
        "nombre_display": "ABC",
        "ideologia": "derecha",
        "url_busqueda": "https://www.abc.es/buscar/?query={query}&df={fecha_ini}&dt={fecha_fin}",
        "selector_lista_articulos": "article",
        "selector_link_articulo": "a",
        "selector_titulo": "h1",
        "selector_cuerpo": "div.cuerpo-texto",
        "selector_fecha": "time",
        "base_url": "https://www.abc.es"
    },

    "lavanguardia": {
        "nombre_display": "La Vanguardia",
        "ideologia": "centro",
        "url_busqueda": "https://www.lavanguardia.com/buscador.html?q={query}&o=crono",
        "selector_lista_articulos": "article",
        "selector_link_articulo": "a",
        "selector_titulo": "h1",
        "selector_cuerpo": "div.article-modules",
        "selector_fecha": "time",
        "base_url": "https://www.lavanguardia.com"
    },

    "eldiario": {
        "nombre_display": "El Diario",
        "ideologia": "izquierda",
        "url_busqueda": "https://www.eldiario.es/buscar/?q={query}",
        "selector_lista_articulos": "article",
        "selector_link_articulo": "a",
        "selector_titulo": "h1",
        "selector_cuerpo": "div.article-body",
        "selector_fecha": "time",
        "base_url": "https://www.eldiario.es"
    },

    "okdiario": {
        "nombre_display": "OKDiario",
        "ideologia": "derecha-radical",
        "url_busqueda": "https://okdiario.com/?s={query}",
        "selector_lista_articulos": "article",
        "selector_link_articulo": "a",
        "selector_titulo": "h1",
        "selector_cuerpo": "div.entry-content",
        "selector_fecha": "time",
        "base_url": "https://okdiario.com"
    }
}

# =============================================================================
# FUNCIONES DE SCRAPING
# =============================================================================

def hacer_request(url, pausa=True):
    """Petición HTTP con manejo de errores y pausa educada."""
    try:
        if pausa:
            time.sleep(PAUSA_ENTRE_PETICIONES)
        response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        return response
    except requests.exceptions.HTTPError as e:
        log.warning(f"HTTP {e.response.status_code} en {url}")
        return None
    except requests.exceptions.ConnectionError:
        log.warning(f"Error de conexión en {url}")
        return None
    except requests.exceptions.Timeout:
        log.warning(f"Timeout en {url}")
        return None
    except Exception as e:
        log.warning(f"Error inesperado en {url}: {e}")
        return None


def extraer_articulo_completo(url, config):
    """Descarga y extrae título, cuerpo y fecha de un artículo."""
    response = hacer_request(url)
    if not response:
        return None

    try:
        soup = BeautifulSoup(response.text, "html.parser")

        titulo_tag = soup.select_one(config["selector_titulo"])
        titulo = titulo_tag.get_text(strip=True) if titulo_tag else ""

        cuerpo_tag = soup.select_one(config["selector_cuerpo"])
        if cuerpo_tag:
            for tag in cuerpo_tag(["script", "style", "aside", "figure"]):
                tag.decompose()
            cuerpo = cuerpo_tag.get_text(separator=" ", strip=True)
        else:
            cuerpo = ""

        fecha_tag = soup.select_one(config["selector_fecha"])
        if fecha_tag:
            fecha = fecha_tag.get("datetime", fecha_tag.get_text(strip=True))
        else:
            fecha = ""

        if not titulo and not cuerpo:
            return None

        return {
            "url":             url,
            "titulo":          titulo,
            "cuerpo":          cuerpo,
            "cuerpo_longitud": len(cuerpo),
            "fecha_publicacion": fecha,
        }

    except Exception as e:
        log.warning(f"Error extrayendo artículo {url}: {e}")
        return None


def scrape_ventana(periodico_key, config, termino, eleccion_key,
                   ventana_key, fecha_ini, fecha_fin, descripcion_ventana,
                   año_merge):
    """
    Scraping para UNA combinación de periódico + término + ventana temporal.
    Cada artículo queda etiquetado con su ventana para el análisis posterior.
    """
    url_busqueda = config["url_busqueda"].format(
        query=termino.replace(" ", "+"),
        fecha_ini=fecha_ini,
        fecha_fin=fecha_fin
    )

    log.info(
        f"  [{ventana_key}] {config['nombre_display']} | "
        f"'{termino}' | {fecha_ini} → {fecha_fin}"
    )

    response = hacer_request(url_busqueda)
    if not response:
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    articulos_html = soup.select(config["selector_lista_articulos"])

    if not articulos_html:
        log.info(f"    → Sin resultados")
        return []

    resultados = []
    procesados = 0

    for art_html in articulos_html:
        if procesados >= MAX_ARTICULOS_POR_BUSQUEDA:
            break

        link_tag = art_html.select_one(config["selector_link_articulo"])
        if not link_tag or not link_tag.get("href"):
            continue

        href = link_tag["href"]
        if href.startswith("/"):
            href = config["base_url"] + href
        elif not href.startswith("http"):
            continue

        datos = extraer_articulo_completo(href, config)
        if not datos:
            continue

        # Análisis del diccionario de términos
        texto_completo = f"{datos['titulo']} {datos['cuerpo']}"
        analisis = analizar_articulo_completo(texto_completo)

        registro = {
            # ── Identificación ────────────────────────────────────────
            "eleccion":            eleccion_key,       # "23J_2023"
            "año_merge":           año_merge,           # 2023 → conecta con datos_final
            "ventana":             ventana_key,         # "15d" | "4sem" | "3m" | "anual"
            "descripcion_ventana": descripcion_ventana,
            "fecha_ini_ventana":   fecha_ini,
            "fecha_fin_ventana":   fecha_fin,

            # ── Periódico ─────────────────────────────────────────────
            "periodico":           config["nombre_display"],
            "ideologia":           config["ideologia"],
            "termino_busqueda":    termino,
            "fecha_scraping":      datetime.now().isoformat(),

            # ── Contenido ─────────────────────────────────────────────
            **datos,

            # ── Análisis de términos (niveles + conteos) ───────────────
            **analisis,
        }

        resultados.append(registro)
        procesados += 1
        log.info(f"    ✓ {datos['titulo'][:65]}...")

    log.info(f"    → {len(resultados)} artículos")
    return resultados


# =============================================================================
# AGREGACIÓN: de artículos individuales a comunidad + año
# Esta tabla es la que se mergea con datos_final.csv
# =============================================================================

def agregar_por_ventana(df_articulos):
    """
    Agrega los artículos scrapeados a nivel eleccion + ventana + periodico.
    Calcula métricas de frecuencia y distribución de niveles.

    Esta tabla permite calcular el índice de aceleración:
        pct_conflictivo_15d / pct_conflictivo_anual
    """
    if df_articulos.empty:
        return pd.DataFrame()

    grupos = df_articulos.groupby(
        ["eleccion", "año_merge", "ventana", "periodico", "ideologia"]
    )

    resumen = grupos.agg(
        n_articulos              = ("url", "count"),

        # Xenofobia — distribución de niveles
        n_xeno_sin_mencion       = ("nivel_xenofobia", lambda x: (x == "sin_mencion").sum()),
        n_xeno_neutro            = ("nivel_xenofobia", lambda x: (x == "neutro").sum()),
        n_xeno_conflictivo       = ("nivel_xenofobia", lambda x: (x == "marco_conflictivo").sum()),
        n_xeno_hostilidad        = ("nivel_xenofobia", lambda x: (x == "hostilidad_explicita").sum()),
        n_xeno_violencia         = ("nivel_xenofobia", lambda x: (x == "violencia_discriminacion").sum()),

        # LGTBIfobia — distribución de niveles
        n_lgtbi_sin_mencion      = ("nivel_lgtbifobia", lambda x: (x == "sin_mencion").sum()),
        n_lgtbi_neutro           = ("nivel_lgtbifobia", lambda x: (x == "neutro").sum()),
        n_lgtbi_conflictivo      = ("nivel_lgtbifobia", lambda x: (x == "marco_conflictivo").sum()),
        n_lgtbi_hostilidad       = ("nivel_lgtbifobia", lambda x: (x == "hostilidad_explicita").sum()),
        n_lgtbi_violencia        = ("nivel_lgtbifobia", lambda x: (x == "violencia_discriminacion").sum()),

        # Conteos medios de términos por artículo
        media_terminos_xeno      = ("count_xeno_conflicto", "mean"),
        media_terminos_lgtbi     = ("count_lgtbi_conflicto", "mean"),

    ).reset_index()

    # Porcentajes sobre el total de artículos
    for cat in ["xeno", "lgtbi"]:
        for nivel in ["neutro", "conflictivo", "hostilidad", "violencia"]:
            col_n   = f"n_{cat}_{nivel}"
            col_pct = f"pct_{cat}_{nivel}"
            resumen[col_pct] = (resumen[col_n] / resumen["n_articulos"]).round(4)

    # Columna resumen: % de artículos con cualquier nivel > neutro
    resumen["pct_xeno_cualquier_conflicto"] = (
        (resumen["n_xeno_conflictivo"] +
         resumen["n_xeno_hostilidad"] +
         resumen["n_xeno_violencia"]) / resumen["n_articulos"]
    ).round(4)

    resumen["pct_lgtbi_cualquier_conflicto"] = (
        (resumen["n_lgtbi_conflictivo"] +
         resumen["n_lgtbi_hostilidad"] +
         resumen["n_lgtbi_violencia"]) / resumen["n_articulos"]
    ).round(4)

    return resumen


def calcular_indice_aceleracion(df_resumen):
    """
    Calcula el índice de aceleración para cada elección y periódico:
        indice = pct_conflictivo_ventana / pct_conflictivo_anual

    Un índice > 1 significa que el lenguaje conflictivo es más frecuente
    en esa ventana que como media del año → aceleración hacia las elecciones.
    """
    if df_resumen.empty:
        return pd.DataFrame()

    # Separar la ventana anual (línea base)
    base = df_resumen[df_resumen["ventana"] == "anual"][
        ["eleccion", "periodico",
         "pct_xeno_cualquier_conflicto",
         "pct_lgtbi_cualquier_conflicto"]
    ].rename(columns={
        "pct_xeno_cualquier_conflicto":  "base_xeno",
        "pct_lgtbi_cualquier_conflicto": "base_lgtbi"
    })

    df_con_base = df_resumen.merge(base, on=["eleccion", "periodico"], how="left")

    # Índice de aceleración (evitar división por cero)
    df_con_base["indice_acel_xeno"] = (
        df_con_base["pct_xeno_cualquier_conflicto"] /
        df_con_base["base_xeno"].replace(0, float("nan"))
    ).round(3)

    df_con_base["indice_acel_lgtbi"] = (
        df_con_base["pct_lgtbi_cualquier_conflicto"] /
        df_con_base["base_lgtbi"].replace(0, float("nan"))
    ).round(3)

    return df_con_base


# =============================================================================
# EJECUCIÓN PRINCIPAL
# =============================================================================

def ejecutar_scraping_completo(
    elecciones=None,
    periodicos=None,
    categorias=None,
    guardar_parciales=True
):
    """
    Scraping completo: para cada elección itera las 4 ventanas,
    todos los periódicos y todos los términos de búsqueda.
    """
    elecciones_a_usar  = elecciones  or list(ELECCIONES.keys())
    periodicos_a_usar  = periodicos  or list(PERIODICOS.keys())
    categorias_a_usar  = categorias  or list(TERMINOS_BUSQUEDA.keys())

    todos_articulos = []

    log.info("=" * 65)
    log.info("INICIO DEL SCRAPING CON VENTANAS TEMPORALES")
    log.info(f"Elecciones:  {elecciones_a_usar}")
    log.info(f"Periódicos:  {periodicos_a_usar}")
    log.info(f"Categorías:  {categorias_a_usar}")
    log.info("=" * 65)

    for eleccion_key, eleccion_config in ELECCIONES.items():
        if eleccion_key not in elecciones_a_usar:
            continue

        log.info(f"\n{'='*65}")
        log.info(f"ELECCIÓN: {eleccion_config['descripcion']}")
        log.info(f"{'='*65}")

        # Calcular las 4 ventanas para esta elección
        ventanas = calcular_ventanas(eleccion_config["fecha_eleccion"])

        for ventana_key, (fecha_ini, fecha_fin, desc_ventana) in ventanas.items():
            log.info(f"\n  VENTANA [{ventana_key}]: {fecha_ini} → {fecha_fin}")

            for categoria, terminos in TERMINOS_BUSQUEDA.items():
                if categoria not in categorias_a_usar:
                    continue

                for termino in terminos:
                    for periodico_key, config in PERIODICOS.items():
                        if periodico_key not in periodicos_a_usar:
                            continue

                        articulos = scrape_ventana(
                            periodico_key, config, termino,
                            eleccion_key, ventana_key,
                            fecha_ini, fecha_fin, desc_ventana,
                            eleccion_config["año_merge"]
                        )
                        todos_articulos.extend(articulos)

                        # Guardado parcial
                        if guardar_parciales and len(todos_articulos) % 100 == 0:
                            pd.DataFrame(todos_articulos).to_csv(
                                f"datos/raw/parcial_{len(todos_articulos)}.csv",
                                index=False
                            )
                            log.info(f"  💾 Guardado parcial: {len(todos_articulos)} artículos")

    # ── Guardado final de artículos individuales ──────────────────────────────
    df_articulos = pd.DataFrame(todos_articulos)
    if not df_articulos.empty:
        df_articulos = df_articulos.drop_duplicates(subset=["url", "ventana"])

    ruta_articulos = "datos/raw/articulos_prensa_raw.csv"
    df_articulos.to_csv(ruta_articulos, index=False)
    log.info(f"\n💾 Artículos individuales: {ruta_articulos} ({len(df_articulos)} filas)")

    # ── Agregación por ventana ────────────────────────────────────────────────
    df_resumen = agregar_por_ventana(df_articulos)
    ruta_resumen = "datos/clean/resumen_por_ventana.csv"
    df_resumen.to_csv(ruta_resumen, index=False)
    log.info(f"💾 Resumen por ventana:    {ruta_resumen} ({len(df_resumen)} filas)")

    # ── Índice de aceleración ────────────────────────────────────────────────
    df_aceleracion = calcular_indice_aceleracion(df_resumen)
    ruta_aceleracion = "datos/clean/indice_aceleracion.csv"
    df_aceleracion.to_csv(ruta_aceleracion, index=False)
    log.info(f"💾 Índice de aceleración:  {ruta_aceleracion} ({len(df_aceleracion)} filas)")

    # ── Resumen en pantalla ───────────────────────────────────────────────────
    log.info("\n" + "="*65)
    log.info("SCRAPING COMPLETADO")
    log.info(f"Total artículos únicos: {len(df_articulos)}")
    log.info("="*65)

    if not df_articulos.empty:
        print("\n📊 ARTÍCULOS POR ELECCIÓN Y VENTANA:")
        print(df_articulos.groupby(
            ["eleccion", "ventana"])["url"].count().unstack(fill_value=0).to_string()
        )
        print("\n📊 ARTÍCULOS POR PERIÓDICO:")
        print(df_articulos.groupby("periodico")["url"].count().to_string())

        if not df_aceleracion.empty:
            print("\n📈 ÍNDICE DE ACELERACIÓN (ventana 15d vs año completo):")
            vista = df_aceleracion[df_aceleracion["ventana"] == "15d"][
                ["eleccion", "periodico",
                 "indice_acel_xeno", "indice_acel_lgtbi"]
            ].sort_values("indice_acel_xeno", ascending=False)
            print(vista.to_string(index=False))

    return df_articulos, df_resumen, df_aceleracion


# =============================================================================
# MODO TEST
# =============================================================================

def test_scraper(periodico="elpais", eleccion="23J_2023", termino="inmigración"):
    """
    Prueba rápida: una sola elección, solo la ventana de 15 días,
    un periódico y un término. Verifica que el scraper funciona
    antes de lanzar el proceso completo.
    """
    log.info(f"🧪 TEST: {periodico} | {eleccion} | '{termino}' | ventana 15d")

    config         = PERIODICOS[periodico]
    eleccion_conf  = ELECCIONES[eleccion]
    ventanas       = calcular_ventanas(eleccion_conf["fecha_eleccion"])
    fecha_ini, fecha_fin, desc = ventanas["15d"]

    resultados = scrape_ventana(
        periodico, config, termino,
        eleccion, "15d", fecha_ini, fecha_fin, desc,
        eleccion_conf["año_merge"]
    )

    if resultados:
        df = pd.DataFrame(resultados)
        print(f"\n✅ Test exitoso — {len(df)} artículos encontrados")
        print(f"\nVentana: {fecha_ini} → {fecha_fin}")
        print(f"\nColumnas: {list(df.columns)}")
        print(f"\nPrimer artículo:")
        print(f"  Título:     {df.iloc[0]['titulo'][:70]}")
        print(f"  Fecha:      {df.iloc[0]['fecha_publicacion']}")
        print(f"  Longitud:   {df.iloc[0]['cuerpo_longitud']} chars")
        print(f"  Xenofobia:  {df.iloc[0]['nivel_xenofobia']}")
        print(f"  LGTBIfobia: {df.iloc[0]['nivel_lgtbifobia']}")
        df.to_csv("datos/raw/test_resultado.csv", index=False)
        print("\n💾 Guardado en datos/raw/test_resultado.csv")
    else:
        print("❌ Test sin resultados.")
        print("   Abre el periódico en el navegador, busca el término,")
        print("   haz F12 → Inspeccionar, y actualiza los selectores CSS")
        print(f"   en PERIODICOS['{periodico}']")

    return resultados


# =============================================================================
# VERIFICAR VENTANAS (utilidad para revisar fechas antes de lanzar)
# =============================================================================

def mostrar_ventanas():
    """Muestra todas las ventanas calculadas para verificar fechas."""
    print(f"\n{'='*65}")
    print("VENTANAS TEMPORALES POR ELECCIÓN")
    print(f"{'='*65}")
    for eleccion_key, conf in ELECCIONES.items():
        print(f"\n📅 {conf['descripcion']} ({eleccion_key})")
        print(f"   Día de elecciones: {conf['fecha_eleccion']}")
        print(f"   Año merge:         {conf['año_merge']}")
        ventanas = calcular_ventanas(conf["fecha_eleccion"])
        for vkey, (fi, ff, desc) in ventanas.items():
            print(f"   [{vkey:6s}] {fi} → {ff}  ({desc})")


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

if __name__ == "__main__":
    import sys

    os.makedirs("datos/raw",   exist_ok=True)
    os.makedirs("datos/clean", exist_ok=True)
    os.makedirs("logs",        exist_ok=True)

    if len(sys.argv) > 1 and sys.argv[1] == "ventanas":
        # Ver fechas antes de lanzar: python scraper_prensa.py ventanas
        mostrar_ventanas()

    elif len(sys.argv) > 1 and sys.argv[1] == "test":
        # Test rápido: python scraper_prensa.py test [periodico] [eleccion]
        periodico = sys.argv[2] if len(sys.argv) > 2 else "elpais"
        eleccion  = sys.argv[3] if len(sys.argv) > 3 else "23J_2023"
        test_scraper(periodico=periodico, eleccion=eleccion)

    else:
        # Scraping completo: python scraper_prensa.py
        # Para empezar limitado:
        # ejecutar_scraping_completo(
        #     elecciones=["23J_2023"],
        #     periodicos=["elpais", "elmundo"]
        # )
        ejecutar_scraping_completo()
