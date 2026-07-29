# =============================================================================
# diccionario_terminos.py
# Diccionario de términos para análisis de xenofobia y LGTBIfobia en prensa
# Estructura en 4 niveles: neutro → marco conflictivo → hostilidad → violencia
# =============================================================================

DICCIONARIO = {

    "xenofobia": {

        # Nivel 1: Solo indica que se habla del tema
        "neutro": [
            "inmigración", "migración", "migrante", "inmigrantes",
            "inmigrante", "refugiado", "refugiados", "asilo",
            "frontera", "integración", "diversidad cultural",
            "multiculturalidad", "extranjero", "extranjeros"
        ],

        # Nivel 2: Encuadre negativo sin insultar directamente
        "marco_conflictivo": [
            "inmigración ilegal", "avalancha migratoria",
            "oleada migratoria", "presión migratoria",
            "efecto llamada", "saturación", "colapso de servicios",
            "crisis migratoria", "menas", "pateras", "cayuco",
            "cayucos", "repatriación", "expulsión masiva",
            "invasión de inmigrantes", "flujo migratorio descontrolado",
            "problema migratorio", "carga para el sistema"
        ],

        # Nivel 3: Discurso de odio más explícito
        "hostilidad_explicita": [
            "moro", "moros", "sudaca", "sudacas",
            "invasión migratoria", "sustitución demográfica",
            "gran sustitución", "great replacement",
            "fuera de españa", "quédate en tu país",
            "población autóctona en peligro",
            "remigración", "islamización",
            "nos invaden", "ilegales" 
        ],

        # Nivel 4: Noticias sobre actos concretos de violencia o discriminación
        "violencia_discriminacion": [
            "agresión racista", "delito de odio racial",
            "discriminación racial", "ataque xenófobo",
            "crimen de odio", "violencia racista",
            "incidente racista", "insulto racista",
            "denuncia por racismo", "expulsión discriminatoria"
        ]
    },

    "lgtbifobia": {

        # Nivel 1: Solo indica que se habla del tema
        "neutro": [
            "lgtbi", "lgbt", "lgtbiq", "gay", "lesbiana",
            "lesbianas", "bisexual", "trans", "transexual",
            "transexuales", "transgénero", "identidad de género",
            "orientación sexual", "matrimonio homosexual",
            "pareja homosexual", "derechos lgtbi"
        ],

        # Nivel 2: Encuadre negativo sin insultar directamente
        "marco_conflictivo": [
            "ideología de género", "adoctrinamiento",
            "agenda lgtbi", "transactivismo", "dictadura woke",
            "ingeniería social", "sexualización infantil",
            "pin parental", "ley trans", "borrado de las mujeres",
            "familia natural", "familia tradicional",
            "terapia de conversión", "lobby lgtb",
            "imposición de género", "modelo de familia alternativo"
        ],

        # Nivel 3: Discurso de odio más explícito
        "hostilidad_explicita": [
            "maricón", "maricones", "bollera", "bolleras",
            "travelo", "travelos", "perversión sexual",
            "anormalidad", "enfermedad mental",
            "desviación sexual", "depravación",
            "contra natura", "aberración"
        ],

        # Nivel 4: Noticias sobre actos concretos de violencia o discriminación
        "violencia_discriminacion": [
            "agresión homófoba", "agresión tránsfoba",
            "lgtbifobia", "delito de odio", "discriminación lgtbi",
            "paliza homófoba", "crimen de odio",
            "violencia homófoba", "violencia tránsfoba",
            "denuncia por homofobia", "transfobia"
        ]
    }
}

# =============================================================================
# FUNCIONES DE DETECCIÓN
# =============================================================================

def detectar_nivel_maximo(texto, categoria):
    """
    Devuelve el nivel MÁS GRAVE detectado en el texto.
    Útil para clasificar cada artículo con una sola etiqueta.

    Returns: "violencia_discriminacion" | "hostilidad_explicita" |
             "marco_conflictivo" | "neutro" | "sin_mencion"
    """
    if not texto or not isinstance(texto, str):
        return "sin_mencion"

    texto_lower = texto.lower()
    terminos_dict = DICCIONARIO[categoria]

    # Orden de prioridad: el más grave primero
    niveles_prioridad = [
        "violencia_discriminacion",
        "hostilidad_explicita",
        "marco_conflictivo",
        "neutro"
    ]

    for nivel in niveles_prioridad:
        for termino in terminos_dict[nivel]:
            if termino in texto_lower:
                return nivel

    return "sin_mencion"


def detectar_todos_niveles(texto, categoria):
    """
    Devuelve un diccionario con TODOS los niveles detectados
    y los términos concretos encontrados.
    Útil para auditar resultados y construir ejemplos en la memoria.

    Returns: dict con estructura {nivel: [terminos_encontrados]}
    """
    if not texto or not isinstance(texto, str):
        return {}

    texto_lower = texto.lower()
    resultado = {}

    for nivel, terminos in DICCIONARIO[categoria].items():
        encontrados = [t for t in terminos if t in texto_lower]
        if encontrados:
            resultado[nivel] = encontrados

    return resultado


def contar_terminos_por_nivel(texto, categoria):
    """
    Devuelve un diccionario con el COUNT de términos por nivel.
    Útil para análisis cuantitativo de intensidad.

    Returns: dict con estructura {nivel: count}
    """
    todos = detectar_todos_niveles(texto, categoria)
    return {nivel: len(terminos) for nivel, terminos in todos.items()}


def analizar_articulo_completo(texto):
    """
    Analiza un artículo para AMBAS categorías.
    Devuelve un dict listo para añadir como columnas al DataFrame.
    """
    return {
        # Nivel máximo por categoría
        "nivel_xenofobia":   detectar_nivel_maximo(texto, "xenofobia"),
        "nivel_lgtbifobia":  detectar_nivel_maximo(texto, "lgtbifobia"),

        # Términos concretos encontrados (para auditoría)
        "terminos_xenofobia":  str(detectar_todos_niveles(texto, "xenofobia")),
        "terminos_lgtbifobia": str(detectar_todos_niveles(texto, "lgtbifobia")),

        # Conteo por nivel (para análisis de intensidad)
        "count_xeno_neutro":      contar_terminos_por_nivel(texto, "xenofobia").get("neutro", 0),
        "count_xeno_conflicto":   contar_terminos_por_nivel(texto, "xenofobia").get("marco_conflictivo", 0),
        "count_xeno_hostilidad":  contar_terminos_por_nivel(texto, "xenofobia").get("hostilidad_explicita", 0),
        "count_xeno_violencia":   contar_terminos_por_nivel(texto, "xenofobia").get("violencia_discriminacion", 0),

        "count_lgtbi_neutro":     contar_terminos_por_nivel(texto, "lgtbifobia").get("neutro", 0),
        "count_lgtbi_conflicto":  contar_terminos_por_nivel(texto, "lgtbifobia").get("marco_conflictivo", 0),
        "count_lgtbi_hostilidad": contar_terminos_por_nivel(texto, "lgtbifobia").get("hostilidad_explicita", 0),
        "count_lgtbi_violencia":  contar_terminos_por_nivel(texto, "lgtbifobia").get("violencia_discriminacion", 0),
    }


# =============================================================================
# TEST RÁPIDO (ejecutar este archivo directamente para verificar)
# =============================================================================

if __name__ == "__main__":
    textos_prueba = [
        ("Neutro", "El gobierno debate nuevas políticas de migración e integración de refugiados."),
        ("Marco conflictivo", "La avalancha migratoria colapsa los servicios públicos por el efecto llamada."),
        ("Hostilidad", "La gran sustitución avanza mientras nos invaden ilegales por las pateras."),
        ("Violencia", "Detenido por un delito de odio racial tras una agresión racista en el metro."),
        ("LGTBI neutro", "El colectivo LGTBI celebra el orgullo con una manifestación en Madrid."),
        ("LGTBI conflictivo", "El pin parental busca frenar la ideología de género en las aulas."),
        ("LGTBI hostilidad", "Critican la perversión y aberración que supone la agenda lgtbi en los colegios."),
        ("LGTBI violencia", "Una agresión homófoba deja herido a un joven en Barcelona. Delito de odio investigado."),
    ]

    print("=" * 60)
    print("TEST DEL DICCIONARIO DE TÉRMINOS")
    print("=" * 60)

    for etiqueta, texto in textos_prueba:
        resultado = analizar_articulo_completo(texto)
        print(f"\n[{etiqueta}]")
        print(f"  Texto: {texto[:70]}...")
        print(f"  → Xenofobia:  {resultado['nivel_xenofobia']}")
        print(f"  → LGTBIfobia: {resultado['nivel_lgtbifobia']}")
        if resultado['terminos_xenofobia'] != '{}':
            print(f"  → Términos xeno:  {resultado['terminos_xenofobia']}")
        if resultado['terminos_lgtbifobia'] != '{}':
            print(f"  → Términos lgtbi: {resultado['terminos_lgtbifobia']}")
