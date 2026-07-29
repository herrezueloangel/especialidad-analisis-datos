# =============================================================================
# diccionario_rrss.py
# Diccionario específico para comentarios de YouTube y otras redes sociales
# Categorías:
# neutro → marco conflictivo → hostilidad explícita → violencia/discriminación
# =============================================================================

DICCIONARIO_RRSS = {

    "xenofobia": {

        # Nivel 1: mención descriptiva o temática
        "neutro": [
            "inmigración",
            "inmigracion",
            "migración",
            "migracion",
            "migrante",
            "migrantes",
            "inmigrante",
            "inmigrantes",
            "refugiado",
            "refugiados",
            "asilo",
            "extranjero",
            "extranjeros",
            "frontera",
            "fronteras",
            "patera",
            "pateras",
            "cayuco",
            "cayucos",
            "musulmán",
            "musulman",
            "musulmana",
            "musulmanes",
            "islam",
            "marroquí",
            "marroqui",
            "marroquíes",
            "marroquies",
            "marroquís",
            "marroquis",
            "africano",
            "africanos"
        ],

        # Nivel 2: encuadre negativo o problemático
        "marco_conflictivo": [
            "inmigración ilegal",
            "inmigracion ilegal",
            "inmigrantes ilegales",
            "ilegal",
            "ilegales",
            "ilegalmente",
            "mena",
            "menas",
            "paguita",
            "paguitas",
            "efecto llamada",
            "fronteras abiertas",
            "presión migratoria",
            "presion migratoria",
            "crisis migratoria",
            "problema migratorio",
            "expulsión",
            "expulsion",
            "repatriación",
            "repatriacion",
            "amenaza migratoria",
            "carga para el sistema",
            "colapso de servicios",
            "saturación",
            "saturacion"
        ],

        # Nivel 3: hostilidad o rechazo explícito
        "hostilidad_explicita": [
            "moro",
            "moros",
            "sudaca",
            "sudacas",
            "invasión",
            "invasion",
            "invasión migratoria",
            "invasion migratoria",
            "nos invaden",
            "están invadiendo",
            "estan invadiendo",
            "sustitución demográfica",
            "sustitucion demografica",
            "gran sustitución",
            "gran sustitucion",
            "gran reemplazo",
            "reemplazo poblacional",
            "islamización",
            "islamizacion",
            "fuera de españa",
            "fuera de España",
            "que se vayan a su país",
            "que se vayan a su pais",
            "vuelve a tu país",
            "vuelve a tu pais",
            "quédate en tu país",
            "quedate en tu pais",
            "deportación",
            "deportacion",
            "deportarlos",
            "deportadlos",
            "expulsarlos",
            "expulsadlos",
            "remigración",
            "remigracion",
            "cerrar fronteras",
            "fronteras cerradas",
            "control fronterizo",
            "inmigración descontrolada",
            "inmigracion descontrolada",
            "entrada masiva",
            "llegada masiva",
            "delincuencia inmigrante",
            "delincuentes extranjeros",
            "primero los españoles",
            "ayudas a inmigrantes",
            "viven de ayudas",
            "viven de paguitas"
        ],

        # Nivel 4: referencia a violencia o discriminación
        "violencia_discriminacion": [
            "agresión racista",
            "agresion racista",
            "ataque xenófobo",
            "ataque xenofobo",
            "delito de odio racial",
            "violencia racista",
            "paliza racista",
            "crimen de odio racial",
            "discriminación racial",
            "discriminacion racial",
            "insulto racista",
            "amenaza racista",
            "denuncia por racismo",
            "fuera todos",
            "que se larguen",
            "que los echen",
            "hay que echarlos",
            "mandarlos de vuelta",
            "devuélvanlos",
            "devuelvanlos",
            "ni uno más",
            "ni uno mas",
            "españa para los españoles",
            "solo españoles",
            "fuera moros",
            "moros fuera",
            "fuera menas",
            "expulsión masiva",
            "expulsion masiva"
            
        ]
    },

    "lgtbifobia": {

        # Nivel 1: mención descriptiva o temática
        "neutro": [
            "lgtbi",
            "lgbt",
            "lgtbiq",
            "lgbti",
            "gay",
            "gays",
            "homosexual",
            "homosexuales",
            "homosexualidad",
            "lesbiana",
            "lesbianas",
            "bisexual",
            "bisexuales",
            "trans",
            "transexual",
            "transexuales",
            "transexualidad",
            "transgénero",
            "transgenero",
            "identidad de género",
            "identidad de genero",
            "orientación sexual",
            "orientacion sexual",
            "pareja gay",
            "pareja homosexual",
            "parejas homosexuales",
            "personas homosexuales",
            "hijo gay",
            "hijo homosexual",
            "orgullo gay",
            "orgullo lgtbi",
            "mujer trans",
            "hombre trans",
            "matrimonio homosexual",
            "matrimonio igualitario"
        ],

        # Nivel 2: encuadre negativo o problemático
        "marco_conflictivo": [
            "homosexualismo",
            "homosexualidad pecado",
            "homosexual pecado",
            "ser gay es pecado",
            "pecado homosexual",
            "adoctrinamiento",
            "adoctrinamiento lgtbi",
            "ideología de género",
            "ideologia de genero",
            "agenda lgtbi",
            "agenda lgbt",
            "agenda woke",
            "dictadura woke",
            "lobby lgtbi",
            "lobby lgbt",
            "lobby gay",
            "transactivismo",
            "imposición de género",
            "imposicion de genero",
            "sexualización infantil",
            "sexualizacion infantil",
            "pin parental",
            "ley trans",
            "borrado de las mujeres",
            "familia natural",
            "familia tradicional"
        ],

        # Nivel 3: hostilidad o insulto explícito
        "hostilidad_explicita": [
            "maricón",
            "maricon",
            "maricones",
            "bollera",
            "bolleras",
            "travelo",
            "travelos",
            "travesti",
            "travestis",
            "degenerado",
            "degenerados",
            "degenerada",
            "degeneradas",
            "aberración",
            "aberracion",
            "aberraciones",
            "contra natura",
            "depravación",
            "depravacion",
            "depravados",
            "perversión sexual",
            "perversion sexual",
            "desviación sexual",
            "desviacion sexual",
            "enfermedad mental",
            "homosexualidad es una enfermedad",
            "ser gay es una enfermedad",
            "transexualidad es una enfermedad",
            "los gays están enfermos",
            "los gays estan enfermos",
            "los homosexuales están enfermos",
            "los homosexuales estan enfermos",
            "propaganda lgtbi",
            "propaganda gay",
            "agenda de género",
            "agenda de genero",
            "adoctrinar niños",
            "adoctrinar a los niños",
            "imponer su ideología",
            "imponer su ideologia",
            "sexualización infantil",
            "sexualizacion infantil",
            "lobby gay",
            "lobby lgtbi"
        ],

        # Nivel 4: referencia a violencia o discriminación
        "violencia_discriminacion": [
            "agresión homófoba",
            "agresion homofoba",
            "agresión tránsfoba",
            "agresion transfoba",
            "paliza homófoba",
            "paliza homofoba",
            "paliza tránsfoba",
            "paliza transfoba",
            "delito de odio",
            "crimen de odio",
            "discriminación lgtbi",
            "discriminacion lgtbi",
            "violencia homófoba",
            "violencia homofoba",
            "violencia tránsfoba",
            "violencia transfoba",
            "amenaza homófoba",
            "amenaza homofoba",
            "amenaza tránsfoba",
            "amenaza transfoba",
            "denuncia por homofobia",
            "denuncia por transfobia",
            "homofobia",
            "transfobia",
            "lgtbifobia",
            "no es normal",
            "no son normales",
            "eso no es normal",
            "están enfermos",
            "estan enfermos",
            "son enfermos",
            "es antinatural",
            "son antinaturales",
            "desviados",
            "pervertidos",
            "viciosos",
            "asquerosos",
            "abominación",
            "abominacion"
        ]
    }
}


# =============================================================================
# FUNCIONES
# =============================================================================

def detectar_nivel_maximo_rrss(texto, categoria):
    """
    Devuelve el nivel más grave detectado.
    """

    if not isinstance(texto, str) or not texto.strip():
        return "sin_mencion"

    texto_lower = texto.lower()
    terminos = DICCIONARIO_RRSS[categoria]

    niveles_prioridad = [
        "violencia_discriminacion",
        "hostilidad_explicita",
        "marco_conflictivo",
        "neutro"
    ]

    for nivel in niveles_prioridad:
        for termino in terminos[nivel]:
            if termino.lower() in texto_lower:
                return nivel

    return "sin_mencion"


def detectar_todos_niveles_rrss(texto, categoria):
    """
    Devuelve los términos detectados en cada nivel.
    """

    if not isinstance(texto, str) or not texto.strip():
        return {}

    texto_lower = texto.lower()
    resultado = {}

    for nivel, terminos in DICCIONARIO_RRSS[categoria].items():

        encontrados = [
            termino
            for termino in terminos
            if termino.lower() in texto_lower
        ]

        if encontrados:
            resultado[nivel] = encontrados

    return resultado


def contar_terminos_por_nivel_rrss(texto, categoria):
    """
    Cuenta los términos detectados en cada nivel.
    """

    niveles = detectar_todos_niveles_rrss(texto, categoria)

    return {
        nivel: len(terminos)
        for nivel, terminos in niveles.items()
    }


def analizar_comentario_rrss(texto):
    """
    Analiza un comentario para xenofobia y LGTBIfobia.
    """

    conteo_xeno = contar_terminos_por_nivel_rrss(
        texto,
        "xenofobia"
    )

    conteo_lgtbi = contar_terminos_por_nivel_rrss(
        texto,
        "lgtbifobia"
    )

    return {
        "nivel_xenofobia_rrss": detectar_nivel_maximo_rrss(
            texto,
            "xenofobia"
        ),

        "nivel_lgtbifobia_rrss": detectar_nivel_maximo_rrss(
            texto,
            "lgtbifobia"
        ),

        "terminos_xenofobia_rrss": str(
            detectar_todos_niveles_rrss(
                texto,
                "xenofobia"
            )
        ),

        "terminos_lgtbifobia_rrss": str(
            detectar_todos_niveles_rrss(
                texto,
                "lgtbifobia"
            )
        ),

        "count_xeno_neutro_rrss": conteo_xeno.get(
            "neutro",
            0
        ),

        "count_xeno_conflicto_rrss": conteo_xeno.get(
            "marco_conflictivo",
            0
        ),

        "count_xeno_hostilidad_rrss": conteo_xeno.get(
            "hostilidad_explicita",
            0
        ),

        "count_xeno_violencia_rrss": conteo_xeno.get(
            "violencia_discriminacion",
            0
        ),

        "count_lgtbi_neutro_rrss": conteo_lgtbi.get(
            "neutro",
            0
        ),

        "count_lgtbi_conflicto_rrss": conteo_lgtbi.get(
            "marco_conflictivo",
            0
        ),

        "count_lgtbi_hostilidad_rrss": conteo_lgtbi.get(
            "hostilidad_explicita",
            0
        ),

        "count_lgtbi_violencia_rrss": conteo_lgtbi.get(
            "violencia_discriminacion",
            0
        )
    }


# =============================================================================
# TEST
# =============================================================================

if __name__ == "__main__":

    textos_prueba = [
        "Los inmigrantes llegan en pateras.",
        "Esto es una invasión y deberían deportarlos.",
        "Todos esos moros fuera de España.",
        "El orgullo gay se celebra en Madrid.",
        "La homosexualidad es una aberración.",
        "Una agresión homófoba dejó herido a un joven."
    ]

    for texto in textos_prueba:

        print("\nTexto:", texto)

        resultado = analizar_comentario_rrss(texto)

        print(
            "Xenofobia:",
            resultado["nivel_xenofobia_rrss"]
        )

        print(
            "LGTBIfobia:",
            resultado["nivel_lgtbifobia_rrss"]
        )