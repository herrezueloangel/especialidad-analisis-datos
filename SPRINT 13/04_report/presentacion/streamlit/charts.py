import numpy as np
import pandas as pd
import plotly.graph_objects as go
import html
import textwrap


# ============================================================
# COLORES
# ============================================================

COLOR_TINTA = "#181818"
COLOR_ROJO = "#A51C30"
COLOR_PAPEL = "#F3EBDD"
COLOR_GRAFICO = "#fffdf7"
COLOR_GRIS = "#6B6863"
COLOR_NARANJA = "#C65D3B"
COLOR_VIOLETA = "#725A9C"
COLOR_VOX = "#4F9D2F"


# ============================================================
# CONFIGURACIÓN TEMÁTICA
# ============================================================

CONFIGURACION_TEMAS = {
    "General": {
        "columna": "delitos_por_100k",
        "nombre": "Total de delitos de odio",
        "color": COLOR_ROJO
    },

    "Inmigración": {
        "columna": "xenofobia_por_100k",
        "nombre": "Racismo y xenofobia",
        "color": COLOR_NARANJA
    },

    "LGTBI": {
        "columna": "lgtbi_por_100k",
        "nombre": (
            "Orientación sexual e identidad de género"
        ),
        "color": COLOR_VIOLETA
    }
}

CONFIGURACION_FACTORES = {
    "Población inmigrante": {
        "columna": "pct_inmigrantes",
        "etiqueta": "% de población inmigrante",
        "porcentaje": True
    },

    "Índice de Gini": {
        "columna": "Gini",
        "etiqueta": "Índice de Gini",
        "porcentaje": False
    },

    "Desigualdad S80/S20": {
        "columna": "Desigualdad (S80/S20)",
        "etiqueta": "Desigualdad (S80/S20)",
        "porcentaje": False
    },

    "Voto extrema derecha": {
        "columna": "pct_Extrema derecha",
        "etiqueta": "% de voto a la extrema derecha",
        "porcentaje": True
    }
}


# ============================================================
# GRÁFICO TEMPORAL — PÁGINA 4
# ============================================================

def crear_evolucion_delitos(
    datos,
    vista
):

    if vista == "Por ámbito":

        return crear_comparacion_ambitos(
            datos
        )

    configuracion = CONFIGURACION_TEMAS[
        vista
    ]
    columna = configuracion["columna"]
    color = configuracion["color"]

    figura = go.Figure()

    figura.add_trace(
        go.Scatter(
            x=datos["anio"],
            y=datos[columna],
            mode="lines+markers",
            line={
                "color": color,
                "width": 4
            },
            marker={
                "size": 13,
                "color": color,
                "line": {
                    "color": COLOR_PAPEL,
                    "width": 2
                }
            },
            hovertemplate=(
                "<b>%{x}</b><br>"
                "%{y:.2f} delitos por 100.000 habitantes"
                "<extra></extra>"
            )
        )
    )

    for _, fila in datos.iterrows():

        figura.add_annotation(
            x=fila["anio"],
            y=fila[columna],
            text=f"{fila[columna]:.2f}",
            showarrow=False,
            yshift=22,
            font={
                "family": "Arial",
                "size": 14,
                "color": COLOR_TINTA
            }
        )

    figura.update_layout(
        height=355,
        title_text="",
        margin={
            "l": 35,
            "r": 20,
            "t": 12,
            "b": 25
        },
        paper_bgcolor=COLOR_GRAFICO,
        plot_bgcolor=COLOR_GRAFICO,
        font={
            "family": "Arial",
            "color": COLOR_TINTA
        },
        showlegend=False,
        hoverlabel={
            "bgcolor": COLOR_TINTA,
            "font_color": COLOR_PAPEL,
            "font_family": "Arial"
        }
    )

    figura.update_xaxes(
        title="",
        tickmode="array",
        tickvals=[
            2015,
            2016,
            2019,
            2023
        ],
        ticktext=[
            "2015",
            "2016",
            "2019",
            "2023"
        ],
        showgrid=False,
        showline=True,
        linewidth=1,
        linecolor=COLOR_TINTA,
        ticks="outside",
        tickfont={
            "size": 14
        }
    )

    figura.update_yaxes(
        title="Delitos por 100.000 habitantes",
        showgrid=True,
        gridcolor="rgba(24, 24, 24, 0.12)",
        zeroline=False,
        showline=False,
        tickfont={
            "size": 13
        },
        title_font={
            "size": 13
        }
    )

    return figura

def crear_comparacion_ambitos(
    datos
):

    datos = datos.sort_values(
        "anio"
    ).copy()

    fila_2015 = datos[
        datos["anio"] == 2015
    ].iloc[0]

    fila_2023 = datos[
        datos["anio"] == 2023
    ].iloc[0]

    ambitos = {
        "Racismo/xenofobia":
            "RACISMO/XENOFOBIA",

        "Orientación sexual/LGTBI":
            "ORIENTACIÓN SEXUAL E IDENTIDAD DE GÉNERO",

        "Ideología":
            "IDEOLOGIA",

        "Sexo/género":
            "DISCRIMINACIÓN POR RAZÓN DE SEXO/GÉNERO",

        "Creencias religiosas":
            "CREENCIAS O PRÁCTICAS RELIGIOSAS",

        "Disfobia":
            "DISFOBIA"
    }

    filas = []

    for etiqueta, columna in ambitos.items():

        casos_2015 = float(
            fila_2015[columna]
        )

        casos_2023 = float(
            fila_2023[columna]
        )

        crecimiento = None

        if casos_2015 > 0:

            crecimiento = (
                (casos_2023 - casos_2015)
                / casos_2015
                * 100
            )

        filas.append(
            {
                "ambito": etiqueta,
                "casos_2015": casos_2015,
                "casos_2023": casos_2023,
                "crecimiento": crecimiento
            }
        )

    comparacion = (
        pd.DataFrame(filas)
        .sort_values(
            "casos_2023",
            ascending=True
        )
    )

    figura = go.Figure()

    # Barras de 2015.
    figura.add_trace(
        go.Bar(
            y=comparacion["ambito"],
            x=comparacion["casos_2015"],
            orientation="h",
            name="2015",
            marker={
                "color": "#AAA49A"
            },
            text=[
                f"{valor:.0f}"
                for valor
                in comparacion["casos_2015"]
            ],
            textposition="outside",
            cliponaxis=False,
            hovertemplate=(
                "<b>%{y}</b><br>"
                "2015: %{x:.0f} casos"
                "<extra></extra>"
            )
        )
    )

    # Barras de 2023.
    figura.add_trace(
        go.Bar(
            y=comparacion["ambito"],
            x=comparacion["casos_2023"],
            orientation="h",
            name="2023",
            marker={
                "color": [COLOR_ROJO] * len(comparacion)
            },
        text=[
            (
                f"{fila['casos_2023']:.0f} "
                f"({fila['crecimiento']:+.0f}%)"
            )
            if pd.notna(fila["crecimiento"])
            else f"{fila['casos_2023']:.0f}"
            for _, fila in comparacion.iterrows()
        ],
            textposition="outside",
            cliponaxis=False,
            customdata=comparacion[
                ["crecimiento"]
            ],
            hovertemplate=(
                "<b>%{y}</b><br>"
                "2023: %{x:.0f} casos<br>"
                "Variación: %{customdata[0]:+.1f}%"
                "<extra></extra>"
            )
        )
    )

    figura.update_layout(
        height=380,
        barmode="group",
        bargap=0.28,
        bargroupgap=0.08,
        margin={
            "l": 20,
            "r": 55,
            "t": 45,
            "b": 30
        },
        paper_bgcolor=COLOR_GRAFICO,
        plot_bgcolor=COLOR_GRAFICO,
        font={
            "family": "Arial",
            "color": COLOR_TINTA
        },
        legend={
            "orientation": "h",
            "x": 0,
            "y": 1.08,
            "title_text": ""
        },
        hoverlabel={
            "bgcolor": COLOR_TINTA,
            "font_color": COLOR_PAPEL,
            "font_family": "Arial"
        }
    )

    figura.update_xaxes(
        title="Número de delitos registrados",
        showgrid=True,
        gridcolor="rgba(24, 24, 24, 0.10)",
        zeroline=False,
        showline=True,
        linecolor=COLOR_TINTA,
        range=[
            0,
            comparacion["casos_2023"].max() * 1.14
        ],
        tickfont={
            "size": 12
        }
    )

    figura.update_yaxes(
        title="",
        showgrid=False,
        tickfont={
            "size": 12
        }
    )

    return figura

# ============================================================
# SCATTERPLOT DE FACTORES — PÁGINA 5
# ============================================================

def crear_scatter_factores(
    datos,
    vista,
    factor,
    anio
):

    configuracion_tema = (
        CONFIGURACION_TEMAS[vista]
    )

    configuracion_factor = (
        CONFIGURACION_FACTORES[factor]
    )

    columna_y = configuracion_tema[
        "columna"
    ]

    columna_x = configuracion_factor[
        "columna"
    ]

    etiqueta_x = configuracion_factor[
        "etiqueta"
    ]

    color = configuracion_tema[
        "color"
    ]

    datos_anio = datos[
        datos["año"] == anio
    ].copy()

    datos_anio = datos_anio[
        [
            "comunidad",
            columna_x,
            columna_y
        ]
    ].dropna()

    correlacion = datos_anio[
        columna_x
    ].corr(
        datos_anio[columna_y]
    )

    figura = go.Figure()

    figura.add_trace(
        go.Scatter(
            x=datos_anio[columna_x],
            y=datos_anio[columna_y],
            mode="markers",
            customdata=datos_anio[
                "comunidad"
            ],
            marker={
                "size": 13,
                "color": color,
                "opacity": 0.82,
                "line": {
                    "color": COLOR_PAPEL,
                    "width": 1.5
                }
            },
            hovertemplate=(
                "<b>%{customdata}</b><br>"
                + etiqueta_x
                + ": %{x:.2f}<br>"
                + "Delitos por 100.000: %{y:.2f}"
                + "<extra></extra>"
            )
        )
    )

    # Línea de tendencia.
    if (
        len(datos_anio) >= 2
        and datos_anio[columna_x].nunique() > 1
    ):

        coeficientes = np.polyfit(
            datos_anio[columna_x],
            datos_anio[columna_y],
            1
        )

        valores_x = np.linspace(
            datos_anio[columna_x].min(),
            datos_anio[columna_x].max(),
            100
        )

        valores_y = (
            coeficientes[0] * valores_x
            + coeficientes[1]
        )

        figura.add_trace(
            go.Scatter(
                x=valores_x,
                y=valores_y,
                mode="lines",
                line={
                    "color": COLOR_TINTA,
                    "width": 2,
                    "dash": "dash"
                },
                hoverinfo="skip"
            )
        )

    figura.update_layout(
        height=425,
        title_text="",
        margin={
            "l": 35,
            "r": 20,
            "t": 12,
            "b": 25
        },
        paper_bgcolor=COLOR_GRAFICO,
        plot_bgcolor=COLOR_GRAFICO,
        font={
            "family": "Arial",
            "color": COLOR_TINTA
        },
        showlegend=False,
        hoverlabel={
            "bgcolor": COLOR_TINTA,
            "font_color": COLOR_PAPEL,
            "font_family": "Arial"
        }
    )

    figura.update_xaxes(
        title=etiqueta_x,
        showgrid=False,
        showline=True,
        linewidth=1,
        linecolor=COLOR_TINTA,
        zeroline=False,
        tickfont={
            "size": 12
        },
        title_font={
            "size": 13
        }
    )

    if configuracion_factor["porcentaje"]:

        figura.update_xaxes(
            ticksuffix=" %"
        )

    figura.update_yaxes(
        title="Delitos por 100.000 habitantes",
        showgrid=True,
        gridcolor="rgba(24, 24, 24, 0.12)",
        zeroline=False,
        showline=False,
        tickfont={
            "size": 12
        },
        title_font={
            "size": 13
        }
    )

    return (
        figura,
        correlacion,
        len(datos_anio)
    )

# ============================================================
# EVOLUCIÓN DE TEMAS DE PRENSA — PÁGINA 7
# ============================================================

def crear_evolucion_temas_prensa(
    datos,
    top_n=5
):

    colores = [
        "#A51C30",
        "#C65D3B",
        "#725A9C",
        "#3E6B63",
        "#B18B45"
    ]

    temas_principales = (
        datos
        .groupby("etiqueta")["documentos"]
        .sum()
        .nlargest(top_n)
        .index
        .tolist()
    )

    datos_grafico = datos[
        datos["etiqueta"].isin(
            temas_principales
        )
    ].copy()

    figura = go.Figure()

    for posicion, tema in enumerate(
        temas_principales
    ):

        serie = (
            datos_grafico[
                datos_grafico["etiqueta"] == tema
            ]
            .sort_values("year_dataset")
        )

        figura.add_trace(
            go.Scatter(
                x=serie["year_dataset"],
                y=serie["porcentaje_corpus"],
                mode="lines+markers",
                name=tema,
                line={
                    "color": colores[
                        posicion % len(colores)
                    ],
                    "width": 3
                },
                marker={
                    "size": 10,
                    "line": {
                        "color": COLOR_PAPEL,
                        "width": 1.5
                    }
                },
                hovertemplate=(
                    "<b>" + tema + "</b><br>"
                    "Año: %{x}<br>"
                    "% del corpus: %{y:.2f}%"
                    "<extra></extra>"
                )
            )
        )

    figura.update_layout(
        height=440,
        margin={
            "l": 35,
            "r": 20,
            "t": 15,
            "b": 25
        },
        paper_bgcolor=COLOR_GRAFICO,
        plot_bgcolor=COLOR_GRAFICO,
        font={
            "family": "Arial",
            "color": COLOR_TINTA
        },
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "left",
            "x": 0,
            "font": {
                "size": 11
            }
        },
        hoverlabel={
            "bgcolor": COLOR_TINTA,
            "font_color": COLOR_PAPEL
        }
    )

    figura.update_xaxes(
        title="",
        tickmode="array",
        tickvals=[2015, 2016, 2019, 2023],
        showgrid=False,
        showline=True,
        linecolor=COLOR_TINTA,
        linewidth=1
    )

    figura.update_yaxes(
        title="% de noticias del año",
        ticksuffix=" %",
        showgrid=True,
        gridcolor="rgba(24, 24, 24, 0.12)",
        zeroline=False
    )

    return figura

# ============================================================
# SENTIMIENTO POR PLATAFORMA Y TEMA — PÁGINA 8
# ============================================================

def obtener_ejemplo_hover(
    ejemplos_hover,
    plataforma,
    tema,
    sentimiento
):

    ejemplo = ejemplos_hover[
        (ejemplos_hover["plataforma"] == plataforma)
        &
        (ejemplos_hover["tema"].str.lower() == tema.lower())
        &
        (ejemplos_hover["sentimiento"] == sentimiento)
    ]

    if ejemplo.empty:
        return "Ejemplo no disponible."

    texto = ejemplo.iloc[0]["texto"]

    texto = html.escape(
        texto,
        quote=False
    )

    if len(texto) > 200:
        texto = (
            texto[:200]
            .rsplit(" ", 1)[0]
            + "..."
        )

    texto = "<br>".join(
        textwrap.wrap(
            texto,
            width=50
        )
    )

    return texto

def crear_sentimiento_plataforma(
    datos,
    plataforma,
    ejemplos_hover
):

    datos_plataforma = datos[
        datos["plataforma"] == plataforma
    ].copy()

    datos_plataforma["tema"] = (
        datos_plataforma["tema"]
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

    nombres_sentimiento = {
        "NEG": "Negativo",
        "NEU": "Neutral",
        "POS": "Positivo"
    }

    colores = {
        "NEG": "#A51C30",
        "NEU": "#9E978D",
        "POS": "#3E6B63"
    }

    temas = [
        ("inmigracion", "Inmigración"),
        ("lgtbi", "LGTBI")
    ]

    conteos = {}
    porcentajes = {}

    for codigo_tema, nombre_tema in temas:

        datos_tema = datos_plataforma[
            datos_plataforma["tema"] == codigo_tema
        ]

        distribucion = (
            datos_tema["sentimiento"]
            .value_counts()
            .reindex(
                ["NEG", "NEU", "POS"],
                fill_value=0
            )
        )

        total_tema = int(
            distribucion.sum()
        )

        conteos[nombre_tema] = distribucion

        if total_tema > 0:

            porcentajes[nombre_tema] = (
                distribucion / total_tema * 100
            )

        else:

            porcentajes[nombre_tema] = (
                distribucion.astype(float)
            )
        datos_plataforma["sentimiento"] = (
                datos_plataforma["sentimiento"]
                .astype(str)
                .str.strip()
                .str.upper()
                .replace(
                    {
                        "NEGATIVO": "NEG",
                        "NEUTRAL": "NEU",
                        "POSITIVO": "POS",
                        "NEGATIVE": "NEG",
                        "POSITIVE": "POS"
                    }
                )
            )

    figura = go.Figure()

    for sentimiento in [
        "NEG",
        "NEU",
        "POS"
    ]:

        valores_porcentaje = [
            float(
                porcentajes["Inmigración"][
                    sentimiento
                ]
            ),
            float(
                porcentajes["LGTBI"][
                    sentimiento
                ]
            )
        ]

        valores_conteo = [
            int(
                conteos["Inmigración"][
                    sentimiento
                ]
            ),
            int(
                conteos["LGTBI"][
                    sentimiento
                ]
            )
        ]

        textos = [
            (
                f"{valor:.1f}%"
                .replace(".", ",")
                if valor >= 4
                else ""
            )
            for valor in valores_porcentaje
        ]

        ejemplos = [

            obtener_ejemplo_hover(
                ejemplos_hover,
                plataforma,
                "inmigracion",
                sentimiento
            ),

            obtener_ejemplo_hover(
                ejemplos_hover,
                plataforma,
                "lgtbi",
                sentimiento
            )
        ]
        
        figura.add_trace(
            go.Bar(
                y=[
                    "Inmigración",
                    "LGTBI"
                ],
                x=valores_porcentaje,
                name=nombres_sentimiento[
                    sentimiento
                ],
                orientation="h",
                marker={
                    "color": colores[sentimiento]
                },
                text=textos,
                textposition="inside",
                insidetextanchor="middle",
                textfont={
                    "color": "#FFFFFF",
                    "size": 14
                },
                customdata=[
                    [valores_conteo[0], ejemplos[0]],
                    [valores_conteo[1], ejemplos[1]]
                ],
                hovertemplate=(

                    "<b>%{y}</b><br>"

                    + nombres_sentimiento[sentimiento]

                    + ": %{x:.1f}%<br>"

                    "%{customdata[0]:,.0f} comentarios"

                    "<br><br>"

                    "<b>💬 Ejemplo real</b><br>"

                    "%{customdata[1]}"

                    "<extra></extra>"

                )
                )
            )
    

    figura.update_layout(
        height=315,
        barmode="stack",
        margin={
            "l": 25,
            "r": 20,
            "t": 55,
            "b": 35
        },
        paper_bgcolor=COLOR_GRAFICO,
        plot_bgcolor=COLOR_GRAFICO,
        font={
            "family": "Arial",
            "color": COLOR_TINTA
        },
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.04,
            "xanchor": "left",
            "x": 0
        },
        hoverlabel={
            "bgcolor": COLOR_TINTA,
            "font_color": COLOR_PAPEL,
            "font_family": "Arial"
        },
        showlegend=True
    )

    figura.update_xaxes(
        title="% de comentarios",
        range=[0, 100],
        ticksuffix=" %",
        showgrid=False,
        showline=True,
        linewidth=1,
        linecolor=COLOR_TINTA,
        zeroline=False
    )

    figura.update_yaxes(
        title="",
        categoryorder="array",
        categoryarray=[
            "LGTBI",
            "Inmigración"
        ],
        showgrid=False,
        tickfont={
            "size": 14,
            "color": COLOR_TINTA
        }
    )

    return figura

# ============================================================
# EVOLUCIÓN DE LA NEGATIVIDAD EN RRSS — PÁGINA 9
# ============================================================

def crear_evolucion_negatividad_rrss(
    datos,
    tema
):

    codigos_tema = {
        "Inmigración": "inmigracion",
        "LGTBI": "lgtbi"
    }

    colores_plataforma = {
        "Prensa": "#181818",      # Negro
        "Menéame": "#2F6DB3",     # Azul
        "ForoCoches": "#D68A2E",  # Naranja
        "YouTube": "#3E6B63"      # Verde
    }

    datos_grafico = datos.copy()

    datos_grafico["tema"] = (
        datos_grafico["tema"]
        .astype(str)
        .str.lower()
        .str.strip()
    )

    datos_grafico = datos_grafico[
        datos_grafico["tema"]
        == codigos_tema[tema]
    ].copy()

    resumen = (
        datos_grafico
        .groupby(
            [
                "plataforma",
                "anio"
            ]
        )["sentimiento"]
        .agg(
            total="size",
            negativos=lambda serie: (
                serie == "NEG"
            ).sum()
        )
        .reset_index()
    )

    resumen["porcentaje_negativo"] = (
        resumen["negativos"]
        / resumen["total"]
        * 100
    )

    figura = go.Figure()

    for plataforma in [
        "Prensa",
        "Menéame",
        "ForoCoches",
        "YouTube"
    ]:

        serie = (
            resumen[
                resumen["plataforma"]
                == plataforma
            ]
            .sort_values("anio")
        )

        figura.add_trace(
            go.Scatter(
                x=serie["anio"],
                y=serie["porcentaje_negativo"],
                mode="lines+markers",
                name=plataforma,
                line={
                    "color": colores_plataforma[
                        plataforma
                    ],
                    "width": 4
                },
                marker={
                    "size": 12,
                    "color": colores_plataforma[
                        plataforma
                    ],
                    "line": {
                        "color": COLOR_PAPEL,
                        "width": 2
                    }
                },
                customdata=serie[
                    [
                        "negativos",
                        "total"
                    ]
                ],
                hovertemplate=(
                    "<b>"
                    + plataforma
                    + "</b><br>"
                    "Año: %{x}<br>"
                    "Comentarios negativos: %{y:.1f}%<br>"
                    "%{customdata[0]:,.0f} negativos<br>"
                    "%{customdata[1]:,.0f} comentarios"
                    "<extra></extra>"
                )
            )
        )

    figura.update_layout(
        height=445,
        margin={
            "l": 40,
            "r": 25,
            "t": 55,
            "b": 30
        },
        paper_bgcolor=COLOR_GRAFICO,
        plot_bgcolor=COLOR_GRAFICO,
        font={
            "family": "Arial",
            "color": COLOR_TINTA
        },
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.03,
            "xanchor": "left",
            "x": 0
        },
        hoverlabel={
            "bgcolor": COLOR_TINTA,
            "font_color": COLOR_PAPEL,
            "font_family": "Arial"
        }
    )

    figura.update_xaxes(
        title="",
        tickmode="array",
        tickvals=[
            2015,
            2016,
            2019,
            2023
        ],
        ticktext=[
            "2015",
            "2016",
            "2019",
            "2023"
        ],
        showgrid=False,
        showline=True,
        linewidth=1,
        linecolor=COLOR_TINTA
    )

    figura.update_yaxes(
        title="% de comentarios negativos",
        ticksuffix=" %",
        rangemode="tozero",
        showgrid=True,
        gridcolor="rgba(24, 24, 24, 0.12)",
        zeroline=False
    )

    return figura


# ============================================================
# EVOLUCIÓN ELECTORAL
# ============================================================

def crear_evolucion_electoral(datos, datos_bloques):

    datos = (
        datos
        .sort_values("orden")
        .reset_index(drop=True)
        .copy()
    )

    # Posiciones fijas para evitar que Plotly interprete
    # las elecciones como fechas.
    datos["posicion"] = range(len(datos))
    bloques = datos_bloques.copy()

    bloques["eleccion"] = bloques.apply(
        lambda fila: (
            "Abr. 2019"
            if fila["año"] == 2019 and fila["mes"] == 4
            else "Nov. 2019"
            if fila["año"] == 2019 and fila["mes"] == 11
            else str(int(fila["año"]))
        ),
        axis=1
    )

    orden_elecciones = [
        "2015",
        "2016",
        "Abr. 2019",
        "Nov. 2019",
        "2023"
    ]

    bloques["posicion"] = bloques[
        "eleccion"
    ].map(
        {
            nombre: posicion
            for posicion, nombre
            in enumerate(orden_elecciones)
        }
    )

    figura = go.Figure()

    colores_bloques = {
    "Centro": "#C98B8B",     
    "Derecha": "#7F9DB5",   
    "Izquierda": "#A694B8"
    }

    for bloque in [
        "Centro",
        "Derecha",
        "Izquierda"
    ]:

        columna = f"pct_{bloque}"

        figura.add_trace(
            go.Scatter(
                x=bloques["posicion"],
                y=bloques[columna],
                mode="lines",
                name=bloque,
                line={
                    "color": colores_bloques[bloque],
                    "width": 2
                },
                opacity=0.55,
                hovertemplate=(
                    "<b>"
                    + bloque
                    + "</b><br>"
                    "% de voto: %{y:.1f}%"
                    "<extra></extra>"
                )
            )
        )

    figura.add_trace(
        go.Scatter(
            x=datos["posicion"],
            y=datos["pct_vox"],
            mode="lines+markers+text",
            line={
                "color": COLOR_VOX,
                "width": 3
            },
            marker={
                "size": 14,
                "color": COLOR_VOX,
                "line": {
                    "color": COLOR_PAPEL,
                    "width": 2
                }
            },
            text=[
                f"{valor:.2f}%".replace(".", ",")
                for valor in datos["pct_vox"]
            ],
            textposition=[
                "top center",     # 2015
                "top center",     # 2016
                "bottom right",   # abril de 2019
                "top center",     # noviembre de 2019
                "top center"      # 2023
            ],
            textfont={
                "family": "Arial",
                "size": 15,
                "color": COLOR_TINTA
            },
            customdata=datos[
                [
                    "eleccion",
                    "votos_vox",
                    "escanos_vox"
                ]
            ].to_numpy(),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Voto a Vox: %{y:.2f}%<br>"
                "Votos: %{customdata[1]:,.0f}<br>"
                "Escaños: %{customdata[2]:.0f}"
                "<extra></extra>"
            ),
            name="Vox"
        )
    )

    # Abril de 2019 ocupa la posición 2.
    posicion_abril = 2

    valor_abril = datos.loc[
        datos["eleccion"] == "Abr. 2019",
        "pct_vox"
    ].iloc[0]

    figura.add_shape(
        type="line",
        x0=posicion_abril,
        x1=posicion_abril,
        y0=0,
        y1=1,
        xref="x",
        yref="paper",
        line={
            "color": COLOR_GRIS,
            "width": 2,
            "dash": "dot"
        }
    )

    figura.add_annotation(
        x=posicion_abril,
        y=valor_abril,
        text=(
            "<b>Entrada en el Congreso</b>"
            "<br>24 escaños"
        ),
        showarrow=True,
        arrowhead=2,
        arrowwidth=1.5,
        arrowcolor=COLOR_TINTA,
        ax=-80,
        ay=-35,
        bgcolor="rgba(243,235,221,0.96)",
        bordercolor=COLOR_VOX,
        borderwidth=1,
        borderpad=7,
        font={
            "family": "Arial",
            "size": 13,
            "color": COLOR_TINTA
        }
    )

    figura.update_layout(
        height=390,
        paper_bgcolor=COLOR_GRAFICO,
        plot_bgcolor=COLOR_GRAFICO,
        showlegend=True,
        margin={
            "l": 45,
            "r": 25,
            "t": 45,
            "b": 20
        },
        font={
            "family": "Arial",
            "color": COLOR_TINTA
        },
        hoverlabel={
            "bgcolor": COLOR_PAPEL,
            "bordercolor": COLOR_ROJO,
            "font": {
                "family": "Arial",
                "color": COLOR_TINTA
            }
        },
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "left",
            "x": 0
        },
    )

    figura.update_xaxes(
        title=None,
        tickmode="array",
        tickvals=datos["posicion"].tolist(),
        ticktext=datos["eleccion"].tolist(),
        range=[
            -0.25,
            len(datos) - 0.75
        ],
        showgrid=False,
        showline=True,
        linecolor=COLOR_TINTA,
        linewidth=1,
        tickfont={
            "size": 13,
            "color": COLOR_GRIS
        }
    )

    figura.update_yaxes(
        title="% de voto",
        rangemode="tozero",
        ticksuffix=" %",
        gridcolor="rgba(107,104,99,0.20)",
        zeroline=False,
        tickfont={
            "size": 12,
            "color": COLOR_GRIS
        },
        title_font={
            "size": 13,
            "color": COLOR_GRIS
        }
    )

    return figura


# ============================================================
# MATRIZ RESUMEN DE FACTORES — PÁGINA 6
# ============================================================

def crear_matriz_factores(datos):

    anios = [2015, 2016, 2019, 2023]

    factores = {
        "Población inmigrante": "pct_inmigrantes",
        "Índice de Gini": "Gini",
        "Desigualdad S80/S20": "Desigualdad (S80/S20)",
        "Voto extrema derecha": "pct_Extrema derecha"
    }

    objetivo = "delitos_por_100k"

    valores = []

    for nombre_factor, columna_factor in factores.items():

        fila = []

        for anio in anios:

            datos_anio = datos[
                datos["año"] == anio
            ][
                [
                    columna_factor,
                    objetivo
                ]
            ].copy()

            datos_anio[columna_factor] = pd.to_numeric(
                datos_anio[columna_factor],
                errors="coerce"
            )

            datos_anio[objetivo] = pd.to_numeric(
                datos_anio[objetivo],
                errors="coerce"
            )

            datos_anio = datos_anio.dropna()

            if len(datos_anio) < 3:
                correlacion = np.nan
            else:
                correlacion = datos_anio[
                    columna_factor
                ].corr(
                    datos_anio[objetivo]
                )

            fila.append(correlacion)

        valores.append(fila)

    textos = []

    for fila in valores:

        textos_fila = []

        for valor in fila:

            if pd.isna(valor):
                texto = "—"
            else:
                texto = (
                    f"{valor:+.2f}"
                    .replace(".", ",")
                )

            textos_fila.append(texto)

        textos.append(textos_fila)

    posiciones = [0, 1, 2, 3]

    anios_hover = [
        ["2015", "2016", "2019", "2023"]
        for _ in factores
    ]

    figura = go.Figure(
        data=go.Heatmap(
            z=valores,
            x=["2015", "2016", "2019", "2023"],
            y=list(factores.keys()),
            text=textos,
            texttemplate="%{text}",
            textfont={
                "size": 18,
                "color": "#171717"
            },
            zmin=-1,
            zmax=1,
            zmid=0,
            colorscale=[
                [0.00, "#4F6965"],
                [0.50, "#F3EBDD"],
                [1.00, "#9E3A3D"]
            ],
            colorbar={
                "title": {
                    "text": "Correlación r",
                    "side": "right"
                },
                "tickvals": [-1, -0.5, 0, 0.5, 1],
                "ticktext": [
                    "−1",
                    "−0,5",
                    "0",
                    "+0,5",
                    "+1"
                ],
                "thickness": 18,
                "len": 0.78
            },
            xgap=5,
            ygap=5,
            hovertemplate=(
                "<b>%{y}</b>"
                "<br>Año: %{x}"
                "<br>Correlación: %{text}"
                "<extra></extra>"
            )
        )
    )

    figura.update_xaxes(
        title="Año analizado",
        type="category",
        categoryorder="array",
        categoryarray=[
            "2015",
            "2016",
            "2019",
            "2023"
        ],
        tickmode="array",
        tickvals=[
            "2015",
            "2016",
            "2019",
            "2023"
        ],
        ticktext=[
            "2015",
            "2016",
            "2019",
            "2023"
        ],
        fixedrange=True,
        showgrid=False,
        zeroline=False,
        tickfont={"size": 14},
        title_font={"size": 14}
    )

    figura.update_layout(
        height=380,
        paper_bgcolor="rgba(255,255,255,0.78)",
        plot_bgcolor="rgba(255,255,255,0)",
        margin={
            "l": 135,
            "r": 45,
            "t": 25,
            "b": 45
        },
        font={
            "family": "Arial",
            "color": "#171717"
        }
    )

    return figura

def crear_evolucion_socioeconomica(datos):

    df = datos.copy()

    variables = {
        "delitos_por_100k": "Delitos de odio",
        "paro_medio": "Desempleo",
        "pct_inmigrantes": "Población inmigrante",
        "Desigualdad (S80/S20)": "Desigualdad S80/S20",
        "Gini": "Índice de Gini"
    }

    variables_disponibles = {
        columna: etiqueta
        for columna, etiqueta in variables.items()
        if columna in df.columns
    }

    if not variables_disponibles:
        raise ValueError(
            "No se encuentran las variables esperadas."
        )

    if "anio" in df.columns:
        columna_anio = "anio"
    elif "año" in df.columns:
        columna_anio = "año"
    else:
        raise ValueError(
            "No se encuentra una columna llamada 'anio' o 'año'."
        )

    # Convertir variables a formato numérico
    for columna in variables_disponibles:
        df[columna] = pd.to_numeric(
            df[columna],
            errors="coerce"
        )

    # Media nacional por año
    evolucion = (
        df
        .groupby(columna_anio)[
            list(variables_disponibles.keys())
        ]
        .mean()
        .reset_index()
        .sort_values(columna_anio)
    )

    # Normalización: primer año = 100
    evolucion_normalizada = evolucion.copy()

    for columna in variables_disponibles:

        valor_inicial = evolucion[columna].iloc[0]

        if pd.isna(valor_inicial) or valor_inicial == 0:

            evolucion_normalizada[columna] = np.nan

        else:

            evolucion_normalizada[columna] = (
                evolucion[columna]
                / valor_inicial
                * 100
            )

    figura = go.Figure()

    colores = {
        "delitos_por_100k": COLOR_ROJO,
        "paro_medio": "#3E6B63",
        "pct_inmigrantes": COLOR_NARANJA,
        "Desigualdad (S80/S20)": "#B18B45",
        "Gini": COLOR_VIOLETA
    }

    for columna, nombre in variables_disponibles.items():

        es_delito = columna == "delitos_por_100k"

        figura.add_trace(
            go.Scatter(
                x=evolucion_normalizada[columna_anio],
                y=evolucion_normalizada[columna],
                mode="lines+markers",
                name=nombre,
                line={
                    "color": colores[columna],
                    "width": 3 if es_delito else 2.5
                },
                marker={
                    "size": 11 if es_delito else 8,
                    "color": colores[columna],
                    "line": {
                        "color": COLOR_PAPEL,
                        "width": 1.5
                    }
                },
                hovertemplate=(
                    "<b>"
                    + nombre
                    + "</b><br>"
                    "Año: %{x}<br>"
                    "Índice: %{y:.1f}"
                    "<extra></extra>"
                )
            )
        )

    figura.add_hline(
        y=100,
        line_dash="dot",
        line_color=COLOR_GRIS,
        opacity=0.65
    )

    figura.update_layout(
        height=410,
        paper_bgcolor=COLOR_GRAFICO,
        plot_bgcolor=COLOR_GRAFICO,
        margin={
            "l": 45,
            "r": 25,
            "t": 65,
            "b": 30
        },
        font={
            "family": "Arial",
            "color": COLOR_TINTA
        },
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.03,
            "xanchor": "left",
            "x": 0,
            "font": {
                "size": 11
            }
        },
        hovermode="x unified",
        hoverlabel={
            "bgcolor": COLOR_TINTA,
            "font_color": COLOR_PAPEL,
            "font_family": "Arial"
        }
    )

    figura.update_xaxes(
        title="",
        tickmode="array",
        tickvals=evolucion_normalizada[columna_anio],
        ticktext=[
            str(int(anio))
            for anio
            in evolucion_normalizada[columna_anio]
        ],
        showgrid=False,
        showline=True,
        linecolor=COLOR_TINTA,
        linewidth=1
    )

    figura.update_yaxes(
        title="Índice base 2015 = 100",
        showgrid=True,
        gridcolor="rgba(24, 24, 24, 0.12)",
        zeroline=False
    )

    return figura

def crear_recuadro_medios(
        datos,
        columna="medio",
        n=20
    ):
        """
        Genera un recuadro HTML con los principales medios
        de comunicación del conjunto de noticias.
        """

        medios = (
            datos[columna]
            .dropna()
            .astype(str)
            .str.strip()
            .value_counts()
            .head(n)
        )

        texto = ", ".join(
            f"{medio} ({total:,})".replace(",", ".")
            for medio, total in medios.items()
        )

        return f"""
        <div style="
            padding:14px 18px;
            border-left:5px solid #9F3034;
            background:#fffdf7;
            font-family:Arial,sans-serif;
            font-size:11px;
            line-height:1.4;
            color:#4b4742;
        ">

            <div style="
                font-family:Georgia,serif;
                font-size:21px;
                font-weight:700;
                color:#181818;
                margin-bottom:8px;
            ">
                Principales medios analizados
            </div>

            {texto}

        </div>
        """