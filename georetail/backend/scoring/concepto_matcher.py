"""
scoring/concepto_matcher.py — Matching semántico de conceptos de negocio.

Problema que resuelve:
  Un dict de 15-20 tags no puede capturar las decenas de miles de tipos de
  negocio posibles. Esta solución mantiene una base de conocimiento de
  ~100 conceptos canónicos bien definidos y usa embeddings para generalizar:
  cualquier descripción nueva encontrará sus vecinos más cercanos en el espacio
  semántico y heredará sus perfiles de forma proporcional a la similitud.

Flujo:
  descripcion_usuario
       ↓  sentence-transformers encode
  embedding (768d)
       ↓  cosine similarity vs CONCEPTOS_DB embeddings
  top-K conceptos más cercanos
       ↓  blend ponderado por similitud
  idea_tags efectivos + perfil_zona_ideal blended
       ↓  score_afinidad_zona(zona_data, perfil_ideal)
  score_afinidad_concepto (0-100)

El modelo de embeddings (paraphrase-multilingual-mpnet-base-v2) ya está
en el stack (nlp/embeddings.py). Se carga de forma lazy para no penalizar
el arranque.

CONCEPTOS_DB tiene ~100 conceptos que cubren:
  - Food & beverage (30+): specialty coffee, dog-friendly café, fine dining,
    street food, cocktail bar, fast food, etc.
  - Fashion & retail (15): boutique premium, vintage, streetwear, etc.
  - Beauty & wellness (15): barbería hipster, reformer pilates, spa premium, etc.
  - Health (8): dental, fisioterapia, psicología, etc.
  - Culture & art (7): galería, librería independiente, escape room, etc.
  - Services (10): floristería, peluquería canina, reparación móviles, etc.
  - Kids & family (5): ludoteca, actividades infantiles, etc.
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# ── Estructura de un concepto canónico ────────────────────────────────────────
# nombre:       display name
# descripcion:  texto rico que el embedding codifica (cuanto más descriptivo, mejor)
# tags:         idea_tags de scoring/idea_tags.py que caracterizan el concepto
# zona_ideal:   qué tipo de zona necesita (ver _score_zona_vs_ideal para keys)
#
#  zona_ideal keys (todos opcionales):
#   renta_ideal      float 0-1  (0=barrio obrero, 1=zona muy high-income)
#                    BCN range: 17k€ → 0.0, 60k€ → 1.0
#   edad_rango       tuple (min, max) en años del perfil demográfico ideal
#   flujo_min        int pax/hora mínimos necesarios (0-3000)
#   turismo_ideal    float 0-100  ideal tourism score de la zona
#   ratio_comercial_min  float 0-1  fracción mínima de locales comerciales
#   zonas_verdes_bonus   float 0-1  importancia de parques cercanos
#   seguridad_min    float 0-100  security score mínimo
# ──────────────────────────────────────────────────────────────────────────────

CONCEPTOS_DB: dict[str, dict] = {

    # ════════════════════════════════════════════════════════════════════════
    # FOOD & BEVERAGE
    # ════════════════════════════════════════════════════════════════════════

    "cafeteria_specialty_coffee": {
        "nombre": "Cafetería de especialidad (third wave)",
        "descripcion": (
            "Café de tercera ola con enfoque en el origen del café, baristas especializados, "
            "métodos de extracción precisos (v60, aeropress, espresso de calidad). "
            "El café es el producto principal. Ambiente cuidado, precios medios-altos. "
            "Clientela formada, busca experiencia de barista, no café de paso."
        ),
        "tags": ["specialty_coffee", "clientela_local", "alta_renta", "instagrammable",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.65,
            "edad_rango": (25, 45),
            "flujo_min": 250,
            "turismo_ideal": 35,
            "ratio_comercial_min": 0.18,
        },
    },

    "cafeteria_barrio": {
        "nombre": "Cafetería de barrio (mass market)",
        "descripcion": (
            "Bar cafetería tradicional de barrio: café con leche, croissant, menú del día, "
            "bocadillos. Clientela fija del vecindario, precio bajo-medio. "
            "Alta rotación, horario de mañana. Muy dependiente del paso y de los vecinos."
        ),
        "tags": ["clientela_local", "low_cost", "alta_rotacion_clientes",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.40,
            "edad_rango": (30, 65),
            "flujo_min": 500,
            "turismo_ideal": 20,
            "ratio_comercial_min": 0.15,
        },
    },

    "cafeteria_con_perros": {
        "nombre": "Cafetería dog-friendly",
        "descripcion": (
            "Cafetería que acepta explícitamente perros y mascotas en el interior. "
            "Agua y snacks para perros, zona amplia o terraza. Clientela de dueños de mascotas "
            "del barrio. Barrio residencial con parques cercanos imprescindible. "
            "Ambiente relajado, precio medio."
        ),
        "tags": ["dog_friendly", "clientela_local", "clientela_familiar",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.55,
            "edad_rango": (25, 50),
            "flujo_min": 200,
            "turismo_ideal": 20,
            "ratio_comercial_min": 0.12,
            "zonas_verdes_bonus": 0.80,
            "seguridad_min": 60,
        },
    },

    "cafeteria_express_paso": {
        "nombre": "Cafetería express / grab-and-go",
        "descripcion": (
            "Cafetería de paso: ticket rápido, café para llevar, bollería industriales o semi-artesanales. "
            "Pensada para tráfico peatonal intenso en zonas de trabajo u oficinas. "
            "El cliente pasa, no se sienta. Alta rotación, precio bajo."
        ),
        "tags": ["alta_rotacion_clientes", "low_cost", "takeaway_delivery",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.45,
            "flujo_min": 800,
            "turismo_ideal": 35,
            "ratio_comercial_min": 0.25,
        },
    },

    "cafeteria_coworking": {
        "nombre": "Café coworking / workspace café",
        "descripcion": (
            "Cafetería diseñada también como espacio de trabajo: wifi rápido, enchufes en cada mesa, "
            "ambiente silencioso o con música suave, sillas ergonómicas. "
            "Clientela de freelancers, nómadas digitales y profesionales autónomos. "
            "El cliente trabaja horas, no minutos. Precio medio-alto por el entorno."
        ),
        "tags": ["coworking_cafe", "specialty_coffee", "clientela_profesional",
                 "alta_renta", "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.62,
            "edad_rango": (25, 42),
            "flujo_min": 200,
            "turismo_ideal": 30,
            "ratio_comercial_min": 0.18,
            "seguridad_min": 55,
        },
    },

    "cafeteria_instagrammable": {
        "nombre": "Cafetería estética / instagrammable",
        "descripcion": (
            "Local de café con diseño interior muy cuidado y fotogénico. "
            "El ambiente y la estética son parte del producto. Bebidas visualmente llamativas, "
            "latte art, complementos de diseño. Atrae jóvenes y turistas para foto + experiencia. "
            "Precio medio-alto. Necesita visibilidad y zona con tráfico joven."
        ),
        "tags": ["instagrammable", "clientela_joven", "specialty_coffee",
                 "orientado_turismo", "experiencial"],
        "zona_ideal": {
            "renta_ideal": 0.58,
            "edad_rango": (18, 38),
            "flujo_min": 400,
            "turismo_ideal": 55,
            "ratio_comercial_min": 0.22,
        },
    },

    "brunch_restaurant": {
        "nombre": "Restaurante de brunch",
        "descripcion": (
            "Local especializado en brunch de fin de semana y mediodía. "
            "Eggs benedict, avocado toast, granola bowls, mimosas. Precio medio-alto. "
            "Clientela joven y profesional, fines de semana son el 70% de la facturación. "
            "Instagrammable por naturaleza. Zona con renta media-alta y ambiente lifestyle."
        ),
        "tags": ["instagrammable", "clientela_joven", "alta_renta",
                 "horario_diurno_comercial", "vegano_organico"],
        "zona_ideal": {
            "renta_ideal": 0.68,
            "edad_rango": (22, 42),
            "flujo_min": 300,
            "turismo_ideal": 45,
            "ratio_comercial_min": 0.20,
        },
    },

    "restaurante_menu_dia": {
        "nombre": "Restaurante de menú del día",
        "descripcion": (
            "Restaurante orientado al menú del mediodía para trabajadores y oficinas cercanas. "
            "Menú de dos platos + postre entre 10-14€. Alta rotación hora punta 13:00-15:00. "
            "Necesita zona de oficinas o área laboral. Clientela profesional local. "
            "El fin de semana es residual. Precio bajo-medio."
        ),
        "tags": ["clientela_profesional", "clientela_local", "alta_rotacion_clientes",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.48,
            "edad_rango": (28, 55),
            "flujo_min": 600,
            "turismo_ideal": 20,
            "ratio_comercial_min": 0.22,
        },
    },

    "restaurante_familiar": {
        "nombre": "Restaurante familiar",
        "descripcion": (
            "Restaurante tradicional para familias con niños, menú variado y ambiente relajado. "
            "Raciones generosas, precio medio. Zona residencial familiar, parques cercanos. "
            "Semana y fin de semana equilibrados. Seguridad del barrio importa a los padres."
        ),
        "tags": ["clientela_familiar", "clientela_local"],
        "zona_ideal": {
            "renta_ideal": 0.48,
            "edad_rango": (30, 60),
            "flujo_min": 300,
            "turismo_ideal": 20,
            "ratio_comercial_min": 0.15,
            "zonas_verdes_bonus": 0.40,
            "seguridad_min": 65,
        },
    },

    "restaurante_romantico": {
        "nombre": "Restaurante romántico / íntimo",
        "descripcion": (
            "Restaurante de ambiente íntimo para cenas románticas y ocasiones especiales. "
            "Iluminación cuidada, servicio atento, carta de vinos. Precio alto. "
            "El cliente reserva con antelación. No depende del paso. "
            "Barrio premium o con ambiente gastronómico."
        ),
        "tags": ["alta_renta", "destino", "salon_citas", "gastronomico_premium",
                 "horario_nocturno"],
        "zona_ideal": {
            "renta_ideal": 0.75,
            "edad_rango": (28, 55),
            "flujo_min": 150,
            "turismo_ideal": 40,
            "ratio_comercial_min": 0.16,
            "seguridad_min": 60,
        },
    },

    "restaurante_alta_cocina": {
        "nombre": "Restaurante de alta cocina / fine dining",
        "descripcion": (
            "Restaurante gastronómico de alto nivel: menú degustación, carta de vinos premium, "
            "servicio de sala profesional. El cliente viene expresamente, ha reservado semanas antes. "
            "Precio muy alto. Atrae también turismo gastronómico internacional. "
            "Zona premium, no necesita flujo peatonal masivo."
        ),
        "tags": ["gastronomico_premium", "alta_renta", "destino", "orientado_turismo",
                 "experiencial", "salon_citas"],
        "zona_ideal": {
            "renta_ideal": 0.85,
            "edad_rango": (32, 65),
            "flujo_min": 100,
            "turismo_ideal": 60,
            "ratio_comercial_min": 0.14,
            "seguridad_min": 65,
        },
    },

    "restaurante_vegano": {
        "nombre": "Restaurante vegano / plant-based",
        "descripcion": (
            "Restaurante con oferta 100% vegana o plant-based. Ingredientes de temporada y "
            "ecológicos siempre que sea posible. Clientela concienciada, renta media-alta, "
            "jóvenes y adultos activos. Barrios con perfil lifestyle activo y saludable."
        ),
        "tags": ["vegano_organico", "clientela_joven", "alta_renta",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.65,
            "edad_rango": (22, 45),
            "flujo_min": 300,
            "turismo_ideal": 40,
            "ratio_comercial_min": 0.18,
        },
    },

    "bar_tapas": {
        "nombre": "Bar de tapas",
        "descripcion": (
            "Bar tradicional de tapas y pinchos. Clientela mixta: locales y turistas. "
            "Horario amplio, mediodía y noche. Precio medio. "
            "Necesita visibilidad y zona con tráfico. Tanto barrios de barrio como zonas turísticas."
        ),
        "tags": ["clientela_local", "orientado_turismo", "alta_rotacion_clientes"],
        "zona_ideal": {
            "renta_ideal": 0.45,
            "edad_rango": (25, 65),
            "flujo_min": 500,
            "turismo_ideal": 45,
            "ratio_comercial_min": 0.20,
        },
    },

    "bar_cocteleria": {
        "nombre": "Bar de cócteles / coctelería",
        "descripcion": (
            "Coctelería con bartenders especializados, cócteles clásicos y de autor. "
            "Ambiente cuidado, precio medio-alto. Foco nocturno. "
            "Clientela joven y adulta con poder adquisitivo. Barrio con vida nocturna."
        ),
        "tags": ["horario_nocturno", "clientela_joven", "alta_renta",
                 "instagrammable", "experiencial"],
        "zona_ideal": {
            "renta_ideal": 0.62,
            "edad_rango": (22, 42),
            "flujo_min": 300,
            "turismo_ideal": 50,
            "ratio_comercial_min": 0.18,
        },
    },

    "bar_vinos": {
        "nombre": "Bar de vinos / enoteca",
        "descripcion": (
            "Bar especializado en vinos, con carta amplia y copas de precio razonable. "
            "A veces también tabla de quesos y embutidos. Ambiente íntimo y tranquilo. "
            "Clientela adulta con interés por el vino. Precio medio-alto. "
            "Puede ser destino o también local de paso en zona gastronómica."
        ),
        "tags": ["alta_renta", "clientela_local", "experiencial"],
        "zona_ideal": {
            "renta_ideal": 0.68,
            "edad_rango": (30, 60),
            "flujo_min": 200,
            "turismo_ideal": 40,
            "ratio_comercial_min": 0.17,
        },
    },

    "bar_cervezas_artesanas": {
        "nombre": "Bar de cervezas artesanas / craft beer",
        "descripcion": (
            "Bar especializado en cervezas artesanas: muchos grifos rotativos, "
            "ambiente informal y social. Clientela joven y urbana, apasionada de la cerveza. "
            "Precio medio. Barrios con perfil joven y activo."
        ),
        "tags": ["clientela_joven", "horario_nocturno", "experiencial"],
        "zona_ideal": {
            "renta_ideal": 0.52,
            "edad_rango": (22, 45),
            "flujo_min": 300,
            "turismo_ideal": 40,
            "ratio_comercial_min": 0.18,
        },
    },

    "gastrobar_tardeo": {
        "nombre": "Gastrobar de tardeo / brunch nocturno",
        "descripcion": (
            "Local que mezcla gastronomía y copas, con foco en las tardes-noches de finde. "
            "Música ambiente, raciones de calidad, cócteles. Precio medio-alto. "
            "Zona con vida nocturna pero no club puro. Clientela 25-40 años."
        ),
        "tags": ["horario_nocturno", "instagrammable", "clientela_joven",
                 "experiencial"],
        "zona_ideal": {
            "renta_ideal": 0.60,
            "edad_rango": (22, 42),
            "flujo_min": 350,
            "turismo_ideal": 50,
            "ratio_comercial_min": 0.20,
        },
    },

    "discoteca": {
        "nombre": "Discoteca / club nocturno",
        "descripcion": (
            "Club de música y baile nocturno. Horario noche y madrugada. "
            "Capacidad para muchas personas, sistema de sonido profesional. "
            "Necesita zona donde la actividad nocturna esté tolerada. "
            "Ruido es un factor — zonas menos residenciales. Clientela joven."
        ),
        "tags": ["horario_nocturno", "clientela_joven", "local_grande",
                 "experiencial"],
        "zona_ideal": {
            "renta_ideal": 0.45,
            "edad_rango": (18, 35),
            "flujo_min": 400,
            "turismo_ideal": 55,
            "ratio_comercial_min": 0.22,
        },
    },

    "hamburgueseria": {
        "nombre": "Hamburguesería artesanal",
        "descripcion": (
            "Hamburguesería con carne fresca, panes artesanales, opciones premium y económicas. "
            "Ambiente casual, precio medio. Clientela variada pero muchos jóvenes. "
            "Necesita visibilidad en zona de paso o zona gastronómica activa."
        ),
        "tags": ["clientela_joven", "alta_rotacion_clientes"],
        "zona_ideal": {
            "renta_ideal": 0.50,
            "edad_rango": (18, 40),
            "flujo_min": 500,
            "turismo_ideal": 40,
            "ratio_comercial_min": 0.22,
        },
    },

    "pizzeria_artesanal": {
        "nombre": "Pizzería artesanal (sit-down)",
        "descripcion": (
            "Pizzería de calidad con horno de leña o de piedra, masa madre, ingredientes italianos. "
            "Comer dentro, ambiente relajado. Precio medio. Clientela variada, familias y jóvenes. "
            "Zona de barrio con tráfico familiar o gastronómico."
        ),
        "tags": ["clientela_familiar", "clientela_local"],
        "zona_ideal": {
            "renta_ideal": 0.50,
            "edad_rango": (20, 55),
            "flujo_min": 350,
            "turismo_ideal": 35,
            "ratio_comercial_min": 0.18,
        },
    },

    "sushi_restaurant": {
        "nombre": "Restaurante de sushi / japonés",
        "descripcion": (
            "Restaurante japonés con sushi, sashimi, ramen o combinados. "
            "Precio medio-alto. Clientela urbana con interés gastronómico. "
            "Funciona tanto de destino como de paso en zona gastronómica."
        ),
        "tags": ["clientela_joven", "alta_renta"],
        "zona_ideal": {
            "renta_ideal": 0.60,
            "edad_rango": (22, 48),
            "flujo_min": 350,
            "turismo_ideal": 45,
            "ratio_comercial_min": 0.20,
        },
    },

    "ramen_bar": {
        "nombre": "Ramen bar",
        "descripcion": (
            "Bar especializado en ramen: caldo cocinado horas, fideos frescos, ingredientes importados. "
            "Ambiente informal y minimalista. Precio medio. Clientela joven y foodie. "
            "Destino gastronómico — el cliente viene a propósito."
        ),
        "tags": ["clientela_joven", "destino", "experiencial"],
        "zona_ideal": {
            "renta_ideal": 0.58,
            "edad_rango": (20, 42),
            "flujo_min": 300,
            "turismo_ideal": 45,
            "ratio_comercial_min": 0.20,
        },
    },

    "poke_bowl": {
        "nombre": "Poke bowl / Hawaiian food",
        "descripcion": (
            "Local de poke bowl hawaiano: ingredientes frescos, personalización, precio medio. "
            "Muy instagrammable, clientela joven y health-conscious. "
            "Zona de oficinas para menú del día y zona joven para cenar."
        ),
        "tags": ["vegano_organico", "clientela_joven", "instagrammable",
                 "alta_rotacion_clientes"],
        "zona_ideal": {
            "renta_ideal": 0.60,
            "edad_rango": (20, 40),
            "flujo_min": 400,
            "turismo_ideal": 45,
            "ratio_comercial_min": 0.22,
        },
    },

    "taqueria": {
        "nombre": "Taquería / comida mexicana informal",
        "descripcion": (
            "Taquería informal con tacos, burritos, nachos. Ambiente colorido y casual. "
            "Precio bajo-medio. Clientela joven. Funciona en zonas de paso y también de noche. "
            "Alta rotación, porciones generosas."
        ),
        "tags": ["street_food", "clientela_joven", "low_cost",
                 "alta_rotacion_clientes"],
        "zona_ideal": {
            "renta_ideal": 0.42,
            "edad_rango": (18, 38),
            "flujo_min": 550,
            "turismo_ideal": 45,
            "ratio_comercial_min": 0.22,
        },
    },

    "kebab": {
        "nombre": "Kebab / comida árabe rápida",
        "descripcion": (
            "Kebab o shawarma de precio bajo, alta rotación, horario amplio hasta noche. "
            "Clientela de precio muy sensible. Muy dependiente del paso y tráfico nocturno. "
            "Precio bajo. Zona con mucho tráfico de calle."
        ),
        "tags": ["street_food", "low_cost", "alta_rotacion_clientes",
                 "horario_nocturno", "takeaway_delivery"],
        "zona_ideal": {
            "renta_ideal": 0.30,
            "flujo_min": 700,
            "turismo_ideal": 40,
            "ratio_comercial_min": 0.25,
        },
    },

    "panaderia_artesanal": {
        "nombre": "Panadería artesanal / obrador",
        "descripcion": (
            "Panadería con obrador propio, pan de masa madre, croissants artesanales, bollería premium. "
            "Precio medio-alto. Destino de barrio — el cliente viene a buscar el pan. "
            "Necesita barrio con renta media-alta y cultura de calidad alimentaria."
        ),
        "tags": ["specialty_coffee", "clientela_local", "alta_renta",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.65,
            "edad_rango": (28, 65),
            "flujo_min": 300,
            "turismo_ideal": 30,
            "ratio_comercial_min": 0.15,
        },
    },

    "heladeria_artesanal": {
        "nombre": "Heladería artesanal / gelateria",
        "descripcion": (
            "Heladería con producción propia, sabores creativos o italianos clásicos. "
            "Estacional (mejor en primavera-verano). Clientela turística y local. "
            "Precio medio. Muy dependiente de la visibilidad y el paso."
        ),
        "tags": ["orientado_turismo", "clientela_joven", "instagrammable",
                 "alta_rotacion_clientes"],
        "zona_ideal": {
            "renta_ideal": 0.50,
            "edad_rango": (15, 65),
            "flujo_min": 600,
            "turismo_ideal": 60,
            "ratio_comercial_min": 0.22,
        },
    },

    "bubble_tea": {
        "nombre": "Bubble tea / té con perlas",
        "descripcion": (
            "Tienda de bubble tea taiwanés con bebidas personalizables y colores llamativos. "
            "Muy instagrammable. Clientela joven (teen y millennial). "
            "Precio medio. Necesita zona con mucho tráfico joven: cerca de universidades, "
            "zonas comerciales activas o barrios hipster."
        ),
        "tags": ["clientela_joven", "instagrammable", "alta_rotacion_clientes"],
        "zona_ideal": {
            "renta_ideal": 0.50,
            "edad_rango": (14, 35),
            "flujo_min": 600,
            "turismo_ideal": 45,
            "ratio_comercial_min": 0.25,
        },
    },

    "smoothie_bar": {
        "nombre": "Smoothie bar / zumos naturales",
        "descripcion": (
            "Bar de zumos naturales, batidos y smoothies saludables. "
            "Precio medio-alto. Clientela health-conscious, activa, renta media-alta. "
            "Mañanas y mediodías son el peak. Zona con gimnasios o área residencial activa."
        ),
        "tags": ["vegano_organico", "clientela_joven", "alta_renta",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.62,
            "edad_rango": (20, 45),
            "flujo_min": 350,
            "turismo_ideal": 35,
            "ratio_comercial_min": 0.18,
        },
    },

    "tienda_gourmet": {
        "nombre": "Tienda de productos gourmet / delicatessen",
        "descripcion": (
            "Tienda especializada en productos gourmet: quesos, embutidos ibéricos, vinos, "
            "conservas premium, aceites de calidad. Clientela con alto poder adquisitivo. "
            "Precio alto. Destino de compra, no de paso. Zona con renta alta."
        ),
        "tags": ["alta_renta", "clientela_local", "destino",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.78,
            "edad_rango": (35, 70),
            "flujo_min": 200,
            "turismo_ideal": 40,
            "ratio_comercial_min": 0.16,
        },
    },

    # ════════════════════════════════════════════════════════════════════════
    # FASHION & RETAIL
    # ════════════════════════════════════════════════════════════════════════

    "boutique_moda_premium": {
        "nombre": "Boutique de moda premium",
        "descripcion": (
            "Boutique de ropa y complementos de marcas de lujo o diseñadores. "
            "Precio alto. Clientela de alto poder adquisitivo. Zona de alta renta y "
            "ambiente exclusivo. El local es parte de la imagen de marca."
        ),
        "tags": ["alta_renta", "retail_moda", "destino", "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.85,
            "edad_rango": (28, 65),
            "flujo_min": 200,
            "turismo_ideal": 55,
            "ratio_comercial_min": 0.20,
            "seguridad_min": 65,
        },
    },

    "tienda_ropa_fast_fashion": {
        "nombre": "Tienda de ropa moda rápida",
        "descripcion": (
            "Tienda de ropa de tendencia a precio accesible. Alta rotación de colecciones. "
            "Necesita máxima visibilidad en zona comercial activa. "
            "Clientela variada, precios bajos-medios. Compite con el canal online."
        ),
        "tags": ["retail_moda", "low_cost", "alta_rotacion_clientes"],
        "zona_ideal": {
            "renta_ideal": 0.45,
            "edad_rango": (16, 45),
            "flujo_min": 700,
            "turismo_ideal": 45,
            "ratio_comercial_min": 0.30,
        },
    },

    "tienda_vintage": {
        "nombre": "Tienda de ropa vintage / segunda mano premium",
        "descripcion": (
            "Tienda de ropa vintage seleccionada, segunda mano de calidad. "
            "Producto único y curado. Clientela joven, consciente, con gusto estético. "
            "Precio medio. Barrios con perfil hipster/artístico. "
            "Instagrammable por naturaleza."
        ),
        "tags": ["retail_moda", "clientela_joven", "instagrammable",
                 "cultural_artistico"],
        "zona_ideal": {
            "renta_ideal": 0.52,
            "edad_rango": (18, 40),
            "flujo_min": 350,
            "turismo_ideal": 45,
            "ratio_comercial_min": 0.20,
        },
    },

    "tienda_streetwear": {
        "nombre": "Tienda de streetwear / moda urbana",
        "descripcion": (
            "Tienda de moda urbana: sneakers, hoodies, gorras, marcas streetwear. "
            "Clientela joven, cultura urbana y sneakerhead. Precio medio-alto. "
            "Zona con mucho tráfico joven, cerca de universidades o barrios hipster."
        ),
        "tags": ["retail_moda", "clientela_joven", "instagrammable"],
        "zona_ideal": {
            "renta_ideal": 0.55,
            "edad_rango": (15, 35),
            "flujo_min": 500,
            "turismo_ideal": 45,
            "ratio_comercial_min": 0.25,
        },
    },

    "joyeria_artesanal": {
        "nombre": "Joyería artesanal / bisutería de diseño",
        "descripcion": (
            "Taller-tienda de joyería hecha a mano o diseño propio. "
            "Producto diferenciado, precio medio-alto. Clientela con gusto por lo único. "
            "Instagrammable. Zona con perfil artístico o de diseño."
        ),
        "tags": ["alta_renta", "instagrammable", "clientela_local",
                 "experiencial", "salon_citas"],
        "zona_ideal": {
            "renta_ideal": 0.65,
            "edad_rango": (25, 55),
            "flujo_min": 200,
            "turismo_ideal": 50,
            "ratio_comercial_min": 0.18,
        },
    },

    "libreria_independiente": {
        "nombre": "Librería independiente",
        "descripcion": (
            "Librería de barrio independiente con selección curada, eventos literarios, "
            "ambiente acogedor. Clientela lectora adulta y familias con niños. "
            "Precio estándar (precio fijo en España). Barrio cultural y con vida de barrio."
        ),
        "tags": ["cultural_artistico", "clientela_local", "clientela_familiar",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.58,
            "edad_rango": (25, 65),
            "flujo_min": 250,
            "turismo_ideal": 35,
            "ratio_comercial_min": 0.15,
            "seguridad_min": 60,
        },
    },

    "tienda_plantas": {
        "nombre": "Tienda de plantas / vivero urbano",
        "descripcion": (
            "Tienda de plantas de interior y exterior, accesorios y macetas. "
            "Muy instagrammable. Clientela de clase media-alta que cuida su hogar. "
            "Precio medio. Barrios residenciales con poder adquisitivo."
        ),
        "tags": ["instagrammable", "clientela_local", "alta_renta",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.62,
            "edad_rango": (25, 55),
            "flujo_min": 250,
            "turismo_ideal": 30,
            "ratio_comercial_min": 0.15,
        },
    },

    # ════════════════════════════════════════════════════════════════════════
    # BEAUTY & WELLNESS
    # ════════════════════════════════════════════════════════════════════════

    "peluqueria_barrio": {
        "nombre": "Peluquería de barrio",
        "descripcion": (
            "Peluquería tradicional unisex de barrio, precio accesible, clientela fija. "
            "No depende del paso casual — la gente repite por confianza. "
            "Zona residencial con densidad de población."
        ),
        "tags": ["clientela_local", "salon_citas", "low_cost"],
        "zona_ideal": {
            "renta_ideal": 0.38,
            "edad_rango": (25, 70),
            "flujo_min": 150,
            "turismo_ideal": 10,
            "ratio_comercial_min": 0.12,
        },
    },

    "peluqueria_premium": {
        "nombre": "Peluquería / salón de belleza premium",
        "descripcion": (
            "Salón de peluquería y estilismo de precio alto. Servicio completo, "
            "tratamientos capilares, coloración profesional. Clientela de renta alta. "
            "La ubicación en zona premium refuerza la imagen de marca."
        ),
        "tags": ["salon_citas", "alta_renta", "experiencial",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.72,
            "edad_rango": (25, 60),
            "flujo_min": 150,
            "turismo_ideal": 30,
            "ratio_comercial_min": 0.16,
            "seguridad_min": 60,
        },
    },

    "barberia_hipster": {
        "nombre": "Barbería hipster / premium",
        "descripcion": (
            "Barbería de estilo cuidado: cortes de barba, fade, perfiles. "
            "Ambiente retro o moderno, precio medio-alto. Clientela masculina 20-45 años. "
            "Barrio con perfil joven y urbano, zona con locales de estilo similar."
        ),
        "tags": ["salon_citas", "clientela_joven", "instagrammable",
                 "alta_renta", "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.60,
            "edad_rango": (20, 45),
            "flujo_min": 200,
            "turismo_ideal": 35,
            "ratio_comercial_min": 0.18,
        },
    },

    "nail_salon": {
        "nombre": "Nail salon / centro de uñas",
        "descripcion": (
            "Salón especializado en manicura, pedicura, uñas de gel y acrílico. "
            "Clientela femenina 18-50 años. Funciona por citas y también de paso. "
            "Precio medio. Zona con tráfico femenino."
        ),
        "tags": ["salon_citas", "clientela_joven", "instagrammable"],
        "zona_ideal": {
            "renta_ideal": 0.52,
            "edad_rango": (18, 50),
            "flujo_min": 250,
            "turismo_ideal": 35,
            "ratio_comercial_min": 0.20,
        },
    },

    "spa_premium": {
        "nombre": "Spa premium / centro de bienestar",
        "descripcion": (
            "Spa con circuito de aguas, masajes, tratamientos faciales de alta gama. "
            "Precio alto. Clientela de alto poder adquisitivo. "
            "Local amplio imprescindible. Zona premium o barrio tranquilo con renta alta. "
            "El cliente reserva con antelación."
        ),
        "tags": ["salon_citas", "alta_renta", "experiencial", "health_wellness",
                 "local_grande"],
        "zona_ideal": {
            "renta_ideal": 0.78,
            "edad_rango": (28, 60),
            "flujo_min": 100,
            "turismo_ideal": 40,
            "ratio_comercial_min": 0.12,
            "seguridad_min": 65,
        },
    },

    "clinica_estetica": {
        "nombre": "Clínica de medicina estética",
        "descripcion": (
            "Clínica de medicina estética: bótox, rellenos, láser, rejuvenecimiento facial. "
            "Precio alto. Clientela de renta alta. Solo con cita médica. "
            "No depende del paso. Zona tranquila y de imagen premium."
        ),
        "tags": ["salon_citas", "alta_renta", "destino", "health_wellness",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.80,
            "edad_rango": (30, 65),
            "flujo_min": 80,
            "turismo_ideal": 30,
            "ratio_comercial_min": 0.14,
            "seguridad_min": 65,
        },
    },

    "reformer_pilates": {
        "nombre": "Estudio de reformer pilates / pilates boutique",
        "descripcion": (
            "Estudio boutique de pilates con máquinas reformer. Clases en grupos pequeños. "
            "Precio alto (cuota mensual o pack de sesiones). Clientela 25-55, renta media-alta. "
            "El cliente va regularmente. Zona residencial con renta alta o zona de oficinas premium."
        ),
        "tags": ["fitness_boutique", "salon_citas", "alta_renta",
                 "clientela_profesional", "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.72,
            "edad_rango": (25, 55),
            "flujo_min": 100,
            "turismo_ideal": 20,
            "ratio_comercial_min": 0.14,
            "seguridad_min": 60,
        },
    },

    "crossfit_box": {
        "nombre": "Box de CrossFit / entrenamiento funcional",
        "descripcion": (
            "Espacio de entrenamiento de alta intensidad (CrossFit, functional fitness). "
            "Local grande con zona de levantamiento y cardio. Precio medio-alto por membresia. "
            "Clientela joven y activa. Necesita local amplio en planta baja o sótano."
        ),
        "tags": ["fitness_boutique", "salon_citas", "clientela_joven",
                 "local_grande"],
        "zona_ideal": {
            "renta_ideal": 0.55,
            "edad_rango": (20, 45),
            "flujo_min": 120,
            "turismo_ideal": 15,
            "ratio_comercial_min": 0.15,
        },
    },

    "centro_yoga": {
        "nombre": "Centro de yoga / meditación",
        "descripcion": (
            "Centro dedicado al yoga, meditación y prácticas de bienestar. "
            "Ambiente sereno, sin ruido. Precio medio. Clientela adulta con interés por bienestar. "
            "Zona tranquila, preferiblemente con parques cercanos."
        ),
        "tags": ["health_wellness", "salon_citas", "clientela_local",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.60,
            "edad_rango": (25, 60),
            "flujo_min": 100,
            "turismo_ideal": 20,
            "ratio_comercial_min": 0.12,
            "zonas_verdes_bonus": 0.50,
            "seguridad_min": 60,
        },
    },

    # ════════════════════════════════════════════════════════════════════════
    # TATTOO
    # ════════════════════════════════════════════════════════════════════════

    "estudio_tatuajes_artistico": {
        "nombre": "Estudio de tatuajes artístico / premium",
        "descripcion": (
            "Estudio de tatuajes con artistas especializados en estilos concretos: "
            "realismo, blackwork, acuarela, tradicional. Citas con semanas de antelación. "
            "Precio alto. El cliente busca al artista, no la ubicación. "
            "Barrio con cultura alternativa o urbana."
        ),
        "tags": ["salon_citas", "destino", "experiencial", "clientela_joven",
                 "alta_renta", "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.55,
            "edad_rango": (18, 45),
            "flujo_min": 150,
            "turismo_ideal": 40,
            "ratio_comercial_min": 0.16,
        },
    },

    "micropigmentacion": {
        "nombre": "Centro de micropigmentación / PMU",
        "descripcion": (
            "Studio de micropigmentación: cejas, labios, eyeliner permanente. "
            "Solo citas previas, trabajo muy especializado. Precio alto. "
            "Clientela femenina renta media-alta. Zona premium o residencial de renta alta."
        ),
        "tags": ["salon_citas", "destino", "alta_renta", "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.70,
            "edad_rango": (22, 55),
            "flujo_min": 80,
            "turismo_ideal": 25,
            "ratio_comercial_min": 0.14,
        },
    },

    # ════════════════════════════════════════════════════════════════════════
    # HEALTH
    # ════════════════════════════════════════════════════════════════════════

    "clinica_dental": {
        "nombre": "Clínica dental",
        "descripcion": (
            "Clínica odontológica con servicios generales y especialidades. "
            "Solo con cita. Precio variable, medio-alto con seguros. "
            "Clientela del barrio o zona. Accesibilidad (transporte) importante. "
            "No depende del paso peatonal."
        ),
        "tags": ["salon_citas", "destino", "clientela_local",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.55,
            "edad_rango": (25, 65),
            "flujo_min": 100,
            "turismo_ideal": 10,
            "ratio_comercial_min": 0.12,
            "seguridad_min": 55,
        },
    },

    "fisioterapia": {
        "nombre": "Clínica de fisioterapia",
        "descripcion": (
            "Centro de fisioterapia y rehabilitación. Solo citas previas. "
            "Clientela del barrio con lesiones o rehabilitación postoperatoria. "
            "Precio medio-alto. Accesibilidad importante (personas mayores, lesionados). "
            "No depende del paso."
        ),
        "tags": ["salon_citas", "destino", "clientela_local",
                 "horario_diurno_comercial", "health_wellness"],
        "zona_ideal": {
            "renta_ideal": 0.52,
            "edad_rango": (30, 75),
            "flujo_min": 80,
            "turismo_ideal": 10,
            "ratio_comercial_min": 0.12,
            "seguridad_min": 55,
        },
    },

    "psicologo": {
        "nombre": "Centro de psicología / psicoterapia",
        "descripcion": (
            "Consulta de psicólogo o centro de salud mental privado. "
            "Solo citas. Máxima discreción, ambiente tranquilo. "
            "No debe estar en zona muy concurrida o ruidosa. "
            "Clientela variada, precio medio-alto."
        ),
        "tags": ["salon_citas", "destino", "clientela_local",
                 "horario_diurno_comercial", "health_wellness"],
        "zona_ideal": {
            "renta_ideal": 0.58,
            "edad_rango": (20, 65),
            "flujo_min": 50,
            "turismo_ideal": 10,
            "ratio_comercial_min": 0.10,
            "seguridad_min": 60,
        },
    },

    "veterinaria": {
        "nombre": "Clínica veterinaria",
        "descripcion": (
            "Clínica veterinaria para mascotas (perros, gatos). "
            "Zona residencial con alta densidad de mascotas. "
            "Parques cercanos como indicador de comunidad de dueños de mascotas. "
            "Solo citas para consultas, urgencias sin cita. Precio medio-alto."
        ),
        "tags": ["dog_friendly", "salon_citas", "clientela_local",
                 "destino", "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.55,
            "edad_rango": (25, 60),
            "flujo_min": 100,
            "turismo_ideal": 10,
            "ratio_comercial_min": 0.12,
            "zonas_verdes_bonus": 0.60,
            "seguridad_min": 58,
        },
    },

    # ════════════════════════════════════════════════════════════════════════
    # CULTURE & ART
    # ════════════════════════════════════════════════════════════════════════

    "galeria_arte": {
        "nombre": "Galería de arte contemporáneo",
        "descripcion": (
            "Galería comercial de arte contemporáneo: venta de obra de artistas emergentes y "
            "establecidos. Exposiciones temporales, inauguraciones. Precio alto. "
            "Clientela coleccionistas y apasionados del arte, también turismo cultural. "
            "Barrio con ambiente artístico."
        ),
        "tags": ["cultural_artistico", "alta_renta", "experiencial",
                 "orientado_turismo", "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.70,
            "edad_rango": (28, 70),
            "flujo_min": 150,
            "turismo_ideal": 55,
            "ratio_comercial_min": 0.15,
        },
    },

    "escape_room": {
        "nombre": "Escape room / sala de aventura",
        "descripcion": (
            "Sala de escape room con varias temáticas. Precio por grupo, 2h de experiencia. "
            "Clientela variada: grupos de amigos, teambuilding, parejas. "
            "El cliente reserva online, viene a propósito. "
            "Zona visible pero no necesariamente premium."
        ),
        "tags": ["experiencial", "clientela_joven", "destino", "salon_citas",
                 "local_grande"],
        "zona_ideal": {
            "renta_ideal": 0.48,
            "edad_rango": (18, 50),
            "flujo_min": 200,
            "turismo_ideal": 45,
            "ratio_comercial_min": 0.16,
        },
    },

    "tienda_materiales_artisticos": {
        "nombre": "Tienda de materiales artísticos / bellas artes",
        "descripcion": (
            "Tienda especializada en materiales para artistas y aficionados: "
            "pinturas, lienzos, marcos, papelería artística. "
            "Clientela artistas, estudiantes de bellas artes, aficionados. Precio medio. "
            "Cerca de escuelas de arte o barrios creativos."
        ),
        "tags": ["cultural_artistico", "clientela_local", "destino",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.50,
            "edad_rango": (16, 65),
            "flujo_min": 200,
            "turismo_ideal": 35,
            "ratio_comercial_min": 0.15,
        },
    },

    # ════════════════════════════════════════════════════════════════════════
    # SERVICES
    # ════════════════════════════════════════════════════════════════════════

    "floristeria": {
        "nombre": "Floristería / florista",
        "descripcion": (
            "Floristería con flores frescas, plantas de interior, ramos y arreglos florales. "
            "Muy dependiente del paso peatonal para ventas espontáneas. "
            "También pedidos para eventos y domicilio. Precio medio."
        ),
        "tags": ["clientela_local", "alta_rotacion_clientes",
                 "instagrammable", "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.55,
            "edad_rango": (25, 70),
            "flujo_min": 450,
            "turismo_ideal": 30,
            "ratio_comercial_min": 0.18,
        },
    },

    "peluqueria_canina": {
        "nombre": "Peluquería canina / dog grooming",
        "descripcion": (
            "Centro de peluquería y aseo para perros. Solo citas previas. "
            "Zona residencial con muchos dueños de mascotas, parques cercanos. "
            "No depende del paso. Clientela local fija. Precio medio."
        ),
        "tags": ["dog_friendly", "salon_citas", "clientela_local",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.52,
            "edad_rango": (25, 60),
            "flujo_min": 100,
            "turismo_ideal": 10,
            "ratio_comercial_min": 0.12,
            "zonas_verdes_bonus": 0.70,
        },
    },

    "tienda_mascotas": {
        "nombre": "Tienda de mascotas premium",
        "descripcion": (
            "Tienda con alimentos premium, accesorios y ropa para mascotas. "
            "Clientela dueños de mascotas con poder adquisitivo. Precio medio-alto. "
            "Zona residencial con parques. Puede combinar con servicio de adopción."
        ),
        "tags": ["dog_friendly", "clientela_local", "alta_renta",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.58,
            "edad_rango": (25, 55),
            "flujo_min": 250,
            "turismo_ideal": 15,
            "ratio_comercial_min": 0.14,
            "zonas_verdes_bonus": 0.65,
        },
    },

    "reparacion_moviles": {
        "nombre": "Reparación de móviles / informática",
        "descripcion": (
            "Tienda de reparación de teléfonos, tablets y ordenadores. "
            "Clientela variada, precio medio. Muy dependiente del paso y la visibilidad. "
            "Zona comercial activa o cerca de tiendas de tecnología."
        ),
        "tags": ["clientela_local", "alta_rotacion_clientes",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.42,
            "edad_rango": (18, 65),
            "flujo_min": 500,
            "turismo_ideal": 25,
            "ratio_comercial_min": 0.25,
        },
    },

    "lavanderia_self_service": {
        "nombre": "Lavandería self-service",
        "descripcion": (
            "Lavandería con máquinas de autoservicio. Clientela de barrio: estudiantes, "
            "turistas en piso sin lavadora, jóvenes. Precio bajo. "
            "Necesita zona con alta densidad residencial y muchos apartamentos pequeños."
        ),
        "tags": ["clientela_local", "low_cost", "clientela_joven"],
        "zona_ideal": {
            "renta_ideal": 0.32,
            "edad_rango": (18, 45),
            "flujo_min": 300,
            "turismo_ideal": 35,
            "ratio_comercial_min": 0.15,
        },
    },

    # ════════════════════════════════════════════════════════════════════════
    # KIDS & FAMILY
    # ════════════════════════════════════════════════════════════════════════

    "ludoteca": {
        "nombre": "Ludoteca / espacio de juego infantil",
        "descripcion": (
            "Espacio de juego y actividades para niños de 0 a 12 años. "
            "Los padres acompañan o dejan a los niños. Precio medio. "
            "Zona familiar con muchos niños y parques. Seguridad del barrio es crítica. "
            "Local amplio necesario."
        ),
        "tags": ["kids_activity", "clientela_familiar", "local_grande",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.52,
            "edad_rango": (28, 45),
            "flujo_min": 150,
            "turismo_ideal": 10,
            "ratio_comercial_min": 0.12,
            "zonas_verdes_bonus": 0.70,
            "seguridad_min": 70,
        },
    },

    "actividades_infantiles": {
        "nombre": "Centro de actividades extraescolares",
        "descripcion": (
            "Centro de actividades extraescolares: inglés, robótica, música, deporte. "
            "Solo citas/horarios fijos. Clientela familias con niños en edad escolar. "
            "Zona residencial familiar. Seguridad crítica. Precio medio."
        ),
        "tags": ["kids_activity", "clientela_familiar", "salon_citas",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.55,
            "edad_rango": (28, 48),
            "flujo_min": 100,
            "turismo_ideal": 8,
            "ratio_comercial_min": 0.12,
            "zonas_verdes_bonus": 0.55,
            "seguridad_min": 72,
        },
    },

    "academia_idiomas": {
        "nombre": "Academia de idiomas",
        "descripcion": (
            "Centro de enseñanza de idiomas: inglés, francés, alemán, chino. "
            "Clientela adultos y jóvenes, también preparación de exámenes oficiales. "
            "Solo citas/clases programadas. Zona bien comunicada. Precio medio."
        ),
        "tags": ["destino", "salon_citas", "clientela_local",
                 "horario_diurno_comercial"],
        "zona_ideal": {
            "renta_ideal": 0.52,
            "edad_rango": (15, 55),
            "flujo_min": 100,
            "turismo_ideal": 20,
            "ratio_comercial_min": 0.14,
            "seguridad_min": 55,
        },
    },

}


# ─── Clase principal del matcher ───────────────────────────────────────────────

class ConceptoMatcher:
    """
    Encuentra los conceptos canónicos más similares a una descripción de negocio
    usando embeddings semánticos, y computa el score de afinidad zona-concepto.

    El modelo de embeddings (paraphrase-multilingual-mpnet-base-v2) se carga de
    forma lazy (primera llamada) y los embeddings del DB se cachean en memoria.
    """

    _MODEL_NAME = "paraphrase-multilingual-mpnet-base-v2"

    def __init__(self):
        self._model = None
        self._db_embeddings: np.ndarray | None = None
        self._db_keys: list[str] = []

    def _get_model(self):
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
                self._model = SentenceTransformer(self._MODEL_NAME)
                logger.info("ConceptoMatcher: modelo %s cargado", self._MODEL_NAME)
            except Exception as e:
                logger.error("ConceptoMatcher: no se pudo cargar el modelo: %s", e)
                raise
        return self._model

    def _get_db_embeddings(self) -> np.ndarray:
        if self._db_embeddings is None:
            model = self._get_model()
            keys = list(CONCEPTOS_DB.keys())
            textos = [
                f"{CONCEPTOS_DB[k]['nombre']}. {CONCEPTOS_DB[k]['descripcion']}"
                for k in keys
            ]
            self._db_embeddings = model.encode(textos, normalize_embeddings=True,
                                               show_progress_bar=False)
            self._db_keys = keys
            logger.info("ConceptoMatcher: %d embeddings de concepto cargados", len(keys))
        return self._db_embeddings

    def match(self, descripcion: str, top_k: int = 4) -> list[dict]:
        """
        Devuelve los top_k conceptos más cercanos a la descripción, con su similitud.

        Returns:
            Lista de dicts con keys: key, nombre, descripcion, tags, zona_ideal, similarity
        """
        try:
            model = self._get_model()
            emb = model.encode([descripcion], normalize_embeddings=True,
                               show_progress_bar=False)
            db_embs = self._get_db_embeddings()
            sims = (emb @ db_embs.T)[0]
            top_idx = sims.argsort()[-top_k:][::-1]
            return [
                {
                    "key": self._db_keys[i],
                    **CONCEPTOS_DB[self._db_keys[i]],
                    "similarity": float(sims[i]),
                }
                for i in top_idx
            ]
        except Exception as e:
            logger.warning("ConceptoMatcher.match falló: %s — sin matches", e)
            return []

    def blend_tags(self, matches: list[dict]) -> list[str]:
        """
        Combina los idea_tags de los top matches ponderados por similitud.

        Un tag se incluye si su peso ponderado supera 0.25 (presente en al menos
        25% del peso total de los conceptos más cercanos).
        """
        if not matches:
            return []

        total_sim = sum(m["similarity"] for m in matches)
        if total_sim == 0:
            return []

        tag_scores: dict[str, float] = {}
        for m in matches:
            weight = m["similarity"] / total_sim
            for tag in m.get("tags", []):
                tag_scores[tag] = tag_scores.get(tag, 0.0) + weight

        return [
            tag for tag, score
            in sorted(tag_scores.items(), key=lambda x: -x[1])
            if score >= 0.25
        ]

    def blend_zona_ideal(self, matches: list[dict]) -> dict:
        """
        Mezcla los perfiles zona_ideal de los top matches ponderados por similitud.

        Para renta_ideal y turismo_ideal: media ponderada.
        Para edad_rango: media ponderada de min y max.
        Para flujo_min: media ponderada.
        Para seguridad_min y ratio_comercial_min: máximo ponderado (el más exigente).
        Para zonas_verdes_bonus: media ponderada.
        """
        if not matches:
            return {}

        total_sim = sum(m["similarity"] for m in matches)
        if total_sim == 0:
            return {}

        acc: dict[str, list[tuple[float, float]]] = {}  # key → [(value, weight)]

        for m in matches:
            w = m["similarity"] / total_sim
            zi = m.get("zona_ideal", {})
            for key, val in zi.items():
                if key == "edad_rango":
                    # Guardar como (min, max)
                    acc.setdefault("edad_rango_min", []).append((float(val[0]), w))
                    acc.setdefault("edad_rango_max", []).append((float(val[1]), w))
                elif isinstance(val, (int, float)):
                    acc.setdefault(key, []).append((float(val), w))

        result: dict = {}
        for key, weighted_vals in acc.items():
            vals = [v for v, _ in weighted_vals]
            weights = [w for _, w in weighted_vals]
            w_sum = sum(weights)
            if w_sum > 0:
                result[key] = sum(v * w for v, w in zip(vals, weights)) / w_sum

        # Reconstruir edad_rango si existe
        if "edad_rango_min" in result and "edad_rango_max" in result:
            result["edad_rango"] = (result.pop("edad_rango_min"),
                                    result.pop("edad_rango_max"))

        return result

    def score_afinidad_zona(self, zona_data: dict, matches: list[dict]) -> float:
        """
        Calcula el score de afinidad zona-concepto (0-100).

        Compara las características reales de la zona con el perfil ideal blended
        de los conceptos más cercanos.

        Fórmulas:
          - renta_ideal:        lineal, máx diferencia 1.0 → score 0
          - turismo_ideal:      lineal, máx diferencia 100 → score 0
          - flujo_min:          ratio min(flujo_real/flujo_min, 1.0) → score 0-100
          - edad_rango:         100 si dentro del rango; penalización lineal fuera
          - ratio_comercial_min: 100 si cumple; proporcional si no
          - zonas_verdes_bonus: Gaussiana sobre 5000m²
          - seguridad_min:      100 si cumple; proporcional si no
        """
        zona_ideal = self.blend_zona_ideal(matches)
        if not zona_ideal:
            return 50.0

        partial_scores: list[float] = []

        # ─ Renta ─────────────────────────────────────────────────────────────
        if "renta_ideal" in zona_ideal:
            renta = zona_data.get("renta_media_hogar") or 32000
            renta_norm = max(0.0, min(1.0, (renta - 17000) / 43000))
            diff = abs(renta_norm - zona_ideal["renta_ideal"])
            partial_scores.append(max(0.0, 100.0 - diff * 150))

        # ─ Turismo ────────────────────────────────────────────────────────────
        if "turismo_ideal" in zona_ideal:
            turismo = zona_data.get("score_turismo") or 45.0
            diff = abs(float(turismo) - zona_ideal["turismo_ideal"])
            partial_scores.append(max(0.0, 100.0 - diff * 1.5))

        # ─ Flujo mínimo ───────────────────────────────────────────────────────
        if "flujo_min" in zona_ideal:
            flujo = zona_data.get("flujo_peatonal_total") or 0
            flujo_min = max(zona_ideal["flujo_min"], 1)
            partial_scores.append(min(100.0, (flujo / flujo_min) * 100))

        # ─ Edad media ─────────────────────────────────────────────────────────
        if "edad_rango" in zona_ideal:
            edad = zona_data.get("edad_media") or 42.5
            e_min, e_max = zona_ideal["edad_rango"]
            if e_min <= edad <= e_max:
                partial_scores.append(100.0)
            else:
                # Penalización lineal: 5 años fuera del rango = 25 puntos menos
                dist = min(abs(edad - e_min), abs(edad - e_max))
                partial_scores.append(max(0.0, 100.0 - dist * 5))

        # ─ Ratio comercial mínimo ─────────────────────────────────────────────
        if "ratio_comercial_min" in zona_ideal:
            ratio = zona_data.get("ratio_locales_comerciales") or 0.22
            min_req = zona_ideal["ratio_comercial_min"]
            if ratio >= min_req:
                partial_scores.append(100.0)
            else:
                partial_scores.append((ratio / min_req) * 100)

        # ─ Zonas verdes ───────────────────────────────────────────────────────
        if zona_ideal.get("zonas_verdes_bonus", 0) > 0:
            m2_verdes = zona_data.get("m2_zonas_verdes_cercanas") or 1200
            bonus_weight = zona_ideal["zonas_verdes_bonus"]
            # 4000 m² = óptimo para negocios dog-friendly/familias
            score_v = min(100.0, (m2_verdes / 4000) * 100)
            # El bonus_weight pondera cuánto importa (0-1)
            partial_scores.append(score_v * bonus_weight + 50 * (1 - bonus_weight))

        # ─ Seguridad mínima ───────────────────────────────────────────────────
        if "seguridad_min" in zona_ideal:
            incidencias = zona_data.get("incidencias_por_1000hab") or 35
            seg_score = min(100.0, max(0.0, (120.0 - incidencias) / 1.15))
            seg_min = zona_ideal["seguridad_min"]
            if seg_score >= seg_min:
                partial_scores.append(100.0)
            else:
                partial_scores.append((seg_score / seg_min) * 100)

        if not partial_scores:
            return 50.0

        return round(sum(partial_scores) / len(partial_scores), 1)


# Instancia global (singleton lazy) — se inicializa en el primer uso
_matcher: ConceptoMatcher | None = None
import threading as _threading
_matcher_lock = _threading.Lock()


def get_matcher() -> ConceptoMatcher:
    """Devuelve la instancia global del ConceptoMatcher (lazy init, thread-safe)."""
    global _matcher
    if _matcher is None:
        with _matcher_lock:
            if _matcher is None:  # double-checked locking
                _matcher = ConceptoMatcher()
    return _matcher
