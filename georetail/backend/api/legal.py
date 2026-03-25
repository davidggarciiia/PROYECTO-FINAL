"""
api/legal.py — GET /api/legal/{sector_codigo}

Devuelve los requisitos legales completos para un sector en Barcelona.

Se llama desde:
  1. El botón "Saber más" cuando /api/buscar devuelve estado "inviable_legal".
  2. El panel de detalle de zona — sección "Requisitos legales".
  3. Flujo pre-búsqueda si el usuario quiere ver la info antes de buscar zonas.

Fuentes de datos:
  - Tabla `requisitos_legales_sector` (PostgreSQL) — licencias, requisitos del local.
    [ESTADO: PENDIENTE — tabla aún no creada. Se usa el dict estático `_SECTORES`
    como fuente de verdad temporal hasta que se migre a BD.]

  - Tabla `restricciones_geograficas_sector` (PostgreSQL) — restricciones de
    densidad y distancia de los Planes de Usos de Barcelona.
    [ESTADO: PENDIENTE — ídem.]

Migración a BD:
  Cuando las tablas existan, `_get_sector_from_db()` leerá de allí.
  El dict `_SECTORES` pasará a ser solo el fallback.
  No hace falta cambiar el endpoint ni los tipos de respuesta.

Fuentes de referencia para los datos:
  - OMAIIA 2024 (Ordenança Municipal d'Activitats i Intervenció Integral Ambiental)
  - Decret 90/2008 (requisitos para estudios de tatuaje)
  - Ley 28/2005 (ley antitabaco — clubs privados de fumadores)
  - Planes de Usos de Barcelona (Eixample 2023, Sant Martí 2025, Ciutat Vella 2017)
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from schemas.models import (
    LicenciaNecesaria, RestriccionGeografica, ViabilidadLegal, ModeloLegal,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["legal"])


# ─── Response ─────────────────────────────────────────────────────────────────

class LegalResponse(BaseModel):
    sector_codigo: str
    nombre: str

    # Viabilidad general del sector — independiente de la zona concreta
    viabilidad: ViabilidadLegal
    alerta: Optional[str] = None

    # Modelo jurídico especial (si aplica)
    # None → empresa/autónomo estándar
    # "club_privado_fumadores" → asociación sin ánimo de lucro
    # "autorizacion_previa"   → autorización sectorial previa a la municipal
    modelo_legal: Optional[ModeloLegal] = None

    # Trámites y licencias necesarios para abrir
    licencias_necesarias: list[LicenciaNecesaria]

    # Requisitos del local físico
    requisitos_local: list[str]

    # Requisitos operativos (personal, seguro, registros, etc.)
    requisitos_operativos: list[str]

    # Restricciones geográficas en Barcelona (densidad, distancia)
    restricciones_geograficas: list[RestriccionGeografica]

    # Aviso legal obligatorio — mostrar siempre en la UI
    disclaimer: str


# ─── Datos estáticos (fallback hasta migración a BD) ─────────────────────────

_DISCLAIMER = (
    "Esta información es orientativa y está actualizada a marzo de 2026. "
    "No constituye asesoramiento jurídico. "
    "Consulta con un gestor especializado antes de firmar cualquier contrato."
)

_SECTORES: dict[str, dict] = {

    # ── Restauración ─────────────────────────────────────────────────────────
    # Fuente: OMAIIA 2024, Planes de Usos BCN
    "restauracion": {
        "nombre": "Restauración (bar, cafetería, restaurante)",
        "viabilidad": "viable",
        "alerta": None,
        "modelo_legal": None,
        "licencias_necesarias": [
            {
                "nombre": "Informe de viabilitat urbanística prèvia (IVU)",
                "organismo": "Ajuntament de Barcelona — OAE",
                "coste_estimado": "0 € (tràmit online)",
                "tiempo_semanas": "1–4",
                "notas": (
                    "Obligatorio antes de iniciar obras o tramitar licencia de actividad. "
                    "Confirma que el uso está permitido en la parcela."
                ),
            },
            {
                "nombre": "Licencia de actividad (Annexe III.2 OMAIIA)",
                "organismo": "Ajuntament de Barcelona + EAC (entitat de control ambiental)",
                "coste_estimado": "2.500–4.000 €",
                "tiempo_semanas": "4–8",
                "notas": (
                    "Requiere proyecto técnico firmado por arquitecto o ingeniero. "
                    "La EAC inspecciona el local antes de emitir el certificado."
                ),
            },
            {
                "nombre": "Proyecto eléctrico — pública concurrencia",
                "organismo": "Colegio oficial de ingenieros industriales",
                "coste_estimado": "600–1.200 €",
                "tiempo_semanas": "1–2",
            },
        ],
        "requisitos_local": [
            "Planta baja (PB) obligatoria para actividad de restauración",
            "Altura libre mínima 2,5m en zona de clientes y 2,1m en almacén y aseos",
            "Almacén mínimo 5m²",
            "Cuarto de residuos proporcional a la superficie (mínimo 3,25m² para 50-100m²)",
            "Salida de humos hasta cubierta del edificio (no a fachada ni patio)",
            "Sistema de ventilación forzada certificado",
            "Aislamiento acústico si hay viviendas en plantas superiores",
            "Aseos para clientes (obligatorio si aforo > 50 pax)",
        ],
        "requisitos_operativos": [
            "Carnet de manipulador de alimentos para todo el personal en cocina",
            "Plan de autocontrol APPCC documentado y actualizado",
            "Seguro de responsabilidad civil obligatorio (mínimo 300.000 €)",
            "Registro sanitario si elabora alimentos para distribución",
        ],
        "restricciones_geograficas": [
            {
                "tipo": "densidad",
                "descripcion": (
                    "Eixample Zona A (calles pacificadas + Sant Antoni): "
                    "máx 5 establecimientos en radio 50m, máx 18 en radio 100m, "
                    "distancia mínima 25m entre locales del mismo tipo."
                ),
                "radio_metros": 50,
                "obligatorio_verificar": True,
            },
            {
                "tipo": "densidad",
                "descripcion": (
                    "Eixample Zona B (resto del Eixample): "
                    "máx 20 establecimientos en radio 100m."
                ),
                "radio_metros": 100,
                "obligatorio_verificar": True,
            },
            {
                "tipo": "densidad",
                "descripcion": (
                    "Sant Martí (vigente desde dic 2025): "
                    "máx 10 establecimientos en radio 100m, distancia mínima 25m. "
                    "Discotecas: 400m entre sí, máx 150m²."
                ),
                "radio_metros": 100,
                "obligatorio_verificar": True,
            },
            {
                "tipo": "prohibicion",
                "descripcion": (
                    "Ciutat Vella: moratoria general activa desde 2017. "
                    "Prácticamente no se conceden nuevas licencias. "
                    "Verificar con el Ajuntament antes de seleccionar cualquier local."
                ),
                "radio_metros": None,
                "obligatorio_verificar": True,
            },
        ],
    },

    # ── Tatuajes ──────────────────────────────────────────────────────────────
    # Fuente: Decret 90/2008, OMAIIA 2024
    "tatuajes": {
        "nombre": "Estudio de tatuajes, piercing y micropigmentación",
        "viabilidad": "viable",
        "alerta": None,
        "modelo_legal": None,
        "licencias_necesarias": [
            {
                "nombre": "Informe de viabilitat urbanística prèvia (IVU)",
                "organismo": "Ajuntament de Barcelona — OAE",
                "coste_estimado": "0 € (tràmit online)",
                "tiempo_semanas": "1–4",
            },
            {
                "nombre": "Licencia de actividad (Annexe III.2 OMAIIA)",
                "organismo": "Ajuntament de Barcelona + EAC",
                "coste_estimado": "2.000–3.500 €",
                "tiempo_semanas": "4–8",
                "notas": (
                    "Se clasifica como Annexe III.2 por el riesgo sanitario. "
                    "La EAC verifica la instalación de autoclave y superficies."
                ),
            },
            {
                "nombre": "Registro sanitario de establecimientos de tatuaje",
                "organismo": "ASPCAT — Agència de Salut Pública de Catalunya",
                "coste_estimado": "0 € (tràmit online)",
                "tiempo_semanas": "2–4",
                "notas": "Obligatorio antes de abrir. Renovación anual.",
            },
            {
                "nombre": "Proyecto eléctrico — pública concurrencia",
                "organismo": "Colegio oficial de ingenieros",
                "coste_estimado": "600–1.200 €",
                "tiempo_semanas": "1–2",
            },
        ],
        "requisitos_local": [
            "Zona de trabajo diferenciada con lavabo de agua caliente y fría propio",
            "Superficies lisas, impermeables y lavables (suelo, paredes, mobiliario de trabajo)",
            "Esterilizador de instrumental tipo autoclave clase B (obligatorio)",
            "Sistema de gestión de residuos sanitarios grupo II (agujas, etc.)",
            "Iluminación mínima 500 lux en zona de trabajo",
            "Ventilación natural o forzada documentada",
        ],
        "requisitos_operativos": [
            "Diploma en higiene y seguridad en tatuaje para cada profesional "
            "(Decret 90/2008, Generalitat de Catalunya — curso mínimo 80h)",
            "Protocolo de esterilización documentado y actualizado",
            "Registro de incidencias sanitarias",
            "Trazabilidad de tintas y pigmentos (lote, fecha, proveedor)",
            "Seguro de responsabilidad civil (mínimo 150.000 € por siniestro)",
            "Consentimiento informado firmado por cada cliente antes del procedimiento",
        ],
        "restricciones_geograficas": [],  # Sin restricción de densidad específica
    },

    # ── Shisha Lounge / Club privado de fumadores ─────────────────────────────
    # Fuente: Ley 28/2005 (antitabaco), OMAIIA 2024, Ley 13/2017 (cannabis Catalunya)
    # IMPORTANTE: cachimbería de bar abierto → ILEGAL en interiores desde 2011.
    # El único modelo legal para consumo de tabaco en interior → club privado de fumadores.
    "shisha_lounge": {
        "nombre": "Shisha Lounge / Club privado de fumadores",
        "viabilidad": "restringido",
        "alerta": (
            "Una cachimbería tradicional abierta al público general es ILEGAL en España "
            "desde la reforma de la Ley 28/2005 (prohibición de fumar en interiores). "
            "El único modelo legal viable es el club privado de fumadores: "
            "asociación sin ánimo de lucro donde solo pueden entrar socios registrados. "
            "Es el modelo que operan todos los shisha lounges activos en Barcelona "
            "(Ivy Shisha Lounge, VIP Room, La Shisha Room, etc.)."
        ),
        "modelo_legal": "club_privado_fumadores",
        "licencias_necesarias": [
            {
                "nombre": "Constitución de asociación sin ánimo de lucro",
                "organismo": "Registro de Asociaciones — Generalitat de Catalunya",
                "coste_estimado": "0–200 € (tasas de registro)",
                "tiempo_semanas": "4–8",
                "notas": (
                    "Requiere: estatutos, acta fundacional, mínimo 3 socios fundadores "
                    "mayores de 21 años que acrediten ser fumadores habituales. "
                    "El titular de la licencia es la asociación, NO una SL ni un autónomo."
                ),
            },
            {
                "nombre": "Informe de viabilitat urbanística prèvia (IVU)",
                "organismo": "Ajuntament de Barcelona — OAE",
                "coste_estimado": "0 € (tràmit online)",
                "tiempo_semanas": "1–4",
                "notas": "Verificar que el uso 'otros servicios terciarios' está permitido en la parcela.",
            },
            {
                "nombre": "Licencia de actividad de club de fumadores (Annexe III.2)",
                "organismo": "Ajuntament de Barcelona + EAC",
                "coste_estimado": "2.000–3.500 €",
                "tiempo_semanas": "4–8",
                "notas": (
                    "Se clasifica urbanísticamente como 'uso terciario — otros servicios terciarios'. "
                    "Si hay emisión de música con DJs o supera umbrales de sonido, "
                    "necesita estudio acústico adicional."
                ),
            },
            {
                "nombre": "Proyecto eléctrico — pública concurrencia",
                "organismo": "Colegio oficial de ingenieros",
                "coste_estimado": "600–1.200 €",
                "tiempo_semanas": "1–2",
            },
        ],
        "requisitos_local": [
            "Local completamente independiente y cerrado (sin acceso directo desde bar o restaurante)",
            "Sin publicidad exterior visible desde la calle que identifique la actividad",
            "Sistema de ventilación dimensionado para el consumo de tabaco "
            "(salida de aire sin afectar a vecinos — requiere estudio de ingeniero)",
            "Control de acceso físico en la entrada (solo socios)",
            "Señalización interior 'Prohibida la entrada a menores de edad'",
            "Cartel identificativo con el nombre de la asociación",
        ],
        "requisitos_operativos": [
            "El titular de la licencia y de la actividad debe ser la asociación (NIF de la asociación)",
            "Los socios fundadores deben acreditar documentalmente que ya eran fumadores "
            "habituales antes de la constitución de la asociación",
            "No se puede comercializar ni vender productos consumibles (tabaco, bebidas, comida): "
            "se gestiona como consumo propio de los socios dentro de la sede social",
            "Registro físico de socios con DNI/NIE actualizado — obligatorio en cada visita",
            "No se puede admitir público esporádico, turistas ni personas sin proceso de "
            "asociación previo y formal",
            "Solo pueden estar en el interior personas asociadas (no acompañantes externos)",
            "Prohibición absoluta de acceso a menores de 18 años",
        ],
        "restricciones_geograficas": [
            {
                "tipo": "distancia",
                "descripcion": (
                    "Algunos distritos restringen la apertura cerca de centros educativos "
                    "u hospitales. La distancia exacta varía — verificar con el Ajuntament "
                    "de Barcelona antes de firmar cualquier contrato de alquiler."
                ),
                "radio_metros": None,
                "obligatorio_verificar": True,
            },
        ],
    },

    # ── Moda y retail ─────────────────────────────────────────────────────────
    # Fuente: OMAIIA 2024 (régimen de comunicación previa para < 120m²)
    "moda": {
        "nombre": "Moda, ropa, complementos y accesorios",
        "viabilidad": "viable",
        "alerta": None,
        "modelo_legal": None,
        "licencias_necesarias": [
            {
                "nombre": "Comunicació prèvia d'activitat (OMAIIA — règim general)",
                "organismo": "Ajuntament de Barcelona — OAE",
                "coste_estimado": "200–500 €",
                "tiempo_semanas": "0 (apertura inmediata tras presentar la documentación)",
                "notas": (
                    "Válido para locales < 120m² sin actividades clasificadas. "
                    "El Ajuntament puede inspeccionar a posteriori. "
                    "Para locales ≥ 120m² → Annexe III.2 (proyecto técnico obligatorio)."
                ),
            },
        ],
        "requisitos_local": [
            "Ancho de puerta mínimo 80cm (accesibilidad universal)",
            "Aseo adaptado para PMR si hay probadores con más de 3 cabinas",
            "Señal de salida de emergencia si el fondo del local > 25m",
        ],
        "requisitos_operativos": [
            "Hoja de reclamaciones visible en el punto de venta",
            "Seguro de responsabilidad civil recomendado (no obligatorio en retail < 120m²)",
            "Registro de caja y obligaciones fiscales como actividad económica",
        ],
        "restricciones_geograficas": [],
    },

    # ── Estética y belleza ────────────────────────────────────────────────────
    # Fuente: OMAIIA 2024, normativa sanitaria Generalitat para aparatología
    "estetica": {
        "nombre": "Peluquería, estética, spa y centros de belleza",
        "viabilidad": "viable",
        "alerta": None,
        "modelo_legal": None,
        "licencias_necesarias": [
            {
                "nombre": "Comunicació prèvia d'activitat (OMAIIA — règim general)",
                "organismo": "Ajuntament de Barcelona — OAE",
                "coste_estimado": "200–500 €",
                "tiempo_semanas": "0",
                "notas": "Para locales < 120m² sin aparatología médica.",
            },
            {
                "nombre": "Autorización sanitaria — aparatos de uso estético",
                "organismo": "Departament de Salut, Generalitat de Catalunya",
                "coste_estimado": "0–300 €",
                "tiempo_semanas": "4–12",
                "notas": (
                    "Solo obligatoria si se usan: láser clase IV, "
                    "IPL intensa pulsada, radiofrecuencia médica, ultrasonidos focalizados (HIFU), "
                    "o cualquier equipo clasificado como producto sanitario clase IIa o superior."
                ),
            },
        ],
        "requisitos_local": [
            "Lavabo de agua caliente y fría en zona de tratamientos",
            "Superficies lavables e impermeables en puestos de trabajo",
            "Almacenamiento separado de productos químicos (tintes, decolorantes, etc.)",
            "Ventilación suficiente para evacuación de vapores de productos",
        ],
        "requisitos_operativos": [
            "Titulación en peluquería (FP) o estética (FP) para el responsable técnico",
            "Seguro de responsabilidad civil obligatorio si se realizan tratamientos invasivos",
            "Protocolo de higiene y esterilización de instrumental documentado",
            "Registro de incidencias adversas si se usa aparatología sanitaria",
        ],
        "restricciones_geograficas": [],
    },
}


# ─── Endpoint ────────────────────────────────────────────────────────────────

@router.get(
    "/legal/{sector_codigo}",
    response_model=LegalResponse,
    summary="Requisitos legales completos para abrir un tipo de negocio en Barcelona",
)
async def get_legal(sector_codigo: str) -> LegalResponse:
    """
    Devuelve el marco legal completo para el sector solicitado.

    Sectores disponibles:
      - restauracion  → bar, cafetería, restaurante
      - tatuajes      → estudio de tatuajes, piercing, micropigmentación
      - shisha_lounge → cachimbería, shisha bar, club privado de fumadores
      - moda          → ropa, accesorios, complementos
      - estetica      → peluquería, spa, clínica de belleza

    Flujo típico:
      1. /api/buscar devuelve estado="inviable_legal" (ej: cachimbería)
      2. Frontend muestra el motivo y un botón "Saber más"
      3. Frontend llama GET /api/legal/shisha_lounge
      4. Se muestra el marco legal completo con el modelo alternativo viable
    """
    datos = _SECTORES.get(sector_codigo)

    if datos is None:
        raise HTTPException(
            status_code=404,
            detail="Sector no reconocido o no disponible.",
        )

    return LegalResponse(
        sector_codigo=sector_codigo,
        nombre=datos["nombre"],
        viabilidad=ViabilidadLegal(datos["viabilidad"]),
        alerta=datos.get("alerta"),
        modelo_legal=ModeloLegal(datos["modelo_legal"]) if datos.get("modelo_legal") else None,
        licencias_necesarias=[LicenciaNecesaria(**lic) for lic in datos["licencias_necesarias"]],
        requisitos_local=datos["requisitos_local"],
        requisitos_operativos=datos["requisitos_operativos"],
        restricciones_geograficas=[
            RestriccionGeografica(**r) for r in datos["restricciones_geograficas"]
        ],
        disclaimer=_DISCLAIMER,
    )
