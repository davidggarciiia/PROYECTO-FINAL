"""
financiero/sector_taxonomy.py — Única fuente de verdad para la clasificación de sectores.

Todos los módulos DEBEN leer desde SECTOR_REGISTRY.
Ningún módulo puede definir su propio mapeo sector→modelo.

Invariantes garantizadas en tiempo de carga (_validate_registry):
  1. pipeline_model es consistente con business_model_type.
  2. El stream primario del gatekeeper coincide con pipeline_model.
  3. No existen sectores con clasificaciones contradictorias entre capas.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

logger = logging.getLogger(__name__)

BusinessModelType = Literal["retail_walkin", "appointment_based", "restaurant", "hybrid"]
PipelineModel     = Literal["traffic", "labor", "space"]
StreamType        = Literal["traffic", "labor", "space"]
Priority          = Literal["primary", "secondary"]

# Versión del registro. Incrementar cuando se añadan/modifiquen sectores,
# para que el código de caché pueda detectar registros obsoletos.
TAXONOMY_VERSION: int = 1

# Mapeo canónico: business_model_type → pipeline_model esperado.
# Si esta tabla y un SectorProfile difieren → error en tiempo de carga.
BMT_TO_PIPELINE_MODEL: dict[str, PipelineModel] = {
    "retail_walkin":     "traffic",
    "appointment_based": "labor",
    "restaurant":        "space",
    "hybrid":            "space",
}


@dataclass(frozen=True)
class SectorProfile:
    """
    Clasificación completa e inmutable de un sector.

    Invariante: pipeline_model == BMT_TO_PIPELINE_MODEL[business_model_type]
    Invariante: el stream primario de gatekeeper_streams == pipeline_model
    Ambas son verificadas en tiempo de carga por _validate_registry().
    """
    business_model_type: str           # estimador / API / correcciones — source of truth
    pipeline_model:      str           # validador_pipeline.py — DERIVADO de business_model_type
    gatekeeper_streams:  tuple         # ((StreamType, Priority), ...) — gatekeeper.py
    sector_display:      str
    subsector_display:   str
    cnae:                str
    tipo_coste:          str           # "stock" | "servicio"
    pipeline_confidence: float


# ── Registro ──────────────────────────────────────────────────────────────────
# Un sector, una entrada. Todo lo que antes estaba en _BUSINESS_MODEL_MAP,
# _SECTOR_STREAMS y _TAXONOMY ahora vive aquí.

SECTOR_REGISTRY: dict[str, SectorProfile] = {
    # ── Restauración (space: aforo × rotación) ────────────────────────────────
    "restauracion": SectorProfile(
        business_model_type="restaurant",  pipeline_model="space",
        gatekeeper_streams=(("space", "primary"), ("traffic", "secondary")),
        sector_display="Restauración",     subsector_display="Restauración casual",
        cnae="5610", tipo_coste="stock",   pipeline_confidence=0.95,
    ),
    "bar": SectorProfile(
        business_model_type="restaurant",  pipeline_model="space",
        gatekeeper_streams=(("space", "primary"), ("traffic", "secondary")),
        sector_display="Restauración",     subsector_display="Bar / pub",
        cnae="5630", tipo_coste="stock",   pipeline_confidence=0.90,
    ),
    "cafeteria": SectorProfile(
        business_model_type="restaurant",  pipeline_model="space",
        gatekeeper_streams=(("space", "primary"), ("traffic", "secondary")),
        sector_display="Restauración",     subsector_display="Cafetería / pastelería",
        cnae="5630", tipo_coste="stock",   pipeline_confidence=0.90,
    ),
    "shisha_lounge": SectorProfile(
        business_model_type="hybrid",      pipeline_model="space",
        gatekeeper_streams=(("space", "primary"),),
        sector_display="Ocio y entretenimiento", subsector_display="Shisha lounge",
        cnae="5630", tipo_coste="stock",   pipeline_confidence=0.85,
    ),
    "deporte": SectorProfile(
        business_model_type="hybrid",      pipeline_model="space",
        gatekeeper_streams=(("space", "primary"),),
        sector_display="Deporte",          subsector_display="Centro deportivo",
        cnae="9313", tipo_coste="servicio", pipeline_confidence=0.80,
    ),
    # ── Appointment-based (labor: personal × horas / duración) ───────────────
    "tatuajes": SectorProfile(
        business_model_type="appointment_based", pipeline_model="labor",
        gatekeeper_streams=(("labor", "primary"),),
        sector_display="Belleza y estética",  subsector_display="Tatuajes y piercings",
        cnae="9602", tipo_coste="servicio",   pipeline_confidence=0.93,
    ),
    "estetica": SectorProfile(
        business_model_type="appointment_based", pipeline_model="labor",
        gatekeeper_streams=(("labor", "primary"),),
        sector_display="Belleza y estética",  subsector_display="Estética y tratamientos",
        cnae="9602", tipo_coste="servicio",   pipeline_confidence=0.92,
    ),
    "peluqueria": SectorProfile(
        business_model_type="appointment_based", pipeline_model="labor",
        gatekeeper_streams=(("labor", "primary"),),
        sector_display="Belleza y estética",  subsector_display="Peluquería",
        cnae="9602", tipo_coste="servicio",   pipeline_confidence=0.95,
    ),
    "clinica": SectorProfile(
        business_model_type="appointment_based", pipeline_model="labor",
        gatekeeper_streams=(("labor", "primary"),),
        sector_display="Salud",               subsector_display="Clínica médica",
        cnae="8690", tipo_coste="servicio",   pipeline_confidence=0.90,
    ),
    "salud": SectorProfile(
        business_model_type="appointment_based", pipeline_model="labor",
        gatekeeper_streams=(("labor", "primary"),),
        sector_display="Salud",               subsector_display="Servicios de salud",
        cnae="8690", tipo_coste="servicio",   pipeline_confidence=0.85,
    ),
    "fisioterapia": SectorProfile(
        business_model_type="appointment_based", pipeline_model="labor",
        gatekeeper_streams=(("labor", "primary"),),
        sector_display="Salud",               subsector_display="Fisioterapia",
        cnae="8621", tipo_coste="servicio",   pipeline_confidence=0.92,
    ),
    "dentista": SectorProfile(
        business_model_type="appointment_based", pipeline_model="labor",
        gatekeeper_streams=(("labor", "primary"),),
        sector_display="Salud",               subsector_display="Clínica dental",
        cnae="8621", tipo_coste="servicio",   pipeline_confidence=0.92,
    ),
    "educacion": SectorProfile(
        business_model_type="appointment_based", pipeline_model="labor",
        gatekeeper_streams=(("labor", "primary"), ("space", "secondary")),
        sector_display="Educación",           subsector_display="Academia / formación",
        cnae="8559", tipo_coste="servicio",   pipeline_confidence=0.80,
    ),
    # ── Retail walk-in (traffic: flujo peatonal × conversión) ─────────────────
    "moda": SectorProfile(
        business_model_type="retail_walkin", pipeline_model="traffic",
        gatekeeper_streams=(("traffic", "primary"),),
        sector_display="Retail moda",         subsector_display="Moda generalista",
        cnae="4771", tipo_coste="stock",      pipeline_confidence=0.92,
    ),
    "supermercado": SectorProfile(
        business_model_type="retail_walkin", pipeline_model="traffic",
        gatekeeper_streams=(("traffic", "primary"),),
        sector_display="Retail alimentación", subsector_display="Supermercado / bazar",
        cnae="4711", tipo_coste="stock",      pipeline_confidence=0.95,
    ),
    "farmacia": SectorProfile(
        business_model_type="retail_walkin", pipeline_model="traffic",
        gatekeeper_streams=(("traffic", "primary"),),
        sector_display="Salud y farmacia",    subsector_display="Farmacia",
        cnae="4773", tipo_coste="stock",      pipeline_confidence=0.95,
    ),
    "electronica": SectorProfile(
        business_model_type="retail_walkin", pipeline_model="traffic",
        gatekeeper_streams=(("traffic", "primary"),),
        sector_display="Retail tecnología",   subsector_display="Electrónica / telefonía",
        cnae="4742", tipo_coste="stock",      pipeline_confidence=0.88,
    ),
    "alimentacion": SectorProfile(
        business_model_type="retail_walkin", pipeline_model="traffic",
        gatekeeper_streams=(("traffic", "primary"),),
        sector_display="Alimentación",        subsector_display="Tienda alimentación",
        cnae="4711", tipo_coste="stock",      pipeline_confidence=0.88,
    ),
}

_DEFAULT_PROFILE = SectorProfile(
    business_model_type="retail_walkin", pipeline_model="traffic",
    gatekeeper_streams=(("traffic", "primary"),),
    sector_display="Comercio / Servicios",  subsector_display="Otro",
    cnae="4799", tipo_coste="stock",        pipeline_confidence=0.50,
)


# ── API pública ────────────────────────────────────────────────────────────────

def get_sector_profile(sector: str) -> SectorProfile:
    """Devuelve el SectorProfile para un código de sector, o el perfil por defecto."""
    return SECTOR_REGISTRY.get(sector, _DEFAULT_PROFILE)


def validate_model_coherence(sector: str, business_model_type: str) -> None:
    """
    Verifica que business_model_type sea consistente con el pipeline_model del registro.

    Registra CRITICAL si hay mismatch. No lanza excepción para no bloquear producción,
    pero el mismatch debe investigarse inmediatamente (indica bug en clasificación o subsector
    que sobreescribió business_model_type de forma incorrecta).
    """
    profile = get_sector_profile(sector)
    expected_bmt = profile.business_model_type
    expected_model = BMT_TO_PIPELINE_MODEL.get(business_model_type, "traffic")
    actual_model   = profile.pipeline_model

    if actual_model != expected_model:
        logger.critical(
            "MODEL_COHERENCE_ERROR sector=%r: business_model_type=%r requiere "
            "pipeline_model=%r pero el registro tiene %r. "
            "Verificar SECTOR_REGISTRY en financiero/sector_taxonomy.py "
            "o la lógica de subsector en aplicar_subsector().",
            sector, business_model_type, expected_model, actual_model,
        )

    if business_model_type != expected_bmt:
        logger.warning(
            "MODEL_DRIFT sector=%r: registry.business_model_type=%r pero el estimador "
            "derivó %r (subsector o benchmark sobreescribió la clasificación canónica).",
            sector, expected_bmt, business_model_type,
        )


# ── Validación en tiempo de carga ─────────────────────────────────────────────
# Si esta función lanza AssertionError, el módulo no carga. El error se ve en el
# arranque del servidor — es intencional: un registro inválido es un bug de código,
# no un error de runtime que deba capturarse silenciosamente.

def _validate_registry() -> None:
    errors: list[str] = []
    for code, p in SECTOR_REGISTRY.items():
        expected_model = BMT_TO_PIPELINE_MODEL.get(p.business_model_type)
        if p.pipeline_model != expected_model:
            errors.append(
                f"  [{code}] business_model_type={p.business_model_type!r} "
                f"requiere pipeline_model={expected_model!r}, tiene {p.pipeline_model!r}"
            )
        primary = [s for s, pri in p.gatekeeper_streams if pri == "primary"]
        if not primary:
            errors.append(f"  [{code}] sin stream primario en gatekeeper_streams")
        elif primary[0] != p.pipeline_model:
            errors.append(
                f"  [{code}] stream primario={primary[0]!r} "
                f"no coincide con pipeline_model={p.pipeline_model!r}"
            )
    if errors:
        raise AssertionError(
            "SECTOR_REGISTRY contiene entradas incoherentes:\n" + "\n".join(errors)
        )


_validate_registry()
