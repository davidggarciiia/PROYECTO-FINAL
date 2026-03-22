"""
scoring/perfil_negocio.py — Perfil numérico del concepto de negocio.

8 dimensiones continuas (0.0-1.0) que describen el ADN de la idea de negocio.

Ejemplos:
  - Specialty coffee:  dependencia_flujo=0.35, nivel_precio=0.70, clientela_turismo=0.15
  - Fine dining:       dependencia_flujo=0.15, nivel_precio=0.95, clientela_turismo=0.50
  - Cafetería express: dependencia_flujo=0.90, nivel_precio=0.30, clientela_turismo=0.35

Estas features NO se añaden aún al vector XGBoost (no tenemos datos históricos
etiquetados con ellas). Se usan para:
  1. score_afinidad_zona: cuánto encaja la zona con el concepto específico
  2. Ajuste de pesos en _recalcular_global (ya lo hace idea_tags)
  3. Futuro: cuando negocios_historico tenga descripciones → reentrenar con estas features

Para datos históricos en scoring/dataset.py se usa DEFAULTS_POR_SECTOR como
aproximación por sector hasta que haya datos reales.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import numpy as np

# ── Nombres de las 8 features de perfil ──────────────────────────────────────
FEATURE_NAMES_PERFIL = [
    "negocio_dependencia_flujo",      # 1=necesita paso peatonal alto, 0=cliente de destino
    "negocio_nivel_precio",           # 0=low cost/presupuesto, 1=premium/lujo
    "negocio_clientela_turismo",      # 1=vive de turistas, 0=solo clientes locales
    "negocio_clientela_vecindario",   # 1=barrio residencial, 0=atrae de toda la ciudad
    "negocio_horario_nocturno",       # 1=foco nocturno/tardeo, 0=solo diurno
    "negocio_experiencial",           # 1=la experiencia es el producto (no transaccional)
    "negocio_citas_previas",          # 1=funciona solo con cita, 0=walk-in puro
    "negocio_sensibilidad_alquiler",  # 1=muy sensible al coste, 0=puede pagar más por ubicación
]

_N = 0.5  # valor neutro por defecto


@dataclass
class PerfilNegocio:
    """
    Perfil numérico del concepto de negocio. Todos los valores en [0.0, 1.0].

    No representa la zona, sino QUÉ NECESITA el negocio de su ubicación.
    """
    dependencia_flujo:     float = _N
    nivel_precio:          float = _N
    clientela_turismo:     float = 0.30
    clientela_vecindario:  float = _N
    horario_nocturno:      float = 0.10
    experiencial:          float = 0.20
    citas_previas:         float = 0.20
    sensibilidad_alquiler: float = _N

    def to_array(self) -> np.ndarray:
        return np.array([
            self.dependencia_flujo,
            self.nivel_precio,
            self.clientela_turismo,
            self.clientela_vecindario,
            self.horario_nocturno,
            self.experiencial,
            self.citas_previas,
            self.sensibilidad_alquiler,
        ], dtype=np.float32)

    @classmethod
    def from_dict(cls, d: dict) -> "PerfilNegocio":
        def _clip(v, default: float = _N) -> float:
            try:
                return max(0.0, min(1.0, float(v)))
            except (TypeError, ValueError):
                return default

        return cls(
            dependencia_flujo=    _clip(d.get("dependencia_flujo")),
            nivel_precio=         _clip(d.get("nivel_precio")),
            clientela_turismo=    _clip(d.get("clientela_turismo"),    0.30),
            clientela_vecindario= _clip(d.get("clientela_vecindario")),
            horario_nocturno=     _clip(d.get("horario_nocturno"),     0.10),
            experiencial=         _clip(d.get("experiencial"),         0.20),
            citas_previas=        _clip(d.get("citas_previas"),        0.20),
            sensibilidad_alquiler=_clip(d.get("sensibilidad_alquiler")),
        )

    def to_dict(self) -> dict:
        return {
            "dependencia_flujo":     round(self.dependencia_flujo, 3),
            "nivel_precio":          round(self.nivel_precio, 3),
            "clientela_turismo":     round(self.clientela_turismo, 3),
            "clientela_vecindario":  round(self.clientela_vecindario, 3),
            "horario_nocturno":      round(self.horario_nocturno, 3),
            "experiencial":          round(self.experiencial, 3),
            "citas_previas":         round(self.citas_previas, 3),
            "sensibilidad_alquiler": round(self.sensibilidad_alquiler, 3),
        }


# ── Defaults por sector (para datos históricos sin descripción) ────────────────
# Representan el perfil MEDIANO del sector en Barcelona.
# Aproximación inicial para scoring/dataset.py. Cuando negocios_historico
# tenga columnas de descripción, reemplazar con perfiles reales extraídos por LLM.
DEFAULTS_POR_SECTOR: dict[str, PerfilNegocio] = {
    "restauracion": PerfilNegocio(
        dependencia_flujo=0.70,     # la mayoría depende del paso
        nivel_precio=0.45,
        clientela_turismo=0.35,
        clientela_vecindario=0.50,
        horario_nocturno=0.30,
        experiencial=0.30,
        citas_previas=0.10,
        sensibilidad_alquiler=0.60,
    ),
    "moda": PerfilNegocio(
        dependencia_flujo=0.75,
        nivel_precio=0.50,
        clientela_turismo=0.25,
        clientela_vecindario=0.40,
        horario_nocturno=0.00,
        experiencial=0.20,
        citas_previas=0.00,
        sensibilidad_alquiler=0.55,
    ),
    "estetica": PerfilNegocio(
        dependencia_flujo=0.30,
        nivel_precio=0.55,
        clientela_turismo=0.10,
        clientela_vecindario=0.80,
        horario_nocturno=0.00,
        experiencial=0.40,
        citas_previas=0.80,
        sensibilidad_alquiler=0.50,
    ),
    "tatuajes": PerfilNegocio(
        dependencia_flujo=0.40,
        nivel_precio=0.55,
        clientela_turismo=0.20,
        clientela_vecindario=0.50,
        horario_nocturno=0.00,
        experiencial=0.70,
        citas_previas=0.90,
        sensibilidad_alquiler=0.50,
    ),
    "shisha_lounge": PerfilNegocio(
        dependencia_flujo=0.40,
        nivel_precio=0.40,
        clientela_turismo=0.30,
        clientela_vecindario=0.60,
        horario_nocturno=0.90,
        experiencial=0.80,
        citas_previas=0.00,
        sensibilidad_alquiler=0.60,
    ),
    "salud": PerfilNegocio(
        dependencia_flujo=0.15,
        nivel_precio=0.60,
        clientela_turismo=0.05,
        clientela_vecindario=0.70,
        horario_nocturno=0.00,
        experiencial=0.10,
        citas_previas=0.95,
        sensibilidad_alquiler=0.30,
    ),
    "deporte": PerfilNegocio(
        dependencia_flujo=0.20,
        nivel_precio=0.50,
        clientela_turismo=0.10,
        clientela_vecindario=0.60,
        horario_nocturno=0.00,
        experiencial=0.50,
        citas_previas=0.50,
        sensibilidad_alquiler=0.55,
    ),
    "educacion": PerfilNegocio(
        dependencia_flujo=0.10,
        nivel_precio=0.50,
        clientela_turismo=0.05,
        clientela_vecindario=0.75,
        horario_nocturno=0.00,
        experiencial=0.30,
        citas_previas=0.80,
        sensibilidad_alquiler=0.45,
    ),
    "alimentacion": PerfilNegocio(
        dependencia_flujo=0.65,
        nivel_precio=0.35,
        clientela_turismo=0.15,
        clientela_vecindario=0.90,
        horario_nocturno=0.00,
        experiencial=0.10,
        citas_previas=0.00,
        sensibilidad_alquiler=0.70,
    ),
    "servicios": PerfilNegocio(
        dependencia_flujo=0.40,
        nivel_precio=0.30,
        clientela_turismo=0.05,
        clientela_vecindario=0.85,
        horario_nocturno=0.00,
        experiencial=0.00,
        citas_previas=0.30,
        sensibilidad_alquiler=0.70,
    ),
}

_NEUTRO = PerfilNegocio()  # para sectores desconocidos


def perfil_para_sector(sector: str) -> PerfilNegocio:
    """Devuelve el perfil por defecto del sector (para datos históricos sin descripción)."""
    return DEFAULTS_POR_SECTOR.get(sector, _NEUTRO)


def perfil_desde_tags(idea_tags: list[str]) -> PerfilNegocio:
    """
    Deriva un PerfilNegocio desde los idea_tags detectados por el LLM.

    Cada tag aporta valores para las dimensiones que afecta. Si varios tags
    dan valores distintos para la misma dimensión, se promedian. Las dimensiones
    sin ningún tag aplican el valor neutro (0.5).

    Se usa cuando el LLM no devuelve perfil_numerico explícito, como fallback.
    """
    # Mapeo tag → efectos sobre las dimensiones del perfil
    _TAG_EFFECTS: dict[str, dict[str, float]] = {
        # dependencia_flujo
        "alta_rotacion_clientes":  {"dependencia_flujo": 0.95},
        "street_food":             {"dependencia_flujo": 0.95, "sensibilidad_alquiler": 0.82},
        "retail_moda":             {"dependencia_flujo": 0.80},
        "instagrammable":          {"dependencia_flujo": 0.70, "experiencial": 0.65},
        "specialty_coffee":        {"dependencia_flujo": 0.38, "nivel_precio": 0.68, "experiencial": 0.52},
        "dog_friendly":            {"dependencia_flujo": 0.35, "clientela_vecindario": 0.82, "clientela_turismo": 0.12, "experiencial": 0.42},
        "coworking_cafe":          {"dependencia_flujo": 0.28, "citas_previas": 0.05, "clientela_vecindario": 0.55},
        "takeaway_delivery":       {"dependencia_flujo": 0.42, "sensibilidad_alquiler": 0.75},
        "destino":                 {"dependencia_flujo": 0.10, "citas_previas": 0.72},
        "salon_citas":             {"dependencia_flujo": 0.20, "citas_previas": 0.85},
        "fitness_boutique":        {"dependencia_flujo": 0.15, "citas_previas": 0.62, "nivel_precio": 0.65},
        "health_wellness":         {"dependencia_flujo": 0.10, "citas_previas": 0.80, "nivel_precio": 0.60},
        "kids_activity":           {"dependencia_flujo": 0.22, "citas_previas": 0.52, "clientela_vecindario": 0.78},
        "cultural_artistico":      {"dependencia_flujo": 0.45, "experiencial": 0.72},
        "gastronomico_premium":    {"dependencia_flujo": 0.15, "nivel_precio": 0.95, "sensibilidad_alquiler": 0.12, "clientela_turismo": 0.50, "citas_previas": 0.75},
        # nivel_precio
        "alta_renta":              {"nivel_precio": 0.80, "sensibilidad_alquiler": 0.18},
        "low_cost":                {"nivel_precio": 0.15, "sensibilidad_alquiler": 0.88},
        "vegano_organico":         {"nivel_precio": 0.62},
        # clientela_turismo / vecindario
        "orientado_turismo":       {"clientela_turismo": 0.88, "clientela_vecindario": 0.18},
        "clientela_local":         {"clientela_turismo": 0.08, "clientela_vecindario": 0.90},
        "clientela_profesional":   {"clientela_vecindario": 0.45},
        # horario
        "horario_nocturno":        {"horario_nocturno": 0.90},
        "horario_diurno_comercial":{"horario_nocturno": 0.02},
        # experiencial
        "experiencial":            {"experiencial": 0.90},
        # sensibilidad alquiler
        "local_grande":            {"sensibilidad_alquiler": 0.78},
        "local_pequeño":           {"sensibilidad_alquiler": 0.32},
    }

    # Acumular contribuciones por dimensión
    contributions: dict[str, list[float]] = {
        "dependencia_flujo":     [],
        "nivel_precio":          [],
        "clientela_turismo":     [],
        "clientela_vecindario":  [],
        "horario_nocturno":      [],
        "experiencial":          [],
        "citas_previas":         [],
        "sensibilidad_alquiler": [],
    }

    for tag in idea_tags:
        if tag in _TAG_EFFECTS:
            for dim, val in _TAG_EFFECTS[tag].items():
                if dim in contributions:
                    contributions[dim].append(val)

    # Promediar contribuciones; usar neutro si no hay ninguna
    _DEFAULTS = {
        "dependencia_flujo": _N, "nivel_precio": _N,
        "clientela_turismo": 0.30, "clientela_vecindario": _N,
        "horario_nocturno": 0.10, "experiencial": 0.20,
        "citas_previas": 0.20, "sensibilidad_alquiler": _N,
    }

    result = {}
    for dim, vals in contributions.items():
        result[dim] = sum(vals) / len(vals) if vals else _DEFAULTS[dim]

    return PerfilNegocio.from_dict(result)
