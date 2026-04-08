"""Perfil numerico del negocio derivado desde la taxonomia conceptual."""
from __future__ import annotations

from dataclasses import dataclass
import numpy as np

from scoring.concept_taxonomy import (
    NEUTRAL_PROFILE,
    PROFILE_KEYS,
    SECTOR_PROFILE_DEFAULTS,
    compilar_concepto_negocio,
)

FEATURE_NAMES_PERFIL = [
    "negocio_dependencia_flujo",
    "negocio_nivel_precio",
    "negocio_clientela_turismo",
    "negocio_clientela_vecindario",
    "negocio_horario_nocturno",
    "negocio_experiencial",
    "negocio_citas_previas",
    "negocio_sensibilidad_alquiler",
]


@dataclass
class PerfilNegocio:
    dependencia_flujo: float = NEUTRAL_PROFILE["dependencia_flujo"]
    nivel_precio: float = NEUTRAL_PROFILE["nivel_precio"]
    clientela_turismo: float = NEUTRAL_PROFILE["clientela_turismo"]
    clientela_vecindario: float = NEUTRAL_PROFILE["clientela_vecindario"]
    horario_nocturno: float = NEUTRAL_PROFILE["horario_nocturno"]
    experiencial: float = NEUTRAL_PROFILE["experiencial"]
    citas_previas: float = NEUTRAL_PROFILE["citas_previas"]
    sensibilidad_alquiler: float = NEUTRAL_PROFILE["sensibilidad_alquiler"]

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
        def _clip(value, default: float) -> float:
            try:
                return max(0.0, min(1.0, float(value)))
            except (TypeError, ValueError):
                return default

        return cls(
            dependencia_flujo=_clip(d.get("dependencia_flujo"), NEUTRAL_PROFILE["dependencia_flujo"]),
            nivel_precio=_clip(d.get("nivel_precio"), NEUTRAL_PROFILE["nivel_precio"]),
            clientela_turismo=_clip(d.get("clientela_turismo"), NEUTRAL_PROFILE["clientela_turismo"]),
            clientela_vecindario=_clip(d.get("clientela_vecindario"), NEUTRAL_PROFILE["clientela_vecindario"]),
            horario_nocturno=_clip(d.get("horario_nocturno"), NEUTRAL_PROFILE["horario_nocturno"]),
            experiencial=_clip(d.get("experiencial"), NEUTRAL_PROFILE["experiencial"]),
            citas_previas=_clip(d.get("citas_previas"), NEUTRAL_PROFILE["citas_previas"]),
            sensibilidad_alquiler=_clip(d.get("sensibilidad_alquiler"), NEUTRAL_PROFILE["sensibilidad_alquiler"]),
        )

    def to_dict(self) -> dict:
        return {key: round(getattr(self, key), 3) for key in PROFILE_KEYS}


DEFAULTS_POR_SECTOR = {
    sector: PerfilNegocio.from_dict(profile)
    for sector, profile in SECTOR_PROFILE_DEFAULTS.items()
}
_NEUTRO = PerfilNegocio()


def perfil_para_sector(sector: str) -> PerfilNegocio:
    return DEFAULTS_POR_SECTOR.get(sector, _NEUTRO)


def perfil_desde_tags(idea_tags: list[str]) -> PerfilNegocio:
    concepto = compilar_concepto_negocio(idea_tags=idea_tags or [])
    return PerfilNegocio.from_dict(concepto["perfil_negocio"])

