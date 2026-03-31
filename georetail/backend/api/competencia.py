"""
api/competencia.py — GET /api/competencia/{zona_id}

Endpoint de carga lazy para el módulo de análisis de competencia.
Se llama cuando el usuario pulsa la pestaña "Competencia" en el panel de detalle.
Similar al módulo financiero: NO se carga en el panel principal para no penalizar
el tiempo de carga.

Devuelve:
  - Scores v2 de competencia (de competencia_detalle_zona si existen, calculados al vuelo si no)
  - Lista de competidores clasificados: amenaza / oportunidad / sinérgicos
  - Análisis de gap de precio del segmento

Radio fijo: 500m (configurable en db/zonas.py → get_competencia_zona).
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from db.sesiones import get_sesion
from db.zonas import get_competencia_zona
from schemas.models import CompetenciaDetalle

logger = logging.getLogger(__name__)
router = APIRouter(tags=["competencia"])

_RADIO_M = 500


@router.get(
    "/competencia/{zona_id}",
    response_model=CompetenciaDetalle,
    summary="Análisis de competencia para una zona (carga lazy)",
)
async def competencia_zona(
    zona_id: str,
    session_id: str = Query(..., description="ID de sesión activa"),
    sector: Optional[str] = Query(None, description="Código de sector. Si omitido, usa el de la sesión."),
) -> CompetenciaDetalle:
    """
    Carga los datos de competencia para la zona.

    Llamar al pulsar la pestaña "Competencia" en el panel de detalle.
    Devuelve:
    - Clasificación de competidores en amenaza/oportunidad/sinérgicos
    - Score v2 de competencia (cluster + amenaza incumbentes + oportunidad + complementarios)
    - Análisis de gap de precio del segmento objetivo
    """
    sesion = await get_sesion(session_id)
    if sesion is None:
        raise HTTPException(status_code=404, detail="Sesión no encontrada o expirada.")

    sector_efectivo = sector or sesion.get("perfil", {}).get("sector")
    if not sector_efectivo:
        raise HTTPException(status_code=400, detail="Sector no especificado y no disponible en la sesión.")

    resultado = await get_competencia_zona(zona_id=zona_id, sector=sector_efectivo, radio_m=_RADIO_M)
    if resultado is None:
        raise HTTPException(status_code=404, detail=f"Zona '{zona_id}' no encontrada.")

    return CompetenciaDetalle(**resultado)
