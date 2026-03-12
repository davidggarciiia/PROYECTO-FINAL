"""agente/validador.py — Validación del tipo de negocio con LLM."""
from __future__ import annotations
import json, logging
from routers.llm_router import completar
from agente.prompts import VALIDACION_SISTEMA

logger = logging.getLogger(__name__)


async def validar_negocio(descripcion: str, session_id: str) -> dict:
    """
    Analiza la descripción del negocio y devuelve:
    - es_retail: si necesita local físico
    - sector: código de sector detectado
    - info_suficiente: si hay suficiente info para buscar
    - preguntas_pendientes: qué falta saber
    - variables_extraidas: datos ya extraídos de la descripción
    - estado: 'ok' | 'cuestionario' | 'error_tipo_negocio' | 'inviable_legal'
    """
    respuesta = await completar(
        mensajes=[{"role":"user","content":f"Descripción del negocio: {descripcion}"}],
        sistema=VALIDACION_SISTEMA,
        endpoint="validacion",
        session_id=session_id,
        max_tokens=600,
        temperature=0.1,
        requiere_json=True,
    )

    try:
        limpio = respuesta.strip()
        if limpio.startswith("```"): limpio = "\n".join(limpio.split("\n")[1:-1])
        return json.loads(limpio)
    except json.JSONDecodeError as e:
        logger.error("JSON inválido en validar_negocio: %s | respuesta: %s", e, respuesta[:200])
        # Fallback conservador: lanzar cuestionario
        return {
            "es_retail": True,
            "sector": None,
            "info_suficiente": False,
            "preguntas_pendientes": ["¿Cuánto puedes pagar de alquiler al mes?"],
            "variables_extraidas": {},
            "estado": "cuestionario",
        }
