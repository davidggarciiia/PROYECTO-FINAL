"""agente/cuestionario.py — Motor del cuestionario adaptativo."""
from __future__ import annotations
import json, logging
from routers.llm_router import completar
from agente.prompts import CUESTIONARIO_SISTEMA

logger = logging.getLogger(__name__)


async def procesar_respuesta(
    session_id: str,
    historial: list[dict],
    respuesta_usuario: str,
    perfil_actual: dict,
) -> dict:
    """
    Procesa la respuesta del usuario y decide si seguir preguntando o terminar.

    Returns dict con:
      - mensaje: texto de respuesta al usuario
      - variables_extraidas: nuevas variables obtenidas
      - estado: 'continua' | 'completo'
      - progreso_pct: 0-100
    """
    # Construir contexto para el LLM
    contexto_perfil = ""
    if perfil_actual:
        campos = {k: v for k, v in perfil_actual.items() if v is not None}
        if campos:
            contexto_perfil = f"\n\nVariables ya conocidas: {json.dumps(campos, ensure_ascii=False)}"

    # Historial + nueva respuesta
    mensajes = historial + [{"role":"user","content":respuesta_usuario}]

    sistema = CUESTIONARIO_SISTEMA + contexto_perfil

    respuesta = await completar(
        mensajes=mensajes,
        sistema=sistema,
        endpoint="cuestionario",
        session_id=session_id,
        max_tokens=400,
        temperature=0.4,
        requiere_json=True,
    )

    try:
        limpio = respuesta.strip()
        if limpio.startswith("```"): limpio = "\n".join(limpio.split("\n")[1:-1])
        resultado = json.loads(limpio)
        # Asegurar campos mínimos
        return {
            "mensaje": resultado.get("mensaje","¿Puedes darme más detalles?"),
            "variables_extraidas": resultado.get("variables_extraidas",{}),
            "estado": resultado.get("estado","continua"),
            "progreso_pct": resultado.get("progreso_pct",30),
        }
    except Exception as e:
        logger.error("Error JSON cuestionario: %s", e)
        return {
            "mensaje": "¿Cuántos metros cuadrados necesitas aproximadamente?",
            "variables_extraidas": {},
            "estado": "continua",
            "progreso_pct": 30,
        }
