"""agente/cuestionario.py — Motor del cuestionario adaptativo."""
from __future__ import annotations
import json, logging
from routers.llm_router import completar
from agente.prompts import CUESTIONARIO_SISTEMA

logger = logging.getLogger(__name__)


async def iniciar_cuestionario(session_id: str, validacion: dict) -> dict:
    """
    Genera la primera pregunta del cuestionario basándose en la validación inicial.

    Llama al LLM para que formule la primera pregunta de forma conversacional,
    teniendo en cuenta qué variables ya se conocen y cuáles faltan.

    Returns:
        dict con 'pregunta' (str) y 'progreso' (int 0-100)
    """
    sector           = validacion.get("sector_detectado", "desconocido")
    variables        = validacion.get("variables_conocidas", {})
    preguntas_faltan = validacion.get("preguntas_necesarias", [])

    contexto = f"Sector detectado: {sector}"
    if variables:
        campos = {k: v for k, v in variables.items() if v is not None}
        if campos:
            contexto += f"\nVariables ya conocidas: {json.dumps(campos, ensure_ascii=False)}"
    if preguntas_faltan:
        contexto += f"\nVariables que faltan por obtener: {', '.join(preguntas_faltan[:4])}"

    mensajes = [{"role": "user", "content": f"Inicia el cuestionario para ayudarme a encontrar el local ideal.\n{contexto}"}]

    respuesta = await completar(
        mensajes=mensajes,
        sistema=CUESTIONARIO_SISTEMA,
        endpoint="cuestionario_inicio",
        session_id=session_id,
        max_tokens=300,
        temperature=0.4,
        requiere_json=True,
    )

    try:
        limpio = respuesta.strip()
        if limpio.startswith("```"):
            limpio = "\n".join(limpio.split("\n")[1:-1])
        resultado = json.loads(limpio)
        return {
            "pregunta": resultado.get("mensaje", "¿Cuánto puedes pagar de alquiler al mes?"),
            "progreso": resultado.get("progreso_pct", 10),
        }
    except Exception as e:
        logger.error("Error JSON iniciar_cuestionario: %s", e)
        return {
            "pregunta": "¿Cuánto puedes pagar de alquiler al mes?",
            "progreso": 10,
        }


async def procesar_respuesta(
    session_id: str,
    historial: list[dict],
    respuesta_usuario: str,
    perfil_actual: dict,
) -> dict:
    """
    Procesa la respuesta del usuario y decide si seguir preguntando o terminar.

    Returns dict con:
      - mensaje:             texto de respuesta / siguiente pregunta al usuario
      - variables_extraidas: nuevas variables obtenidas en esta respuesta
      - estado:              'continua' | 'completo'
      - progreso_pct:        0-100
    """
    # Construir contexto de variables ya conocidas para el LLM
    contexto_perfil = ""
    if perfil_actual:
        campos = {k: v for k, v in perfil_actual.items() if v is not None}
        if campos:
            contexto_perfil = f"\n\nVariables ya conocidas: {json.dumps(campos, ensure_ascii=False)}"

    mensajes = historial + [{"role": "user", "content": respuesta_usuario}]
    sistema  = CUESTIONARIO_SISTEMA + contexto_perfil

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
        if limpio.startswith("```"):
            limpio = "\n".join(limpio.split("\n")[1:-1])
        resultado = json.loads(limpio)
        return {
            "mensaje":            resultado.get("mensaje", "¿Puedes darme más detalles?"),
            "variables_extraidas": resultado.get("variables_extraidas", {}),
            "estado":             resultado.get("estado", "continua"),
            "progreso_pct":       resultado.get("progreso_pct", 30),
        }
    except Exception as e:
        logger.error("Error JSON cuestionario: %s", e)
        return {
            "mensaje":            "¿Cuántos metros cuadrados necesitas aproximadamente?",
            "variables_extraidas": {},
            "estado":             "continua",
            "progreso_pct":       30,
        }
