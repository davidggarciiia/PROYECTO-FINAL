"""agente/validador.py — Validación del tipo de negocio con LLM."""
from __future__ import annotations
import json, logging
from routers.llm_router import completar
from agente.prompts import VALIDACION_SISTEMA
from agente.traductor import traducir

logger = logging.getLogger(__name__)


async def validar_negocio(descripcion: str, session_id: str) -> dict:
    """
    Analiza la descripción del negocio y devuelve un dict normalizado con:
      - es_retail:              bool — necesita local físico
      - inviable_legal:         bool — bloqueado por legislación
      - motivo_legal:           str | None — explicación si inviable_legal (en español)
      - motivo:                 str | None — explicación si no es retail (en español)
      - informacion_suficiente: bool — hay suficiente info para buscar
      - sector_detectado:       str — código de sector ('restauracion', etc.)
      - variables_conocidas:    dict — datos ya extraídos de la descripción
      - preguntas_necesarias:   list[str] — variables que aún faltan
    """
    respuesta = await completar(
        mensajes=[{"role": "user", "content": f"Business description: {descripcion}"}],
        sistema=VALIDACION_SISTEMA,
        endpoint="validacion",
        session_id=session_id,
        max_tokens=600,
        temperature=0.1,
        requiere_json=True,
    )

    try:
        limpio = respuesta.strip()
        if limpio.startswith("```"):
            limpio = "\n".join(limpio.split("\n")[1:-1])
        parsed = json.loads(limpio)
    except json.JSONDecodeError as e:
        logger.error("JSON inválido en validar_negocio: %s | respuesta: %s", e, respuesta[:200])
        return {
            "es_retail":             True,
            "inviable_legal":        False,
            "motivo_legal":          None,
            "motivo":                None,
            "informacion_suficiente": False,
            "sector_detectado":      "desconocido",
            "variables_conocidas":   {},
            "preguntas_necesarias":  ["¿Cuánto puedes pagar de alquiler al mes?"],
        }

    estado = parsed.get("estado", "cuestionario")

    # Traducir motivo_rechazo al español solo si existe (es texto visible al usuario)
    motivo_en = parsed.get("motivo_rechazo")
    motivo_es = await traducir(motivo_en, session_id) if motivo_en else None

    return {
        "es_retail":             parsed.get("es_retail", True) and estado != "error_tipo_negocio",
        "inviable_legal":        estado == "inviable_legal",
        "motivo_legal":          motivo_es,
        "motivo":                motivo_es,
        "informacion_suficiente": parsed.get("info_suficiente", False),
        "sector_detectado":      parsed.get("sector") or "desconocido",
        "variables_conocidas":   parsed.get("variables_extraidas") or {},
        "preguntas_necesarias":  parsed.get("preguntas_pendientes") or [],
    }
