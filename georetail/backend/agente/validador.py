"""agente/validador.py - Validacion del tipo de negocio con LLM."""
from __future__ import annotations

import json
import logging

from agente import extraer_json
from agente.prompts import VALIDACION_SISTEMA
from agente.traductor import traducir
from routers.llm_router import completar
from scoring.concepto.taxonomy import compilar_concepto_negocio, lookup_canonical_tag

logger = logging.getLogger(__name__)


def _normalizar_tags_visibles(tags: list | None) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []

    for tag in tags or []:
        if not isinstance(tag, str):
            continue
        clean = tag.strip()
        if not clean or clean in seen:
            continue
        if lookup_canonical_tag(clean) is None:
            continue
        seen.add(clean)
        result.append(clean)

    return result


async def validar_negocio(descripcion: str, session_id: str) -> dict:
    """Analiza la descripcion y devuelve una clasificacion normalizada."""
    descripcion_safe = descripcion[:1000]
    respuesta = await completar(
        mensajes=[{"role": "user", "content": f"<business_description>\n{descripcion_safe}\n</business_description>"}],
        sistema=VALIDACION_SISTEMA,
        endpoint="validacion",
        session_id=session_id,
        max_tokens=800,
        temperature=0.1,
        requiere_json=True,
    )

    try:
        parsed = json.loads(extraer_json(respuesta))
    except json.JSONDecodeError as exc:
        logger.error("JSON invalido en validar_negocio: %s | respuesta: %s", exc, respuesta[:200])
        concepto_fallback = compilar_concepto_negocio(sector="desconocido", descripcion=descripcion_safe)
        return {
            "es_retail": True,
            "inviable_legal": False,
            "motivo_legal": None,
            "motivo": None,
            "informacion_suficiente": False,
            "sector_detectado": "desconocido",
            "idea_tags": concepto_fallback["idea_tags"],
            "perfil_negocio": concepto_fallback["perfil_negocio"],
            "concepto_negocio": concepto_fallback,
            "variables_conocidas": {},
            "preguntas_necesarias": ["Cuanto puedes pagar de alquiler al mes?"],
        }

    estado = parsed.get("estado", "cuestionario")
    motivo_en = parsed.get("motivo_rechazo")
    motivo_es = await traducir(motivo_en, session_id) if motivo_en else None

    concepto_negocio = compilar_concepto_negocio(
        sector=parsed.get("sector"),
        base_concepts=parsed.get("base_concepts"),
        modifiers=parsed.get("modifiers"),
        idea_tags=_normalizar_tags_visibles(parsed.get("idea_tags")),
        perfil_hint=parsed.get("perfil_numerico") if isinstance(parsed.get("perfil_numerico"), dict) else None,
        descripcion=descripcion_safe,
        confidence=parsed.get("confidence"),
        ambiguities=parsed.get("ambiguities") or [],
        justificacion_breve=parsed.get("justificacion_breve"),
    )

    return {
        "es_retail": parsed.get("es_retail", True) and estado != "error_tipo_negocio",
        "inviable_legal": estado == "inviable_legal",
        "motivo_legal": motivo_es if estado == "inviable_legal" else None,
        "motivo": motivo_es if estado == "error_tipo_negocio" else None,
        "informacion_suficiente": parsed.get("info_suficiente", False),
        "sector_detectado": concepto_negocio.get("sector") or parsed.get("sector") or "desconocido",
        "idea_tags": concepto_negocio.get("idea_tags") or [],
        "perfil_negocio": concepto_negocio.get("perfil_negocio") or {},
        "concepto_negocio": concepto_negocio,
        "variables_conocidas": parsed.get("variables_extraidas") or {},
        "preguntas_necesarias": parsed.get("preguntas_pendientes") or [],
    }

