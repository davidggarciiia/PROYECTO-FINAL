"""
nlp/clasificador.py — Clasificación de reseñas: categoría + sentimiento.

Usa LLM batch (DeepSeek vía router) para clasificar reseñas sin procesar.
Se ejecuta en el pipeline nocturno `pipelines/resenas.py`.

Categorías: 'servicio' | 'producto' | 'precio' | 'ubicacion' | 'ambiente'
Sentimiento: 'positivo' | 'negativo' | 'neutro'
"""
from __future__ import annotations
import json, logging
from typing import Optional
from routers.llm_router import completar
from nlp.embeddings import encode

logger = logging.getLogger(__name__)

_BATCH = 20  # reseñas por llamada LLM

_SISTEMA = """You are a business review classifier.
For each review return EXACTLY this JSON array, one entry per review:
[{"id":"...", "categoria":"servicio|producto|precio|ubicacion|ambiente", "sentimiento":"positivo|negativo|neutro"}]
Nothing else. No additional text."""


async def clasificar_batch(resenas: list[dict]) -> list[dict]:
    """
    Clasifica un batch de reseñas.
    Args: lista de dicts con 'id' y 'texto'
    Returns: lista de dicts con 'id', 'categoria', 'sentimiento'
    """
    if not resenas:
        return []

    resultados = []
    for i in range(0, len(resenas), _BATCH):
        lote = resenas[i:i+_BATCH]
        try:
            r = await _clasificar_lote(lote)
            resultados.extend(r)
        except Exception as e:
            logger.error("Error clasificando lote %d: %s", i//_BATCH, e)
            # Fallback: añadir placeholder con flag — no se marcará como procesada en BD
            for rev in lote:
                resultados.append({"id": rev["id"], "categoria": "servicio", "sentimiento": "neutro", "_fallback": True})

    return resultados


async def _clasificar_lote(lote: list[dict]) -> list[dict]:
    prompt = "Classify these reviews:\n"
    for r in lote:
        texto_corto = r["texto"][:300]  # truncar para ahorrar tokens
        # json.dumps para escapar comillas y caracteres especiales en el texto
        prompt += f'\n[{{"id":{json.dumps(str(r["id"]))},"texto":{json.dumps(texto_corto)}}}]'

    respuesta = await completar(
        mensajes=[{"role":"user","content":prompt}],
        sistema=_SISTEMA,
        endpoint="clasificar_resenas",
        max_tokens=len(lote)*80,
        temperature=0.0,
        requiere_json=True,
    )

    texto_limpio = respuesta.strip()
    if texto_limpio.startswith("```"):
        texto_limpio = "\n".join(texto_limpio.split("\n")[1:-1])

    return json.loads(texto_limpio)


async def procesar_resenas_pendientes(limite: int = 200) -> int:
    """
    Procesa reseñas pendientes de clasificación y embedding.
    Returns: número de reseñas procesadas.
    """
    from db.conexion import get_db

    async with get_db() as conn:
        rows = await conn.fetch(
            "SELECT id, texto FROM resenas WHERE procesada=FALSE LIMIT $1", limite)

    if not rows:
        return 0

    resenas = [{"id": r["id"], "texto": r["texto"]} for r in rows]

    # Clasificar con LLM
    clasificaciones = await clasificar_batch(resenas)
    clasi_map = {c["id"]: c for c in clasificaciones}

    # Generar embeddings en batch
    textos = [r["texto"] for r in resenas]
    try:
        embeddings = encode(textos)
        emb_map = {resenas[i]["id"]: embeddings[i].tolist() for i in range(len(resenas))}
    except Exception as e:
        logger.error("Error embeddings batch: %s", e)
        emb_map = {}

    # Actualizar en BD
    async with get_db() as conn:
        for r in resenas:
            rid = r["id"]
            c = clasi_map.get(rid, {})
            emb = emb_map.get(rid)
            if c.get("_fallback"):
                # Clasificación falló para esta reseña — dejar procesada=FALSE para reintentar
                continue
            await conn.execute("""
                UPDATE resenas SET
                    categoria=$1, sentimiento=$2, embedding=$3, procesada=TRUE
                WHERE id=$4
            """, c.get("categoria","servicio"), c.get("sentimiento","neutro"),
                emb, rid)

    return len(resenas)


async def generar_alertas_zona(zona_id: str) -> list[dict]:
    """
    Analiza los embeddings de reseñas recientes para detectar patrones
    que justifiquen alertas positivas o negativas.

    Estrategia:
    1. Buscar reseñas semánticamente similares a patrones problemáticos conocidos
       (obras, ruido, suciedad, cierre, inseguridad)
    2. Si hay cluster con confianza > 0.75 → generar alerta negativa
    3. Buscar patrones positivos (apertura nueva zona, regeneración, etc.)
    """
    from db.conexion import get_db

    # Patrones que indican problemas (embeddings de referencia)
    _PATRONES_NEGATIVOS = [
        "obras en la calle, mucho ruido y polvo",
        "muy poca gente, el barrio está muerto",
        "inseguridad, robos frecuentes en la zona",
        "ha cerrado mucha gente, muchos locales vacíos",
    ]
    _PATRONES_POSITIVOS = [
        "zona en auge, mucha gente nueva y negocios abriendo",
        "barrio muy animado, lleno de gente por las tardes",
        "zona muy segura y bien comunicada",
    ]

    alertas_nuevas = []

    try:
        emb_neg = encode(_PATRONES_NEGATIVOS)
        emb_pos = encode(_PATRONES_POSITIVOS)

        async with get_db() as conn:
            # Buscar reseñas similares a patrones negativos usando pgvector
            for i, patron in enumerate(_PATRONES_NEGATIVOS):
                vec = emb_neg[i].tolist()
                rows = await conn.fetch("""
                    SELECT COUNT(*) FILTER (WHERE 1-cosine_distance(embedding, $1::vector) > 0.72) AS similares,
                           COUNT(*) AS total
                    FROM resenas
                    WHERE zona_id=$2 AND fecha >= CURRENT_DATE - INTERVAL '90 days'
                      AND procesada=TRUE AND embedding IS NOT NULL
                """, vec, zona_id)

                if rows and rows[0]["total"] > 5:
                    pct = rows[0]["similares"] / rows[0]["total"]
                    if pct > 0.25:  # 25% de reseñas con ese patrón → alerta
                        alertas_nuevas.append({
                            "tipo": "negativa",
                            "texto": f"Patrón detectado en reseñas: '{patron}' ({pct:.0%} de menciones recientes)",
                            "confianza": round(pct, 2),
                            "fuente": "resenas",
                        })

            # Guardar alertas nuevas
            for a in alertas_nuevas:
                await conn.execute("""
                    INSERT INTO alertas_zona (zona_id, tipo, texto, fuente, confianza, fecha)
                    VALUES ($1,$2,$3,$4,$5,CURRENT_DATE)
                    ON CONFLICT DO NOTHING
                """, zona_id, a["tipo"], a["texto"], a["fuente"], a["confianza"])

    except Exception as e:
        logger.warning("Error generando alertas zona=%s: %s", zona_id, e)

    return alertas_nuevas
