"""
nlp/embeddings.py — Generación de embeddings con sentence-transformers.

Modelo: paraphrase-multilingual-mpnet-base-v2 (768 dims)
Se carga en startup una sola vez y se reutiliza en todas las llamadas.
Soporta español, catalán, inglés y otros idiomas europeos.
"""
from __future__ import annotations
import logging
import numpy as np

logger = logging.getLogger(__name__)
_modelo = None


def cargar_modelo() -> None:
    """Carga el modelo en memoria. Llamar una vez en startup."""
    global _modelo
    try:
        from sentence_transformers import SentenceTransformer
        _modelo = SentenceTransformer("paraphrase-multilingual-mpnet-base-v2")
        logger.info("SentenceTransformer cargado OK")
    except Exception as e:
        logger.error("Error cargando SentenceTransformer: %s", e)


def encode(textos: list[str], batch_size: int = 32) -> np.ndarray:
    """
    Genera embeddings para una lista de textos.
    Returns: np.ndarray de shape (len(textos), 768)
    """
    if not _modelo:
        raise RuntimeError("Modelo embeddings no cargado. Llama a cargar_modelo() primero.")
    if not textos:
        return np.array([], dtype=np.float32)
    return _modelo.encode(textos, batch_size=batch_size, show_progress_bar=False,
                           convert_to_numpy=True, normalize_embeddings=True)


def encode_uno(texto: str) -> np.ndarray:
    """Encode un único texto. Devuelve vector (768,)."""
    return encode([texto])[0]


async def actualizar_perfil_zona(zona_id: str) -> None:
    """
    Recalcula el embedding de perfil semántico de una zona promediando
    los embeddings de sus últimas 100 reseñas.

    Fuente: tabla `resenas` donde procesada=TRUE y embedding IS NOT NULL.
    Destino: tabla `perfiles_zona_embedding`.
    """
    from db.conexion import get_db
    async with get_db() as conn:
        # Leer embeddings de reseñas recientes de la zona
        rows = await conn.fetch("""
            SELECT embedding FROM resenas
            WHERE zona_id=$1 AND procesada=TRUE AND embedding IS NOT NULL
            ORDER BY fecha DESC LIMIT 100
        """, zona_id)

        if not rows:
            return

        # Parsear los vectores (pgvector devuelve listas)
        vecs = np.array([list(r["embedding"]) for r in rows], dtype=np.float32)
        if vecs.shape[1] != 768:
            logger.error(
                "Dimensión de embedding inesperada para zona %s: %d (esperado 768)",
                zona_id, vecs.shape[1],
            )
            return
        perfil = vecs.mean(axis=0)
        perfil = perfil / (np.linalg.norm(perfil) + 1e-8)  # normalizar

        # Guardar en perfiles_zona_embedding
        await conn.execute("""
            INSERT INTO perfiles_zona_embedding (zona_id, embedding)
            VALUES ($1, $2)
            ON CONFLICT (zona_id) DO UPDATE SET
                embedding=EXCLUDED.embedding, updated_at=NOW()
        """, zona_id, perfil.tolist())
