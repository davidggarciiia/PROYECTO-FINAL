"""db/redis_client.py — Cliente Redis async."""
from __future__ import annotations
import logging
import redis.asyncio as aioredis
from redis.asyncio import Redis
from config import get_settings

logger = logging.getLogger(__name__)
_redis: Redis | None = None


async def init_redis() -> None:
    global _redis
    _redis = aioredis.from_url(
        get_settings().REDIS_URL,
        encoding="utf-8",
        decode_responses=True,
        max_connections=50,
        socket_connect_timeout=5,
        socket_timeout=5,
        retry_on_timeout=True,
    )
    await _redis.ping()
    logger.info("Redis OK")


async def close_redis() -> None:
    global _redis
    if _redis:
        await _redis.aclose()
        _redis = None


def get_redis() -> Redis:
    if not _redis:
        raise RuntimeError("Redis no inicializado")
    return _redis
