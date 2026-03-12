"""db/conexion.py — Pool asyncpg a PostgreSQL."""
from __future__ import annotations
import json
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg
from asyncpg import Connection, Pool

from config import get_settings

logger = logging.getLogger(__name__)
_pool: Pool | None = None


async def init_db_pool() -> None:
    global _pool
    dsn = get_settings().DATABASE_URL
    _pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=5,
        max_size=20,
        command_timeout=10,
        max_inactive_connection_lifetime=300,
        init=_init_connection,
    )
    logger.info("Pool asyncpg OK min=5 max=20")


async def _init_connection(conn: Connection) -> None:
    await conn.execute("SET TIME ZONE 'UTC'")
    await conn.set_type_codec(
        "jsonb",
        encoder=lambda v: json.dumps(v, ensure_ascii=False),
        decoder=json.loads,
        schema="pg_catalog",
        format="text",
    )


async def close_db_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


@asynccontextmanager
async def get_db() -> AsyncGenerator[Connection, None]:
    if not _pool:
        raise RuntimeError("Pool BD no inicializado")
    async with _pool.acquire() as conn:
        yield conn
