from typing import AsyncGenerator, Optional
import aiomysql
from aiomysql import Pool, Connection
from fastapi import HTTPException, status
from settings.Settings import get_settings

_mysql_pool: Optional[Pool] = None

async def init_mysql_pool() -> None:
    """Create the MySQL pool once."""
    global _mysql_pool
    if _mysql_pool is not None:
        return

    s = get_settings()
    _mysql_pool = await aiomysql.create_pool(
        host=s.MYSQL_HOST,
        user=s.MYSQL_USER,
        password=s.MYSQL_PASSWORD,
        db=s.MYSQL_DB,
        port=s.MYSQL_PORT,
        minsize=3,
        maxsize=20,
        autocommit=False,
    )


async def close_mysql_pool() -> None:
    """Close the pool on shutdown."""
    global _mysql_pool
    if _mysql_pool is None:
        return

    _mysql_pool.close()
    await _mysql_pool.wait_closed()
    _mysql_pool = None


def get_mysql_pool() -> Pool:
    """
    Use this in background tasks (no Depends available there).
    """
    if _mysql_pool is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="MySQL pool not initialized",
        )
    return _mysql_pool


async def get_mysql_conn() -> AsyncGenerator[Connection, None]:
    """FastAPI dependency that yields a pooled MySQL connection."""
    pool = get_mysql_pool()
    async with pool.acquire() as conn:
        yield conn


