from typing import List, Optional, Dict, Any
from model.models import Suggestion
from model.db import get_async_connection  # your existing connection pool


async def add_suggestion(s: Suggestion) -> int:
    pool = await get_async_connection()

    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO ems_suggestions
            (suggestion_type, priority, title, description, metadata, executed, executed_at, remarks)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id;
        """, s.suggestion_type, s.priority, s.title, s.description,
                s.metadata, s.executed, s.executed_at, s.remarks)

        return row["id"]

async def get_suggestion(id: int) -> Optional[Suggestion]:
    pool = await get_async_connection()

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM ems_suggestions WHERE id=$1", id)
        if not row:
            return None
        return Suggestion(**dict(row))

async def list_suggestions(limit=50) -> List[Suggestion]:
    pool = await get_async_connection()

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM ems_suggestions 
            ORDER BY ts DESC 
            LIMIT $1;
        """, limit)

        return [Suggestion(**dict(r)) for r in rows]

async def list_pending() -> List[Suggestion]:
    pool = await get_async_connection()

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM ems_suggestions 
            WHERE executed = FALSE
            ORDER BY ts DESC;
        """)

        return [Suggestion(**dict(r)) for r in rows]

async def mark_executed(id: int, remarks: str = None):
    pool = await get_async_connection()

    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE ems_suggestions
            SET executed = TRUE, executed_at = NOW(), remarks = COALESCE($2, remarks)
            WHERE id = $1;
        """, id, remarks)
