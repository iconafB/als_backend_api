from __future__ import annotations
from datetime import date, datetime
from typing import Any, Dict, List, Optional
from fastapi import HTTPException, status
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from models.black_list_table import blacklist_tbl
from database.master_database_prod import async_sessionmaker
from utils.logger import define_logger

black_list_logger=define_logger("black_list_writer","logs/black_list_writer.log")


def _parse_dma_date(value: Any) -> Optional[date]:
    """
    DMASA DateAdded can be "", None, or a date string.
    We try common formats and return a Python date, else None.
    """
    if not value:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None

        # Try a few common formats
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue

        # Last attempt: ISO 8601 (handles 2025-01-21T00:00:00 etc.)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        except ValueError:
            return None

    return None


async def insert_opted_in_to_blacklist(
    session: AsyncSession,
    opted_in_records: List[Dict[str, Any]],
    batch_size: int = 500,
) -> int:
    """
    Insert / upsert opted-in DMASA records into blacklist_tbl.

    opted_in_records items expected shape:
      {
        "DataEntry": "0786542554",
        "DateAdded": "2025-01-21",
        "OptedOut": False
      }

    Writes:
      - cell        = DataEntry
      - dmasa_status= False
      - dma_date    = parsed DateAdded (nullable)

    Uses Postgres ON CONFLICT (cell) DO UPDATE to avoid duplicate key errors.

    Returns: number of rows attempted (not exact inserted count).
    """
    try:
        batch_size = int(batch_size)
        if batch_size <= 0:
            batch_size = 500
    except (TypeError, ValueError):
        batch_size = 1000
    
    if not opted_in_records:
        return 0

    total = 0

    # Build rows for insert
    rows: List[Dict[str, Any]] = []
    for item in opted_in_records:
        cell = (item.get("DataEntry") or "").strip()
        dmasa_status=item.get("OptedOut")

        if not cell:
            continue

        rows.append(
            {
                "cell": cell,
                "dmasa_status": dmasa_status,               # opted-in => not opted out
                "dnc_status": None,                 # unchanged / unknown
                "dma_date": _parse_dma_date(item.get("DateAdded")),
            }
        )

    if not rows:
        return 0

    # Batch upsert
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]

        stmt = insert(blacklist_tbl).values(chunk)

        # If cell already exists, update the DMASA fields
        stmt = stmt.on_conflict_do_update(
            index_elements=[blacklist_tbl.cell],
            set_={
                "dmasa_status": stmt.excluded.dmasa_status,
                "dma_date": stmt.excluded.dma_date,
            },
        )

        await session.execute(stmt)
        await session.commit()

        total += len(chunk)
    
    return total


async def write_opted_ins_to_blacklist_bg(opted_in_records:List[Dict[str,Any]],batch_size:int=500)->None:

    if not opted_in_records:
        black_list_logger.info("No opted-in records provided.Skip black list insert")
        return
    

    async with async_sessionmaker() as session:
        try:
            written=await insert_opted_in_to_blacklist(session=session,opted_in_records=opted_in_records,batch_size=batch_size)
            black_list_logger.info(f"Blacklist table background insert complete. Rows processed={written}")
        except Exception as e:
            await session.rollback()
            black_list_logger.exception(f"an exception occurred while writing to the blacklist table:{e}")

