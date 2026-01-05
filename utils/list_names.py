from fastapi import HTTPException,status
from sqlalchemy import distinct,func
from sqlalchemy.ext.asyncio.session import AsyncSession
from datetime import datetime, date, time, timedelta
from sqlalchemy import select, func, distinct, and_

from models.lead_history_table import lead_history_tbl
from sqlmodel import Session,select
from datetime import datetime
from utils.logger import define_logger

load_list_names_logger=define_logger("fetch list names","logs/load_list_names")

    #Generate a unique list_name for a given campaign code and today's date.
    #Uses an async SQLModel session to count distinct list_name entries.
    

async def get_list_names(camp_code: str, session: AsyncSession) -> str:
    

    try:
        # Today's date and timestamp range
        today = date.today()
        start_of_day = datetime.combine(today, time.min)
        start_of_next_day = start_of_day + timedelta(days=1)

        # Async query: count distinct list_name for this campaign and today's date
        stmt = (
            select(func.count(distinct(lead_history_tbl.list_name)))
            .where(lead_history_tbl.camp_code == camp_code)
            .where(
                and_(
                    lead_history_tbl.created_at >= start_of_day,
                    lead_history_tbl.created_at < start_of_next_day,
                )
            )
        )

        result = await session.execute(stmt)
        count_value = result.scalar() or 0  # If no rows, default to 0

        # Generate the next index
        index = count_value + 1

        # Format list_name: camp_YY-MM-DD_indexCS
        date_str = today.strftime("%y-%m-%d")
        list_name = f"{camp_code}_{date_str}_{index}CS"

        return list_name
    

    except Exception as e:
        load_list_names_logger.error(f"Error generating list_name: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Error occurred while fetching list name")
    

    
