from fastapi import HTTPException,status
from sqlalchemy import distinct,func
from sqlalchemy.ext.asyncio.session import AsyncSession
from datetime import datetime, date, time, timedelta
from sqlalchemy import select, func, distinct, and_,text

from models.lead_history_table import lead_history_tbl
from sqlmodel import Session,select
from datetime import datetime
from utils.logger import define_logger

load_list_names_logger=define_logger("fetch_list_names","logs/load_list_names")

    #Generate a unique list_name for a given campaign code and today's date.
    #Uses an async SQLModel session to count distinct list_name entries.
    

async def get_list_name(camp_code:str,session:AsyncSession)->str:

    current_date=date.today()
    print(f"enter the list name method for generating list names for campaign:{camp_code}")

    try:
        stmt=text("""
            SELECT COUNT(DISTINCT list_name) AS cnt
            FROM lead_history_tbl
            WHERE camp_code = :camp_code
              AND date_used = :current_date
        """)
        result=await session.execute(stmt,{"camp_code":camp_code,"current_date":current_date})
        count_distinct=result.scalar_one()
        index=1 if count_distinct==0 else count_distinct + 1

    except Exception as e:
        load_list_names_logger.exception(f"An exception occurred while generating a list name:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An exception occurred while generating a list name")
    
    current_date_str=current_date.strftime("%Y-%m-%d")
    list_name=f"{camp_code}_{current_date_str[2:]}CS"
    print(f"print the list name:{list_name}")
    return list_name









async def get_list_names(camp_code: str, session: AsyncSession) -> str:
    
    try:
        print("enter method for generating list names")
        print(f"campaign code:{camp_code}")

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

        index=0

        if count_value==0:
            index=1
        else:
            index = count_value + 1
        # Format list_name: camp_YY-MM-DD_indexCS

        date_str = today.strftime("%y-%m-%d")
        list_name = f"{camp_code}_{date_str}_{index}CS"
        return list_name
    
    except Exception as e:
        load_list_names_logger.error(f"Error generating list_name: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Error occurred while fetching list name")
    

    
