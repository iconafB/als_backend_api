import aiomysql
from aiomysql import Connection
from typing import List
from fastapi import HTTPException, status
from utils.logger import define_logger
from settings.Settings import get_settings
from database.my_sql_pool import get_mysql_conn,get_mysql_pool
from database.my_sql_pool import Pool 
dnc_logger = define_logger("als dnc logs", "logs/dnc_route.log")
# Asynchronous function to fetch DNC numbers
from typing import List
from aiomysql import Connection
from fastapi import HTTPException, status
from utils.logger import define_logger
from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlalchemy import text


dnc_logger = define_logger("als_dnc_logs", "logs/dnc_route.log")

async def dnc_list_numbers(conn: Connection) -> List[str]:
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT `Number` FROM global_dnc")
            rows = await cursor.fetchall()
            return [r[0] for r in rows] 
    except Exception as e:
        dnc_logger.exception(f"Error fetching DNC numbers:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Error fetching DNC numbers")

# Background-safe bulk insert with executemany on the dnc database


async def send_dnc_list_to_db(dnc_list: list, camp_code: str) -> None:

    """
    Runs as a FastAPI BackgroundTask.
    Acquires its own pooled connection inside the task.
    Uses executemany for speed.
    """


    table_name = f"{camp_code}_dnc"

    try:
        pool = get_mysql_pool()

        async with pool.acquire() as conn:

            async with conn.cursor() as cursor:
                # 1) Fetch existing numbers once (simple, but heavy if table is massive)
                await cursor.execute(f"SELECT `Number` FROM `{table_name}`")
                existing_rows = await cursor.fetchall()
                existing_set = {row[0] for row in existing_rows}

                # 2) De-dupe incoming (preserve order) and filter out existing
                unique_incoming = list(dict.fromkeys(dnc_list))
                to_insert = [n for n in unique_incoming if n not in existing_set]

                if not to_insert:
                    dnc_logger.info(f"0 new numbers added to {table_name}")
                    return

                # 3) Batch insert
                insert_query = f"INSERT INTO `{table_name}` (`Number`) VALUES (%s)"
                await cursor.executemany(insert_query, [(n,) for n in to_insert])

                # 4) One commit (fast)
                await conn.commit()

        dnc_logger.info(f"{len(to_insert)} new numbers added to the {table_name} database")

    except Exception as e:
        dnc_logger.exception(f"Error sending DNC numbers to DB:{str(e)}")
        raise  HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"an error occurred while sending numbers to the dnc database")



async def bulk_insert_global_dnc_records(dnc_list:List[str],session:AsyncSession,batch_size:int=10000)->int:
    try:
        if not dnc_list:
            return 0
        
        cleaned=[v.strip() for v in dnc_list if v and v.strip()]
        if not cleaned:
            return 0
        
        stmt=text(f"""
                    INSERT INTO global_dnc_numbers (cell)
                    SELECT x
                    FROM unnest(:cells::text[]) AS x
                    ON CONFLICT (cell) DO NOTHING
                    RETURNING 1
                """)
        
        async with session.begin(): #one transaction
            for i in range(0,len(cleaned),batch_size):
                batch=cleaned[i:i+batch_size]
                result=await session.execute(stmt,{"vals":batch})

                inserted_total+=result.rowcount() #rowcount with RETURNING
        return inserted_total
    
    except Exception as e:
        dnc_logger.exception(f"an exception occurred while adding dnc records:{str(e)}")
        return False