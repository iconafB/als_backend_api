import aiomysql
from fastapi import HTTPException, status
from utils.logger import define_logger

dnc_logger = define_logger("als dnc logs", "logs/dnc_route.log")

# Asynchronous function to fetch DNC numbers
async def dnc_list_numbers():
    conn = None
    try:
        # Establish async connection to the MySQL database

        conn = await aiomysql.connect(
            host='localhost',
            user='root',
            password='scriptbit',
            db='dnc_db'
        )
        
        async with conn.cursor() as cursor:
            # SQL query to fetch all numbers from the global_dnc table
            await cursor.execute("SELECT Number FROM global_dnc")
            records = await cursor.fetchall()
            # List comprehension to populate the DNC list
            new_dnc_list = [row[0] for row in records]
            
    except Exception as e:
        # Log the error
        dnc_logger.error(f"Error fetching DNC numbers: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Error fetching DNC numbers")
    
    finally:
        # Close the connection if it exists
        if conn:
            conn.close()
    
    return new_dnc_list


async def send_dnc_list_to_db(dnc_list: list, camp_code: str):
    
    conn = None
    try:
        # Async connection to MySQL
        conn = await aiomysql.connect(
            host='localhost',
            user='root',
            password='scriptbit',
            db='dnc_db'
        )
        async with conn.cursor() as cursor:
            count = 0
            for number in dnc_list:
                # Check if number already exists
                select_query = f"SELECT Number FROM {camp_code}_dnc WHERE Number=%s"
                await cursor.execute(select_query, (number,))
                numbers_present_on_dnc = cursor.rowcount
                
                # Insert if not present
                if numbers_present_on_dnc == 0:
                    insert_query = f"INSERT INTO {camp_code}_dnc (Number) VALUES (%s)"
                    await cursor.execute(insert_query, (number,))
                    await conn.commit()
                    count += 1

        dnc_logger.info(f"{count} new numbers added to the {camp_code}_dnc database")

    except Exception as e:
        dnc_logger.error(f"Error sending DNC numbers to DB: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error connecting to the DNC MySQL DB"
        )

    finally:
        if conn:
            conn.close()


