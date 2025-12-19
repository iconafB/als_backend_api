from fastapi import HTTPException,status,UploadFile
from sqlalchemy import text
import re,csv
from schemas.insert_data import StatusedData,EnrichedData
from io import StringIO,TextIOBase
from typing import AsyncGenerator,Dict,Tuple,List
from sqlmodel.ext.asyncio.session import AsyncSession
from utils.logger import define_logger

status_data_logger=define_logger("als_status_logger_logs","logs/status_data.log")


BATCH_SIZE=10000

def get_status_tuple(datadictlist, num):
    # Mapping of num → list of fields to extract from each dict
    field_map = {
        1: ["cell", "idnum", "name", "surname", "dob", "date_created","gender", "salary", "status"],
        2: ["cell", "address1", "address2", "suburb", "city", "postal"],
        3: ["cell", "email"],
        4: ["cell", "company", "job"],
        5: ["cell", "car", "model"],
        6: ["cell", "bank", "bal"],
    }
    if num not in field_map:
        raise ValueError(f"Invalid num value: {num}")

    fields = field_map[num]
    rows = []
    for r in datadictlist:
        tuple_data = tuple(r[field] for field in fields)
        # Special case: num==1 needs typedata appended
        if num == 1:
            tuple_data = tuple_data + ("Status",)

        rows.append(tuple_data)
    
    return rows


# ----------------- CSV Streaming -----------------
async def statused_data_generator_file(file: UploadFile) -> AsyncGenerator[Dict, None]:

    """Stream CSV file and yield StatusedData dicts."""

    async for line_bytes in file.__aiter__():

        print("enter the statused data generator file")

        line_str = line_bytes.decode("utf-8").strip()
        print(f"line string:{line_str}")
        if not line_str:
            continue
        row = next(csv.reader([line_str]))
        cell = '0' + str(row[15]) if len(str(row[15])) > 0 else None
        if not cell or not re.match(r'^(\d{10})$', cell):
            continue

        data = {
            "idnum": str(row[3]),
            "cell": cell,
            "date_created": row[1],
            "salary": row[2],
            "name": row[6],
            "surname": row[7],
            "address1": row[9],
            "address2": row[10],
            "suburb": row[11],
            "city": row[12],
            "postal": row[13],
            "email": row[16],
            "status": row[17],
            "dob": str(row[3]),
            "gender": str(row[3]),
            "company": row[18] if len(row) > 18 else None,
            "job": row[19] if len(row) > 19 else None,
            "car": row[20] if len(row) > 20 else None,
            "model": row[21] if len(row) > 21 else None,
            "bank": row[22] if len(row) > 22 else None,
            "bal": row[23] if len(row) > 23 else None
        }
        try:
            model = StatusedData(**data)
            yield model.model_dump()
        except Exception:
            continue


# ----------------- Tuple Preparation -----------------
def table_tuple_generator(datadict_batch: List[Dict]):

    """Split batch into tuples per table."""

    info_rows, location_rows, contact_rows = [], [], []
    employment_rows, car_rows, finance_rows = [], [], []

    for data in datadict_batch:

        info_rows.append((
            data["cell"], data["idnum"], data["name"], data["surname"],
            data["dob"], data["date_created"], data["gender"],
            data["salary"], data["status"], "Status"
        ))
        location_rows.append((
            data["cell"], data["address1"], data["address2"],
            data["suburb"], data["city"], data["postal"]
        ))
        contact_rows.append((
            data["cell"], data["email"]
        ))
        employment_rows.append((
            data["cell"], data["company"], data["job"]
        ))
        car_rows.append((
            data["cell"], data["car"], data["model"]
        ))
        finance_rows.append((
            data["cell"], data["bank"], data["bal"]
        ))

    return info_rows, location_rows, contact_rows, employment_rows, car_rows, finance_rows



async def insert_vendor_list_status(sqlsmt: str, vendor_list: list[tuple], session: AsyncSession,batch_size:int=1000):
   
    if not vendor_list:
        return {"success":True,"inserted":0,"message":"No data to insert"}
    
    total_inserted=0

    try:
        for i in range(0,len(vendor_list),batch_size):
            batch = vendor_list[i:batch_size]
            await session.execute(sqlsmt,batch)
            total_inserted+=len(batch)
        
        await session.commit()

        return {"success":True,"inserted":total_inserted,"message":f"Successfully inserted {total_inserted} rows in batches of {batch_size}"}
    
    except Exception as e:
        await session.rollback()
        status_data_logger.exception(f"An exception occurred while inserting the data:{e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An exception occurred while inserting status data")


