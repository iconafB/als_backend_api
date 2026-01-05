from fastapi import HTTPException,UploadFile,status
from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlalchemy import text
import asyncio
import io
import polars as pl
import pandas as pd 
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import random
import string
from typing import List,Tuple
from utils.dedupes.manual_dedupe_queries import INSERT_MANUAL_DEDUPE_QUERY,INSERT_MANUAL_DEDUPE_INFO_TBL_QUERY
from utils.logger import define_logger

dedupe_logger=define_logger("als_dedupe_campaign_logs","logs/dedupe_route.log")

async def insert_campaign_dedupe_batch(session:AsyncSession,data:List[Tuple[str,str,str,str]],user,batch_size:int=5000):
    if not data:
        return 0
    
    insert_stmt=text(INSERT_MANUAL_DEDUPE_QUERY)
    total_inserted=0

    try:
        for i in range(0,len(data),batch_size):
            batch=data[i:i+batch_size]
            batch_dicts = [
                {
                    "id": r[0],
                    "cell": r[1],
                    "campaign_name": r[2],
                    "status": r[3],
                    "code": r[4]
                }
                for r in batch
            ]
            
            await session.execute(insert_stmt,batch_dicts)
            total_inserted+=len(batch_dicts)
        #this should move just in case an error occurs on other commits
        return total_inserted
    except Exception as e:
        #redundant may do nothing
        await session.rollback()
        dedupe_logger.exception(f"An exception occurred while inserting dedupe data:{e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while adding dedupe manually")
    



async def insert_manual_dedupe_info_tbl(session:AsyncSession,update_data:List[Tuple[str,str]],user,batch_size:int=5000):
    try:
        if not update_data:
            return 0
        
        total_processed=0
        insert_stmt=text(INSERT_MANUAL_DEDUPE_INFO_TBL_QUERY)
        for i in range(0,len(update_data),batch_size):
            batch=update_data[i:i+batch_size]
            batch_dicts = [{"cell": r[0], "extra_info": r[1]} for r in batch]
            await session.execute(insert_stmt,batch_dicts)
            total_processed+=len(batch_dicts)

        #remove this commit breaks atomicity 
        return total_processed
    
    except Exception as e:
        #redundant but it's here just for safety
        session.rollback()
        dedupe_logger.exception(f"an exception occured while inserting dedupe data on the info_tbl:{e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while occurred while inserting dedupe data on the info_tbl")



def generate_random_string(campaign_name:str,length=5):
    return campaign_name +''.join(random.choices(string.ascii_letters+string.digits,k=length))

def read_file_into_dict_list_sync(file:UploadFile,campaign_name:str,camp_code:str)->list:
    #read the file path
    file_extension=Path(file.filename).suffix.lower()
    if file_extension not in ['.csv','.xls','.xlsx']:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"Invalid file type.Only CSV or Excel files are allowed.")
    try:
        contents=file.file.read()
        #reset the file pointer
        file.seek(0)
        if file_extension==".csv":
            df=pl.read_csv(io.BytesIO(contents))
        else:
            excel_df=pd.read_excel(io.BytesIO(contents))
            df=pl.from_pandas(excel_df)

        if 'id' not in df.columns or 'cell' not in df.columns:

            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="The required columns ('id_numbers' and 'cell_numbers') are missing from the file.")
        

        dedupe_code=generate_random_string(campaign_name=campaign_name)

        data = [
            {
                **entry, 
                'campaign_name': campaign_name, 
                'camp_code': camp_code,
                'dedupe_code': dedupe_code
            }
            for entry in df.select(['id', 'cell','client_status']).to_dicts()
        ]
        
        return data
      
    except Exception as e:
        dedupe_logger.exception(f"An exception occurred while read a dedupe file")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"Error processing file:{str(e)}")


#Asynchronous function to handle file processing without blocking
async def read_file_into_dict_list(file:UploadFile,campaign_name:str,camp_code:str)->list:
    loop=asyncio.get_event_loop()
    # Run the blocking task in a separate thread to avoid blocking the event loop
    data=await loop.run_in_executor(None,read_file_into_dict_list_sync,file,campaign_name,camp_code)
    return data


#clean this function

async def insert_dedupe_data_in_batches(session:AsyncSession,data:List[dict],batch_size:int=1000):
    total_records_inserted=0
    total_batches_processed=0
    try:
        if len(data)>10000:
            batch_size=5000
        if len(data)>50000:
            batch_size=10000
          
        for i in range(0,len(data),batch_size):
            batch=data[i:i+batch_size]
            #
            values_placeholder=",".join(["(:id,:cell,:client_status,:campaign_name,:camp_code,:dedupe_code)"]*len(batch))
            
            #clean your code
            query=text(f"""
                INSERT INTO dedupe_history_tracker(id,cell,client_status,campaign_name,camp_code,dedupe_code)
                VALUES {values_placeholder}
            """)

            #flatten the batch data to fit the placeholders
            flattened_values=[
                {
                    "id":record["id"],
                    "cell":record["cell"],
                    "client_status":record["client_status"],
                    "campaign_name":record["campaign_name"],
                    "camp_code":record['camp_code'],
                    "dedupe_code":record['dedupe_code']
                }
                for record in batch
            ]
            
            #Execute the query
            await session.execute(query,flattened_values)
            # commit the batch after insertion
            await session.commit()
            total_batches_processed+=1
            total_records_inserted+=len(batch)
            
        return {
            "message":"Data inserted successfully",
            "total_records_inserted":total_records_inserted,
            "total_batches_processed":total_batches_processed
        }
    
    except Exception as e:
        await session.rollback()
        dedupe_logger.exception(f"an exception occurred while inserting data into the dedupe tracker table:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error while loading data on the dedupe tracker table")
    
