from fastapi import APIRouter,status,HTTPException,UploadFile,File,Depends
from fastapi.responses import JSONResponse
from models.ping_table import ping_tbl
from typing import List
import pandas as pd
from datetime import datetime
from sqlmodel import SQLModel,select,text
from utils.pings import send_pings_to_dedago
from utils.logger import define_logger
from database.master_database_prod import get_async_master_prod_session
from schemas.pings import PingStatusResponse,PingStatusPayload,PingStatusUpdateResponse,SendPingsToDedago
from sqlalchemy.ext.asyncio import AsyncSession
ping_router=APIRouter(tags=["Pings Endpoints"],prefix="/pings")

pings_logger=define_logger("als pings logs","logs/pings.log")

#get submit pings to dedago
@ping_router.post("submit-pings",description="Get All Pings")

async def submit_pings_to_dedago(file:UploadFile=File(...)):
    try:
        try:
            file_read=await file.read()
            df=pd.DataFrame(file_read.splitlines())
            list_contents=df.values.tolist()

            numbers=[]

            for list in list_contents:
                #potential hazard
                numbers.append(list[0].decode)
            send_numbers=send_pings_to_dedago(numbers)

            if send_numbers.status_code!=200:
                pings_logger.error(f"error occurred sending ping to dedago")
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"ping submission failed:{send_numbers.response.json()},status code:{send_numbers.status_cdoe}")
            
            return {
                "Count":len(numbers),
                "List":numbers
            }
        except Exception as e:
            pings_logger.critical(f"error in reading pings file:{e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"Error when reading file:{file.filename}")
        
    except Exception as e:
        return {"message":f"error:{e}"}


#update ping status

@ping_router.post("/als/leads_ping",status_code=status.HTTP_200_OK)

async def update_als_ping_status(ping_status:List[PingStatusPayload],session:AsyncSession=Depends(get_async_master_prod_session)):
    try:
        todaysdate=datetime.today().strftime("%Y-%m-%d")
        
        for rec in ping_status:
            stmt = f"""INSERT INTO ping_tbl(cell, ping_status, ping_duration, date_pinged) 
            VALUES ('{rec.telnr}', '{rec.status}', '{rec.duration}', '{todaysdate}') ON CONFLICT(cell) 
            DO UPDATE SET ping_status = EXCLUDED.ping_status, ping_duration = EXCLUDED.ping_duration, date_pinged = 
            EXCLUDED.date_pinged; """

            await session.execute(text(stmt))
            await session.commit()
        session.close()
        length=len(ping_status)

        with open("Ping_Results.txt","+a") as file:
            for rec in ping_status:
                file.write(f"{rec.telnr},{rec.status},{rec.duration}")
                file.write("\n")
        pings_logger.info(f"{length} records updated from the pings table and written on the Ping_Results text file") 

        return PingStatusUpdateResponse(message=f"{len(ping_status)} records updated from the pings table",status=True,pings_updated=length)
    except Exception as e:
        pings_logger.exception(f"an exception occurred while updating:{len(ping_status)}:{e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An exception occurred while updating the pings table")

#send pings
@ping_router.post("/service",description="Send Pings")
async def send_pings():
    return {"message":"send pings to another service"}
#get specific pings


@ping_router.get("/get_pinged_data_for_pinging",status_code=status.HTTP_200_OK)

async def submit_pings_to_dedago(file:UploadFile=File(...,description="Cell numbers pings to send to dedago")):
    try:
        contents=await file.read()
        dataFrame=pd.DataFrame(contents.splitlines())
        list_contents=dataFrame.values.tolist()
        
        numbers=[d[0].decode() for d in list_contents]

        send_numbers=send_pings_to_dedago(numbers)

        if send_numbers.status_code!=200:
            pings_logger.info(f"{send_numbers.json()}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"Error occurred while sending pings to dedago")
        
        pings_logger.info(f"{len(numbers)} pings sent to dedago")
        return SendPingsToDedago(message=f"{len(numbers)} pings sent to dedago",pings_sent=len(numbers))
    
    except Exception as e:
        pings_logger.exception(f"exception occurred at:{e}")
        raise HTTPException(status_code=status)

