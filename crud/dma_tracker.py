from fastapi import HTTPException,status
from sqlalchemy import func
from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlmodel import select
from schemas.dma_tracker_schema import CreateDMARecord,CreateDMARecordResponse,PaginatedDMAResponse,DeleteDMASingleRecord,DeleteRecordByAuditID
from models.dma_service import dma_audit_id_tbl
from utils.logger import define_logger

dma_records_logger=define_logger("als_dma_tracker_logs","logs/dma_tracker_route.log")
#this class will be tracking and managing  the dma tracker table 

class DMARecordsManagement:
    #consumed by a background task

    async def insert_record(self,insert_record:CreateDMARecord,session:AsyncSession)->CreateDMARecordResponse:
        try:
            record=dma_audit_id_tbl(**insert_record.model_dump())
            session.add(record)
            await session.commit()
            await session.refresh(record)
            dma_records_logger.info(f"{insert_record.number_of_records} records inserted into the table for campaign:{insert_record.camp_code}")
            return CreateDMARecordResponse.model_validate(record)
        except HTTPException:
            raise
        except Exception as e:
            await session.rollback()
            dma_records_logger.exception(f"An exception occurred while creating a record on dma tracker table:{e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while insert record")
    

    async def fetch_all_records(self,page:int,page_size:int,session:AsyncSession)->PaginatedDMAResponse:
        try:
            total = await session.scalar(select(func.count()).select_from(dma_audit_id_tbl))
            total=int(total or 0)
            offset=(page - 1)*page_size

            stmt=(select(dma_audit_id_tbl).order_by(dma_audit_id_tbl.id.desc()).offset(offset).limit(page_size))
            results=(await session.execute(stmt)).scalars().all()

            records=[CreateDMARecordResponse.model_validate(c) for c in results]
            return PaginatedDMAResponse(page=page,page_size=page_size,total=total,results=records)
        
        except HTTPException:
            raise
        except Exception as e:
            await session.rollback()
            dma_records_logger.exception(f"an internal server error occurred while fetching records:{e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while fetching records")
    
    async def fetch_record_by_id(self,id:int,session:AsyncSession)->CreateDMARecordResponse:

        try:
            stmt=(select(dma_audit_id_tbl).where(dma_audit_id_tbl.id==id))
            result=(await session.execute(stmt)).scalars().first()
            if result==None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Record with id:{id} does not exist")
            return CreateDMARecordResponse.model_validate(result)
        except HTTPException:
            raise

        except Exception as e:
            await session.rollback()
            dma_records_logger.exception(f"an exception occured while fetching a record:{e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while fetching record:{id}")
        

    async def fetch_records_by_camp_code(self,page:int,page_size:int,camp_code:str,session:AsyncSession)->PaginatedDMAResponse:
       try:
            total = await session.scalar(select(func.count()).where(dma_audit_id_tbl.camp_code==camp_code).select_from(dma_audit_id_tbl))
            total=int(total or 0)
            offset=(page - 1)*page_size

            if total==0:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"records with campaign code:{camp_code} do not exist")
            stmt=(select(dma_audit_id_tbl).where(dma_audit_id_tbl.camp_code==camp_code).order_by(dma_audit_id_tbl.id.desc()).offset(offset).limit(page_size))
            results=(await session.execute(stmt)).scalars().all()
            records=[CreateDMARecordResponse.model_validate(c) for c in results]

            return PaginatedDMAResponse(page=page,page_size=page_size,total=total,results=records)
       
       except HTTPException:
           raise
       except Exception as e:
           await session.rollback()
           dma_records_logger.exception(f"An exception occurred while fetching record by campaign code:{camp_code}:{e}")
           raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An exception occurred while fetch record from the database with campaign code:{camp_code}")


    async def delete_db_record_by_id(self,id:int,session:AsyncSession)->DeleteDMASingleRecord:
        try:
            stmt=select(dma_audit_id_tbl).where(dma_audit_id_tbl.id==id)
            result=(await session.execute(stmt)).scalars().first()
            if result is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"record with id:{id} does not exist")
            await session.delete(result)
            await session.commit()
            return DeleteDMASingleRecord(message=f"database record:{id} deleted",message_status=True)
        
        except HTTPException:
            raise
        except Exception as e:
            await session.rollback()
            dma_records_logger.exception(f"an exception occurred while deleting record:{id} from the database:{e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An exception occurred while deleting a database record:{e}")
    
    async def delete_record_by_audit_id(self,audit_id:str,session:AsyncSession)->DeleteRecordByAuditID:
        try:
            stmt=(select(dma_audit_id_tbl).where(dma_audit_id_tbl.audit_id==audit_id))
            results=(await session.execute(stmt)).scalars().first()
            if results is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Record with audit id:{audit_id} does not exist")
            await session.delete(results)
            await session.commit()
            return DeleteRecordByAuditID(message=f"record with audit id:{audit_id} deleted successfully",message_status=True)
        except HTTPException:
            raise
        except Exception as e:
            await session.rollback()
            dma_records_logger.exception(f"an internal server error occurred while deleting a record by audit id:{audit_id}{e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while deleting records using the audit_id:{audit_id}")

    async def sum_number_of_dma_records(self,session:AsyncSession)->int:
        try:
            total_sum=await session.scalar(select(func.coalesce(func.sum(dma_audit_id_tbl.number_of_records),0)))
            return int(total_sum or 0)
        
        except HTTPException:
            raise
        except Exception as e:
            await session.rollback()
            dma_records_logger.exception(f"an exception occurred while counting the number of records:{e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while fetching the total number of records")
        
    
    async def update_dedupe_status(self,session:AsyncSession,audit_id:str):
        try:
            #find the record by dedupe record
            stmt=select(dma_audit_id_tbl).where(dma_audit_id_tbl.audit_id==audit_id)
            result=(await session.execute(stmt)).scalars().first()
            if result is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Record with audit id:{audit_id} does not exist")
            
            if result.is_processed==False and result.dedupe_status in ("Downloady Not Ready","Dedupe InComplete"):
                result.dedupe_status="Dedupe Complete"
                session.add(result)
                await session.commit()

            return True
        except HTTPException:
            raise

        except Exception as e:
            dma_records_logger.exception(f"An exception occurred while updating the dedupe status:{e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while updating dedupe status")
        
