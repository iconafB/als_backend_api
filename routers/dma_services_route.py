from fastapi import APIRouter,Depends,status,Query,Path
from sqlalchemy.ext.asyncio.session import AsyncSession
from typing import Optional
from database.master_database_prod import get_async_master_prod_session
from utils.auth import get_current_active_user
from schemas.dma_tracker_schema import (CreateDMARecordResponse,PaginatedDMAResponse,DeleteDMASingleRecord,DeleteRecordByAuditID,TotalDMARecordsResponse,DMACreditsResponse)
from crud.dma_tracker import DMARecordsManagement
from utils.dmasa_service_helpers import DMAService

dma_service_router=APIRouter(prefix="/dma-records",tags=["dma-records-overview"])
#single instance
dma_manager=DMARecordsManagement()

@dma_service_router.get("/credits",status_code=status.HTTP_200_OK,description="Check the remaining dma credits",response_model=DMACreditsResponse)
async def check_dma_credits(dma_object:DMAService,user=Depends(get_current_active_user)):
    credits=await dma_object.check_credits()
    return DMACreditsResponse(credits=credits)



@dma_service_router.get("/all",status_code=status.HTTP_200_OK,description="list all dma records overview",response_model=PaginatedDMAResponse)
async def get_all_records(page:int=Query(1,ge=1,description="Page value should be greater than one"),page_size:int=Query(10,ge=1,le=100,description="The page value should be greater than one and less than 100"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await dma_manager.fetch_all_records(page,page_size,session)

@dma_service_router.get("/total",status_code=status.HTTP_200_OK,description="Fetch the total number of records submitted for dma",response_model=TotalDMARecordsResponse)
async def total_number_of_dma_records(session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    total=await dma_manager.sum_number_of_dma_records(session)
    return total


@dma_service_router.get("/search-dma-records",status_code=status.HTTP_200_OK,description=f"Search dma records using campaign codes and audit ids",response_model=PaginatedDMAResponse)
async def search_dma_records_overview(session:AsyncSession=Depends(get_async_master_prod_session),page:int=Query(1,ge=1,description="Page number must be greater than one"),page_size:int=Query(10,ge=1,le=100,description="Number of items per unit page"),audit_id:Optional[str]=Query(None,description="Enter audit id if available"),campaign_code:Optional[str]=Query(None,description="Campaign code if it's available")):
    return await dma_manager.search_dma_overview_records(session,page,page_size,audit_id,campaign_code)

@dma_service_router.get("/{camp_code}",status_code=status.HTTP_200_OK,description="list all dma records overview by campaign code",response_model=PaginatedDMAResponse)
async def get_all_records_by_campaign_code(camp_code:str=Path(...,description="Provide a campaign code"),page:int=Query(1,ge=1,description="The page value should be greater than on"),page_size:int=Query(10,ge=1,le=100,description="list all records overview by campaign code"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await dma_manager.fetch_records_by_camp_code(page,page_size,camp_code,session)

@dma_service_router.get("/{id}",status_code=status.HTTP_200_OK,description="Get dma record by record id",response_model=CreateDMARecordResponse)
async def get_dma_record_by_id(id:int=Path(...,description="Record id"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await dma_manager.fetch_record_by_id(id,session)

@dma_service_router.delete("/{id}",status_code=status.HTTP_200_OK,description="Delete dma record using the record id",response_model=DeleteDMASingleRecord)
async def delete_record_by_id(id:int=Path(...,description="Record id"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await dma_manager.delete_db_record_by_id(id,session)

@dma_service_router.delete("/{audit_id}",status_code=status.HTTP_200_OK,description="Delete dma records overview using an audit id",response_model=DeleteRecordByAuditID)
async def delete_record_by_audit_id(audit_id:str=Path(...,description="Provide audit id for a database record"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await dma_manager.delete_record_by_audit_id(audit_id,session)

