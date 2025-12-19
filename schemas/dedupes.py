from sqlmodel import SQLModel
from pydantic import BaseModel
from models.dedupe_history_tracker import ClientStatus
from datetime import datetime
from typing import Optional,List

class DedupesSchema(SQLModel):
    cell_numbers:str
    id_numbers:str
    campaign_name:str
    status:str


class DataInsertionSchema(BaseModel):
    data_extraction_time:str
    insertion_time:str
    number_of_leads:int
    Success:bool

    
class AddDedupeListResponse(BaseModel):
    FileName:str
    TotalRecordsInserted:int
    TotalBatches:int
    TotalBatchedTime:int
    TotalTimeTaken:int
    DedupeKey:str



class SubmitDedupeReturnResponse(BaseModel):
       success:bool
       number_of_updated_ids_with_return_status:int
       number_of_retrieved_pending_ids_from_campaign_dedupe_table:int
       number_of_deleted_pending_ids_from_campaign_dedupe_table:int
       number_of_updated_ids_from_info_tbl:int
       number_of_deleted_pending_ids_with_status_code_u:int


class AddManualDedupeResponse(BaseModel):
     success:bool
     campaign_dedupe_records:int
     info_table_records:int
     key:int


class InsertDataDedupeTracker(BaseModel):
     message:str
     number_of_batches:int
     number_of_records:int


class TrackerResults(BaseModel):
    id:str
    cell:str
    campaign_name:str
    camp_code:str
    client_status:ClientStatus
    dedupe_code:str
    date:Optional[datetime]
    
    class Config:
         orm_mode=True


class PaginatedResultsResponse(BaseModel):
     page:int
     page_size:int
     total:int
     total_pages:int
     records:List[TrackerResults]
     class Config:
          orm_mode=True


class CampaignAggregatedInformation(BaseModel):
     campaign_name:str
     record_count:int
     class Config:
          orm_mode=True

class PaginatedAggregatedDedupeResult(BaseModel):
     page:int
     page_size:int
     total:int
     total_pages:int
     records:List[CampaignAggregatedInformation]

     class Config:
          orm_mode=True



