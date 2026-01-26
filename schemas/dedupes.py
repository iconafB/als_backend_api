from sqlmodel import SQLModel
from pydantic import BaseModel,Field
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
       number_of_records_from_campaign_dedupe_table_with_mathing_dedupe_code:int
       number_of_update_campaign_dedupe_table_with_return_status_with_id_numbers:int
       number_of_records_with_status_process_from_campaign_dedupe_table:int
       number_of_deleted_records_from_campaign_dedupe_table:int
       number_of_records_updated_on_the_info_table:int
       number_of_records_deleted_with_status_updated_on_the_campaign_dedupe_table:int



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




class DedupeCampaign(BaseModel):
     campaign_name:str
     aggregate_count:int

class DedupeCampaignResponse(BaseModel):
     campaign_name:str
     aggregate_count:int

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


class ManualDedupeUploadResponse(BaseModel):
     success:bool=Field(...,description="Whether the upload completed successfully")
     key: Optional[str] = Field(None, description="Batch key/code generated for this upload")
     rows_inserted: int = Field(0, description="Number of valid rows processed from the Excel file")
     message: Optional[str] = Field(None, description="Human-readable message (useful for errors or empty files)")
     class Config:
          
          orm_mode=True

class SubmitDedupeReturnResponse(BaseModel):
     success:bool
     campaign_name:str
     dedupe_code:str
     total_lines_in_file:int
     valid_ids_processed:int
     removed_ids_count:int
     message:Optional[str]=None
     processed_at:datetime

     class Config:
          orm_mode=True






