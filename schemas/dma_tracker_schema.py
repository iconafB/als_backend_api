from pydantic import BaseModel,EmailStr
from typing import List

class CreateDMARecord(BaseModel):
    audit_id:int
    number_of_records:int
    notification_email:EmailStr
    camp_code:str

class CreateDMARecordResponse(BaseModel):
    id:int
    audit_id:int
    number_of_records:int
    notification_email:str
    camp_code:str
    created_at:str

class PaginatedDMAResponse(BaseModel):
    page:int
    page_size:int
    total:int
    results:List[CreateDMARecordResponse]


class DeleteDMASingleRecord(BaseModel):
    message:str
    message_status:bool


class DeleteRecordByAuditID(BaseModel):
    message:str
    message_status:bool


class TotalDMARecordsResponse(BaseModel):
    total_number_of_records:int