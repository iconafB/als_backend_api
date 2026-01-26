# schemas/dma_tracker_schema.py
from __future__ import annotations

from datetime import datetime
from typing import List

from pydantic import BaseModel, EmailStr, Field, ConfigDict, field_validator


class CreateDMARecord(BaseModel):

    audit_id: str
    number_of_records: int
    notification_email: EmailStr
    camp_code: str
    @field_validator("audit_id", mode="before")
    @classmethod
    def audit_id_to_str(cls, v):
        return str(v)


class CreateDMARecordResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)
    id: int = Field(alias="pk")
    audit_id: str
    number_of_records: int
    notification_email: str
    dedupe_status:str
    camp_code: str
    created_at: datetime
    is_processed: bool


class PaginatedDMAResponse(BaseModel):
    page: int
    page_size: int
    total: int
    results: List[CreateDMARecordResponse]


class DeleteDMASingleRecord(BaseModel):
    message: str
    message_status: bool


class DeleteRecordByAuditID(BaseModel):
    message: str
    message_status: bool


class TotalDMARecordsResponse(BaseModel):
    total_number_of_records: int


class DMACreditsResponse(BaseModel):
    credits: str
