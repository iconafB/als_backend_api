from sqlmodel import SQLModel,Field
from typing import Optional
from sqlalchemy import func,Index,UniqueConstraint
from enum import Enum
from datetime import datetime

class ClientStatus(str,Enum):
    Marketed="Marketed"
    Not_Marketed="Not Marketed"
    None_Status="None"

 
class Dedupe_History_Tracker(SQLModel,table=True):
    pk:int | None=Field(default=None,primary_key=True)
    id:str=Field(nullable=False,foreign_key="info_tbl.id")
    cell:str=Field(nullable=False,foreign_key="info_tbl.cell")
    campaign_name:str=Field(nullable=False)
    camp_code:str=Field(nullable=False)
    client_status:ClientStatus=Field(nullable=False)
    dedupe_code:str=Field(nullable=False)
    date:Optional[datetime]=Field(sa_column_kwargs={"server_default":func.now()},nullable=False)

    #indexes to optimize queries for these fields
    __table_args__=(
        Index("idx_id",'id'),
        Index('idx_cell','cell'),
        Index('idx_campaign_name','campaign_name'),
        Index('idx_date','date'),
        Index('idx_camp_code','camp_code'),
        UniqueConstraint('id','cell','campaign_name','camp_code',name='_id_cell_campaign_uc') # Ensure no duplicate entries for the same (id, cell, campaign_name)
    )

