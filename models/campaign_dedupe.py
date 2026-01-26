from sqlmodel import SQLModel,Field
from typing import Optional,TYPE_CHECKING
from models.campaigns_table import campaign_tbl
from datetime import date

if TYPE_CHECKING:
    from models.information_table import info_tbl


class Campaign_Dedupe(SQLModel,table=True):
    lead_pk:Optional[int]=Field(primary_key=True,nullable=False) 
    cell:str=Field(foreign_key="info_tbl.cell",index=True,nullable=False)
    id:str=Field(foreign_key="info_tbl.id",index=True,nullable=False)
    campaign_name:str=Field(nullable=False)
    status:str=Field(nullable=False)
    last_used:date=Field(nullable=False)
    code:str=Field(nullable=False)


