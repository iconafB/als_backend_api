from sqlmodel import SQLModel,Field,Relationship
from typing import Optional,TYPE_CHECKING
from models.campaigns_table import campaign_tbl

if TYPE_CHECKING:
    from models.information_table import info_tbl
class Campaign_Dedupe(SQLModel,table=True):
    lead_pk:Optional[int]=Field(primary_key=True,nullable=False) 
    cell:str=Field(foreign_key="info_tbl.cell",index=True)
    id:str=Field(foreign_key="info_tbl.id",index=True)
    campaign_name:Optional[str]=None
    status:Optional[str]=None
    code:Optional[str]=None
    