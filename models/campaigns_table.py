from sqlmodel import SQLModel,Field,Relationship
from typing import Optional,List,TYPE_CHECKING
from sqlalchemy import func
from datetime import datetime

if TYPE_CHECKING:
    from models.campaign_rules_table import campaign_rule_tbl
    from models.lead_history_table import lead_history_tbl

class campaign_tbl(SQLModel,table=True):
    pk:int=Field(primary_key=True,nullable=False)
    camp_code:str=Field(default=None,nullable=False,unique=True,index=True)
    campaign_name:Optional[str]=None
    branch:Optional[str]=None
    is_new:Optional[bool]=Field(nullable=False,default=False)
    
    #rules:List["campaign_rule_tbl"]=Relationship(back_populates="campaign")
    #lead_history_tbl:List["lead_history_tbl"]=Relationship(back_populates="campaign")


