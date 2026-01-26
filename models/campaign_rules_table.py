from sqlalchemy import func,Column,DateTime,String,ForeignKey
from sqlmodel import SQLModel,Field,Relationship
from typing import Optional,List,TYPE_CHECKING
from datetime import datetime



if TYPE_CHECKING:
    from models.rules_table import rules_tbl
    from models.campaigns_table import campaign_tbl

class campaign_rule_tbl(SQLModel,table=True):
    cr_code:Optional[int]=Field(primary_key=True,nullable=False,default=None)
    camp_code:str=Field(sa_column=Column(String,ForeignKey("campaign_tbl.camp_code",ondelete="CASCADE"),nullable=False,index=True))
    rule_code:int=Field(sa_column=Column(ForeignKey("new_rules_tbl.rule_code",ondelete="CASCADE"),nullable=False,unique=True,index=True))
    date_rule_created:datetime=Field(nullable=False)
    is_active:bool=Field(nullable=False)

    
    # rules:Optional["rules_tbl"]=Relationship(back_populates="campaign_rules",sa_relationship_kwargs={"lazy":"selectin"})


    