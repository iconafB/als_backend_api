from sqlmodel import Field,Relationship,SQLModel
from typing import Optional
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from models.contact_table import contact_tbl
    from models.campaigns_table import campaign_tbl
    from models.rules_table import rules_tbl
    from models.information_table import info_tbl


class lead_history_tbl(SQLModel,table=True):
    lead_pk:Optional[int]=Field(primary_key=True,default=None)
    cell:str=Field(nullable=False,foreign_key="info_tbl.cell",index=True)
    camp_code:str=Field(nullable=False,foreign_key="campaign_tbl.camp_code")
    date_used:Optional[date]=Field(default=None,nullable=False)
    list_id:str=Field(nullable=False)
    list_name:str=Field(nullable=False)
    load_type:str=Field(nullable=False)
    rule_code:int=Field(nullable=False,foreign_key="rules_tbl.rule_code")
    
