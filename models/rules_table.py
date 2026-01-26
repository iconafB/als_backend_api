from sqlmodel import Field,SQLModel,Column,JSON,String
from typing import Optional,List,TYPE_CHECKING
from datetime import datetime
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy import DateTime,Column,func,Boolean,BigInteger,UniqueConstraint,Integer
from sqlalchemy.dialects.postgresql import JSONB
if TYPE_CHECKING:
    from models.campaign_rules_table import campaign_rule_tbl
    from models.lead_history_table import lead_history_tbl

class new_rules_tbl(SQLModel, table=True):
    rule_code: Optional[int] = Field(nullable=False, primary_key=True)
    rule_name: str=Field(sa_column=Column(String,unique=True,nullable=False),description="In the context of this project rule_name is the campaign code")
    rule_json: dict = Field(default_factory=dict,sa_column=Column(MutableDict.as_mutable(JSONB)),description="These are parameters that are used to filter leads that fit a campaign spec")
    created_by: int=Field(default=None,description="The user who created a rule for a campaign specification",nullable=False)
    is_active: bool=Field(default=False,sa_column=Column(Boolean))
    created_at: datetime=Field(sa_column=Column(DateTime(timezone=True),server_default=func.now(),nullable=False))



class rules_tbl(SQLModel,table=True):
    rule_code:Optional[int]=Field(nullable=False,primary_key=True)
    rule_name:str=Field(nullable=False)
    rule_sql:str=Field(nullable=False)
    rule_location:str=Field(nullable=False)

class rules_catalog(SQLModel,table=True):
    #surrogate primary key
    __tablename__="rules_catalog"
    id:Optional[int]=Field(default=None,sa_column=Column(BigInteger,primary_key=True,autoincrement=True))
    #Business key used by foreign keys
    rule_code:int=Field(sa_column=Column(Integer,unique=True,nullable=False))