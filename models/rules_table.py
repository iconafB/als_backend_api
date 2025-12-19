from sqlmodel import Field,SQLModel,Relationship,Column,JSON,String
from typing import Optional,List,TYPE_CHECKING
from datetime import datetime
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy import DateTime,Column,func
from sqlalchemy.dialects.postgresql import JSONB
if TYPE_CHECKING:
    from models.campaign_rules_table import campaign_rule_tbl
    from models.lead_history_table import lead_history_tbl

class rules_tbl(SQLModel, table=True):
    rule_code: Optional[int] = Field(default=None, primary_key=True)
    rule_name: str=Field(sa_column=Column(String,unique=True,nullable=False),description="In the context of this project rule_name is the campaign code")
    rule_json: dict = Field(default_factory=dict,sa_column=Column(MutableDict.as_mutable(JSONB)),description="These are parameters that are used to filter leads that fit a campaign spec")
    created_by: int=Field(default=None,description="The user who created a rule for a campaign specification")
    is_active: bool
    created_at: datetime=Field(sa_column=Column(DateTime(timezone=True),server_default=func.now(),nullable=False))
