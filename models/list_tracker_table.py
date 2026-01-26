from sqlmodel import SQLModel,Field
from typing import Optional


class list_tracker_tbl(SQLModel,table=True):
    __tablename__='list_tracker_tbl'
    pk:Optional[int]=Field(primary_key=True,nullable=False)
    list_name:str=Field(nullable=False)
    list_id:str=Field(nullable=True)
    audit_id:str=Field(nullable=True,foreign_key="dma_audit_id_tbl.audit_id")
    rule_code:str=Field(nullable=False,foreign_key="new_rules_tbl.rule_code")
    branch:str=Field(nullable=True)

