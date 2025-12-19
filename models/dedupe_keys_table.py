from sqlmodel import SQLModel,Field,Relationship

class manual_dedupe_key_tbl(SQLModel,table=True):
    id:int=Field(primary_key=True,default=None)
    rule_name:str=Field(foreign_key="rules_tbl.rule_name",nullable=False)
    dedupe_key:str=Field(nullable=False)
    