from sqlmodel import SQLModel,Field,Relationship
from models.information_table import info_tbl

from typing import Optional
class location_tbl(SQLModel,table=True):
    __tablename__ = "location_tbl"
    pk:Optional[int]=Field(primary_key=True,default=None,description="Primary key for the location record")
    cell:str=Field(foreign_key="info_tbl.cell",index=True,nullable=False,max_length=10,description="Cell number associated with the location")
    line_one:Optional[str]=Field(default=None,max_length=255,description="Address line 1")
    line_two:Optional[str]=Field(default=None,max_length=255,description="Address line 2")
    line_three:Optional[str]=Field(default=None,max_length=255,description="Address line 3")
    line_four:Optional[str]=Field(default=None,max_length=255,description="Address line 4")
    postal_code:Optional[str]=Field(default=None,max_length=10,index=True,description="Postal Code")
    province:Optional[str]=Field(default=None,max_length=100,description="Province")
    suburb:Optional[str]=Field(default=None,max_length=100,description="Suburb")
    city:Optional[str]=Field(default=None,max_length=100,description="City")
    
    