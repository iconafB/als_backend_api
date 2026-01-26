from sqlmodel import SQLModel,Field
from models.information_table import info_tbl
from typing import Optional
from sqlalchemy import Column,String
class location_tbl(SQLModel,table=True):
    __tablename__ = "location_tbl"
    pk:Optional[int]=Field(primary_key=True,default=None,description="Primary key for the location record")
    cell:Optional[str]=Field(foreign_key="info_tbl.cell",index=True,unique=True,nullable=False,max_length=10,primary_key=True,description="Cell number associated with the location")
    line_one:Optional[str]=Field(default=None,sa_column=Column(String,nullable=True))
    line_two:Optional[str]=Field(default=None,sa_column=Column(String,nullable=True))
    line_three:Optional[str]=Field(default=None,sa_column=Column(String,nullable=True))
    line_four:Optional[str]=Field(default=None,sa_column=Column(String,nullable=True))
    postal_code:Optional[str]=Field(default=None,sa_column=Column(String,nullable=True))
    province:Optional[str]=Field(default=None,sa_column=Column(String,nullable=True))
    suburb:Optional[str]=Field(default=None,sa_column=Column(String,nullable=True))
    city:Optional[str]=Field(default=None,sa_column=Column(String,nullable=True))





    
    