from sqlalchemy import func,Column,Text,String
from sqlmodel import SQLModel,Field,Relationship
from typing import Optional,List,TYPE_CHECKING
from datetime import datetime


if TYPE_CHECKING:
    from models.information_table import info_tbl
class car_tbl(SQLModel,table=True):
    __tablename__="car_tbl"
    
    cell:str=Field(index=True,unique=True,nullable=False,foreign_key="info_tbl.cell",primary_key=True)
    make:Optional[str]=Field(default=None,sa_column=Column(Text,nullable=True))
    model:Optional[str]=Field(default=None,sa_column=Column(Text,nullable=True))
    year:Optional[str]=Field(default=None,sa_column=Column(Text,nullable=True))

