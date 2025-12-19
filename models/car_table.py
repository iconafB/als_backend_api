from sqlalchemy import func
from sqlmodel import SQLModel,Field,Relationship
from typing import Optional,List,TYPE_CHECKING
from datetime import datetime


if TYPE_CHECKING:
    from models.information_table import info_tbl
class car_tbl(SQLModel,table=True):
    pk:int=Field(primary_key=True,nullable=False)
    cell:str=Field(foreign_key="info_tbl.cell",index=True)
    make:Optional[str]=None
    model:Optional[str]=None
    year:Optional[str]=None

