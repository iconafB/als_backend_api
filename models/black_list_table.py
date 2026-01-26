from sqlmodel import SQLModel,Field
from typing import Optional,List,TYPE_CHECKING
from datetime import date
from sqlalchemy import String,Column,Boolean

if TYPE_CHECKING:
    from models.information_table import info_tbl

class blacklist_tbl(SQLModel,table=True):
    __tablename__="blacklist_tbl"
    cell:str=Field(foreign_key="info_tbl.cell",index=True,max_length=10,primary_key=True)
    dmasa_status:Optional[bool]=Field(default=None,sa_column=Column(Boolean,nullable=False))
    dnc_status:Optional[bool]=Field(default=None,sa_column=Column(Boolean,nullable=True))
    dma_date:Optional[date]=None


