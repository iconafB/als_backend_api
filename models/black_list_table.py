from sqlmodel import SQLModel,Field
from typing import Optional,List,TYPE_CHECKING
from datetime import date


if TYPE_CHECKING:
    from models.information_table import info_tbl

class blacklist_tbl(SQLModel,table=True):
    pk:int=Field(default=None,nullable=False,primary_key=True)
    cell:str=Field(foreign_key="info_tbl.cell",index=True)
    dmasa_status:Optional[bool]=None
    dnc_status:Optional[bool]=None
    dma_date:Optional[date]=None


