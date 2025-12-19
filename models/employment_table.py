from sqlmodel import SQLModel,Field,Relationship
from typing import Optional,TYPE_CHECKING
from models.information_table import info_tbl

if TYPE_CHECKING:
    from models.information_table import info_tbl
class employment_tbl(SQLModel,table=True):
    pk:int=Field(primary_key=True,default=None)
    cell:Optional[str]=Field(foreign_key="info_tbl.cell",unique=True,index=True)
    job:Optional[str]=None
    occupation:Optional[str]=None
    campany:Optional[str]=None
    
    # employement_cell:info_tbl=Relationship(back_populates="cell_employment")