from sqlmodel import SQLModel,Field,Relationship
from typing import Optional,TYPE_CHECKING
from models.information_table import info_tbl
from sqlalchemy import Column,String,ForeignKey
if TYPE_CHECKING:
    from models.information_table import info_tbl
class employment_tbl(SQLModel,table=True):
    __tablename__="employment_tbl"
    
    cell:Optional[str]=Field(foreign_key="info_tbl.cell",primary_key=True,index=True,max_length=10,nullable=False)
    job:Optional[str]=Field(default=None,sa_column=Column(String,nullable=True))
    occupation:Optional[str]=Field(default=None,sa_column=Column(String,nullable=True))
    company:Optional[str]=Field(default=None,sa_column=Column(String,nullable=True))
    
    # employement_cell:info_tbl=Relationship(back_populates="cell_employment")