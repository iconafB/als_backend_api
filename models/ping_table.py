from sqlmodel import SQLModel,Field,Relationship
from typing import Optional
from datetime import date
from models.contact_table import contact_tbl
class ping_tbl(SQLModel,table=True):
    __tablename__="ping_tbl"
    cell:str=Field(foreign_key="info_tbl.cell",index=True,nullable=False,max_length=10,primary_key=True)
    ping_status:str=Field(nullable=False,default=None)
    ping_duration:str=Field(nullable=False,default=None)
    date_pinged:Optional[date]=Field(sa_column_kwargs={"server_default":"NOW()"},nullable=False)

    #contact:Optional[contact_tbl]=Relationship(back_populates="pings")


