from sqlalchemy import func
from sqlmodel import SQLModel,Field,Relationship
from typing import Optional,List,TYPE_CHECKING
from datetime import datetime


if TYPE_CHECKING:

    from models.contact_table import contact_tbl
    from models.employment_table import employment_tbl
    from models.location_table import location_tbl
    from models.finance_table import finance_tbl
    from models.car_table import car_tbl
    from models.black_list_table import blacklist_tbl
    from models.ping_table import ping_tbl
    from models.lead_history_table import lead_history_tbl

class info_tbl(SQLModel,table=True):
    pk:Optional[int]=Field(primary_key=True,default=None)
    cell:str=Field(default=None,index=True,unique=True)
    id:str=Field(default=None,unique=True,index=True)
    title:Optional[str]=None
    fore_name:Optional[str]=None
    last_name:Optional[str]=None
    date_of_birth: Optional[str]=None
    race:Optional[str]=None
    gender:Optional[str]=None
    marital_status:Optional[str]=None
    salary:Optional[float]=None
    status:Optional[str]=None
    derived_income:Optional[float]=None
    typedata:Optional[str]=None
    last_used:Optional[datetime]=None
    extra_info:Optional[str]=None

    # finance:location_tbl=Relationship(back_populates="info_location")
    # contact:contact_tbl=Relationship(back_populates="info_contact")
    # info_car:car_tbl=Relationship(back_populates="car_info")
    # blacklist_record:blacklist_tbl=Relationship(back_populates="blacklist_info")
    # cell_employment:employment_tbl=Relationship(back_populates="employment_cell")

    #created_at:Optional[datetime]=Field(sa_column_kwargs={"server_default":func.now()},nullable=False,default=None)
    
    #relationships 1:1 (back-populated)
    # contact:Optional["contact_tbl"]=Relationship(back_populates="info_tbl")
    # employements:Optional["employment_tbl"]=Relationship(back_populates="info_tbl")
    # locations:Optional["location_tbl"]=Relationship(back_populates="info_tbl")
    # finance:Optional['finance_tbl']=Relationship(back_populates="info_tbl")
    # car:Optional["car_tbl"]=Relationship(back_populates="info_tbl")
    # blacklist:Optional["blacklist_tbl"]=Relationship(back_populates="info_tbl")
    # ping:Optional["ping_tbl"]=Relationship(back_populates="info_tbl")
    #many lead history records
    #lead_history:List["lead_history_tbl"]=Relationship(back_populates="info_tbl")
