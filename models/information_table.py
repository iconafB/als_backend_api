from sqlalchemy import Column,String,DateTime,Float,Text,Index
from sqlmodel import SQLModel,Field
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

class info_tbl(SQLModel, table=True):
    
    __tablename__ = "info_tbl"
    info_pk: Optional[int] = Field(default=None, primary_key=True)
    # cell is your stable unique key
    cell: str = Field(primary_key=True,nullable=False)
    # allow NULLs -> fixes the NotNullViolationError
    id: Optional[str] = Field(default=None,sa_column=Column(String(13), nullable=True,index=True,unique=True))  # SA ID is 13 digits
    title: Optional[str] = Field(default=None, sa_column=Column(String(50), nullable=True))
    fore_name: Optional[str] = Field(default=None, sa_column=Column(String(120), nullable=True))
    last_name: Optional[str] = Field(default=None, sa_column=Column(String(120), nullable=True))
    date_of_birth: Optional[str] = Field(default=None, sa_column=Column(String(10), nullable=True))
    created_at: Optional[str] = Field(default=None, sa_column=Column(String(25), nullable=True))
    race: Optional[str] = Field(default=None, sa_column=Column(String(80), nullable=True))
    gender: Optional[str] = Field(default=None, sa_column=Column(String(20), nullable=True))
    marital_status: Optional[str] = Field(default=None, sa_column=Column(String(40), nullable=True))
    salary: Optional[float] = Field(default=None, sa_column=Column(Float, nullable=True))
    status: Optional[str] = Field(default=None, sa_column=Column(String(120), nullable=True))
    derived_income: Optional[float] = Field(default=None, sa_column=Column(Float, nullable=True))
    typedata: Optional[str] = Field(default=None, sa_column=Column(String(40), nullable=True))
    last_used: Optional[datetime] = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
    extra_info: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))
    ping_status: Optional[str] = Field(default=None,sa_column=Column(Text,nullable=True))
    ping_date: Optional[str]=Field(default=None,sa_column=Column(DateTime,nullable=True))
    norm_cell: str = Field(sa_column=Column(String(10), nullable=False,index=True,unique=True))


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
