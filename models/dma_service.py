from sqlmodel import SQLModel,Field
from typing import Optional
from sqlalchemy import func
from datetime import datetime

#store the audit id, number of records,records processed and created_at date, this table tracks the dma

class dma_audit_id_tbl(SQLModel,table=True):
    id:int | None=Field(primary_key=True,nullable=False)
    audit_id:str=Field(nullable=False,default=None,index=True)
    number_of_records:int=Field(nullable=False,default=None)
    notification_email:str=Field(nullable=False,default=None,index=True)
    camp_code:str=Field(nullable=False,foreign_key="campaign_tbl.camp_code")
    dedupe_status:str=Field(nullable=False,default="Dedupe Incomplete")
    is_processed:bool=Field(nullable=False,default=False)
    created_at:datetime=Field(sa_column_kwargs={"server_default":func.now()},nullable=False,default=None)



class dma_records_table(SQLModel,table=True):
    id:int=Field(primary_key=True,nullable=False)
    audit_id:str=Field(nullable=False,default=None,index=True,foreign_key="")
    data_entry:str=Field(nullable=False,default=None)
    date_added:str=Field(nullable=False,default=None)
    opted_out:bool=Field(nullable=False,default=None)
    created_at:Optional[datetime]=Field(sa_column_kwargs={"server_default":func.now()},nullable=False,default=None)



#who processed it, connect these tables

#need to add list name here to fetch it easily and populate the right tables
#need to know when do we first use this table
#have an that is a primary key and of serial type


#The branch code and camapign code can also go to the list tracker table and be removed from this table
class dma_validation_data(SQLModel,table=True):
    id:str=Field(primary_key=True,nullable=False,default=None)
    fore_name:str=Field(nullable=False,default=None)
    last_name:str=Field(nullable=False,default=None)
    cell:str=Field(nullable=False,default=None)
    audit_id:str=Field(nullable=False,default=None)
    #is fetched and check ifit's opted out or in
    is_processed:bool=Field(nullable=False,default=None)
    branch:str=Field(nullable=False,default=None)
    #campaign code
    camp_code:str=Field(nullable=False,default=None)
    #these values should not be here since
    # list_name:str=Field(nullable=False,default=None)
    # list_id:str=Field(nullable=True,default=None)
    #This field can be initially set to False up until the dma returns than updated accordingly to True or False
    opted_out:bool=Field(nullable=True,default=None) 
    created_at:Optional[datetime]=Field(sa_column_kwargs={"server_default":func.now()},nullable=False,default=None)

class list_tracker_table(SQLModel,table=True):
    id:Optional[int]=Field(primary_key=True,default=None,nullable=False)
    list_name:str=Field(nullable=True,default=None,index=True)
    list_id:str=Field(nullable=True,default=None)
    camp_code:str=Field(nullable=True,default=None,index=True)
    branch:str=Field(nullable=True,default=None)
    audit_id:str=Field(nullable=True,default=None)
    #rule name is the campaign code why should this thing be here, redundant must be removed when redesigning it
    rule_name:str=Field(nullable=True,default=None)
    created_at:Optional[datetime]=Field(sa_column_kwargs={"server_default":func.now()},nullable=False,default=None)





