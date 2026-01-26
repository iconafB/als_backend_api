from sqlmodel import SQLModel,Field
from typing import Optional
from sqlalchemy import func
from datetime import datetime

class users_tbl(SQLModel,table=True):
    
    id:Optional[int]=Field(default=None,primary_key=True)
    email:str=Field(default=None,nullable=False)
    password:str=Field(default=None,nullable=False,min_length=5)
    first_name:str=Field(default=None,nullable=False)
    last_name:str=Field(default=None,nullable=False)
    is_active:bool=Field(default=None,nullable=False)
    created_at:Optional[datetime]=Field(sa_column_kwargs={"server_default":func.now()},nullable=False)
    is_admin:Optional[bool]=Field(nullable=False)
    







