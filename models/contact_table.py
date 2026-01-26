from sqlalchemy import func
from typing import TYPE_CHECKING
from sqlmodel import SQLModel,Field,Relationship
from typing import Optional,List
from datetime import datetime
from pydantic import EmailStr
if TYPE_CHECKING:
    from models.information_table import info_tbl
    from models.finance_table import finance_tbl
    from models.car_table import car_tbl
    from models.black_list_table import blacklist_tbl
    from models.lead_history_table import lead_history_tbl
    from models.ping_table import ping_tbl

class contact_tbl(SQLModel,table=True):
    cell:Optional[str]=Field(index=True,foreign_key="info_tbl.cell",primary_key=True)
    home_number:Optional[str]=Field(default=None,nullable=True,max_length=10)
    work_number:Optional[str]=Field(default=None,nullable=True,max_length=10)
    mobile_number_one:Optional[str]=Field(default=None,nullable=True,max_length=10)
    mobile_number_two:Optional[str]=Field(default=None,nullable=True,max_length=10)
    mobile_number_three:Optional[str]=Field(default=None,nullable=True,max_length=10)
    mobile_number_four:Optional[str]=Field(default=None,nullable=True,max_length=10)
    mobile_number_five:Optional[str]=Field(default=None,nullable=True,max_length=10)
    mobile_number_six:Optional[str]=Field(default=None,nullable=True,max_length=10)
    email:Optional[EmailStr]=Field(default=None,nullable=True)

    # info_contact:info_tbl=Relationship(back_populates="contact")


   
    


