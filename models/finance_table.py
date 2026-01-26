from sqlmodel import SQLModel,Field,Relationship
from typing import Optional,TYPE_CHECKING
from datetime import datetime
from sqlalchemy import Text,Column,String,Boolean,Float

#Indexes the tables fool

if TYPE_CHECKING:
    from models.information_table import info_tbl

   
class finance_tbl(SQLModel,table=True):
    __tablename__="finance_tbl"
    cell:str=Field(foreign_key="info_tbl.cell",index=True,unique=True,max_length=10,nullable=False,primary_key=True)
    cipro_reg:Optional[bool]=Field(default=None,sa_column=Column(Boolean,nullable=True))
    deed_office_reg:Optional[bool]=Field(default=None,sa_column=Column(Boolean,nullable=True))
    vehicle_owner:Optional[bool]=Field(default=None,sa_column=Column(Boolean,nullable=True))
    credit_score:Optional[float]=Field(default=None,sa_column=Column(Float,nullable=True))
    monthly_expenditure:Optional[float]=Field(default=None,sa_column=Column(Float,nullable=True))
    owns_credit_card:Optional[bool]=Field(default=None,sa_column=Column(Boolean,nullable=True))
    credit_card_bal:Optional[float]=Field(default=None,sa_column=Column(Float,nullable=True))
    owns_st_card:Optional[bool]=Field(default=None,sa_column=Column(Boolean,nullable=True))
    st_card_rem_bal:Optional[float]=Field(default=None,sa_column=Column(Float,nullable=True))
    has_loan_acc:Optional[bool]=Field(default=None,sa_column=Column(Boolean,nullable=True))
    loan_acc_rem_bal:Optional[float]=Field(default=None,sa_column=Column(Float,nullable=True))
    has_st_loan:Optional[bool]=Field(default=None,sa_column=Column(Boolean,nullable=True))
    st_loan_bal:Optional[float]=Field(default=None,sa_column=Column(Float,nullable=True))
    has1mth_loan_bal:Optional[bool]=Field(default=None,sa_column=Column(Boolean,nullable=True))
    bal_1mth_load:Optional[float]=Field(default=None,sa_column=Column(Float,nullable=True))
    sti_insurance:Optional[bool]=Field(default=None,sa_column=Column(Boolean,nullable=True))
    has_sequestration:Optional[bool]=Field(default=None,sa_column=Column(Boolean,nullable=True))
    has_admin_order:Optional[bool]=Field(default=None,sa_column=Column(Boolean,nullable=True))
    under_debt_review:Optional[bool]=Field(default=None,sa_column=Column(Boolean,nullable=True))
    has_judgements:Optional[bool]=Field(default=None,sa_column=Column(Boolean,nullable=True))
    bank:Optional[str]=Field(default=None,sa_column=Column(String,nullable=True))
    bal:Optional[float]=Field(default=None,sa_column=Column(Float,nullable=True))




