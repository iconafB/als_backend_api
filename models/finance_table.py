from sqlmodel import SQLModel,Field,Relationship
from typing import Optional,TYPE_CHECKING
from datetime import datetime
from sqlalchemy import func

#Indexes the tables fool

if TYPE_CHECKING:
    from models.information_table import info_tbl

   
class finance_tbl(SQLModel,table=True):
    pk:int=Field(primary_key=True,default=None,nullable=False)
    cell:str=Field(foreign_key="info_tbl.cell",index=True)
    cipro_reg:Optional[bool]=None
    deed_office_reg:Optional[bool]=None
    vehicle_owner:Optional[bool]=None
    credit_score:Optional[float]=None
    monthly_expenditure:Optional[float]=None
    owns_credit_card:Optional[bool]=None
    owns_st_card:Optional[bool]=None
    credit_card_bal:Optional[float]=None
    st_card_rem_bal:Optional[float]=None
    has_loan_acc:Optional[bool]=None
    loan_acc_rem_bal:Optional[float]=None
    has_st_loan:Optional[float]=None
    st_loan_bal:Optional[float]=None
    has1mth_loan_bal:Optional[bool]=None
    bal_1mth_load:Optional[float]=None
    sti_insurance:Optional[bool]=None
    has_sequestration:Optional[bool]=None
    has_admin_order:Optional[bool]=None
    under_debt_review:Optional[bool]=None
    has_judgements:Optional[bool]=None
    bank:Optional[str]=None
    bal:Optional[float]=None
    # info_location:info_tbl=Relationship(back_populates="finance")
