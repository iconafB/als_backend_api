
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from typing import List,Tuple
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import text,or_,and_
from utils.insert_enriched_data_sql_queries import INFO_TBL_ENRICHED,CONTACT_TBL_SQL,FINANCE_TBL_SQL,CAR_TBL_SQL,EMPLOYMENT_TBL_SQL,LOCATION_TBL_SQL
# Helper function to convert dict to tuple
from typing import List, Dict
from utils.logger import define_logger
from models.information_table import info_tbl
from models.location_table import location_tbl
from models.car_table import car_tbl
from models.finance_table import finance_tbl
from models.contact_table import contact_tbl
from models.employment_table import employment_tbl

BATCH_SIZE=10000

enriched_data_logger=define_logger("als_erinched_logger_logs","logs/enriched_data.log")

def transform_tuples_to_dicts(datadictlist: List[Dict], selector: int) -> List[Dict]:

    """
    Convert raw datadictlist into list-of-dict for the given table (selector 1-6).
    Matches the expected column names in SQL queries.
    """
    if selector == 1:  # info_tbl
        return [
            {
                "cell": d['mobile_Number'],
                "id": d['IDNo'],
                "title": d['Title'],
                "fore_name": d['forename'],
                "last_name": d['lastname'],
                "date_of_birth": d['birth_date'],
                "created_at": None,
                "race": d['Race'],
                "gender": d['gender'],
                "marital_status": d['Marital_Status'],
                "salary": None,
                "status": None,
                "derived_income": d['derived_income'],
                "typedata": "Enriched",
                "extra_info": None
            }
            for d in datadictlist
        ]

    if selector == 2:  # contact_tbl

        return [
            {
                "cell": d['mobile_Number'],
                "home_number": d['Home_number'],
                "work_number": d['Work_number'],
                "mobile_number_one": d['mobile_Number'],
                "mobile_number_two": d['mobile_Number2'],
                "mobile_number_three": d['mobile_Number3'],
                "mobile_number_four": d['mobile_Number4'],
                "mobile_number_five": d['mobile_Number5'],
                "mobile_number_six": d['mobile_Number6'],
                "email": None
            }
            for d in datadictlist
        ]

    if selector == 3:  # finance_tbl

        return [
            {
                "cell": d['mobile_Number'],
                "cipro_reg": d['cipro_reg'],
                "deed_office_reg": d['Deed_office_reg'],
                "vehicle_owner": d['vehicle_owner'],
                "credit_score": d['cr_score_tu'],
                "monthly_expenditure": d['monthly_expenditure'],
                "owns_credit_card": d['owns_cr_card'],
                "credit_card_bal": d['cr_card_rem_bal'],
                "owns_st_card": d['owns_st_card'],
                "st_card_rem_bal": d['st_card_rem_bal'],
                "has_loan_acc": d['has_loan_acc'],
                "loan_acc_rem_bal": d['loan_acc_rem_bal'],
                "has_st_loan": d['has_st_loan'],
                "st_loan_bal": d['st_loan_bal'],
                "has1mth_loan_bal": d['has_1mth_loan'],
                "bal_1mth_load": d['onemth_loan_bal'],
                "sti_insurance": d['sti_insurance'],
                "has_sequestration": d['has_sequestration'],
                "has_admin_order": d['has_admin_order'],
                "under_debt_review": d['under_debt_review'],
                "has_judgements": d['has_judgements']
            }
            for d in datadictlist
        ]

    if selector == 4:  # car_tbl
        return [
            {
                "cell": d['mobile_Number'],
                "make": d['make'],
                "model": d['model'],
                "year": d['year']
            }
            for d in datadictlist
        ]

    if selector == 5:  # employment_tbl
        return [
            {
                "cell": d['mobile_Number'],
                "job": None,
                "occupation": None,
                "company": None
            }
            for d in datadictlist
        ]

    if selector == 6:  # location_tbl
        return [
            {
                "cell": d['mobile_Number'],
                "line_one": d['line1'],
                "line_two": d['line2'],
                "line_three": d['line3'],
                "line_four": d['line4'],
                "postal_code": d['PCode'],
                "province": d['Province'],
                "city": None
            }
            for d in datadictlist
        ]
    
    return []



async def insert_table_by_count(session:AsyncSession,datalistdict:List[Dict]):

     # Map selector to SQL query
    sql_map = {
        1: ("info_tbl", INFO_TBL_ENRICHED),
        2: ("contact_tbl", CONTACT_TBL_SQL),
        3: ("finance_tbl", FINANCE_TBL_SQL),
        4: ("car_tbl", CAR_TBL_SQL),
        5: ("employment_tbl", EMPLOYMENT_TBL_SQL),
        6: ("location_tbl", LOCATION_TBL_SQL)
    }
    results={}

    for selector in range(1,7):
        table_name,sql=sql_map[selector]
        data_list=transform_tuples_to_dicts(datalistdict,selector)
        rows_inserted=await bulk_insert_method(session,sql,data_list)
        results[table_name]=rows_inserted

    return results




async def bulk_insert_method(session:AsyncSession,sql:text,data_list:List[Dict]):
    
    if not data_list:
        enriched_data_logger.info(f"No data to insert")
        return False
    try:
        result=await session.execute(sql,data_list)
        inserted_rows=result.fetchall() #list of rows returned
        await session.commit()
        return len(inserted_rows)
    except Exception as e:
        enriched_data_logger.exception(f"an exception occurred while inserting enriched data:{e}")
        await session.rollback()
        return 0



FIELD_INDEX = {
    "Title": 0,
    "forename": 1,
    "lastname": 2,
    "IDNo": 3,
    "Race": 4,
    "gender": 5,
    "Marital_Status": 6,
    "line1": 7,
    "line2": 8,
    "line3": 9,
    "line4": 10,
    "PCode": 11,
    "Province": 12,
    "Home_number": 13,
    "Work_number": 14,
    "mobile_Number": 15,
    "mobile_Number2": 16,
    "mobile_Number3": 17,
    "mobile_Number4": 18,
    "mobile_Number5": 19,
    "mobile_Number6": 20,
    "derived_income": 21,
    "cipro_reg": 22,
    "Deed_office_reg": 23,
    "vehicle_owner": 24,
    "cr_score_tu": 25,
    "monthly_expenditure": 26,
    "owns_cr_card": 27,
    "cr_card_rem_bal": 28,
    "owns_st_card": 29,
    "st_card_rem_bal": 30,
    "has_loan_acc": 31,
    "loan_acc_rem_bal": 32,
    "has_st_loan": 33,
    "st_loan_bal": 34,
    "has_1mth_loan": 35,
    "onemth_loan_bal": 36,
    "sti_insurance": 37,
    "has_sequestration": 38,
    "has_admin_order": 39,
    "under_debt_review": 40,
    "deceased_status": 41,
    "has_judgements": 42,
    "make": 43,
    "model": 44,
    "year": 45,
    "birth_date": 3,  # derived from IDNo later
}




def get_enriched_tuple(datadictList:list,num):

    if num==1:

        return [(d["mobile_Number"],d["IDNo"],d["Title"],d["forename"],d["lastname"],d["birth_date"],None,d["Race"],d["gender"],d["Marital_Status"],None,None,d["derived_income"],"Enriched",None) for d in datadictList]
    elif num==2:

        return [(d["mobile_Number"],d["Home_Number"],d["Work_Number"], d["mobile_Number"],d["mobile_Number"],d["mobile_Number2"],d["mobile_Number3"],d["mobile_Number4"],d["mobile_Number5"],d["mobile_Number6"],None )for d in datadictList]
    elif num==3:

        return [(d["mobile_Number"],d["cipro_reg"],d["Deed_office_reg"],d["vehicle_owner"],d["cr_score_tu"],d["monthly_expenditure"],d["owns_cr_card"],d["cr_card_rem_bal"],d["owns_st_card"],d["st_card_rem_bal"],d["has_loan_acc"],d["loan_acc_rem_bal"],d["has_st_loan"],d["st_loan_bal"],d["has_1mth_loan"],d["onemth_loan_bal"],d["sti_insurance"],d["has_sequestration"],d["has_admin_order"],d["under_debt_review"],d["has_judgements"]) for d in datadictList]
    elif num==4:
         
         return [(d["mobile_Number"],d["make"],d["model"],d["year"])for d in datadictList]
    elif num==5:

        return [(d["mobile_Number"],None,None,None)for d in datadictList]
    elif num==6:

        return [(d["mobile_Number"],d["line1"],d["line2"],d["line3"],d["line4"],d["PCode"],d["Province"],None) for d in datadictList]
    return []
    

#insert enriched data into the information table

async def insert_enriched_data_information_table(data:List[Tuple],session:AsyncSession):

    columns=["cell","id","title","fore_name","last_name","date_of_birth","created_at","race","gender","marital_status","salary","status","derived_income","typedata","extra_info"]
    total_rows,total_batches=0.0
    for i in range(0,len(data),BATCH_SIZE):
        batch=data[i:i+BATCH_SIZE]
        #convert tuples to list
        data_dicts=[{columns[idx]:value for idx,value in enumerate(row)} for row in batch]
        insert_stmt=pg_insert(info_tbl).values(data_dicts)
        
        update_stmt=insert_stmt.on_conflict_do_update(index_elements=["cell"],set_={
            "title":insert_stmt.excluded.title,
            "race":insert_stmt.excluded.race,
            "id":insert_stmt.excluded.id,
            "gender":insert_stmt.excluded.gender,
            "marital_status":insert_stmt.excluded.marital_status,
            "derived_income":insert_stmt.excluded.derived_income
        },
        where={
            and_(
                info_tbl.cell==insert_stmt.excluded.cell,
                or_(
                    info_tbl.id==insert_stmt.excluded.id,
                    info_tbl.fore_name==insert_stmt.fore_name,
                    info_tbl.last_name==insert_stmt.last_name,
                )
            )
        }
        )

        await session.execute(update_stmt)
        total_rows+=len(batch)
        total_batches+=1
        
    return total_rows,total_batches

#insert enriched data into the location table
async def insert_enriched_data_location_table(data:List[Tuple],session:AsyncSession):
    #INSERT INTO location_tbl(cell, line_one, line_two, line_three, line_four, postal_code, province, city) VALUES(%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT(cell) DO NOTHING;

    columns = ["cell", "line_one", "line_two", "line_three","line_four","postal_code","province", "city"]
    total_rows,total_batches=0.0
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        data_dict=[{columns[idx]:value for idx,value in enumerate(row)} for row in batch]
        stmt = pg_insert(location_tbl).values(data_dict).on_conflict_do_nothing(index_elements=["cell"])
        await session.execute(stmt)
        total_rows+=len(batch)
        total_batches+=1

    return total_rows,total_batches

#insert enriched data into the car table(car_tbl)
async def insert_enriched_data_car_table(data:List[Tuple],session:AsyncSession):
    #INSERT INTO car_tbl(cell, make, model, year) VALUES(%s, %s, %s, %s) ON CONFLICT(cell) DO NOTHING;
    columns = ["cell", "make", "model", "year"]
    total_rows,total_batches=0.0
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        data_dict=[{columns[idx]:value for idx,value in enumerate(row)} for row in batch]
        stmt = pg_insert(car_tbl).values(data_dict).on_conflict_do_nothing(index_elements=["cell"])
        await session.execute(stmt)
        total_rows+=len(batch)
        total_batches+=1

    return total_rows,total_batches

#insert enriched data into the finance table 
async def insert_enriched_data_finance_table(data:List[Tuple],session:AsyncSession):
    
    columns = ["cell", "cipro_reg", "deed_office_reg", "vehicle_owner","credit_score","monthly_expenditure","owns_credit_card","credit_card_bal","owns_st_card","st_card_rem_bal","has_loan_acc","loan_acc_rem_bal", "has_st_loan", "st_loan_bal", "has1mth_loan_bal", "bal_1mth_load", "sti_insurance", "has_sequestration", "has_admin_order", "under_debt_review", "has_judgements"]
    total_rows,total_batches=0.0
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        data_dict=[{columns[idx]:value for idx,value in enumerate(row)} for row in batch]
        stmt = pg_insert(finance_tbl).values(data_dict).on_conflict_do_nothing(index_elements=["cell"])
        await session.execute(stmt)
        total_rows+=len(batch)
        total_batches+=1

    return total_rows,total_batches


async def insert_enriched_data_contact_table(data:List[Tuple],session:AsyncSession):
    #INSERT INTO car_tbl(cell, make, model, year) VALUES(%s, %s, %s, %s) ON CONFLICT(cell) DO NOTHING;
    columns = ["cell", "home_number", "work_number", "mobile_number_one", "mobile_number_two", "mobile_number_three", "mobile_number_four", "mobile_number_five", "mobile_number_six", "email"]
    total_rows,total_batches=0.0
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        data_dict=[{columns[idx]:value for idx,value in enumerate(row)} for row in batch]
        stmt = pg_insert(contact_tbl).values(data_dict).on_conflict_do_nothing(index_elements=["cell"])
        await session.execute(stmt)
        total_rows+=len(batch)
        total_batches+=1

    return total_rows,total_batches



#insert enriched data into the car table(car_tbl)

async def insert_enriched_data_employment_table(data:List[Tuple],session:AsyncSession):
    columns = ["cell", "job", "occupation", "company"]
    
    total_rows,total_batches=0.0
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        data_dict=[{columns[idx]:value for idx,value in enumerate(row)} for row in batch]
        stmt = pg_insert(employment_tbl).values(data_dict).on_conflict_do_nothing(index_elements=["cell"])
        await session.execute(stmt)
        total_rows+=len(batch)
        total_batches+=1

    return total_rows,total_batches

table_enriched_map = {
    1: insert_enriched_data_information_table,
    2: insert_enriched_data_location_table,
    3: insert_enriched_data_contact_table,
    4: insert_enriched_data_employment_table,
    5: insert_enriched_data_car_table,
    6: insert_enriched_data_finance_table,
}

# Async insert helper

async def insert_vendor_list(session: AsyncSession, sqlstmt: str, vendor_list: List[tuple]):
    try:
        #This is wrong it will start a transaction within a transaction
        conn = await session.connection()
        await conn.exec_driver_sql(sqlstmt, vendor_list)

    except Exception as e:
        await session.rollback()
        raise e
