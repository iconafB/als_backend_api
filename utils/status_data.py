from fastapi import Depends,status,HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from typing import Tuple,List
from utils.logger import define_logger
from models.contact_table import contact_tbl
from models.information_table import info_tbl
from models.employment_table import employment_tbl
from models.location_table import location_tbl
from models.car_table import car_tbl
from models.finance_table import finance_tbl

status_data_logger=define_logger("als status logger logs","logs/status_data.log")


BATCH_SIZE=10000


def map_location_tuple_to_dic(t:Tuple)->dict:

    return {"cell":t[0],"line_one":t[1],"line_two":t[2],"suburb":t[3],"city":t[4],"postal_code":t[5]}


""" 
#execute whatever is return from these methods
async def insert_data_into_finance_table(data:list,session:AsyncSession,user):
    total_records=len(data)
    total_inserted=0
    total_batches_processed=0

    try:
        # INSERT INTO finance_tbl(cell, bank, bal) VALUES (%s, %s, %s) ON CONFLICT (cell) DO NOTHING;
        for i in range(0,total_records,BATCH_SIZE):
            batch=data[i:i+BATCH_SIZE]
            data_dict=[{"cell":t[0],"bank":t[1],"bal":t[2]} for t in batch]
            insert_stmt=pg_insert(finance_tbl).values(data_dict)
            insert_stmt=insert_stmt.on_conflict_do_nothing(index_elements=[finance_tbl.cell])
            result=await session.execute(insert_stmt)
            print(result)
            await session.commit()
            total_inserted+=len(batch)
            total_batches_processed+=1

        status_data_logger.info(f"user:{user.id} with email:{user.email} inserted a total number of records:{total_inserted} in batches of:{total_batches_processed}")  
        return {"finance_batches_processed":total_batches_processed,"total_finance_records":total_inserted}
    
    except Exception as e:
        status_data_logger.exception(f"An exception occurred while inserting status data into the finance table:{e}")
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while inserting data into the finance table")
    

async def insert_data_into_car_table(data:List[Tuple],session:AsyncSession,user):
    # INSERT INTO car_tbl(cell, make, model) VALUES (%s, %s, %s) ON CONFLICT (cell) DO NOTHING;
    # list of tuples passed into this method [("0711567334","hatch","corolla"),...]
    total_records=len(data)
    total_inserted=0
    total_batches_processed=0

    try:

        for i in range(0,total_records,BATCH_SIZE):
            batch=data[i:i+BATCH_SIZE]
            data_dict=[{"cell":t[0],"make":t[1],"model":t[2]} for t in batch]
            insert_stmt=pg_insert(car_tbl).values(data_dict)
            insert_stmt=insert_stmt.on_conflict_do_nothing(index_elements=[car_tbl.cell])
            result=await session.execute(insert_stmt)
            print(result)
            await session.commit()
            total_inserted+=len(batch)
            total_batches_processed+=1

        status_data_logger.info(f"user:{user.id} with email:{user.email} inserted a total number of records:{total_inserted} in batches of:{total_batches_processed}")  
        return {"total_batches_processed":total_batches_processed,"total_records":total_inserted}
    
    except Exception as e:
        status_data_logger.exception(f"An exception occurred while inserting data into the car table:{e}")
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while inserting data on the car table")
    



async def insert_data_into_contact_table(data:List[Tuple],session:AsyncSession,user):

    total_records=len(data)
    total_inserted=0
    total_batches_processed=0
    try:
        for i in range(0,total_records,BATCH_SIZE):
            batch=data[i:i+BATCH_SIZE]
            data_dict=[{"cell":t[0],"email":t[1]} for t in batch]
            insert_stmt=pg_insert(contact_tbl).values(data_dict)
            insert_stmt=insert_stmt.on_conflict_do_nothing(index_elements=[contact_tbl.cell])
            result=await session.execute(insert_stmt)
            print(result)
            await session.commit()
            total_inserted+=len(batch)
            total_batches_processed+=1

        status_data_logger.info(f"user:{user.id} with email:{user.email} inserted a total number of records:{total_inserted} in batches of:{total_batches_processed}")  
        return {"total_batches_processed":total_batches_processed,"total_records":total_inserted}
    
    except Exception as e:
        return False
#return type for these methods

async def insert_data_into_employment_table(data:List[Tuple],session:AsyncSession,user):
    #list of tuples passed [("07165453","takealot","software engineer")]
    total_records=len(data)
    total_inserted=0
    total_batches_processed=0

    try:
        for i in range(0,total_records,BATCH_SIZE):
            batch=data[i: i+BATCH_SIZE]
            data_dict=[{"cell":t[0],"campany":t[1],"job":t[2]} for t in batch]
            insert_stmt=pg_insert(employment_tbl).values(data_dict)
            insert_stmt=insert_stmt.on_conflict_do_nothing(index_elements=[employment_tbl.cell])
            result=await session.execute(insert_stmt)
            print(result)
            await session.commit()
            total_inserted+=len(batch)
            total_batches_processed+=1
        status_data_logger.info(f"user:{user.id} with email:{user.email} inserted a total number of records:{total_inserted} in batches of:{total_batches_processed}")  
        return {"total_batches_processed":total_batches_processed,"total_records":total_inserted}

     
    except Exception as e:
        status_data_logger.exception(f"an exception occurred while inserting data into the contact tabel:{str(e)}")
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while inserting data into the employment table")


async def insert_data_into_location_table(data:List[Tuple],session:AsyncSession,user):
    total_records=len(data)
    total_inserted=0
    batches_processed=0

    try:
        for i in range(0,total_records,BATCH_SIZE):
            batch=data[i:i + BATCH_SIZE]
            data_dict=[{"cell":t[0],"line_one":t[1],"line_two":t[2],"suburb":t[3],"city":t[4],"postal_code":t[5]} for t in batch]
            insert_stmt=pg_insert(location_tbl).values(data_dict)
            insert_stmt=insert_stmt.on_conflict_do_nothing(index_elements=[location_tbl.cell])
            result=await session.execute(insert_stmt)
            print(result)
            await session.commit()
            batches_processed+=1
            total_inserted+=len(batch)
        
        status_data_logger.info(f"user:{user.id} with email:{user.email} inserted a total number of records:{total_inserted} in batches of:{batches_processed}")  
        
        return {"total_batches_processed":batches_processed,"total_records":total_inserted}
    
    except Exception as e:
        status_data_logger.exception(f"an exception occured while inserting data on the location table by:{str(e)}")
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"an exception occurred while inserting data into the location table")


   
async def insert_data_into_information_table(data:List[Tuple],session:AsyncSession,user):
    try:
        total_records=len(data)
        total_inserted=0
        batches_processed=0

        for i in range(0,total_records,BATCH_SIZE):
            batch=data[i: i+BATCH_SIZE]
            data_dict=[{"cell":t[0],"id":t[1],"fore_name":t[2],"last_name":t[3],"date_of_birth":t[4],"created_at":t[5],"gender":t[6],"salary":t[7],"status":t[8],"typedata":t[9]} for t in batch]

            insert_stmt=pg_insert(info_tbl).values(data_dict)
            update_statement=insert_stmt.on_conflict_do_update(index_elements=[info_tbl.cell],set_={
                    "created_at": insert_stmt.excluded.created_at,
                    "salary": insert_stmt.excluded.salary,
                    "status": insert_stmt.excluded.status,
                },where=(
                    (info_tbl.id == insert_stmt.excluded.id)
                    |
                    (
                        (info_tbl.fore_name == insert_stmt.excluded.fore_name)
                        |
                        (info_tbl.last_name == insert_stmt.excluded.last_name)
                    )
                )
                )
            result=await session.execute(update_statement)
            await session.commit()
            batches_processed+=1
            total_inserted+=len(batch)
            print(i)
        
        status_data_logger.info(f"user:{user.id} with email:{user.email} inserted data into the information table(info_tbl)")
        
        return {
            "total_batches_processed": batches_processed,
            "total_records_processed": total_records
        }
    
    except Exception as e:
        status_data_logger.exception(f"An exception occurred while inserting data into the information table:{e}")
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while inserting data into the information table")




 """

def get_status_tuple(dataList:list,num:int):

    #build tuples to insert on the table,build a list comprehension
    try:
        if num==1:
            #tuple for the info_tbl
            return [(d['cell'],d['id'],d['fore_name'],d['last_name'],d['date_of_birth'],d['created_at'],d['gender'],d['salary'],d['status'],d['typedata']) for d in dataList]
        
        elif num==2:
            #tuple for the location_tbl
            return [(d['cell'],d['line_one'],d['line_two'],d['suburb'],d['city'],d['postal_code']) for d in dataList]
        
        elif num==3:
            #tuple for the contact_tbl

            return [(d['cell'],d['email']) for d in dataList]
        
        elif num==4:
            #tuple for the employment_tbl
            return [(d['cell'],d['company'],d['job']) for d in dataList]
        
        elif num==5:
            #tuple for the car_tbl
            return [(d['cell'],d['make'],d['model']) for d in dataList]
        
        else:
            #tuple for the finance_tbl
            return [(d['cell'],d['bank'],d['bal']) for d in dataList]
    
    except Exception as e:
        status_data_logger.exception(f"an exception occurred while generating the status tuple:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while generating the status tuple")



#this method returns a list of tuples

def get_status_tuple_filed_map(dataList:list,num:int):

    #information table
    if num==1:
        return [(r["cell"],r["idnum"],r["name"],r["surname"],r["dob"],r["date_created"],r["gender"],r["salary"],r["status"],"Status",) for r in dataList]
    #location table
    elif num==2:
        return [(r["cell"],r["address1"],r["address2"],r["suburb"],r["city"],r["postal"]) for r in dataList]
    #contact table
    elif num==3:
        return [(r["cell"],r["email"]) for r in dataList]
    #employment table
    elif num==4:
        return [(r["cell"],r["company"],r["job"]) for r in dataList]
    #car table
    elif num==5:
        return [(r["cell"],r["car"],r["model"]) for r in dataList]
    #finance table
    elif num==6:
        return [(r["cell"],r["bank"],r["bal"]) for r in dataList]
    else:
        return []


#insert methods 
#information table (with ON CONFLICT DO UPDATE)

async def insert_status_data_information_table(data:List[Tuple],session:AsyncSession):

    columns=["cell","id","fore_name","last_name","date_of_birth","created_at","gender","salary","status","typedata"]
    update_cols={
        "created_at":pg_insert(info_tbl).excluded.created_at,
        "salary":pg_insert(info_tbl).excluded.salary,
        "status":pg_insert(info_tbl).excluded.status
    }
    total_rows,total_batches=0.0
    for i in range(0,len(data),BATCH_SIZE):
        batch=data[i:i+BATCH_SIZE]
        data_dict = [{col: t[idx] for idx, col in enumerate(columns)} for t in batch]
        stmt=pg_insert(info_tbl).values(data_dict).on_conflict_do_update(index_elements=["cell"], set_=update_cols)
        await session.execute(stmt)
        total_rows+=len(batch)
        total_batches+=1
    return total_rows,total_batches

#insert into the location table
async def insert_status_data_location_table(data:List[Tuple],session:AsyncSession):

    columns = ["cell", "line_one", "line_two", "suburb", "city", "postal_code"]

    total_rows,total_batches=0.0
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        data_dict = [{col: t[idx] for idx, col in enumerate(columns)} for t in batch]
        stmt = pg_insert(location_tbl).values(data_dict).on_conflict_do_nothing(index_elements=["cell"])
        await session.execute(stmt)
        total_rows+=len(batch)
        total_batches+=1
    return total_rows,total_batches

#insert into the contact table
async def insert_status_data_contact_table(data:List[Tuple],session:AsyncSession):

    columns = ["cell", "email"]

    total_rows,total_batches=0.0
    for i in range(0, len(data), BATCH_SIZE):
        batch=data[i:i + BATCH_SIZE]
        data_dict=[{col: t[idx] for idx, col in enumerate(columns)} for t in batch]
        stmt = pg_insert(contact_tbl).values(data_dict).on_conflict_do_nothing(index_elements=["cell"])
        await session.execute(stmt)
        total_rows+=len(batch)
        total_batches+=1

    return total_rows,total_batches

#execute insert into employment table
async def insert_status_data_employment_table(data:List[Tuple],session:AsyncSession):
    
    columns = ["cell", "company", "job"]
    total_rows,total_batches=0.0

    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        data_dict = [{col: t[idx] for idx, col in enumerate(columns)} for t in batch]
        stmt=pg_insert(employment_tbl).values(data_dict).on_conflict_do_nothing(index_elements=["cell"])
        await session.execute(stmt)
        total_rows+=len(batch)
        total_batches+=1
    return total_rows,total_batches

#insert into the car table
async def insert_status_data_car_table(data:List[Tuple],session:AsyncSession):
    columns = ["cell", "make", "model"]
    total_rows,total_batches=0.0
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        data_dict = [{col: t[idx] for idx, col in enumerate(columns)} for t in batch]
        stmt=pg_insert(car_tbl).values(data_dict).on_conflict_do_nothing(index_elements=["cell"])
        await session.execute(stmt)
        total_rows+=len(batch)
        total_batches+=1
    return total_rows,total_batches
#insert into the finance table

async def insert_status_data_finance_table(data: List[Tuple], session: AsyncSession):
    columns = ["cell", "bank", "bal"]
    total_rows,total_batches=0.0
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        data_dict = [{col: t[idx] for idx, col in enumerate(columns)} for t in batch]
        stmt = pg_insert(finance_tbl).values(data_dict).on_conflict_do_nothing(index_elements=["cell"])
        await session.execute(stmt)
        total_rows+=len(batch)
        total_batches+=1

    return total_rows,total_batches


table_map = {
    1: insert_status_data_information_table,
    2: insert_status_data_location_table,
    3: insert_status_data_contact_table,
    4: insert_status_data_employment_table,
    5: insert_status_data_car_table,
    6: insert_status_data_finance_table,
}
