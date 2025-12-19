from sqlalchemy.ext.asyncio.session import AsyncSession
import time
import re
import datetime
from sqlalchemy import text
from utils.logger import define_logger
from utils.add_dedupe_list_helpers_sql_queries import INSERT_INTO_CAMPAIGN_DEDUPE_TABLE_QUERY,INSERT_INTO_INFO_TABLE_QUERY

dedupe_logger=define_logger("als_dedupe_campaign_logs","logs/dedupe_route.log")


async def add_dedupe_list_helper(session:AsyncSession,tuple_list:list[tuple],batch_size:int=1000):
    total_inserted=0
    total_batches=0
    batch_times=[]
    start_total=time.perf_counter()

    for i in range(0,len(tuple_list),batch_size):
        batch=tuple_list[i:i + batch_size]
        campaign_dedupe_list=[{"id":d[0],"cell":d[1],"camp_code":d[2],"status":d[3],"key":d[4]} for d in batch]
        info_table_list=[(t[1],'DEDUPE') for t in batch]
        start_batch=time.perf_counter() # start batch timer
        insert_data_to_campaign_dedupe_tbl=text(INSERT_INTO_CAMPAIGN_DEDUPE_TABLE_QUERY)
        insert_data_to_info_tbl=text(INSERT_INTO_INFO_TABLE_QUERY)
        await session.execute(insert_data_to_campaign_dedupe_tbl,campaign_dedupe_list)
        await session.execute(insert_data_to_info_tbl,info_table_list)
        await session.commit()
        #update counters
        end_batch=time.perf_counter()
        batch_times.append(end_batch - start_batch)
        total_inserted+=len(batch)
        total_batches+=1

    end_total=time.perf_counter()
    total_time=end_total - start_total
    dedupe_logger.info(f"inserted records:{len(tuple_list)} in total of {total_batches} batches, in this total time:{total_time}")
    
    return {"total_inserted":total_inserted,"total_batches":total_batches,"batch_times":batch_times,"total_time":total_time}



#validate id numbers
def validate_sa_numbers(id_number:str):
    #validate id numbers and return full details
    if not re.fullmatch(r"\d{13}",id_number):
        return {"valid":False,"id":id_number,"error":"ID Number must be 13 digits"}
    yy=int(id_number[0:2])
    mm=int(id_number[2:4])
    dd=int(id_number[4:6])
    gender_digit=int(id_number[6])
    citizenship_digit=id_number[10]
    checksum=int(id_number[-1])
    current_year=datetime.datetime.now().year % 100
    year=2000 + yy if yy<=current_year else 1900 + yy
    try:
        birth_date=datetime.date(year,mm,dd)
        print("print the birth_date")
        print(birth_date)
    except ValueError:
        return {"valid":False,"id":id_number,"error":"Invalid birth date"}
    
    def luhn(number:str)->int:
        total=0
        reverse_digits= number[::-1]
        for i,digit in enumerate(reverse_digits):
            n=int(digit)
            if i%2==1:
                n*=2
                if n >9:
                    n-=9
            total+=n

        return total%10
    
    if luhn(id_number[:-1]+"0")!=checksum:
        return {"valid":False,"id":id_number,"error":"Invalid checksum"}
    
    gender= "Female" if gender_digit < 5 else "Male"

    #citizenship = "SA Citizen" if citizenship_digit == "0" else "Permanent Resident"

    return {"id":id_number,"Valid":True,"gender":gender}


def validate_id_list(id_list:list[str]):
    valid_ids=[]
    invalid_ids=[]
    for id_num in id_list:
        result=validate_sa_numbers(id_num)
        if result["valid"]:
            valid_ids.append(result)
        else:
            invalid_ids.append(result)
    return valid_ids,invalid_ids







