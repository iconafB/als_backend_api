from fastapi import APIRouter,status as http_status,Depends,Query,HTTPException,UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio.session import AsyncSession
import os
import time
import pandas as pd
import re,io,csv
from pathlib import Path
from typing import List,Dict
from utils.logger import define_logger
from utils.status_data import insert_data_into_finance_table,insert_data_into_location_table,insert_data_into_employment_table,insert_data_into_car_table,insert_data_into_contact_table
from utils.auth import get_current_active_user
from schemas.insert_data import StatusedData,EnrichedData,UploadStatusResponse,TableInsertCount
from schemas.status_data_routes import InsertStatusDataResponse,InsertEnrichedDataResponse
from database.master_db_connect import get_async_session
from utils.insert_enriched_data_helpers import  insert_table_by_count,insert_vendor_list,FIELD_INDEX,get_enriched_tuple,table_enriched_map

from schemas.insert_data import InsertEnrichedDataResponseModel,InsertStatusDataResponseModel,TableResult,BulkStatusResponse,TableInsertStatusSummary,BulkEnrichedResponse,TableInsertEnrichedSummary

from utils.insert_enriched_data_sql_queries import INFO_TBL_ENRICHED,CONTACT_TBL_SQL,FINANCE_TBL_SQL,CAR_TBL_SQL,EMPLOYMENT_TBL_SQL,LOCATION_TBL_SQL

from utils.insert_status_data_helper import get_status_tuple,insert_vendor_list_status,statused_data_generator_file,table_tuple_generator
from utils.status_data import get_status_tuple_filed_map,table_map

from utils.insert_status_data_sql_queries import INFO_STATUS_SQL,LOCATION_STATUS_SQL,CONTACT_STATUS_SQL,EMPLOYMENT_STATUS_SQL,CAR_STATUS_SQL,FINANCE_STATUS_SQL


BATCH_SIZE=10000
status_data_logger=define_logger("als_status_logger_logs","logs/status_data.log")

insert_data_router=APIRouter(tags=["Data Insertion"],prefix="/insert-data")



@insert_data_router.post("/status_data",status_code=http_status.HTTP_200_OK,description="Insert status data by providing the status data file name",response_model=BulkStatusResponse)
# #return the time taken for the queries, number of leads affected

async def insert_status_data(filename:str=Query(...,description="Provided the name of the filename with status data"),session:AsyncSession=Depends(get_async_session),user=Depends(get_current_active_user)):
    summary={}
    try:
        delta_time_1=time.time()

        status_data=[]
        #read a csv file that already exist on the server using the query parameter filename
        #check if the file exists
        if not os.path.exists(filename):
            raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"filename:{filename} does not exist on this system")
        
        #read the csv file into the pandas dataframe
        status_dataframe=pd.read_csv(filename)
        # convert the csv data into a list
        csv_list=status_dataframe.values.tolist()
        #append the uploaded list into the status data array
        status_data=status_data + csv_list
        #read the total number of leads
        leads_total=len(status_data)
        #seconds passed
        delta_time2=time.time()
        #total time difference
        total_time=delta_time2 - delta_time_1
        #rows list 
        rows=[]

        status_data=['0'+ str(row[15]) for row in status_data]

        for row in status_data:
            cell=row[15]

            if re.match(r'^\d{10}$', str(cell)):

                if row[1] is not None:
                    date_created_old=row[1].split('')[0]
                #test the id number if its has 13 characters

                idnum=str(row[3])
                salary=str(row[2])
                name = row[6]
                surname = row[7]
                address1 = row[9]
                address2 = row[10]
                suburb = row[11]
                city = row[12]
                postal = row[13]
                email = row[16]
                status = row[17]
                dob = idnum
                gender = idnum
                date_created=date_created_old

                result=StatusedData(
                    idnum=idnum,
                    cell=cell,
                    date_created=date_created,
                    salary=salary,
                    name=name,
                    surname=surname,
                    address1=address1,
                    address2=address2,
                    suburb=suburb,
                    city=city,
                    postal=postal,
                    email=email,
                    status=status,
                    dob=dob,
                    gender=gender
                )

                #append the dictionary
                rows.append(result.model_dump())
             
        #test the rows list if it has the right information
        #run queries by making updates on the information table,location table,contact table,employment table,car_table,finance table
        
        for table_num in range(1,7):
            tuples_insert=get_status_tuple_filed_map(rows,table_num)
            if tuples_insert:
                total_rows,total_batches=await table_map[table_num](tuples_insert,session)
                summary[table_num]=TableInsertStatusSummary(table_num=table_num,total_rows=total_rows,total_batches=total_batches)
                status_data_logger.info(f"Table:{table_num}:{len(tuples_insert)} rows processed")
        status_data_logger.info(f"{len(rows)} updated on finance, car , location, employment, and finance table(e)")
        #commit once after all tables
        await session.commit()
        return BulkStatusResponse(status="success",message="All table updated successfully",summary=summary)
    except Exception as e:
        await session.rollback()
        status_data_logger.exception(f"an internal server error occurred while inserting status data:%s",{e})
        raise HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"{str(e)}")





@insert_data_router.post("/enriched_data",status_code=http_status.HTTP_200_OK,response_model=BulkEnrichedResponse)

async def insert_enriched_data(filename:str=Query(...,description="Provide the name for excel filename with status data"),session:AsyncSession=Depends(get_async_session),user=Depends(get_current_active_user)):
    
    summary={}
    
    try:
        data_extraction_time_start=time.time()
        try:
            all_sheets=pd.read_excel(filename + '.xlsx',sheet_name=None,dtype=str)
            sheets=all_sheets.keys()
        except Exception as e:
            status_data_logger.exception(f"An internal server error occurred while extracting data on an excel sheet:{e}")
            raise HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while reading data from the excel file")
        for sheet_name in sheets:
            sheet=pd.read_excel(filename,sheet_name=sheet_name)
            sheet.to_csv(f"{sheet_name}.csv",index=False)
        enriched_data=[]
        detected_csv=[i for i in os.listdir() if i.endswith(".csv")]
        for d in detected_csv:
            csv_frame=pd.read_csv(d)
            csv_list=csv_frame.values.tolist()
            #append the contents of csv_list into the enriched data list
            enriched_data=enriched_data + csv_list
        total_leads=len(enriched_data)
        data_extraction_time_end=time.time()
        total_data_extraction_time=data_extraction_time_end - data_extraction_time_start
        
        #rows_list=[]

        # for e in enriched_data:
        #     Title=e[0]
        #     forename=e[1]
        #     lastname=e[2]
        #     IDNo=e[3]
        #     Race=e[4]
        #     gender=e[5]
        #     marital_status=e[6]
        #     line1=e[7]
        #     line2=e[8]
        #     line3=e[9]
        #     line4=e[10]
        #     PCode=e[11]
        #     Province=e[12]
        #     Home_Number=e[13]
        #     Work_Number=e[14]
        #     mobile_Number = e[15]
        #     mobile_Number2 = e[16]
        #     mobile_Number3 = e[17]
        #     mobile_Number4 = e[18]
        #     mobile_Number5 = e[19]
        #     mobile_Number6 = e[20]
        #     derived_income = e[21]
        #     cipro_reg = e[22]
        #     Deed_office_reg = e[23]
        #     vehicle_owner = e[24]
        #     cr_score_tu = e[25]
        #     monthly_expenditure = e[26]
        #     owns_cr_card = e[27]
        #     cr_card_rem_bal = e[28]
        #     owns_st_card = e[29]
        #     st_card_rem_bal = e[30]
        #     has_loan_acc = e[31]
        #     loan_acc_rem_bal = e[32]
        #     has_st_loan = e[33]
        #     st_loan_bal = e[34]
        #     has_1mth_loan = e[35]
        #     onemth_loan_bal = e[36]
        #     sti_insurance = e[37]
        #     has_sequestration = e[38]
        #     has_admin_order = e[39]
        #     under_debt_review = e[40]
        #     deceased_status = e[41]
        #     has_judgements = e[42]
        #     make = e[43]
        #     model = e[44]
        #     year = e[45]
        #     birth_date = e[3]

        #     new_convert = EnrichedData(Title=Title, forename=forename, lastname=lastname, IDNo=IDNo,Race=Race,gender=gender, Marital_Status=marital_status, line1=line1,line2=line2,line3=line3, line4=line4, PCode=PCode, Province=Province,Home_number=Home_Number, Work_number=Work_Number,mobile_Number=mobile_Number, mobile_Number2=mobile_Number2,mobile_Number3=mobile_Number3, mobile_Number4=mobile_Number4,mobile_Number5=mobile_Number5, mobile_Number6=mobile_Number6,derived_income=derived_income, cipro_reg=cipro_reg,Deed_office_reg=Deed_office_reg, vehicle_owner=vehicle_owner,cr_score_tu=cr_score_tu, monthly_expenditure=monthly_expenditure,owns_cr_card=owns_cr_card, cr_card_rem_bal=cr_card_rem_bal,owns_st_card=owns_st_card, st_card_rem_bal=st_card_rem_bal,has_loan_acc=has_loan_acc, loan_acc_rem_bal=loan_acc_rem_bal,has_st_loan=has_st_loan, st_loan_bal=st_loan_bal,has_1mth_loan=has_1mth_loan, onemth_loan_bal=onemth_loan_bal,sti_insurance=sti_insurance, has_sequestration=has_sequestration,has_admin_order=has_admin_order, under_debt_review=under_debt_review,deceased_status=deceased_status, has_judgements=has_judgements,make=make,model=model, year=year, birth_date = birth_date)

        #     rows_list.append(new_convert.model_dump())

        rows_list = [EnrichedData(**{field: e[idx] for field, idx in FIELD_INDEX.items()}).model_dump() for e in enriched_data]
        for table_num in range(1,7):
            enriched_tuple=get_enriched_tuple(rows_list,table_num)
            if enriched_tuple:
                table_rows,table_batches=await table_enriched_map[table_num](enriched_tuple,session)
                summary[table_num]=TableInsertEnrichedSummary(table_num=table_num,table_rows=table_rows,table_batches=table_batches)
                status_data_logger.info(f"Table:{table_num}:{len(enriched_tuple)} rows processed")
        await session.commit()
        insertion_time1=time.time()
        #generate list to insert
        # INSERT INTO info_tbl(cell, id, title, fore_name, last_name, date_of_birth, created_at, race, gender, marital_status, salary, status, derived_income, typedata, extra_info) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT(cell) DO UPDATE SET title = EXCLUDED.title, race = EXCLUDED.race, id = EXCLUDED.id, gender = EXCLUDED.gender, marital_status = EXCLUDED.marital_status, derived_income = EXCLUDED.derived_income WHERE info_tbl.cell = EXCLUDED.cell and ((info_tbl.id = EXCLUDED.id) OR (info_tbl.fore_name = EXCLUDED.fore_name or info_tbl.last_name = EXCLUDED.last_name));
        insertion_time2=time.time()
        total_insertion_time=insertion_time2 - insertion_time1
        return BulkEnrichedResponse(status="success",message="Enriched data uploaded successfully",summary=summary)
    
    except Exception as e:
        #you need log here
        status_data_logger.exception(f"an exception occurred while inserting enriched data:{str(e)}")
        raise HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while inserting enriched data")





# @insert_data_router.post("/als/insert_enriched_data",description="Provide the file name for enriched data",status_code=http_status.HTTP_200_OK,response_model=InsertEnrichedDataResponseModel)

# async def insert_enriched_data(filename: str = Query(...,description="Filename is the name of the excel file within the server with an enrinched data"), session: AsyncSession = Depends(get_async_session)):

#     start_time = time.time()

#     try:
#         all_sheets = pd.read_excel(f"{filename}.xlsx", sheet_name=None, dtype=str)
#         sheets = all_sheets.keys()

#         for sheet_name in sheets:
#             sheet = pd.read_excel(f"{filename}.xlsx", sheet_name=sheet_name, dtype=str)
#             data_dict_list = sheet.to_dict(orient="records")

#             # Validate and parse using Pydantic
#             enriched_objects = [EnrichedData(**data) for data in data_dict_list]
#             # Convert to tuples
#             info_rows = get_tuple([obj.model_dump() for obj in enriched_objects], 1)
#             contact_rows = get_tuple([obj.model_dump() for obj in enriched_objects], 2)
#             finance_rows = get_tuple([obj.model_dump() for obj in enriched_objects], 3)
#             car_rows = get_tuple([obj.model_dump() for obj in enriched_objects], 4)
#             employment_rows = get_tuple([obj.model_dump() for obj in enriched_objects], 5)
#             location_rows = get_tuple([obj.model_dump() for obj in enriched_objects], 6)

#             # Async inserts, this does not work chief, repeated code

#             await insert_vendor_list(session, INFO_SQL, info_rows)
#             await insert_vendor_list(session, CONTACT_SQL, contact_rows)
#             await insert_vendor_list(session, FINANCE_SQL, finance_rows)
#             await insert_vendor_list(session, CAR_SQL, car_rows)
#             await insert_vendor_list(session, EMPLOYMENT_SQL, employment_rows)
#             await insert_vendor_list(session, LOCATION_SQL, location_rows)

#     except Exception as e:
#         status_data_logger.exception(f"An exception occurred while inserting enriched data:{e}")
#         raise HTTPException(status_code=500, detail="An internal server error occurred while inserting enriched data into database")

#     elapsed = time.time() - start_time

#     status_data_logger.info(f"enrinched data inserted successfully on the database,records uploaded:{len(enriched_objects)}")
    
#     return InsertEnrichedDataResponseModel(status="Data Inserted Success Fully",elapsed_seconds= elapsed)



