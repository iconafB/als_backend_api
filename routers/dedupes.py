from fastapi import APIRouter,HTTPException,Depends,UploadFile,File,Query
from fastapi import status as http_status
from sqlmodel import Session,select,func
from typing import Annotated,List
import openpyxl
import random
from io import BytesIO
import re 
import os
import pandas as pd
import string
import time
from typing import Optional
from datetime import datetime
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from models.campaigns import Deduped_Campaigns,dedupe_campaigns_tbl
from models.dedupe_keys_table import manual_dedupe_key_tbl
from models.campaign_dedupe import Campaign_Dedupe
from database.master_db_connect import get_async_session
from crud.dedupe_campaign_rules import get_single_dedupe_campaign_rule_by_rule_name
from crud.dedupe_campaigns import (get_deduped_campaign,get_leads_from_db_for_dedupe_campaign_for_TEBBDY_TERDDY,get_leads_from_db_for_dedupe_campaign_for_TEFWFDY_and_TLEFHQD,get_leads_for_DITFCS_DIFFWT_TELEFFWN,get_leads_for_campaigns_list,get_leads_for_TELEAGNI_TELEBDNI_TELEDDNI_with_derived_income_and_limit,get_leads_for_OMLIFE,get_leads_for_MIWAYHKT,get_leads_for_DIFFWT,get_leads_for_DIAGTE,get_leads_for_AGTEDI,get_leads_for_DITFCS,get_leads_for_CRISPIP3,get_leads_for_TELEFFWN_with_gender_derived_income_and_limit,get_dedupe_campaigns_aggregated_count_db,search_cell_number_history_db,search_id_number_history_db,search_dedupe_campaign_by_campaign_name_db)
from crud.campaign_rules import (get_rule_by_rule_code_db,get_campaign_rule_by_rule_name_db)
from crud.campaigns import get_active_campaign_to_load
from models.information_table import info_tbl
from models.campaigns import manual_dedupe_keys
from models.dedupe_history_tracker import Dedupe_History_Tracker
from crud.dedupe_campaigns import bulk_upsert_update_info_tbl_in_batches,select_code_from_campaign_dedupe_table
from schemas.dedupe_campaigns import CreateDedupeCampaign,SubmitDedupeReturnSchema,ManualDedupeListReturn,CreateDedupeCampaign,DeleteCamapignSchema,UpdateDedupeCampaign
from schemas.dedupes import AddDedupeListResponse,SubmitDedupeReturnResponse,AddManualDedupeResponse,InsertDataDedupeTracker,PaginatedResultsResponse,PaginatedAggregatedDedupeResult
from database.database import get_session
from utils.auth import get_current_user,get_current_active_user
#from utils.status_data import get_status_tuple,insert_data_into_finance_table,insert_data_into_location_table,insert_data_into_contact_table,insert_data_into_employment_table,insert_data_into_car_table
from utils.campaigns import build_dynamic_dedupe_main_query,build_dynamic_query,build_dynamic_query_finance_tbl

from utils.logger import define_logger
from utils.add_dedupe_list_helpers import add_dedupe_list_helper
from utils.dedupes.submit_return_helpers import update_campaign_dedupe_status,fetch_delete_update_pending_campaign_ids,calculate_ids_campaign_dedupe_with_status_r
from utils.dedupes.manual_dedupe_helpers import insert_campaign_dedupe_batch, insert_manual_dedupe_info_tbl,read_file_into_dict_list,insert_dedupe_data_in_batches
from database.master_db_connect import get_async_session
from utils.load_campaign_helpers import load_leads_for_campaign
from schemas.dedupes import DataInsertionSchema
from schemas.status_data_routes import InsertStatusDataResponse,InsertEnrichedDataResponse
from crud.dedupe_campaigns import create_manual_dedupe_key

dedupe_logger=define_logger("als_dedupe_campaign_logs","logs/dedupe_route.log")


dedupe_routes=APIRouter(tags=["Dedupes"],prefix="/dedupes")

# #dedupe campaign crud
# @dedupe_routes.post("",status_code=http_status.HTTP_201_CREATED,description="create dedupe campaign by provide the campaign name,campaign code and branch",response_model=dedupe_campaigns_tbl)

# async def create_dedupe_campaign(dedupe:CreateDedupeCampaign,session:Session=Depends(get_session),user=Depends(get_current_active_user)):
#     try:
#         dedupe_campaign_query=select(dedupe_campaigns_tbl).where((dedupe_campaigns_tbl.camp_code==dedupe.camp_code)&(dedupe_campaigns_tbl.campaign_name==dedupe.campaign_name)&(dedupe_campaigns_tbl.branch==dedupe.branch))
        
#         dedupe_campaign=session.exec(dedupe_campaign_query).first()

#         if not dedupe_campaign:
#             dedupe_logger.info(f"user:{user.id} with email:{user.email} attempted to create:{dedupe.campaign_name} with campaign code:{dedupe.camp_code} at branch:{dedupe.branch}")
#             raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST,detail=f"dedupe campaign:{dedupe.campaign_name} already exist")
#         payload=dedupe.model_dump()
#         campaign=dedupe_campaigns_tbl.model_validate(payload)
#         session.add(campaign)
#         session.commit()
#         session.refresh(campaign)
#         dedupe_logger.info(f"user:{user.id} with email:{user.email} created campaign:{dedupe.campaign_name} with campaign code:{dedupe.camp_code} at branch:{dedupe.branch}")
#         return campaign

#     except Exception as e:
#         dedupe_logger.error(f"{str(e)}")
#         session.rollback()
#         return HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"error occurred while creating dedupe campaign:{dedupe.campaign_name} with campaign code:{dedupe.camp_code} for branch:{dedupe.branch}")


# @dedupe_routes.get("/{camp_code}",description="Get dedupe campaign by providing the campaign code",status_code=http_status.HTTP_200_OK,response_model=dedupe_campaigns_tbl)

# async def get_dedupe_campaign(camp_code:str,session:Session=Depends(get_session),user=Depends(get_session)):
#     try:
#         #find the campaign using campaign code
#         campaign_query=select(dedupe_campaigns_tbl).where(dedupe_campaigns_tbl.camp_code==camp_code)
#         campaign=session.exec(campaign_query).first()
#         if not campaign:
#             dedupe_logger.info(f"user:{user.id} with email:{user.email} requested campaign:{camp_code} and it does not exist")
#             raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"campaign:{camp_code} does not exist")
#         return campaign
#     except Exception as e:
#         dedupe_logger.error(f"{str(e)}")
#         return HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occuurred while retrieving campaign:{camp_code}")

#get all dedupe campaigns

# @dedupe_routes.get("",description="Get all dedupe camapigns",status_code=http_status.HTTP_200_OK,response_model=List[dedupe_campaigns_tbl])

# async def get_all_dedupe_campaigns(session:Session=Depends(get_session),user=Depends(get_current_user)):
#     try:
#         dedupe_logger.info(f"user:{user.id} with email:{user.email} retrieved all the dedupe campaigns")
#         all_campaigns_query=select(dedupe_campaigns_tbl)
#         all_dedupe_campaign=session.exec(all_campaigns_query).all()
#         return all_dedupe_campaign
#     except Exception as e:
#         dedupe_logger.error(f"{str(e)}")
#         return HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"internal server error occurred while fetching dedupe campaigns")


#get active dedupe campaigns
# @dedupe_routes.get("/active",status_code=http_status.HTTP_200_OK,description="Get all active dedupe campaigns",response_model=List[dedupe_campaigns_tbl])

# async def get_active_dedupe_campaigns(session:Session=Depends(get_session),user=Depends(get_current_active_user)):
#     try:
#         active_dedupe_campaign_queries=select(dedupe_campaigns_tbl).where(dedupe_campaigns_tbl.is_active==True)
        
#         active_dedupe_campaigns=session.exec(active_dedupe_campaign_queries).all()

#         dedupe_logger.info(f"user:{user.id} with email:{user.email} retrieved {len(active_dedupe_campaigns)} dedupe campaigns")

#         return active_dedupe_campaigns
    
#     except Exception as e:
#         dedupe_logger.error(f"{str(e)}")
#         raise HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while fetching all active campaigns")



#deactivate/delete campaigns
# @dedupe_routes.patch("/delete/{camp_code}",status_code=http_status.HTTP_204_NO_CONTENT)

# async def delete_dedupe_campaign(camp_code:str,session:Session=Depends(get_session),user=Depends(get_current_active_user)):
#     try:
#         campaign_query=select(dedupe_campaigns_tbl).where((dedupe_campaigns_tbl.camp_code==camp_code)&(dedupe_campaigns_tbl.is_active==True))
#         campaign=session.exec(campaign_query).first()
#         if not campaign:
#             dedupe_logger.info(f"campaign with campaign code:{camp_code} does not exist")
#             raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"camapign with campaign code:{camp_code} does not exist")
#         campaign.is_active=False
#         session.add(campaign)
#         session.commit()
#         dedupe_logger.info(f"user:{user.id} with email:{user.email} deleted dedupe campaign with code:{camp_code}")
#         return DeleteCamapignSchema(campaign_code=camp_code,message=f"Dedupe campaign:{camp_code} deleted")
#     except Exception as e:
#         dedupe_logger.error(f"{str(e)}")
#         raise HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while deleting dedupe campaign:{camp_code}")

#update campaign names,codes and branch should only be done by the db administrator

# @dedupe_routes.patch("/update/{camp_code}",status_code=http_status.HTTP_200_OK,description="Update campaign name,campaign code,or branch only provide the field you want to update",response_model=dedupe_campaigns_tbl)

# async def update_campaign(campaign:UpdateDedupeCampaign,session:Session=Depends(get_session),user=Depends(get_current_active_user)):
#     try:
#         campaign_query=select(dedupe_campaigns_tbl).where(dedupe_campaigns_tbl.camp_code==campaign.camp_code)
#         campaign_found=session.exec(campaign_query).first()
#         if not campaign_found:
#             raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"campaign:{campaign.camp_code}")
#         update_campaign=campaign.dict(exclude_unset=True)
#         for key,value in update_campaign.items():
#             setattr(campaign_found,key,value)
#         session.commit()
#         session.refresh(campaign_found)
#         dedupe_logger.info(f"dedupe campaign:{campaign.camp_code} with campaign name:{campaign.camp_name} updated by user:{user.id} with email:{user.email}")
#         return campaign_found
#     except Exception as e:
#         dedupe_logger.error(f"{str(e)}")
#         raise HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while updating the campaign:{campaign.camp_code}")


@dedupe_routes.post("/add-dedupes-manually",status_code=http_status.HTTP_200_OK)

async def add_dedupes_manually(campaign_name:str=Query(description="Please provide the campaign name"),file:UploadFile=File(...,description="Please provide a file for a manual dedupe"),session:AsyncSession=Depends(get_async_session),user=Depends(get_current_user)):
    
    #we are going to use these rows
    all_rows=[]

    file_path=f"temp_{file.filename}"

    #check the extension of the file format being uploaded
    if not file.filename.endswith((".csv",".xlsx",".xls")):
        raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST,detail="Invalid file format")
    try:
        #read the file's content into a BytesIO object(memory file)
        file_content=await file.read()
        file_stream=BytesIO(file_content)

        if file.filename.endswith(".csv"):
            #csv processing logic
            pass

        else:
            #check the campaign codes chiefs??
            workbook=openpyxl.load_workbook(file_stream,read_only=True)

            for sheet in workbook.worksheets:

                extracted_data=[
                  [
                      str(row[0] if row[0] is not None else ""),
                      str(row[1] if row[1] is not None else "")
                  ]
                  for row in sheet.iter_rows(min_col=1,max_col=2,values_only=True)
                  if row[0] is not None or row[1] is not None
                ]
                
                all_rows.extend(extracted_data)
                
            #insert the data into the campaign_dedupe using bulk insert, we still need the campaign name and is_verified field filled
            data_to_insert=[
                {
                    "id":row[0],
                    "cell":row[1],
                    "campaign_codes":campaign_name,
                    "is_verified":True
                }
                for row in all_rows
            ]

            batch_size=1000
            total_rows=len(data_to_insert)

            for i in range(0,total_rows,batch_size):
                batch=data_to_insert[i:i+batch_size]
                insert_stmt=insert(Campaign_Dedupe).values(batch)
                await session.execute(insert_stmt)
            await session.commit()

            dedupe_logger.info(f"user:{user.id} with email:{user.email} uploaded a deduped file for campaign:{campaign_name}")

            return {"message":f"file with name:{file.filename} for campaign:{campaign_name} uploaded successfully"}

    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        dedupe_logger.exception(f"An exception occurred while adding manual dedupe:{e}")
        raise HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"{str(e)}") 

#get the rowcount to get the number leads


#add dedupe list for a campaign, campaign code is an arguement and return a filename with leads

@dedupe_routes.post("/add-dedupe-list",status_code=http_status.HTTP_200_OK,response_model=AddDedupeListResponse)

async def add_dedupe_list(camp_code:str,session:AsyncSession=Depends(get_async_session),user=Depends(get_current_active_user)):
   
    try:
        #fetch a dedupe campaign that matches the given campaign code
        # campaign=session.exec(select(dedupe_campaigns_tbl).where(Deduped_Campaigns.camp_code==camp_code)).first()
        #this function must return the campaign code for the dedupe campaign, use that code to find the matching rule
        campaign=await get_active_campaign_to_load(camp_code,session,user)
        #raise an exception if the dedupe campaign does not exist prompting the user to create a rule for that campaign
        if campaign==None:
            dedupe_logger.info(f"dedupe campaign with code:{camp_code} does not exist or it is not a dedupe campaign,requested by user:{user.id} with email:{user.email}")
            raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"Campaign with campaign code:{camp_code} does not exist or it is not a dedupe campaign")
        #search the dedupe campaigns rules tbl using the campaign code
        # dedupe_rule_query=select(dedupe_campaign_rules_tbl).where(dedupe_campaign_rules_tbl.rule_name==camp_code)
        #find the dedupe campaign rule
        campaign_rule=await get_campaign_rule_by_rule_name_db(camp_code,session,user)
        if campaign_rule==None:
            dedupe_logger.info(f'campaign rule with rule name:{camp_code} does not exist')
            raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"campaign rule with rule name:{campaign_rule} does not exist")
        #now we have the actual list here,
        results=await load_leads_for_campaign(campaign_rule.rule_name,session)

        #get leads length
        leads_length=len(results)
        if leads_length==0:
            dedupe_logger.info(f"No leads found for dedupe campaign:{camp_code}")
            raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST,detail=f"No leads found for the dedupe campaign:{camp_code}")
        #set the today's list
        todaysdate=datetime.today().strftime('%Y-%m-%d')

        filename=camp_code + '-'+ todaysdate
        #insert leads inside the campaign_dedupe table
        suffix=''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(4))
        #create file key
        key=filename + suffix
        #prepare list of tuple to insert into the table campaign_dedupe table
        keys_to_keep=['id','cell']
        insert_list_to_campaign_dedupe_table=[{k: d[k] for k in keys_to_keep} for d in results]
        print("print the prepared list")
        print(insert_list_to_campaign_dedupe_table)
        #convert the list of dictionaries into a list of tuples
        dedupe_values={"campaign_code":camp_code,"status":'P',"key":key}
        list_to_insert_to_campaign_dedupe=[{**d,**dedupe_values} for d in insert_list_to_campaign_dedupe_table]
        #convert the entries to a list of tuples to insert to the database
        list_to_insert_to_db=[tuple(d.values()) for d in list_to_insert_to_campaign_dedupe]
        #insert the add dedupe list into the db
        insert_result=await add_dedupe_list_helper(session,list_to_insert_to_db,batch_size=1000)

        if '/' in filename:
            new_string_name=filename.replace("/","-")
            print("print")
            print(new_string_name)
        else:
            new_string_name=filename

        #convert the list to a dataframe, using list of tuples
        converted_list=[tuple(d.values()) for d in results]
        #convert the list of dictionaries to list of tuples
        df=pd.DataFrame(converted_list)
        #convert the dataframe to an excel file
        df.to_excel(new_string_name + '.xlsx',index=False)
        dedupe_logger.info(f"user:{user.id} with email:{user.email} added a dedupe list")
        return AddDedupeListResponse(FileName=new_string_name + '.txt',TotalRecordsInserted=insert_result["total_inserted"],TotalBatches=insert_result["total_batches"],TotalBatchedTime=insert_result["batch_times"],TotalTime=insert_result["total_time"])
        #
        # number_of_leads=None
        # rule_name=campaign_rule.rule_name
        # leads_salary=None
        # leads_gender=None

        # leads_derived_income=None

        # if campaign_rule.gender is not None:
        #     leads_gender=campaign_rule.gender
        
        # if campaign_rule.derived_income is not None:
        #     leads_derived_income=campaign_rule.derived_income

        # if campaign_rule.salary is not None:
        #     leads_salary=campaign_rule.salary

        # if campaign_rule.limit is not None:
        #     number_of_leads=campaign_rule.limit
            
        # if campaign_rule==None:
        #     dedupe_logger(f"no campaign rule has been created for dedupe campaign:{camp_code}")
        #     raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"no campaign rule has been created for dedupe campaign:{camp_code}")
        

        # campaign_codes=['TEBBDY','TERDDY','TEUAPIDY','TLEBHQD','TLED3HQD','TLEAHQD','TEFWFDY','TLEFHQD']
        
        #"TEFWFDY/TLEFHQD": "SELECT i.id, fore_name, last_name, i.cell FROM info_tbl i, campaign_dedupe c WHERE EXISTS (SELECT 1 FROM campaign_dedupe WHERE  c.cell = i.cell) and c.status = \"R\" and c.campaign_name = \"TEFWFDY/TLEFHQD\" and (i.id is not null) order by random() limit 3000"
        #"TEBBDY/TERDDY/TEUAPIDY/TLEBHQD/TLED3HQD/TLEAHQD": "SELECT i.id, fore_name, last_name, i.cell FROM info_tbl i, campaign_dedupe c WHERE EXISTS (SELECT 1 FROM campaign_dedupe WHERE  c.cell = i.cell) and c.status = \"R\" and c.campaign_name = \"TEBBDY/TERDDY/TEUAPIDY/TLEBHQD/TLED3HQD/TLEAHQD\" and (i.id is not null) order by random() limit 3000"

        # query = select(literal(1)).where(TableName.some_column == "some_value") 
        #  "TEBBDY/TERDDY/TEUAPIDY/TLEBHQD/TLED3HQD/TLEAHQD":

        #sub_campaign_codes=['TEFWFDY','TLEFHQD', 'TEBBDY', 'TERDDY','TEUAPIDY','TLEBHQD','TLED3HQD','TLEAHQD']

        #fetch leads for OMLIFE


        # if rule_name=='OMLIFE' and leads_derived_income is not None:

        #     leads=await get_leads_for_OMLIFE(session,leads_derived_income,number_of_leads)

        #     if leads is None:
                
        #         dedupe_logger.info(f"user:{user.id},user email:{user.email} made a request for leads that OMLIFE and no leads were found on the information table")
        #         raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST,detail=f"user:{user.id},user email:{user.email} made a request for leads that OMLIFE and no leads were found on the information table")
        
        # elif rule_name=="MIWAYHKT" and leads_derived_income is not None:
            
        #     leads=await get_leads_for_MIWAYHKT(session,leads_derived_income,number_of_leads)

        #     if leads is None:
        #         dedupe_logger.info(f"user:{user.id},user email:{user.email} made a request for leads that matches specifications MIWAYHKT and no leads were found on the information table with a derived income greater or equal to {leads_derived_income}")
        #         raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST,detail=f"user:{user.id},user email:{user.email} made a request for leads that matches MIWAYHKT specifications and no leads were found on the information table with a derived income greater or equal to {leads_derived_income}")
        
        # elif rule_name=="DIFFWT" and leads_salary is not None and leads_gender is not None:
            
        #     leads=await get_leads_for_DIFFWT(session,leads_salary,leads_gender,number_of_leads)
            
        #     if leads is None:
        #         dedupe_logger.info(f"user:{user.id},user email:{user.email} made a request for leads that matches specifications DIFFWT and no leads were found on the information table with a derived income greater or equal to {leads_derived_income}")
        #         raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST,detail=f"user:{user.id},user email:{user.email} made a request for leads that matches DIFFWT specifications and no leads were found on the information table with a derived income greater or equal to {leads_derived_income}")


        # elif rule_name=="DIAGTE":
            
        #     leads=await get_leads_for_DIAGTE(session,leads_salary,number_of_leads)
        #     if leads is None:
        #         dedupe_logger.info(f"user:{user.id},user email:{user.email} made a request for leads that matches specifications DIAGTE and no leads were found on the information table with a derived income greater or equal to {leads_salary}")
                
        #         raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST,detail=f"user:{user.id},user email:{user.email} made a request for leads that matches DIAGTE specifications and no leads were found on the information table with a derived income greater or equal to {leads_salary}")
       
        # elif rule_name in ["AGTEDI","TelAGW","TeleBudg","TeleDial"] and leads_derived_income is None:
            
        #     leads=await get_leads_for_AGTEDI(session,leads_salary,leads_length)
        #     if leads is None:
        #         dedupe_logger.info(f"user:{user.email} made an invalid request leads for campaign:AGTEDI,TelAGW,TeleBudg,or TeleDial")
        #         raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"user:{user.email} made an invalid request leads for campaign:AGTEDI,TelAGW,TeleBudg,or TeleDial")

        # elif rule_name=="DITFCS":

        #     leads=await get_leads_for_DITFCS(session,leads_salary,leads_length,leads_gender)

        #     if leads is None:
        #         dedupe_logger.info(f"user:{user.email} made an invalid request leads for campaign:DITFCS")

        #         raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"user:{user.email} made an invalid request leads for campaign:DITFCS")
        
        # elif rule_name=="CRISPIP3":

        #     leads=await get_leads_for_CRISPIP3(session,leads_derived_income)
        #     if leads is None:
        #         dedupe_logger.info(f"user:{user.email} made an invalid request leads for campaign:CRISPIP3")
        #         raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"user:{user.email} made an invalid request leads for campaign:CRISPIP3")
            
        
        # elif rule_name in ["DIAGTE","AGTEDI","TelAGW","TeleBudg","TeleDial","TELEAGNI","TELEBDNI","TELEDDNI"]:

        #     leads=await get_leads_for_campaigns_list(session,leads_salary,number_of_leads)
        #     if leads is None:
        #         dedupe_logger.info(f"user:{user.email} made an invalid request leads for campaign:CRISPIP3")
        #         raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"user:{user.email} made an invalid request leads for campaign:CRISPIP3")
            

        # elif rule_name in ["DITFCS","DIFFWT","TELEFFWN"] and leads_gender is not None and leads_salary is not None:
            
        #     leads=await get_leads_for_DITFCS_DIFFWT_TELEFFWN(session,leads_salary,leads_gender,leads_length)
        #     if leads is None:
        #         dedupe_logger.info(f"user:{user.email} made an invalid request leads for campaign:DITFCS, DIFFWT or TELEFFWN")
        #         raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"user:{user.email} made an invalid request leads for campaign:DITFCS, DIFFWT, or TELEFFWN")
            
        # elif rule_name in ["TELEAGNI","TELEBDNI","TELEDDNI"] and leads_derived_income is not None:

        #     leads=await get_leads_for_TELEAGNI_TELEBDNI_TELEDDNI_with_derived_income_and_limit(session,leads_derived_income,leads_length)
        #     if leads is None:
        #         dedupe_logger.info(f"user:{user.email} made an invalid request leads for campaign:DITFCS, DIFFWT or TELEFFWN")
        #         raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"user:{user.email} made an invalid request leads for campaign:DITFCS, DIFFWT, or TELEFFWN")
        
        # elif rule_name=="TELEFFWN" and leads_derived_income is not None and leads_gender is not None:
        #     leads=await get_leads_for_TELEFFWN_with_gender_derived_income_and_limit(session,leads_derived_income,leads_gender,leads_length)
        #     if leads is None:
                
        #         dedupe_logger.info(f"user:{user.email} made an invalid request leads for campaign:DITFCS, DIFFWT or TELEFFWN")
        #         raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"user:{user.email} made an invalid request leads for campaign:DITFCS, DIFFWT, or TELEFFWN")
        

        # elif rule_name in ["TEFWFDY","TLEFHQD"]:

        #     leads=await get_leads_from_db_for_dedupe_campaign_for_TEFWFDY_and_TLEFHQD(leads_length)
        #     if leads is None:
        #         dedupe_logger.info(f"user:{user.email} made an invalid request leads for campaign:DITFCS, DIFFWT or TELEFFWN")
        #         raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"user:{user.email} made an invalid request leads for campaign:DITFCS, DIFFWT, or TELEFFWN")
        
        # #assume this is a new dedupe campaign and it has been created on the dedupe campaign table and it's rule has been created

        # elif rule_name is not None:
        #     campaign=await get_dedupe_campaign(rule_name,session)
        #     return True
        # else:
        #     leads=await get_leads_from_db_for_dedupe_campaign_for_TEBBDY_TERDDY(leads_length)
            
        #     if leads is None:
        #         dedupe_logger.info(f"user:{user.email} made an invalid request leads for campaign:TEBBDY OR TERDDY")
        #         raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"user:{user.email} made an invalid request leads for campaign:TEBBDY OR TERDDY")
        

        # if derived_income is not None:

        #     select_query=select_query.where((info_tbl.derived_income>=derived_income) & (info_tbl.extra_info.is_(None)))
        
        # if salary is not None:

        #     select_query=select_query.where((info_tbl.salary>=salary) | (info_tbl.salary.is_(None)))
        
        #test when the lead was last used as the last step

        # cutoff_date=datetime.utcnow() - timedelta(days=30)

        # select_query=select_query.where(info_tbl.last_used < cutoff_date)

        # select_query=select_query.where(func.extract("year",info_tbl.created_at)==2019) or select_query.where(func.extract("year",info_tbl.created_at)==2020)

        
        #consider date for the following query
        #execute the query and fetch the users with
        # we are no longer fetching leads with this code
        #fetched_leads=session.exec(select_query.order_by(func.random()).limit(limit)).fetchall()
        
        #fetched_leads=len(leads)

        #raise an exception if no leads are found

        # if fetched_leads<=0:
        #     dedupe_logger.info(f"zero leads match campaign:{camp_code} with code:{camp_code}")
        #     raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"zero leads that matches this spec")
        
        #calculate the total leads
        # if fetched_leads>0:
        #     todaysdate=datetime.today().strftime('%Y-%m-%d')

        #     filename=camp_code + '-'+ todaysdate
        #     #insert leads inside the campaign_dedupe table
        #     suffix=''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(4))
        #     #create file key
        #     key=filename + suffix
        #     #print the key
        #     print(f"print the key:{key}")
        #     leads_length=len(leads)

        #     #new update data list with data for the campaign dedupes table

        #     updated_data=[(data[0],data[1],camp_code,'P',key) for data in leads]

            # campaign_object=[
            #     Campaign_Dedupe(id=data[0],cell=data[1],campaign_name=camp_code,status=data[2],code=data[3]) for data in updated_data
            # ]  
        

           
            # await session.exec(text("INSERT INTO Campaign_Dedupe(id,cell,campaign_name,status) VALUES(:id,:cell,:campaign_name,:status)"),[{"id":id,"cell":cell,"campaign_name":campaign_name,"status":status,"code":code} for id,cell,campaign_name,status,code in updated_data])
            # await session.commit()
            # dedupe_logger.info(f"inserted {leads_length}dedupe records into the campaign dedupe table")
       
        #add everything
            #session_campaign_object=session.add_all(campaign_object)

            # if not session_campaign_object == None:
            #     dedupe_logger.error(f"An error occurred while loading data to the campaign dedupe table")
            #     raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"error occurred while creating session object")
        
            # #commit the session
            # session.commit()
            # dedupe_logger.info("updated the campaign dedupes table")
            # #update data from the information table

            # informatable_data=[(data[1],'DEDUPE') for data in fetched_leads]

            # information_table_length=len(informatable_data)

            # await session.exec(text("INSERT INTO info_tbl(cell,extra_info) VALUES(:cell,:extra_info)"),[{"cell":cell,"extra_info":extra_info} for cell,extra_info in informatable_data])
            # await session.commit()

            # dedupe_logger.info(f"inserted {information_table_length} records on the information table")
           
            #Insert data into the information table
            #information_object=[info_tbl(cell=data[0],extra_info=data[1]) for data in informatable_data]
        
            #Bulk insert into the information table
            #session.add(information_object)
            #commit the data on the information table
            #session.commit()
            #dedupe_logger.info(f"inserted {information_table_length} on the information table")

        #search the filename and check if the file name has a slash(/) on its name
        # else:
        #     dedupe_logger.info(f"Campaign:{camp_code} entered is not a dedupe campaign")
        #     raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"Campaign:{camp_code} is not a dedupe campaign")
        
        # if '/' in filename:
        #     new_filename=filename.replace("/","-")
        # else:
        #     new_filename=filename
        
        #convert the leads to a dataframe
        #df=pd.DataFrame(fetched_leads)
        #convert the dataframe to an excel file
        #df.to_excel(new_filename +'.xlsx',index=False)
        #the above line creates a 
        #return AddDedupeListResponse(status=True,file_name=filename,campaign_name=camp_code,key=key)
    
    except Exception as e:
        dedupe_logger.exception(f"Internal server error occurred while adding a dedupe list for campaign {camp_code}:{e}")
        await session.rollback()
        raise HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"Internal server error occured while adding a dedupe list for campaign:{camp_code}")


#create a dedupe campaign,use the crud operation on the crud file

# @dedupe_routes.post("/dedupe-campaigns",status_code=http_status.HTTP_200_OK)

# async def create_dedupe_campaign(campaign:CreateDedupeCampaign,session:Session=Depends(get_session),user=Depends(get_current_user)):
    
#     try:
#         deduped_campaign=session.exec(select(Deduped_Campaigns).where(Deduped_Campaigns.camp_name==campaign.campaign_name and Deduped_Campaigns.camp_code==campaign.campaign_code)).first()
#         if not deduped_campaign:
#             dedupe_logger.info(f"campaign:{campaign.campaign_name} with campaign code:{campaign.campaign_code} already exist")
#             raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"campaign:{campaign.campaign_name} with campaign code:{campaign.campaign_code} already exist")
        
#         new_dedupe_campaign=Deduped_Campaigns(brach=campaign.branch,camp_name=campaign.campaign_name,camp_code=campaign.campaign_code,camp_rule={"minimum_salary":campaign.minimum_salary,"maximum_salary":campaign.maximum_salary,"derived_income":campaign.derived_income,"gender":campaign.gender,"limit":campaign.limit})
#         if not new_dedupe_campaign:
#             dedupe_logger.critical(f"error creating dedupe campaign:{campaign.campaign_name}")
#             raise HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"error occurred while creating deduped campaign:{campaign.campaign_name}")
#         session.add(new_dedupe_campaign)
#         session.commit()
#         session.refresh(new_dedupe_campaign)
#         dedupe_logger(f"dedupe campaign:{campaign.campaign_name} successfully created")

#         return new_dedupe_campaign
    
#     except Exception as e:
#         dedupe_logger.exception(f"error:{e}")
#         raise HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"internal server error occurred while creating a dedupe campaign")


#submit dedupe return route
@dedupe_routes.post("/submit-dedupe-return",status_code=http_status.HTTP_200_OK,description="Submit file returned from dedupe,this route make sure that only approved cell/id numbers are used for the dedupe campaign and all unapproved numbers are released from being flagged",response_model=SubmitDedupeReturnResponse)

async def submit_dedupe_return(data:SubmitDedupeReturnSchema,dedupe_file:UploadFile=File(...,description="File prepared for manual dedupe"),session:AsyncSession=Depends(get_async_session),user=Depends(get_current_active_user)):
    try:
        id_pattern=re.compile(r"^\d{13}$")

        file_contents=await dedupe_file.read()

        if not file_contents:
            dedupe_logger.info(f"user {user.id} with email:{user.email} uploaded an empty file")
            raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST,detail=f"Empty file uploaded")
        #search for the code/key on the table where's store


        text=file_contents.decode("utf-8",errors="strict")
        #look for the campaign
        # extract value 13-digits IDs

        result_count=await calculate_ids_campaign_dedupe_with_status_r(session,data.code)   
        #comment
        if result_count==0:
            dedupe_logger.info(f"No results associated with the dedupe key:{data.code}")

            raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST,detail=f"No results associated with the dedupe key:{data.code}")
        
        list_contents=[line for line in text.splitlines() if id_pattern.match(line)]
        #this is a tuple may cause extreme errors
        db_list=tuple(list_contents)

        #update the campaign_dedupe 
        updated_count=await update_campaign_dedupe_status(db_list,data.code,session,user)

        results=await fetch_delete_update_pending_campaign_ids(data.code,session,user)
        
        return SubmitDedupeReturnResponse(success=True,updated_ids_with_return_status=updated_count,retrieved_pending_ids_from_campaign_dedupe_table=results['retrieved_pending_ids_from_campaign_dedupe_table'],deleted_pending_ids_from_campaign_dedupe_table=results["deleted_ids_from_campaign_dedupe_table"],updated_ids_from_info_tbl=results["updated_ids_from_info_tbl"],deleted_pending_ids_with_status_code_u=results["deleted_stmt_from_campaign_dedupe"])
    

    except Exception as e:
        dedupe_logger.exception(f"an internal server error occurred while submitting dedupe by:{user.id} with email:{user.email} {e}")
        raise HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while submitting dedupe with dedupe key:{data.code}")
    

@dedupe_routes.post("/add-manual-dedupe-list2",status_code=http_status.HTTP_201_CREATED,response_model=AddManualDedupeResponse)

async def add_manual_dedupe_list2(filename:Annotated[UploadFile,File(description="Upload excel file with cell numbers")],camp_code:str=Query(description="Campaign Code"),user=Depends(get_current_active_user),session:AsyncSession=Depends(get_async_session)): 
    try:
        #read the file safely
        file_content=await filename.read()
        if not file_content:
            raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST,detail=f"Empty file uploaded")
        #validate the id number and cell number
        za_id_pattern=re.compile(r"^\d{13}$")
        za_cell_pattern=re.compile(r"^0[678]\d{8}$")
        #check the file format
        try:
            wb=openpyxl.load_workbook(BytesIO(file_content),read_only=True)
        except Exception as e:
            dedupe_logger.exception(f"Error occurred while reading from the workbook",e)
            raise HTTPException(status_code=http_status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f"the file uploaded is not a valid or readable Excel workbook (.xlsx)") 
        
        rows=[]

        try:
            for sheet in wb.worksheets:
                #skip header row
                for id_num,cell_num in sheet.iter_rows(min_row=2,max_col=2,values_only=True):
                    if id_num is None or cell_num is None:
                        continue
                    id_num_str=str(id_num).strip()
                    cell_str=str(cell_num).strip()
                    if not za_id_pattern.match(id_num_str):
                        continue
                    if not za_cell_pattern.match(cell_str):
                        continue
                    #line=[id_num_str,cell_num]
                    rows.append((id_num_str,cell_str))

        finally:
            wb.close()

        if not rows:
            dedupe_logger.info(f"An error occurred while loading data into the rows list")
            raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST,detail=f"No data could be extracted")
        
        suffix=''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(4))
        key=filename.filename + suffix
        #data to insert in the campaign_dedupe table
        data=[(r[0],r[1],camp_code,'P',key) for r in rows]

        #call the insert method
        inserted_rows_campaign_dedupe_table=await insert_campaign_dedupe_batch(session,data,user)
        #insert into the info_tbl
        info_tbl_data=[(r[1],'DEDUPE') for r in rows]

        inserted_rows_on_info_tbl=await  insert_manual_dedupe_info_tbl(session,info_tbl_data,user)

        #store the dedupe key somewhere
        dedupe_key=await create_manual_dedupe_key(session,rule_name=camp_code,dedupe_key=key,number_of_leads=len(data),user=user)

        return AddManualDedupeResponse(success=True,campaign_dedupe_records=inserted_rows_campaign_dedupe_table,info_table_records=inserted_rows_on_info_tbl,key=dedupe_key.dedupe_key)
    
    except HTTPException:
        raise

    except Exception as e:
        dedupe_logger.exception(f"An internal server error while adding manual dedupe:{e}")
        raise HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail="An internal server error occurred while adding a manual dedupe")
    



@dedupe_routes.post("/load_dedupe_tracker",status_code=http_status.HTTP_200_OK,description="upload a csv or excel file with cell numbers and/or id numbers along with the dedupe campaign name and code",response_model=InsertDataDedupeTracker)
async def upload_dedupe_campaign_records(campaign_name:str=Query(...,description="campaign name with records"),camp_code:str=Query(...,description="camp_code for the campaign"),session:AsyncSession=Depends(get_async_session),file:UploadFile=File(...,description="Dedupe file with id numbers or cell phones that must be a csv file or excel file"),user=Depends(get_current_active_user)):
    try:
        #extract the information on the file
        data=await read_file_into_dict_list(file,campaign_name=campaign_name,camp_code=camp_code)
        #load the table tracker with the extracted data
        result=await insert_dedupe_data_in_batches(session,data)
        return InsertDataDedupeTracker(message=result["message"],number_of_batches=result["total_batches_processed"],number_of_records=result["total_records_inserted"])
    
    except Exception as e:
        dedupe_logger.exception(f"An exception occurred while uploading a dedupe file:{e}")
        raise HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while uploading a dedupe file for campaign:{campaign_name} with campaign code:{camp_code}")


#get all records for each campaign

@dedupe_routes.get("/dedupe/search/campaigns",status_code=http_status.HTTP_200_OK,description="get all records for each campaign",response_model=PaginatedResultsResponse)
async def search_dedupe_campaign_by_campaign_name(campaign_name:str=Query(...,description="Provide the campaign name"),page:int=Query(1,ge=1,description="This value should be greater than one"),page_size:int=Query(10,ge=1),session:AsyncSession=Depends(get_async_session),user=Depends(get_current_active_user)):
    return await search_dedupe_campaign_by_campaign_name_db(campaign_name,page,page_size,session)

    # try:

    #     #calculate the offset based on the page number and page size
    #     offset=(page-1)*page_size
    #     #Query to count the total number of records
    #     query_count=select([func.count()]).select_from(Dedupe_History_Tracker).where(Dedupe_History_Tracker.campaign_name.ilike(f"%{campaign_name}%"))
    #     total_count=await session.execute(query_count)
    #     total_count=total_count.scalar()
    #     #calculate total pages based on total count and page_size
    #     total_pages=(total_count + page_size -1) // page_size
    #     #query to fetch paginated results
    #     query=select(Dedupe_History_Tracker)

    #     if campaign_name:
    #         query=query.where(Dedupe_History_Tracker.campaign_name.ilike(f"%{campaign_name}%"))

    #     query=query.limit(page_size).offset(offset)

    #     result=await session.execute(query)

    #     records=result.scalars().all()
    #     if not records:
    #         raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"No records found for campaign:{campaign_name}")
        
    #     return PaginatedResultsResponse(
    #         page=page,
    #         page_size=page_size,
    #         total=total_pages,
    #         total_pages=total_pages,
    #         records=records
    #     )
    
    # except Exception as e:
    #     dedupe_logger.exception(f"an exception occurred while fetching all records associated:{campaign_name}:{str(e)}")
    #     raise HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while fetch records for campaign:{campaign_name}")


#search by id
@dedupe_routes.get("/dedupe/search/id",status_code=http_status.HTTP_200_OK,description="Search the id history",response_model=PaginatedResultsResponse)
async def search_by_id_number(id_number:str=Query(None,description="Enter the id number to search it's history"),page:int=Query(1,ge=1),page_size:int=Query(10,ge=1),session:AsyncSession=Depends(get_async_session),user=Depends(get_current_active_user)):
    return await search_id_number_history_db(id_number,page,page_size,session,user)

    # if not re.match(r'^\d{13}$',id_number):
    #     raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST,detail=f"Invalid South African ID Number. It must be a 13-digit numeric string")
    # offset=(page-1)*page_size
    # query_count=select([func.count()]).select_from(Dedupe_History_Tracker).where(Dedupe_History_Tracker.id==id_number)
    # total_count=await session.execute(query_count)
    # total_count=total_count.scalar()
    # total_pages=(total_count + page_size - 1) // page_size 
    # query=select(Dedupe_History_Tracker).where(Dedupe_History_Tracker.id==id_number)
    # query = query.limit(page_size).offset(offset)
    # result = await session.execute(query)
    # records = result.scalars().all()
    # if not records:
    #     raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"No records found for this id number:{id_number}")  
    
    # return PaginatedResultsResponse(
    #     page=page,
    #     page_size=page_size,
    #     total=total_count,
    #     total_pages=total_pages,
    #     records=records
    # )



#search by cell number
@dedupe_routes.get("/dedupe/search/cell",status_code=http_status.HTTP_200_OK,description="Search the history of the cell number",response_model=PaginatedResultsResponse)
async def search_cell_phone_number_history(cell:str=Query(...,description="Enter cell number"),campaign_name:Optional[str]=Query(None,description="Enter the campaign name if available"),page:int=Query(1,ge=1),page_size:int=Query(10,ge=1),session:AsyncSession=Depends(get_async_session),user=Depends(get_current_active_user)):
    return await search_cell_number_history_db(cell,page,page_size,session,user,campaign_name)

    # try:
    #     if not re.match(r'^\d{10}$',cell):
    #         raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST,detail=f"cell number:{cell} is invalid, cell number must have 10-digit")
    #     offset=(page - 1)*page_size
    #     query_count=select([func.count()]).select_from(Dedupe_History_Tracker).where(Dedupe_History_Tracker.cell==cell)
    #     #if the campaign name is provided add it as a filter to the query
    #     if campaign_name:
    #         query_count=query_count.where(Dedupe_History_Tracker.campaign_name==campaign_name)
    #     total_count=await session.execute(query_count)
    #     total_count=total_count.scalar()
    #     total_pages=(total_count + page_size -1) // page_size
    #     query=select(Dedupe_History_Tracker).where(Dedupe_History_Tracker.cell==cell)
    #     if campaign_name:
    #         query=query.where(Dedupe_History_Tracker.campaign_name==campaign_name)
    #     query=query.limit(page_size).offset(offset)
    #     result=await session.execute(query)
    #     records=result.scalars().all()

    #     if not records:
    #         raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"Cell number:{cell} has no history")
        
    #     return PaginatedResultsResponse(page=page,page_size=page_size,total=total_pages,records=records)
    
    # except Exception as e:
    #     dedupe_logger.exception(f"an exception occurred while fetching cell number history:{str(e)}")
    #     raise HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while retrieving cell number:{cell} history")

#show aggregated count of entries for each campaign by campaign name

@dedupe_routes.get("/dedupe/campaigns/campaigns_aggregated_count",status_code=http_status.HTTP_200_OK,response_model=PaginatedAggregatedDedupeResult)
async def get_campaign_aggregated_count(page:int=Query(1,ge=1),page_size:int=Query(10,ge=1),session:AsyncSession=Depends(get_async_session),user=Depends(get_current_active_user)):
    return await get_dedupe_campaigns_aggregated_count_db(page,page_size,session,user)

    # try:
    #     #calculate the offset for pagination
    #     offset=(page - 1)*page_size
    #     #query building the query to get all campaign names and the count of associated records
    #     query=select(Dedupe_History_Tracker.campaign_name,func.count(Dedupe_History_Tracker.pk).label("record_count")).group_by(Dedupe_History_Tracker.campaign_name)
    #     #Apply pagination by limiting the number of results
    #     query=query.limit(page_size).offset(offset)
    #     #execute the query
    #     result=await session.execute(query)
    #     records=result.all()

    #     if not records:
    #         raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND,detail=f"No records found")
    #     count_query=select([func.count()]).select_from(Dedupe_History_Tracker)
    #     total_count=await session.execute(count_query)
    #     total_count=total_count.scalar()
    #     #calculate total pages
    #     total_pages=(total_count + page_size -1) // page_size
    #     return PaginatedAggregatedDedupeResult(page=page,page_size=page_size,total=total_count,total_pages=total_pages,records=records)
    
    
    # except Exception as e:
    #     dedupe_logger.exception(f"an exception occurred while getting aggregated data:{e}")
    #     raise HTTPException(status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while fetching aggregated data")

