from fastapi import APIRouter,status,Depends,HTTPException,Path,Query,BackgroundTasks
from sqlalchemy.ext.asyncio.session import AsyncSession
from utils.dmasa_service_helpers import DMAService 
from utils.load_data_to_als_service import LoadDataToALSService
from datetime import date
from schemas.campaigns import CreateCampaign,LoadCampaignResponse,LoadCampaign,UpdateCampaignName,CreateCampaignResponse,PaginatedCampaigResponse,PaginatedInfiniteResponse,CampaignSpecLevelResponse,CampaignsTotal
from database.master_database_prod import get_async_master_prod_session
from utils.dynamic_sql_rule_function import fetch_rule_sql,remove_order_by_random,build_left_anti_join_sql,replace_double_quotes_with_single,execute_built_sql_query,ensure_info_pk_selected
from utils.list_names import get_list_name
from utils.auth import get_current_active_user
from utils.logger import define_logger
from utils.load_campaign_helpers import load_leads_for_campaign
from crud.campaigns import (create_campaign_db,get_all_campaigns_by_branch_db,get_campaign_by_code_db,update_campaign_name_db,get_active_campaign_to_load,get_all_campaigns_db,get_all_campaigns_infinite_scroll_db,get_spec_level_campaign_name_db,get_total_campaigns_on_the_db,search_campaigns_from_db,get_campaign_by_code_and_branch)
from crud.campaign_rules import (get_campaign_rule_by_rule_name_db,get_rule_code_legacy_table_by_rule_name,get_rule_code_from_new_table_by_rule_name)
from utils.leads_cleaner_load_campaign import clean_and_process_results
from utils.load_als_data_REQ_helper import load_leads_to_als_REQ,load_leads_to_als_req,inject_info_pk
from utils.dynamic_sql_rule_function import fetch_rule_sql,execute_built_sql_query
from utils.blacklist_helper import write_opted_ins_to_blacklist_bg


campaigns_logger=define_logger("als_campaign_logger","logs/campaigns_route.log")

campaigns_router=APIRouter(tags=["Campaigns"],prefix="/campaigns")

def normalize_numbers(value)->bool:

    if isinstance(value,bool):
        return value
    if isinstance(value,str):
        return value.strip().lower() == "true"
    
    return False



def normalize_cell(value: str) -> str:
    value = str(value).strip()
    return value if value.startswith("0") else f"0{value}"

#poor structure
@campaigns_router.post("/create-campaign",status_code=status.HTTP_201_CREATED,description="Create a new campaign by providing a branch, campaign code and campaign name",response_model=CreateCampaignResponse)
async def create_campaign(campaign:CreateCampaign,session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await create_campaign_db(campaign,session,user)


#calculate the number of campaigns on the the db
@campaigns_router.get("/total",status_code=status.HTTP_200_OK,description="Get the total number of campaigns on the system",response_model=CampaignsTotal)
async def get_the_total_number_of_campaigns(session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await get_total_campaigns_on_the_db(session,user)


#load a campaign given the campaign code: camp_code
@campaigns_router.post("/load-campaign",description="load campaign by providing campaign code and branch name",status_code=status.HTTP_200_OK,response_model=LoadCampaignResponse)

async def load_campaign(load_campaign:LoadCampaign,dma_object:DMAService,load_data_als:LoadDataToALSService,background_task:BackgroundTasks,session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    #calculate the number of entries in table campaign_rules
    try:

        # get the the campaign to load
        #find_campaign=await get_campaign_by_code_db(load_campaign.camp_code,session,user)

        find_campaign=await get_campaign_by_code_and_branch(load_campaign.camp_code,load_campaign.branch,session,user)
        #load the new way
        if find_campaign.is_new:
            #find an active campaign rule
            campaign_code=await get_active_campaign_to_load(load_campaign.camp_code,session)
            if campaign_code is None:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"Campaign has not been created on the database")
            
            #fetch the associated rule,this works
            #fetch on the new new table, rule_name not camp_code
            rule=await get_campaign_rule_by_rule_name_db(campaign_code,session,user)

            rule_code=rule.rule_code
            #is_deduped=rule.get('is_deduped',False)
            is_deduped=False
            if rule is None:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"campaign rule does not exist")
            #call the helper to build the associated leads, this works, this returns everything no need for dnc filtering
            results=await load_leads_for_campaign(rule.rule_name,session,user)

        #loading legacy campaign rules as sql statements
        
        else:
            rule_name_query=await fetch_rule_sql(session,load_campaign.camp_code)

            cleaned_query=ensure_info_pk_selected(rule_name_query)

            is_deduped=False

            if rule_name_query is None:
                campaigns_logger.info(f"The requested campaign:{load_campaign.camp_code} for loading does not exist")
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"The requested campaign:{load_campaign.camp_code} for loading does not exist")
            #if rule_name_query is not None fetch the leads by excuting the query string, this will result in a list of dictionaries
            random_removed=remove_order_by_random(cleaned_query)
            build_dnc_query=build_left_anti_join_sql(random_removed)
            cleaned_query=replace_double_quotes_with_single(build_dnc_query)
            results=await execute_built_sql_query(session,cleaned_query)
            rule_code=await get_rule_code_legacy_table_by_rule_name(load_campaign.camp_code,session)
        
        print("print the results from the db")
        print(results)
        #this is where we populate the dma_numbers tracker table
        if len(results)==0:
            campaigns_logger.info(f"campaign:{load_campaign.camp_code} for branch:{load_campaign.branch} does not have active leads")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"No present data for active campaign with campaign code:{rule.rule_name}")
        
        #get list name 
        list_name=await get_list_name(load_campaign.camp_code,session)
        #build a list to send for dma,just phone numbers
        #dma_list_filtered=[item['cell'] for item in results]
        #build the numbers to send for dma 
        dma_list='\n'.join([item.get("cell") for item in results if item.get("cell")])
        #send the data for dma
        #check if the dma credits are still avaliable
        credits=await dma_object.check_credits()

        dma_credits=int(credits)
        
        if len(results)>dma_credits:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"Insufficient DMA credits for dedupe")

        if dma_credits<100:
            #this is where you can send an email
            #run it as a background task
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"DMA Credits have been exhausted")
        
        dma_audit_id=await dma_object.upload_data_for_dedupe(dma_list,session,load_campaign.camp_code)
        #ready=await dma_object.wait_for_download_to_be_ready(session=session,audit_id=dma_audit_id,retries=40,delay=3)

        # if not ready:
        #     current_status=await dma_object.check_dedupe_status(dma_audit_id,session)
        #raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY,detail=f"DMA returned empty output with output status:{current_status} after polling the dmasa api,records cannot be sent to dedago immediately")

        result=await dma_object.wait_for_readoutput_non_empty(dma_audit_id=dma_audit_id,session=session,attempts=40,delay=3,include_date=False)
        #if it's true continue executing normally

        if not result:
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY,detail="An unexpected error occurred while reading DMASA output")
        
        if result is not True:
            opted_in,opted_out=result

        print("print dedupe done")
        if result is True:
            opted_in=[]
            opted_out=[]
        
        #handling background job processing for sending numbers into the blacklist table

        if len(opted_in)>0 and len(opted_out)>0:
            print("print background processing and opted in processing")
            #send the opted in numbers to a blacklist table
            #call the method to write this data to the blacklist table
            background_task.add_task(write_opted_ins_to_blacklist_bg,opted_in,1000)
            #continue processing the opted out numbers with status True
            #build a new results array 
            relevent_numbers={item["DataEntry"] for item in opted_out}
            results=[item for item in results if item["cell"] not in relevent_numbers]
        
        dma_length=len(results)
        print("")
        print("print results after dedupe")
        print(results)
        print()
        print(f"print the length of the processed array:{dma_length}")
        print()
        results_dicts = [{"id": r["id"],"fore_name": r["fore_name"],"last_name": r["last_name"],"cell": r["cell"]} for r in results ]

        print("print results_dicts")
        print(results_dicts)

        feeds,feeds_cleaning=clean_and_process_results(results_dicts)
        print()
        print("print feeds")
        print(feeds)
        #build insert list here
        insert=[]
        #load token for a branch
        token=load_data_als.get_token(load_campaign.branch)
        payload=load_data_als.set_payload(load_campaign.branch,feeds,load_campaign.camp_code,list_name)
       

        response=load_data_als.load_data_to_als(load_campaign.branch,load_campaign.camp_code,feeds,token,list_name)
        #get the list id from dedago
        dedago_status_code=response['status_code']
        todaysdate = date.today()
       
        if dedago_status_code==200:
            list_id=str(response['list_id'])
            #list of tuples
            insert=[(item['phone_number'],load_campaign.camp_code,todaysdate,list_name,list_id,'AUTOLOAD',rule_code) for item in feeds]
            campaigns_logger.info(f"dedago status code:{dedago_status_code},list_id:{list_id} for campaign:{load_campaign.camp_code} branch:{load_campaign.branch} for list name:{list_name}")
            
            #background task function
            updated_feeds=inject_info_pk(results,feeds)
            background_task.add_task(load_leads_to_als_req,feeds,updated_feeds,insert,is_deduped)
            
        else:
            campaigns_logger.exception(f"An exception occurred while generating the list id")
            raise HTTPException(status_code=response['status_code'],detail=f"An internal server error occurred while generating the list id from dedago")
        print("")
        return LoadCampaignResponse(campaign_code=load_campaign.camp_code,branch=load_campaign.branch,list_name=list_name,audit_id=dma_audit_id,records_processed=dma_length)
    
    except HTTPException:
        raise

    except Exception as e:
        await session.rollback()
        campaigns_logger.exception(f"An exception occurred while loading a campaign:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred")
    

@campaigns_router.get("/infinite",status_code=status.HTTP_200_OK,description="Get all campaigns infinite scroll",response_model=PaginatedInfiniteResponse)
async def get_all_campaigns_infinite_scroll(page:int=Query(1,ge=1,description="Page Number"),page_size:int=Query(10,le=100,description="number of records per page"),searchTerm:str|None=Query(None,description="Search for campaign name or campaign code"),user=Depends(get_current_active_user),session:AsyncSession=Depends(get_async_master_prod_session)):
    try:
        return await get_all_campaigns_infinite_scroll_db(session,page,page_size,searchTerm,user)
    except HTTPException:
        raise
    except Exception:
        campaigns_logger.exception(f"An exception occured for the infinite scroll")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred for infinite scroll")


@campaigns_router.get("/search-campaigns",status_code=status.HTTP_200_OK,description="search campaigns by campaign name,campaign code and branch",response_model=PaginatedCampaigResponse)

async def search_campaign(page:int=Query(1,ge=1,description="Minimum number of records"),page_size:int=Query(10,ge=1,le=100,description="Maximum number of items per page"),session:AsyncSession=Depends(get_async_master_prod_session),campaign_name:str=Query(None,description="Search by campaign name"),branch:str=Query(None,description="Search by branch name"),camp_code:str=Query(None,description="Search by camp_code"),user=Depends(get_current_active_user)):
    return await search_campaigns_from_db(session,page,page_size,campaign_name,branch,camp_code)


@campaigns_router.get("/{camp_code}",status_code=status.HTTP_200_OK,description="Get campaign by campaing code from the master database",response_model=CreateCampaignResponse)
async def get_campaign_by_code(camp_code:str=Path(...,description="Campaign Code"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    try:
        campaign=await get_campaign_by_code_db(camp_code,session,user)
        if campaign is None:
            campaigns_logger.info(f"user:{user.id} with email:{user.email} requested campaign:{camp_code} that does not exist")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Campaign:{camp_code} does not exist")
        campaigns_logger.info(f"user:{user.id} with email:{user.email} successfully fetched campaign:{camp_code}")
        return campaign
    except HTTPException:
        raise 

    except Exception as e:
        campaigns_logger.error(f"{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while fetching campaign:{camp_code}")

  
@campaigns_router.patch("/update/{camp_code}",status_code=status.HTTP_200_OK,description="update campaign name",response_model=CreateCampaignResponse)
async def update_campaign_name(new_campaign_name:UpdateCampaignName,camp_code:str=Path(...,description="Provide the campaign code"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    try:
        campaign=await get_campaign_by_code_db(camp_code,session,user)
        if campaign is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"requested campaign:{new_campaign_name.campaign_name} does not exists")
        result=await update_campaign_name_db(new_campaign_name,camp_code,session,user)
        if result==None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"campaign with campaign code:{camp_code} does not exist")
        return result
    
    except HTTPException:
        raise  
    except Exception as e:
        campaigns_logger.exception(f"An internal server error while updating campaign:{camp_code}:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while updating campaign with campaign code:{camp_code}")


@campaigns_router.get("/branch/{branch}",status_code=status.HTTP_200_OK,description="Get all campaigns by branch",response_model=PaginatedCampaigResponse)
async def get_all_campaigns_by_branch(branch:str,page:int=Query(1,ge=1,description="Page number"),page_size:int=Query(10,ge=1,le=100,description="Number of records per page"),user=Depends(get_current_active_user),session:AsyncSession=Depends(get_async_master_prod_session)):
    return await get_all_campaigns_by_branch_db(branch,session,user,page,page_size)

@campaigns_router.get("",status_code=status.HTTP_200_OK,description="Get all campaigns",response_model=PaginatedCampaigResponse)
async def get_all_campaigns(page:int=Query(1,ge=1,description="Page number"),page_size:int=Query(10,ge=1,le=100,description="Number of records per page"),user=Depends(get_current_active_user),session:AsyncSession=Depends(get_async_master_prod_session)):
    return await get_all_campaigns_db(session,page,page_size,user)


@campaigns_router.post("/spec-levels/{rule_name}",status_code=status.HTTP_200_OK,description="check the specification level",response_model=CampaignSpecLevelResponse)
async def check_spec_level(rule_name:str=Path(...,description="Pass the campaign code to check the campaign specification"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
   return await get_spec_level_campaign_name_db(rule_name,session,user)
