from fastapi import APIRouter,status,Depends,HTTPException,Path,Query,BackgroundTasks,Request,Response
from sqlmodel import Session,select
import time
from sqlalchemy.ext.asyncio.session import AsyncSession
from utils.dmasa_service_helpers import DMAService 
from utils.load_data_to_als_service import LoadDataToALSService
from datetime import datetime,timedelta
from models.campaigns_table import campaign_tbl
from models.information_table import info_tbl
from models.dma_service import dma_audit_id_tbl,list_tracker_table
from models.rules_table import rules_tbl
from models.dma_service import dma_validation_data
from schemas.campaigns import CreateCampaign,LoadCampaignResponse,LoadCampaign,UpdateCampaignName,CreateCampaignResponse,PaginatedCampaigResponse,PaginatedInfiniteResponse,CampaignSpecLevelResponse,CampaignsTotal
from database.database import get_session
from database.master_db_connect import get_async_session
from database.master_database_test import get_async_master_test_session
#from utils.load_als_service import get_loader_als_loader_service
from utils.dnc_util import dnc_list_numbers
from utils.list_names import get_list_names
from utils.auth import get_current_active_user
from utils.logger import define_logger
from utils.dmasa_service import DMA_Class,get_dmasa_service
from utils.load_campaign_helpers import load_leads_for_campaign,filter_dnc_numbers
from utils.load_data_to_als_service import get_als_service


from crud.campaigns import (create_campaign_db,get_all_campaigns_by_branch_db,get_campaign_by_code_db,get_campaign_by_name_db,update_campaign_name_db,get_active_campaign_to_load,get_all_campaigns_db,get_all_campaigns_infinite_scroll_db,get_spec_level_campaign_name_db,get_total_campaigns_on_the_db,search_campaigns_from_db)
from crud.campaign_rules import (get_campaign_rule_by_rule_name_db)
from utils.leads_cleaner_load_campaign import clean_and_process_results
from utils.load_als_data_REQ_helper import load_leads_to_als_REQ

campaigns_logger=define_logger("als_campaign_logs","logs/campaigns_route.log")

campaigns_router=APIRouter(tags=["Campaigns"],prefix="/campaigns")

@campaigns_router.post("/create-campaign",status_code=status.HTTP_201_CREATED,description="Create a new campaign by providing a branch, campaign code and campaign name",response_model=CreateCampaignResponse)


def normalize_numbers(value)->bool:

    if isinstance(value,bool):
        return value
    if isinstance(value,str):
        return value.strip().lower() == "true"
    
    return False

async def create_campaign(campaign:CreateCampaign,session:AsyncSession=Depends(get_async_session),user=Depends(get_current_active_user)):
    
    try:
        result= await create_campaign_db(campaign,session,user)
        if result==False:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"campaign name:{campaign.campaign_name} from branch:{campaign.branch} with campaign code:{campaign.camp_code} already exist")
        campaigns_logger.info(f"user:{user.id} with email:{user.email} created campaign:{campaign.campaign_name} from branch:{campaign.branch}")
        return result
    except HTTPException:
        raise

    except Exception as e:
        await session.rollback()
        campaigns_logger.error(f"an internal server error occurred while creating campaign:{campaign.campaign_name} with campaign code:{campaign.camp_code}:{e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while creating campaign:{campaign.campaign_name} for branch:{campaign.branch}")


#calculate the number of campaigns on the the db
@campaigns_router.get("/total",status_code=status.HTTP_200_OK,description="Get the total number of campaigns on the system",response_model=CampaignsTotal)

async def get_the_total_number_of_campaigns(session:AsyncSession=Depends(get_async_session),user=Depends(get_current_active_user)):
    return await get_total_campaigns_on_the_db(session,user)


@campaigns_router.get("/check-dma-credits",status_code=status.HTTP_200_OK,description="return the number of dma credits")
async def check_dma_credits(dma_object:DMAService,user=Depends(get_current_active_user)):
    return await dma_object.check_credits()


#load a campaign given the campaign code: camp_code
@campaigns_router.post("/load-campaign",description="load campaign by providing campaign code and branch name",status_code=status.HTTP_200_OK,response_model=LoadCampaignResponse)

async def load_campaign(load_campaign:LoadCampaign,dma_object:DMAService,load_data_als:LoadDataToALSService,background_task:BackgroundTasks,session:AsyncSession=Depends(get_async_session),user=Depends(get_current_active_user)):
    #calculate the number of entries in table campaign_rules

    try:
        # this is where the loading start
        # #find an active campaign rule
        campaign_code=await get_active_campaign_to_load(load_campaign.camp_code,session)

        if campaign_code==None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"Campaign has not been created")
        
        #fetch the associated rule,this works
        rule=await get_campaign_rule_by_rule_name_db(campaign_code,session,user)

        #call the helper to build the associated leads, this works

        results=await load_leads_for_campaign(rule.rule_name,session)

        #this is where we populate the dma_numbers tracker table

        if len(results)==0:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"No present leads for active campaign code:{rule.rule_name}")
        
        #fetch numbers from the dnc list
        dnc_listed_numbers=await dnc_list_numbers()
        #This is questionable
        #dnc_set=set(dnc_listed_numbers)
        filtered_data=[item for item in results if item['cell'] not in dnc_listed_numbers]

        #get list name 
        list_name=await get_list_names(load_campaign.camp_code)
        #build a list to send for dma,just phone numbers

        dma_list_filtered=[item['cell'] for item in filtered_data]

        dma_list='\n'.join(dma_list_filtered)

        #send the data for dma
        #check if the dma credits are still avaliable
        credits=await dma_object.check_credits()

        if credits>=100:

            dma_audit_id=await dma_object.upload_data_for_dedupe(dma_list,session)

        else:
            #send email to address the issue
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"DMA credits have been exhausted")
        
        #wait for dma to return
        check_dedupe_status=await dma_object.wait_for_download_to_be_ready(session,dma_audit_id)

        #if the status is true read the output
        if check_dedupe_status:

            read_dma_output=await dma_object.read_dedupe_output(dma_audit_id)
        else:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"Campaign sent for dma but the dma api did not respond in time")
        
        #Add zeros on the phone numbers, this works
        optout=['0'+ str(obj['DataEntry']) for obj in read_dma_output]

        dma_length=len(optout)

        not_opted_out=[item["DataEntry"] for item in optout if "DataEntry" in item and not normalize_numbers(item.get("OptedOut"))]

        new_result=[obj for obj in results if obj[3] not in optout]

        new_list=[item for item in results if item[3] not in not_opted_out]
        #useless conversion

        #results_new = [[r['id'], r['fore_name'], r['last_name'], r['cell']] for r in new_result]
        results_new = [[r['id'], r['fore_name'], r['last_name'], r['cell']] for r in new_list]

        results_dicts = [{"id": r[0], "fore_name": r[1], "last_name": r[2], "cell": r[3]} for r in results_new]

        feeds,feeds_cleaning=clean_and_process_results(results_dicts)

        #load token for a branch
        token=load_data_als.get_token(load_campaign.branch)

        payload=load_data_als.set_payload(load_campaign.branch,feeds,load_campaign.camp_code,list_name)

        response=await load_data_als.send_data_to_dedago(token,payload)
        #get the list id
        dedago_status_code=response['status_code']

        todaysdate = datetime.today().strftime('%Y-%m-%d')

        if dedago_status_code==200:
            list_id=response['list_id']
            #list of tuples
            insert=[(item['phone_number'],load_campaign.camp_code,todaysdate,list_name,list_id,'AUTOLOAD',load_campaign.camp_code) for item in feeds]
            campaigns_logger.info(f"als status code:{dedago_status_code} for campaign:{load_campaign.camp_code} branch:{load_campaign.branch} for list name:{list_name}")
            #background task function
            background_task.add_task(load_leads_to_als_REQ,feeds,insert,is_dedupe=False)
        else:
            campaigns_logger.exception(f"An exception occurred while generating the list id")
            raise HTTPException(status_code=response['status_code'],detail=f"An internal server error occurred while generating the list id")
        
        return LoadCampaignResponse(campaign_code=load_campaign.camp_code,branch=load_campaign.branch,list_name=list_name,audit_id=dma_audit_id,records_processed=dma_length)
    
    
    except HTTPException:
        raise

    except Exception as e:
        campaigns_logger.exception(f"An exception occurred while loading a campaign:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred")


@campaigns_router.get("/infinite",status_code=status.HTTP_200_OK,description="Get all campaigns infinite scroll",response_model=PaginatedInfiniteResponse)

async def get_all_campaigns_infinite_scroll(page:int=Query(1,ge=1,description="Page Number"),page_size:int=Query(10,le=100,description="number of records per page"),searchTerm:str|None=Query(None,description="Search for campaign name or campaign code"),user=Depends(get_current_active_user),session:AsyncSession=Depends(get_async_session)):
    try:
        return await get_all_campaigns_infinite_scroll_db(session,page,page_size,searchTerm,user)
    
    except HTTPException:
        raise

    except Exception:
        campaigns_logger.exception(f"An exception occured for the infinite scroll")

        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred for infinite scroll")


@campaigns_router.get("/search-campaigns",status_code=status.HTTP_200_OK,description="search campaigns by campaign name,campaign code and branch",response_model=PaginatedCampaigResponse)
async def search_campaign(page:int=Query(1,ge=1,description="Minimum number of records"),page_size:int=Query(10,ge=1,le=100,description="Maximum number of items per page"),session:AsyncSession=Depends(get_async_session),campaign_name:str=Query(None,description="Search by campaign name"),branch:str=Query(None,description="Search by branch name"),camp_code:str=Query(None,description="Search by camp_code"),user=Depends(get_current_active_user)):
    
    return await search_campaigns_from_db(session,page,page_size,campaign_name,branch,camp_code)


@campaigns_router.get("/{camp_code}",status_code=status.HTTP_200_OK,description="Get campaign by campaing code from the master database",response_model=CreateCampaignResponse)

async def get_campaign_by_code(camp_code:str=Path(...,description="Campaign Code"),session:AsyncSession=Depends(get_async_session),user:Session=Depends(get_current_active_user)):
    try:
        # campaign_query=select(campaign_tbl).where(campaign_tbl.camp_code==camp_code)
        # campaign=session.exec(campaign_query).first()
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
async def update_campaign_name(new_campaign_name:UpdateCampaignName,camp_code:str=Path(...,description="Provide the campaign code"),session:AsyncSession=Depends(get_async_session),user=Depends(get_current_active_user)):
    try:
        #get the campaign to be updated
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
async def get_all_campaigns_by_branch(branch:str,page:int=Query(1,ge=1,description="Page number"),page_size:int=Query(10,ge=1,le=100,description="Number of records per page"),user=Depends(get_current_active_user),session:AsyncSession=Depends(get_async_session)):
    return await get_all_campaigns_by_branch_db(branch,session,user,page,page_size)


@campaigns_router.get("",status_code=status.HTTP_200_OK,description="Get all campaigns",response_model=PaginatedCampaigResponse)

async def get_all_campaigns(page:int=Query(1,ge=1,description="Page number"),page_size:int=Query(10,ge=1,le=100,description="Number of records per page"),user=Depends(get_current_active_user),session:AsyncSession=Depends(get_async_session)):
    
    return await get_all_campaigns_db(session,page,page_size,user)


@campaigns_router.post("/spec-levels/{rule_name}",status_code=status.HTTP_200_OK,description="check the specification level",response_model=CampaignSpecLevelResponse)
async def check_spec_level(rule_name:str=Path(...,description="Pass"),session:AsyncSession=Depends(get_async_session),user=Depends(get_current_active_user)):
   return await get_spec_level_campaign_name_db(rule_name,session,user)





