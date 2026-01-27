from fastapi import APIRouter,HTTPException,status,Depends,Query,Path
from sqlmodel import select
from datetime import datetime
from sqlmodel.ext.asyncio.session import AsyncSession
from models.rules_table import new_rules_tbl
from models.campaigns_table import campaign_tbl
from models.campaign_rules_table import campaign_rule_tbl
from schemas.campaign_rules import CampaignSpecResponse
from schemas.campaign_rules_input import UpdateCampaignRulesResponse
from schemas.rules_schema import RuleSchema,RuleResponseModel,UpdateCampaignRule,UpdatingCampaignRuleResponse,DeactivateRuleResponseModel,ActivateRuleResponseModel,UpdatingSalarySchema,UpdatingDerivedIncomeSchema,UpdateAgeSchema,GetCampaignRuleResponse,GetAllCampaignRulesResponse,ChangeRuleResponse,UpdateNumberOfLeads,UpdateNumberOfLeadsResponse,DeleteCampaignRuleResponse
from database.master_database_prod import get_async_master_prod_session
from utils.logger import define_logger
from utils.auth import get_current_active_user
from crud.campaign_rules import (create_campaign_rule_db,get_all_campaign_rules_db, get_rule_by_rule_code_db, get_campaign_rule_by_rule_name_db,update_campaign_name_db,deactivate_campaign_db,activate_campaign_db,update_salary_for_campaign_rule_db,update_derived_income_for_campaign_rule_db,search_for_a_campaign_rule_db,fetch_campaign_code_from_campaign_tbl_db,fetch_rule_code_from_rules_tbl_and_campaign_rules_tbl_db,update_campaign_rule_and_insert_rule_code_db,insert_new_campaign_rule_on_campaign_rule_tbl_db,update_number_of_leads_db,update_campaign_rule_age_db,remove_campaign_rule_db,total_campaign_rules_db,change_rule_db)
from utils.campaigns import (load_campaign_query_builder)
from utils.campaign_rules_helper import transform_rule_json

#from utils.parse_validation_methods import parse_and_validate_rule

campaign_rule_router=APIRouter(tags=["Campaign Rules"],prefix="/campaign_rules")
campaign_rules_logger=define_logger("als_campaign_rules_logger","logs/campaign_rules_logs")

@campaign_rule_router.post("",status_code=status.HTTP_200_OK,description="Create a campaign rule by the necessary info",response_model=RuleResponseModel)
async def create_campaign_rule(rule:RuleSchema,campaign_code:str=Query(...,description="Enter the campaign code associated with this rule"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    create_rule=await create_campaign_rule_db(campaign_code=campaign_code,rule=rule,session=session,user=user)
    campaign_rules_logger.info(f"user :{user.id} with email: {user.email} created campaign rule:{create_rule.rule_name}")
    return create_rule


@campaign_rule_router.get("/total",status_code=status.HTTP_200_OK,description="Get the total number of campaign rules")
async def get_total_campaign_rules(session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await total_campaign_rules_db(session,user)


#fetch all the rules on the database ,add pagination and search
@campaign_rule_router.get("",status_code=status.HTTP_200_OK,description="Get all campaign rules",response_model=GetAllCampaignRulesResponse)

async def fetch_all_campaign_rules(page:int=Query(1,ge=1,description="Value should be greater than or equal 1"),page_size:int=Query(10,ge=1,le=100,description="Number of items in a page,maximum is 100"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await get_all_campaign_rules_db(page,page_size,session,user)
    
#search for a campaign rule
@campaign_rule_router.get("/search",status_code=status.HTTP_200_OK,description="Search for  campaign rule using a rule name,salary, andderived income")

async def search_for_campaign_rule(page:int=Query(1,ge=1,description="Current page number for pagination(starts at 1)"),page_size:int=Query(10,ge=1,le=100,description="Number of records per page"),rule_name:str=Query(None,description="Search for a campaign rule using the rule name or campaign code"),salary:int=Query(None,description="Search for a campaign rule using the salary"),derived_income:int=Query(None,description="Search for campaign rule using the derived income"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await search_for_a_campaign_rule_db(page,page_size,session,user,rule_name,salary,derived_income,sort_by="created_at",sort_order="desc")


@campaign_rule_router.put("/als/change_rule",status_code=status.HTTP_200_OK,response_model=ChangeRuleResponse)

async def change_rule(rule_code: int,camp_code: str,session: AsyncSession = Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    try: 
       print("enter the crud method again")

       print(f"rule_code:{rule_code} campaign code:{camp_code}")
       campaign_code=await fetch_campaign_code_from_campaign_tbl_db(camp_code,session)

       if campaign_code==None:
           raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f'{camp_code} does not exist, create campaign before assigning a rule for it')
       print("enter the second method")
       rule_code_query=await fetch_rule_code_from_rules_tbl_and_campaign_rules_tbl_db(campaign_code,session)
       if rule_code_query!=None:
           result=await update_campaign_rule_and_insert_rule_code_db(rule_code,camp_code,session)
           message=f"rule number {rule_code_query} was found active, deactivated and {rule_code} is now active for campaign:{result}"
           return ChangeRuleResponse(success=True,message=message)
       print("enter the insert new campaign rule on campaign rule table database")
       
       result_code=await insert_new_campaign_rule_on_campaign_rule_tbl_db(camp_code,rule_code,session)

       print(f"print the new campaign rule on campaign rule table after completing the insertion:{result_code}")

       not_active_messge= f'NO ACTIVE RULE was found active for campaign {result_code} but rule {rule_code} was made active for it'

       return ChangeRuleResponse(success=True,message=not_active_messge)
    
    except HTTPException:
        raise

    except Exception:
        await session.rollback()
        campaign_rules_logger.exception(f"an exception occurred while changing the campaigning rules")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An internal server error occurred while changing the campaign rule")

@campaign_rule_router.put("/als/v1/change_rule",status_code=status.HTTP_200_OK,response_model=ChangeRuleResponse,description="Assign campaign rule to active campaigns")
async def assign_campaign_rule_to_campaign(rule_code:int,camp_code:str,session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await change_rule_db(rule_code,camp_code,session,user)

@campaign_rule_router.get("/campaign_spec",description="Provide a rule name or campaign code to get the number of leads for that spec. The rule name is the spec name",status_code=status.HTTP_200_OK)

async def check_number_of_leads_for_campaign_rule(rule_name:str,user=Depends(get_current_active_user),session:AsyncSession=Depends(get_async_master_prod_session)):
    try:
        spec_query=select(new_rules_tbl).where(new_rules_tbl.rule_name==rule_name)
        spec_number_call=await session.exec(spec_query)
        result=spec_number_call.first()

        if result is None:
            campaign_rules_logger.exception(f"user {user.id} with email {user.email} caused exception:{str(e)}")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Campaign spec has nothing")
        #campaign spec for a generic campaign
        query,params=load_campaign_query_builder(result)
        #check the spec
        number_of_leads=await session.execute(query,params)
        results=number_of_leads.fetchall()

        campaign_rules_logger.info(f"user {user.id} with email {user.email} checked specification for campaign:{rule_name}")
        return CampaignSpecResponse(Success=True,Number_Of_Leads=len(results))
    except HTTPException:
        raise
    except Exception as e:
        campaign_rules_logger.exception(f"user {user.id} with email {user.email} caused exception:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while checking spec for campaign code:{rule_name}")

#assign campaign rule to a campaign
@campaign_rule_router.post("/assign/{rule_code}",description="assign a campaign rule to an existing campaign")

async def assign_active_rule_to_campaign(rule_code:int,camp_code:str,session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    try:
        campaign_code_query=select(campaign_tbl).where(campaign_tbl.camp_code==camp_code)
        
        campaign_code=await session.exec(campaign_code_query).first()
        
        if campaign_code==None:
            campaign_rules_logger.info(f"campaign with campaign code:{camp_code} does not exist")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"campaign with campaign code:{camp_code} does not exist")
       
        #search the campaign code from the 
        find_campaign_rule_query=select(campaign_rule_tbl,new_rules_tbl).join(new_rules_tbl,campaign_rule_tbl.rule_code==new_rules_tbl.rule_code).where(campaign_rule_tbl.camp_code==camp_code,campaign_rule_tbl.is_active==True)
        find_campaign_rule=await session.exec(find_campaign_rule_query).first()

        if not find_campaign_rule:
            campaign_rules_logger.info(f"campaign rule with rule code:{rule_code} does not exist")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"rule code:{rule_code} does not exist")
        
        todaysdate=datetime.today().strftime("%Y-%m-%d")
        #update the code
        campaign_code_rule_tbl_query=select(campaign_rule_tbl).where(campaign_rule_tbl.camp_code==camp_code)
        
        campaign_rule_tbl=await session.exec(campaign_code_rule_tbl_query).first()
        
        campaign_rule_tbl.is_active=False

        session.add(campaign_rule_tbl)
        await session.commit()
        message=f"rule code:{find_campaign_rule.rule_code} has been deactivated and rule code:{rule_code} is now active"
        new_rule=campaign_rule_tbl(camp_code=camp_code,rule_code=rule_code,date_rule_created=todaysdate,is_active=True)
        session.add(new_rule)
        await session.commit()
        campaign_rules_logger.info(f"Campaign rule:{rule_code} activated")
        session.close()
        return UpdateCampaignRulesResponse(message=message,update_date=todaysdate)
    except HTTPException:
        raise
    except Exception as e:
        campaign_rules_logger(f"{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="internal server error occurred")

#fetch rule code based on campaign code
@campaign_rule_router.get("/{rule_name}",status_code=status.HTTP_200_OK,description="Get campaign rule by rule name",response_model=GetCampaignRuleResponse)

async def get_campaign_rule_by_rule_name(rule_name:str=Path(...,description="Provide the rule name which is the same as the campaign code"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    try:
        get_rule=await get_campaign_rule_by_rule_name_db(rule_name,session,user)
        if get_rule==None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Camapign rule with campaign code:{rule_name} does not exist")
        transfromed_rule=transform_rule_json(get_rule)
        return transfromed_rule
    except HTTPException:
        raise
    except Exception as e:
        campaign_rules_logger.error(f"An internal server error occurred while fetching campaign rule:{rule_name}:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"internal server error occurred while fetching campaign rule name:{rule_name}")

#define the response for this model
@campaign_rule_router.get("/new/{rule_code}",status_code=status.HTTP_200_OK,description="Get campaign rule using the rule code",response_model=GetCampaignRuleResponse)

async def get_campaign_rule_by_rule_code(rule_code:int=Path(...,description="rule code parameter"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    rule=await get_rule_by_rule_code_db(rule_code,session,user)
    if rule==None:
        campaign_rules_logger.info(f"user with user id:{user.id} with email:{user.email} requested campaign rule with rule code:{rule_code} but it does not exist")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Campaign rule with rule code:{rule_code} does not exist")
    return transform_rule_json(rule)


#change the rule code
@campaign_rule_router.patch("/{rule_code}",status_code=status.HTTP_200_OK,response_model=UpdatingCampaignRuleResponse)

async def update_campaign_rule_name(new_campaign_name:UpdateCampaignRule,rule_code:int=Path(...,description="Provide the rule code for the campaign rule"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    update_rule_name,update_rule_code=await update_campaign_name_db(rule_code,new_campaign_name.new_campaign_rule_name,session,user)
    return UpdatingCampaignRuleResponse(rule_code=update_rule_code,new_rule_name=update_rule_name)

@campaign_rule_router.patch("/{rule_code}/deactivate",status_code=status.HTTP_200_OK,response_model=DeactivateRuleResponseModel)

async def deactivate_campaign_rule(rule_code:int=Path(...,description="Please provide a rule code"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await deactivate_campaign_db(rule_code,session,user)

@campaign_rule_router.patch("/{rule_code}/activate",status_code=status.HTTP_200_OK,description="Activate campaign rule by providing the rule code",response_model=ActivateRuleResponseModel)

async def activate_campaign_rule(rule_code:int,session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    
    return await activate_campaign_db(rule_code,session,user)


@campaign_rule_router.patch("/salary/{rule_code}",status_code=status.HTTP_200_OK,description="Upate the salary for a campaign rule, please note that for range based salary rules with operator between specify the lower limit and upper limit field(s),the salary field should be ignored, for other types of rules only populate the salary field")

async def update_salary_for_campaign_rule(salary:UpdatingSalarySchema,rule_code:int=Path(...,description="Provide a rule code a campaign rule"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await update_salary_for_campaign_rule_db(rule_code,salary,session,user)

@campaign_rule_router.patch("/derived_income/{rule_code}",status_code=status.HTTP_200_OK,description="Update the derived income for a campaign rule, please note that for range based derived income with operator between specify the lower limit and/or upper limit field(s),the derived_income_value should be ignored, for other types of rules only populate the derived_income_value field.")
async def update_derived_income(derived_income:UpdatingDerivedIncomeSchema,rule_code:int=Path(...,description="Provide the rule code"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await update_derived_income_for_campaign_rule_db(rule_code,derived_income,session,user)


@campaign_rule_router.patch("/update-age/{rule_code}",status_code=status.HTTP_200_OK,description="Update the age for a campaign rule")
async def update_age_for_campaign_rule(ageSchema:UpdateAgeSchema,rule_code:int=Path(...,description="Provide the rule_code"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await update_campaign_rule_age_db(rule_code,ageSchema,session,user)

@campaign_rule_router.patch("/update-leads/{rule_code}",status_code=status.HTTP_200_OK,description="Update the number of leads to load",response_model=UpdateNumberOfLeadsResponse)
async def update_number_of_leads(number_of_leads:UpdateNumberOfLeads,rule_code:int=Path(...,description="Rule code for an active campaign rule"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    try:
        leads_update=await update_number_of_leads_db(session,rule_code,number_of_leads.number_of_leads,user)
        if leads_update==None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"Campaign rule does not exist")
        return UpdateNumberOfLeadsResponse(numer_of_leads=leads_update)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="An internal server error occurred")

@campaign_rule_router.delete("/delete/{rule_code}",status_code=status.HTTP_202_ACCEPTED,description="Delete the campaign rule completely from the system",response_model=DeleteCampaignRuleResponse)
async def delete_campaign_rule(rule_code:int,session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    return await remove_campaign_rule_db(rule_code,session,user)

