from fastapi import APIRouter,status,HTTPException,Depends,Path,Query
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from crud.dedupe_campaign_rules import (create_dedupe_campaign_rule,get_all_dedupe_campaign_rules,get_all_active_dedupe_campaign_rules,get_single_dedupe_campaign_rule_by_rule_name,delete_dedupe_campaign_rule_by_rule_name,update_derived_income_for_campaign_rule,update_gender_for_dedupe_campaign_rule,update_salary_for_campaign_rule)
from models.campaign_rules import dedupe_campaign_rules_tbl
from schemas.campaign_rules import CreateDedupeCampaignRule
from database.master_database_prod import get_async_master_prod_session
from utils.logger import define_logger
from utils.auth import get_current_active_user

dedupe_campaign_rule_logger=define_logger("als_dedupe_campaign_rules_logger","logs/dedupe_campaign_rules")

dedupe_campaign_router=APIRouter(tags=["Dedupe Campaign Rules"],prefix="/dedupe_campaign_rules")

@dedupe_campaign_router.post("",status_code=status.HTTP_201_CREATED,response_model=dedupe_campaign_rules_tbl,description="Create a deduped campaign")

async def create_dedupe_campaign_rule(rule:CreateDedupeCampaignRule,session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    
    try:
        dedupe_campaign_rule_logger.info(f"user:{user.id} with email:{user.email} created dedupe campaign rule:{rule.rule_name}")
        return await create_dedupe_campaign_rule(rule,session)
    except Exception as e:
        await session.rollback()
        dedupe_campaign_rule_logger.exception(f"an error occurred while creating a deduped campaign rule:{e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="An internal server error occurred while creating a dedupe campaign rule")

#get all dedupe campaign rules
@dedupe_campaign_router.get("",status_code=status.HTTP_200_OK,response_model=List[dedupe_campaign_rules_tbl],description="Get All deduped Campaigns")

async def get_all_dedupe_campaign_rules(skip:int=0,limit:int=100,session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    try:
        dedupe_campaign_rule_logger.info(f"user:{user.id} with email:{user.email} retrieved deduped campaign rules")
        return await get_all_dedupe_campaign_rules(session,skip,limit)
    except Exception as e:
        dedupe_campaign_rule_logger.exception(f"an exception occurred for user:{user.id} with:{user.email} while fetching dedupe campaign rules:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"an internal server error occurred while fetching deduped camapigns for user:{user.id} with email:{user.email}")

#get all active dedupe campaign rules
@dedupe_campaign_router.get("/active",status_code=status.HTTP_200_OK,response_model=List[dedupe_campaign_rules_tbl],description="Get all active dedupe camapign rules")
async def get_all_active_dedupe_campaign_rules(skip:int=0,limit:int=100,session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    try:
        result=await get_all_active_dedupe_campaign_rules(session,skip,limit)
        dedupe_campaign_rule_logger.info(f"user:{user.id} with email:{user.email} retrieved:{len(result)}")
        return result
    except Exception as e:
        dedupe_campaign_rule_logger.exception(f"internal server error occurred while fetching deduped campaign rule for user:{user.id} with email:{user.email}:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"an internal server error occurred while fetch deduped campaign rules for user:{user.id} with email:{user.email}")
    
#get campaign by rule_name or campaign code
@dedupe_campaign_router.get("/{rule_name}",description="Get deduped campaign by rule name or campaign code",status_code=status.HTTP_200_OK,response_model=dedupe_campaign_rules_tbl)
async def get_deduped_campaign_rule_by_name(rule_name:str=Path(...,description="Campaign Code or Rule Name"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    try:
       result=await get_deduped_campaign_rule_by_name(rule_name,session)

       if result==None:
           dedupe_campaign_rule_logger.info(f"user:{user.id} with email:{user.email} did not find campaign:{rule_name}")
           raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"dedupe campaign with rule name:{rule_name} does not exist")
       
       dedupe_campaign_rule_logger.info(f"user:{user.id} with email:{user.email} retrieved campaign:{rule_name}")
       return result
    
    except Exception as e:
        dedupe_campaign_rule_logger.exception(f"An exception occurred while fetching campaign {rule_name}:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while fetching dedupe campaign:{rule_name}")

#dedactivate the dedupe campaign rule
@dedupe_campaign_router.patch("/{rule_name}",status_code=status.HTTP_202_ACCEPTED,description="Deactivate a campaign rule")
async def deactivate_dedupe_campaign_rule(rule_name:str,session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    try:
        result=await delete_dedupe_campaign_rule_by_rule_name(rule_name,session)

        if result==None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Dedupe campaign rule:{rule_name} not found")
        return result
    except Exception as e:
        dedupe_campaign_rule_logger.exception(f"an exception occurred while deactivating campaign:{rule_name} with user_id:{user.id} and email:{user.email}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"an exception occurred while deactivating dedupe campaign rule:{rule_name} by user:{user.id} with email:{user.email}")

#update dedupe campaign rule salary

@dedupe_campaign_router.patch("/salary/{rule_name}",status_code=status.HTTP_200_OK,description="Update dedupe campaign rule salary using the rule name/campaign code",response_model=dedupe_campaign_rules_tbl)
async def update_dedupe_campaign_rule_salary(rule_name:str=Path(...,description="provide rule name or campaign code"),salary:int=Query(...,description="Provide new salary value"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    try:
        result=await update_dedupe_campaign_rule_salary(rule_name,salary,session)
        if result==None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"No dedupe campaign found with the following rule name/campaign code:{rule_name}")
        return result
    except Exception as e:
        dedupe_campaign_rule_logger.exception(f"an internal server error occurred while updating salary for dedupe camapign:{rule_name} by user:{user.id} with email:{user.email}:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while updating salary for dedupe camapign:{rule_name}")

#update derived income
@dedupe_campaign_router.patch("/derived_income/{rule_name}",status_code=status.HTTP_200_OK,description="Update derived income for a deduped campaign rule",response_model=dedupe_campaign_rules_tbl)
async def update_derived_income(rule_name:str=Path(...,description="Rule name or camapign code"),derived_income:int=Query(...,description="Provide derived income value"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    try:
        result=await update_derived_income_for_campaign_rule(rule_name,derived_income,session)
        if result==None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"deduped campaign:{rule_name} does not exist")
        return result
    except Exception as e:
        dedupe_campaign_rule_logger.exception(f"an internal server error occurred while updating derived income for dedupe campaign {rule_name}:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while updating the derived income for dedupe campaign:{rule_name}")

#update gender for a deduped campaign rule
@dedupe_campaign_router.patch("/gender/{rule_name}",status_code=status.HTTP_200_OK,description="Update the gender for a deduped campaign rule",response_model=dedupe_campaign_rules_tbl)
async def update_gender_deduped_campaign(rule_name:str=Path(...,description="Rule name or campaign name"),session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    try:
        result=await update_gender_for_dedupe_campaign_rule(rule_name)
        if result==None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Deduped campaign:{rule_name} does not exist")
        dedupe_campaign_rule_logger.info(f"deduped campaign rule:{rule_name} gender updated by user:{user.id} with email:{user.email}")
        return result
    except Exception as e:
        dedupe_campaign_rule_logger.exception(f"an internal server error occurred while updating gender by user {user.id} with email:{user.email}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="an internal server error occurred while updating gender for dedupe camapign:{rule_name}")

#change dedupe campaign to generic campaign
@dedupe_campaign_router.patch("/{rule_name}",status_code=status.HTTP_200_OK,response_model=dedupe_campaign_rules_tbl,description="Change dedupe campaign to a generic campaign")
async def change_dedupe_campaign_to_generic_campaign(rule_name:str,session:AsyncSession=Depends(get_async_master_prod_session),user=Depends(get_current_active_user)):
    try:
        result=await change_dedupe_campaign_to_generic_campaign(rule_name,session)
        if result==None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"dedupe campaign:{rule_name} does not exist")
        dedupe_campaign_rule_logger.info(f"dedupe campaign:{rule_name} changed to a generic campaign by user:{user.id} with email:{user.email}")
        return result
    except Exception as e:
        dedupe_campaign_rule_logger.exception(f"an internal server error occurred:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"an internal server error occurred while changing dedupe campaign to generic campaign")

#assign dedupe campaign rules
