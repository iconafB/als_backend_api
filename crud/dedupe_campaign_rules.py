from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from typing import List,Optional
from models.campaign_rules import dedupe_campaign_rules_tbl
from schemas.dedupe_campaigns import CreateDedupeCampaign


#create dedupe campaign rule

async def create_dedupe_campaign_rule(rule:CreateDedupeCampaign,session:AsyncSession)->dedupe_campaign_rules_tbl:
    session.add(rule)
    await session.commit()
    await session.refresh(rule)
    return rule

async def get_all_dedupe_campaign_rules(session:AsyncSession,skip:int=0,limit:int=100)->List[dedupe_campaign_rules_tbl]:
    results=await session.exec(select(dedupe_campaign_rules_tbl).offset(skip).limit(limit))
    return results.all()

async def get_all_active_dedupe_campaign_rules(session:AsyncSession,skip:int=0,limit:int=100)->List[dedupe_campaign_rules_tbl]:
    results=await session.exec(select(dedupe_campaign_rules_tbl).where(dedupe_campaign_rules_tbl.is_active==True).offset(skip).limit(limit))
    return results.all()

#read one dedupe campaign rule

async def get_single_dedupe_campaign_rule_by_rule_name(rule_name:str,session:AsyncSession)->Optional[dedupe_campaign_rules_tbl]:
    result=await session.exec(select(dedupe_campaign_rules_tbl).where(dedupe_campaign_rules_tbl.rule_name==rule_name))
    if not result:
        return None
    return result.one_or_none()

async def assign_dedupe_campaign_to_campaign_rule(camp_code:str,session:AsyncSession):
    #get the dedupe campaign
    return


#deactivate a dedupe campaign rule
async def delete_dedupe_campaign_rule_by_rule_name(rule_name:str,session:AsyncSession)->bool:
    
    result=await session.exec(select(dedupe_campaign_rules_tbl).where(dedupe_campaign_rules_tbl.rule_name==rule_name))
    
    db_item=result.one_or_none()

    if not db_item:
        return None
    db_item.is_active=False
    session.add(db_item)
    await session.commit()
    return True

#update salary for the campaign rule
async def update_salary_for_campaign_rule(rule_name:str,salary:int,session:AsyncSession)->Optional[dedupe_campaign_rules_tbl]:
    
    result=await session.exec(select(dedupe_campaign_rules_tbl).where(dedupe_campaign_rules_tbl.rule_name==rule_name))
    db_item=result.one_or_none()
    if not result:
        return None
    db_item.salary=salary
    session.add(db_item)
    await session.commit()
    await session.refresh(db_item)
    session.close()
    return db_item

#update derive_income
async def update_derived_income_for_campaign_rule(rule_name:str,derived_income:int,session:AsyncSession)->Optional[dedupe_campaign_rules_tbl]:
    result=await session.exec(select(dedupe_campaign_rules_tbl).where(dedupe_campaign_rules_tbl.rule_name==rule_name))
    
    db_item=result.one_or_none()
    if not db_item:
        return None
    
    db_item.derived_income=derived_income
    session.add(db_item)
    await session.commit()
    await session.refresh(db_item)
    return db_item

#update gender
async def update_gender_for_dedupe_campaign_rule(rule_name:str,session:AsyncSession)->Optional[dedupe_campaign_rules_tbl]:
    result=await session.exec(select(dedupe_campaign_rules_tbl).where(dedupe_campaign_rules_tbl.rule_name==rule_name))
    
    db_item=result.one_or_none()
    if not db_item:
        return None
    if db_item.gender=="male":
        db_item.gender='female'
        session.add(db_item)
        await session.commit()
        await session.refresh(db_item)
        return db_item
    else:
        db_item.gender='male'
        session.add(db_item)
        await session.commit()
        await session.refresh(db_item)
        return db_item 
    
#change dedupe campaign to a generic campaign

async def change_dedupe_campaign_to_generic_campaign(rule_name:str,session:AsyncSession)->Optional[dedupe_campaign_rules_tbl]:
    
    result=await session.exec(select(dedupe_campaign_rules_tbl).where(dedupe_campaign_rules_tbl.rule_name==rule_name))
    db_item=result.one_or_none()
    if not db_item:
        return None
    db_item.is_deduped=False
    session.add(db_item)
    await session.commit()
    await session.refresh(db_item)
    return db_item

