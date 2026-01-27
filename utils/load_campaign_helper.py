
from typing import List,Tuple
from utils.dynamic_sql_rule_function import build_dynamic_rule_engine
from fastapi import Depends,HTTPException,status
from sqlalchemy.ext.asyncio.session import AsyncSession
from crud.campaign_rules import get_campaign_rule_by_rule_name_db
from crud.rule_engine_db import get_rule_by_name_db

def clean_leads(leads: List) -> List[dict]:
    feeds = []
    for lead in leads:
        vendor_code, first_name, last_name, phone = lead
        # Normalize fields
        first_name = first_name if first_name and first_name.strip() and first_name.lower() != 'null' else last_name or None
        last_name = last_name if last_name and last_name.strip() else None
        phone = phone if phone and phone.strip() else None

        # Skip leads without phone number
        if not phone:
            continue

        # Append cleaned leads
        feeds.append({
            "vendor_lead_code": vendor_code,
            "first_name": first_name,
            "last_name": last_name,
            "phone_number": phone
        })
    
    return feeds



def filter_dnc(leads: List[Tuple], dnc_list: List[str]) -> List[Tuple]:
    """Remove leads that are on the DNC list"""
    return [lead for lead in leads if lead[3] not in dnc_list]


def filter_dnc_numbers(leads_results,dnc_list):
    
    dnc_set=set(dnc_list)
    return [r for r in leads_results if r['cell'] not in dnc_set]


async def load_leads_for_campaign(rule_name:str,session:AsyncSession):

    try:
        print("print the rule name")
        print(rule_name)
        result=await get_rule_by_name_db(rule_name,session)
        if result==None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"The requested rule does not exist")
        
        stmt,params=build_dynamic_rule_engine(result[0].rule_json)
        rows=await session.execute(stmt,params)
        return rows.mappings().all()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while loading for a campaign:{str(e)}")