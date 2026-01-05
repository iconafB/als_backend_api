from fastapi import HTTPException,status
from typing import List,Tuple,Any,Sequence
from datetime import datetime, date,timezone
from sqlalchemy import text
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.orm import Session as _SessionBase

from sqlalchemy.dialects.postgresql import insert as pg_insert
from models.lead_history_table import lead_history_tbl
from models.information_table import info_tbl
from utils.logger import define_logger
from utils.update_info_tbl_campaign_dedupe_helper import update_records_for_infoTable_campaign_dedupe_tbl

campaigns_logger=define_logger("als campaign logs","logs/campaigns_route.log")

#this function will also take a boolean variable for checking if the campaign is dedupe or not

async def load_leads_to_als_REQ(feeds: List,insert:List[Tuple[str,str,str,str,str,str,str]],camp:str,is_dedupe:bool,session: AsyncSession):
   
     #sql querry can be moved to a file
    lead_history_tbl_sqlsmt = text("""
        INSERT INTO lead_history_tbl (
            cell, camp_code, date_used, list_name, list_id, load_type, rule_code
        )
        VALUES (
            :cell, :camp_code, :date_used, :list_name, :list_id, :load_type, :rule_code
        )
    """)
        
    info_tbl_upsert_sql = text("""
            INSERT INTO info_tbl (cell, last_used) 
            VALUES (:cell, :last_used)
            ON CONFLICT(cell) 
            DO UPDATE SET last_used = EXCLUDED.last_used
            """
        )
    
    todaysdate = datetime.today().strftime('%Y-%m-%d')
    new_feeds = [i["phone_number"] for i in feeds if i.get("phone_number")]
    
    update_feeds = [(cell.strip(), todaysdate) for cell in new_feeds]
    #new_list_with_vendor_lead_codes=[item['vendor_lead_code'] for item in feeds]
    
    db_list = tuple(item['vendor_lead_code'] for item in feeds)
    try:

        async with session.begin():

            await session.execute(lead_history_tbl_sqlsmt, [
                {
                    "cell": cell,
                    "camp_code": camp_code,
                    "date_used": date_used,
                    "list_name": list_name,
                    "list_id": list_id,
                    "load_type": load_type,
                    "rule_code": rule_code 
                }
                for cell, camp_code, date_used, list_name, list_id, load_type, rule_code in insert
            ])

            if update_feeds:
                await session.execute(info_tbl_upsert_sql, [
                    {"cell": cell, "last_used": last_used}
                    for cell, last_used in update_feeds
                ])
        #executed for dedupe campaigns only
            if is_dedupe==True:

                if db_list:
                    update_info_tbl_stmt=f"UPDATE info_tbl SET extra_info = NULL WHERE id IN {db_list}"
                    update_campaign_dedupe_stmt = f"UPDATE campaign_dedupe SET status = 'U' WHERE id IN {db_list}"
                    await session.execute(text(update_info_tbl_stmt))
                    await session.execute(text(update_campaign_dedupe_stmt))
            
            await session.commit()

    except Exception as e:
        await session.rollback()
        campaigns_logger.exception(f"An exception occurred while updating lead history table:{e}")
        raise 

    # if is_dedupe==True:
    #     vendor_lead_codes = [feed['vendor_lead_code'] for feed in feeds]
    #     vendor_lead_codes_tuple = tuple(vendor_lead_codes) if vendor_lead_codes else ()
    #     total_ids=await update_records_for_infoTable_campaign_dedupe_tbl(vendor_lead_codes_tuple=vendor_lead_codes_tuple,session=session)





