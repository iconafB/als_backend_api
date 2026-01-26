from fastapi import HTTPException,status
from typing import List,Tuple,Any,Sequence
from datetime import datetime, date,timezone
from sqlalchemy import text,update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from models.lead_history_table import lead_history_tbl
from models.information_table import info_tbl
from utils.logger import define_logger
from utils.update_info_tbl_campaign_dedupe_helper import update_records_for_infoTable_campaign_dedupe_tbl
from database.master_database_prod import async_session_maker

campaigns_logger=define_logger("als_campaign_logs","logs/campaigns_route.log")

#this function will also take a boolean variable for checking if the campaign is dedupe or not

def chunked(seq:list[int],size:int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

    
def inject_info_pk(results: list[dict], feeds: list[dict]) -> list[dict]:
    # Build lookup: id -> info_pk
    id_to_info_pk = {
        r["id"]: r["info_pk"]
        for r in results
    }

    # Inject info_pk into feeds where vendor_lead_code matches id

    for feed in feeds:
        vendor_code = feed.get("vendor_lead_code")
        if vendor_code in id_to_info_pk:
            feed["info_pk"] = id_to_info_pk[vendor_code]
    
    return feeds





async def load_leads_to_als_REQ(feeds: List[tuple],insert:list[tuple],is_deduped:bool):
   
     #sql querry can be moved to a file

    print("print the feeds inside the load_leads_to_als method")
    print(feeds)
    print()
    print("print the insert list inside the load_leads_to_als_method")
    print(insert)

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
    print()
    print("print the query")
    print(lead_history_tbl_sqlsmt)
    todaysdate = datetime.today().strftime('%Y-%m-%d')
    new_feeds = [i["phone_number"] for i in feeds if i.get("phone_number")]
    
    update_feeds = [(cell.strip(), todaysdate) for cell in new_feeds]
    #new_list_with_vendor_lead_codes=[item['vendor_lead_code'] for item in feeds]
    
    db_list = tuple(item['vendor_lead_code'] for item in feeds)

    try:
        async with async_session_maker() as session:

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
            if is_deduped==True:
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





async def load_leads_to_als_req(feeds:list[tuple],updated_feeds:list[dict],insert:list[tuple],is_deduped:bool):

    """
    Background task that writes to:
      lead_history_tbl (bulk insert)
    info_tbl (bulk upsert last_used)
    """


    print("print the updated feeds")
    print(updated_feeds)

    sql_insert_history=text("""
        INSERT INTO lead_history_tbl(cell, camp_code, date_used, list_name, list_id, load_type, rule_code)
        VALUES (:cell, :camp_code, :date_used, :list_name, :list_id, :load_type, :rule_code)
    """)

    
    # sql_upsert_info=text("""
    #     INSERT INTO info_tbl(cell, last_used) 
    #     VALUES (%s, %s) ON CONFLICT(info_pk) 
    #     DO UPDATE SET last_used = EXCLUDED.last_used WHERE info_tbl.cell = EXCLUDED.cell
    # """)

    sql_upsert_info = text("""
                    INSERT INTO info_tbl (info_pk, cell, last_used)
                    VALUES (:info_pk, :cell, :last_used)
                    ON CONFLICT (info_pk)
                    DO UPDATE
                    SET last_used = EXCLUDED.last_used
                    WHERE info_tbl.cell = EXCLUDED.cell
                    """)

    today: date = date.today()

    lead_history_tbl_list=[
        {
            "cell":row[0],
            "camp_code":row[1],
            "date_used":row[2],
            "list_name":row[3],
            "list_id":row[4],
            "load_type":row[5],
            "rule_code":row[6]
        }
        for row in insert
    ]

    #build params for info_tbl upsert
    upsert_params_info_tbl=[
        {"info_pk":item["info_pk"],"cell":item["phone_number"],"last_used":today}
        for item in updated_feeds
        if item.get("phone_number")
    ]



    async with async_session_maker() as session:

        try:
            # bulk insert leads history

            if lead_history_tbl_list:
                await session.execute(sql_insert_history,lead_history_tbl_list)
            
            #bulk insert info_tbl last_used

            if upsert_params_info_tbl:
                await session.execute(sql_upsert_info,upsert_params_info_tbl)

            if is_deduped:

                #build the updating array
                updating_list=[item['vendor_lead_code'] for item in feeds]

                chunk_sized=500

                stmt=text("""
                            UPDATE info_tbl
                            SET extra_info = NULL
                            WHERE id = ANY(:ids)
                        """
                        )
                
                stmt_update_dedupe=text("""
                                        UPDATE campaign_dedupe
                                        SET status = 'U'
                                        WHERE id = ANY(:ids)
                                        """
                                        )
                

                for chunk in chunked(updating_list,chunk_sized):
                    await session.execute(stmt,{"ids":chunk})
                    await session.execute(stmt_update_dedupe,{"ids":chunk})
            
            await session.commit()
            campaigns_logger.info(f"approximately:{len(upsert_params_info_tbl)} records updated on the information table(info_tbl)")
        
        #needs attention proper error handling

        except Exception as e:
            campaigns_logger.exception(f"an exception occurred while updating table:{e}")
            raise 
        


