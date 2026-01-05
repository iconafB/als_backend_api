from fastapi import HTTPException,status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text,bindparam,delete
from sqlalchemy.dialects.postgresql import ARRAY,TEXT
from sqlmodel import select,func
from typing import Optional,Tuple,List
from models.campaign_dedupe import Campaign_Dedupe
from utils.logger import define_logger
from utils.dedupes.submit_dedupe_return_query import UPDATE_DEDUPE_CAMPAIGN_RETURN_SQL,DELETE_LEADS_SQL,UPDATE_INFO_TABLE_DEDUPE_SQL,DELETE_CAMPAIGN_DEDUPE_SQL,UPDATE_DEDUPE_RETURN_QUERY,FECTHING_PENDING_IDS_CAMPAIGN_STATUS_AND_CODE,DELETE_PENDING_IDS_CAMPAIGN_STATUS_AND_CODE,UPDATE_PENDING_IDS_ON_THE_INFO_TBL,DELETE_STMT_ON_CAMPAIGN_DEDUPE

dedupe_logger=define_logger("als_dedupe_campaign_logs","logs/dedupe_route.log")

async def update_campaign_dedupe_helper(key:str,dedupe_list:list[str],session:AsyncSession):
    try:
        if not dedupe_list:
            return 0
        update_query=text(UPDATE_DEDUPE_CAMPAIGN_RETURN_SQL)
        result= await session.execute(update_query,{"code":key,"id_list":dedupe_list})
        await session.commit()
        return result.scalar()
    except Exception as e:
        dedupe_logger.exception(f"An exception occurred while updating campaign dedupe table:{e}")
        raise


async def campaign_dedupes_cleaner_helper(key:str,session:AsyncSession)->Optional[Tuple[list[int],list[int],list[int]]]|bool:
    try:
        query_stmt=select(Campaign_Dedupe.code).where(Campaign_Dedupe.status=='P',Campaign_Dedupe.code==key)
        results=await session.execute(query_stmt)
        rows=results.fetchall()
        id_numbers=[row[0] for row in rows]
        if not id_numbers:
            return 0,0,0
        
        delete_stmt=text(DELETE_LEADS_SQL)
        delete_result=await session.execute(delete_stmt,{"ids":id_numbers})
        update_stmt=text(UPDATE_INFO_TABLE_DEDUPE_SQL)
        updated_result=await session.execute(update_stmt,{"ids":id_numbers})
        delete_stmt_campaign_dedupe_where_status_is_u=text(DELETE_CAMPAIGN_DEDUPE_SQL)
        delete_dedupe_campaign=await session.execute(delete_stmt_campaign_dedupe_where_status_is_u,{"code":'U'})
        #commit transaction
        await session.commit()
        return len(delete_result.fetchall()),len(updated_result.fetchall()),len(delete_dedupe_campaign.fetchall())
    except Exception as e:
        dedupe_logger.exception(f"An exception occurred while fetching dedupe records with the following key:{key},{e}")
        await session.rollback()
        return None



#calculate the number of records with ids return

async def calculate_ids_campaign_dedupe_with_status_r(session:AsyncSession,code:str):
    try:
        stmt=select(func.count()).select_from(Campaign_Dedupe).where(Campaign_Dedupe.code==code,Campaign_Dedupe.status=='R')
        result=await session.execute(stmt)
        result_count:int=result.scalar_one() or 0
        return result_count
    
    except Exception as e:
        dedupe_logger.exception(f"an exception occured while calculating the number of rows with key:{code},exception:{e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"an exception occured while calculating the number of rows with key:{code}")


#update status dedupe for submit return,return the number of rows returned

async def update_campaign_dedupe_status(dedupe_ids:list[str],code:str,session:AsyncSession,user)->int:
    try:
        if not dedupe_ids:
            return 0
        stmt=text(UPDATE_DEDUPE_RETURN_QUERY).bindparams(bindparam("code", type_=TEXT),bindparam("dedupe_ids", type_=ARRAY(TEXT)))
        # asyncpg requires lists to be passed as Python lists
        result=await session.execute(stmt,{"code":code,"dedupe_ids":dedupe_ids})
        #count the rows without loading them to memory
        updated_rows=0
        async for _ in result.scalars():
            updated_rows+=1
        dedupe_logger.info(f"user:{user.id} with {user.email} updated records of length:{updated_rows} from the campaign_dedupe table")
        
        return updated_rows
    
    except Exception as e:
        dedupe_logger.exception(f"An exception occorred updating the campaign dedupe status:{e}")
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while updating campaign dedupe status")


async def fetch_delete_update_pending_campaign_ids(code:str,session:AsyncSession,user):

    try:
        #Fetch the list that fetches these ids
        #stmt=text(FECTHING_PENDING_IDS_CAMPAIGN_STATUS_AND_CODE)

        #result=await session.execute(stmt,{"code":code})
        #fetching everything to memory potential dangerous,may waste resources
        query_result=select(Campaign_Dedupe.id).where(Campaign_Dedupe.status=='P',Campaign_Dedupe.code==code)
        next_result=await session.execute(query_result)
        #this is needed but potentially dangerous
        ids=[id for id, in next_result.fetchall()]

        #ids=result.scalars().all()

        #db_list:List[str]=[d for d in ids if d is not None]

        if len(ids)>0:
            delete_stmt=text(DELETE_PENDING_IDS_CAMPAIGN_STATUS_AND_CODE)

            delete_result=await session.execute(delete_stmt,{"ids":ids})
            #loads everything to memory this is wrong, may crash the server if the load is too big
            #deleted_ids=len(delete_result.scalars().all())
            deleted_count=0
            async for _ in delete_result.scalars():
                deleted_count+=1
            update_stmt=text(UPDATE_PENDING_IDS_ON_THE_INFO_TBL)
            updated_result=await session.execute(update_stmt,{"ids":ids})

            info_table_updated_count=0

            async for _ in updated_result.scalars():
                info_table_updated_count+=1
            #updated_ids=len(update_result.scalars().all())

            delete_campaign_dedupe_stmt=text(DELETE_STMT_ON_CAMPAIGN_DEDUPE)

            deleted_campaign_dedupe_result=await session.execute(delete_campaign_dedupe_stmt,{"u_code": "U"})
            
            deleted_campaign_dedupe_count=0

            async for _ in deleted_campaign_dedupe_result.scalars():

                deleted_campaign_dedupe_count+=1

            #deleted_campaign_dedupe_ids=len(deleted_campaign_dedupe_result.scalars().all())

            #commit all the changes at once,remove this commit, this is one session anyway
            dedupe_logger.info(f"user:{user.id} with email:{user.email} deleted:{deleted_count} ids from campaign_dedupe table,updated:{info_table_updated_count} on the info_tbl(information table),and deleted:{deleted_campaign_dedupe_count} ids on the campaign_dedupe table")
            
            return {
                "retrieved_pending_ids":len(ids),
                "deleted_ids_from_campaign_dedupe_table":deleted_count,
                "updated_ids_from_info_tbl":info_table_updated_count,
                "deleted_stmt_from_campaign_dedupe":deleted_campaign_dedupe_count
                }
        

        
        else:
            return {
                "retrieved_pending_ids_from_campaign_dedupe_table":0,
                "deleted_ids_from_campaign_dedupe_table":0,
                "updated_ids_from_info_tbl":0,
                "deleted_stmt_from_campaign_dedupe":0
                }
    

    except Exception as e:
        dedupe_logger.exception(f"An exception occurred while requesting ids from the db by {user.id} with email {user.email}:{e}")
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while updating and deleting ids on the inf_tbl and campaign_dedupe table")



