from fastapi import HTTPException,status
from sqlmodel import select
from sqlalchemy import text,func
import re

from sqlmodel.ext.asyncio.session import AsyncSession
from typing import List,Optional,Sequence,Dict,Tuple
from models.campaigns import dedupe_campaigns_tbl
from models.dedupe_history_tracker import Dedupe_History_Tracker
from schemas.dedupes import PaginatedAggregatedDedupeResult,PaginatedResultsResponse,DedupeCampaignResponse

from models.dedupe_keys_table import manual_dedupe_key_tbl
from schemas.dedupe_campaigns import CreateDedupeCampaign
from utils.logger import define_logger

dedupe_logger=define_logger("als_dedupe_campaign_logs","logs/dedupe_route.log")
#create a deduped campaign
async def create_dedupe_campaign(campaign:CreateDedupeCampaign,session:AsyncSession)->dedupe_campaigns_tbl:
    session.add(campaign)
    await session.commit()
    await session.refresh(campaign)
    return campaign



#change a dedupe campaign to a generic campaign
async def change_dedupe_campaign_to_generic_campaign(camp_code:str,branch:str,session:AsyncSession)->Optional[dedupe_campaigns_tbl]:
    result=await session.exec(select(dedupe_campaigns_tbl).where((dedupe_campaigns_tbl.camp_code==camp_code)&(dedupe_campaigns_tbl.branch==branch)))
    db_item=result.one_or_none()
    if not db_item:
        return None
    db_item.is_deduped=False
    session.add(db_item)
    await session.commit()
    await session.refresh(db_item)
    return db_item



#deactivate deduped campaign
async def deactivate_deduped_campaign(camp_code:str,session:AsyncSession):

    result=await session.exec(select(dedupe_campaigns_tbl).where(dedupe_campaigns_tbl.camp_code==camp_code))
    db_item=result.one_or_none()
    if not db_item:
        return None
    db_item.is_active=False
    session.add(db_item)
    await session.commit()
    await session.refresh(db_item)
    return db_item

#fetch dedupe campaign by campaign code
async def get_deduped_campaign(camp_code:str,session:AsyncSession):
    
    result=await session.exec(select(dedupe_campaigns_tbl).where(dedupe_campaigns_tbl.camp_code==camp_code))
    
    db_item=result.one_or_none()
    if not db_item:
        return None
    return db_item

# "TEFWFDY/TLEFHQD"
async def get_leads_from_db_for_dedupe_campaign_for_TEFWFDY_and_TLEFHQD(session:AsyncSession,limit:int=3000):
    
    DEDUPED_CAMPAIGN_RAW_SQL = """
                    SELECT i.id, fore_name, last_name, i.cell
                    FROM info_tbl i, campaign_dedupe c
                    WHERE EXISTS (
                            SELECT 1
                            FROM campaign_dedupe
                            WHERE campaign_dedupe.cell = i.cell
                          )
                      AND c.status = 'R'
                      AND c.campaign_name = 'TEFWFDY/TLEFHQD'
                      AND (i.id IS NOT NULL)
                    ORDER BY random()
                    LIMIT :limit
                """
    
    if limit<0:
        return None
    stmt=text(DEDUPED_CAMPAIGN_RAW_SQL,{"limit":limit})

    results=await session.execute(stmt)
    leads=results.fetchall()
    return leads

# "TEBBDY/TERDDY/TEUAPIDY/TLEBHQD/TLED3HQD/TLEAHQD"

async def get_leads_from_db_for_dedupe_campaign_for_TEBBDY_TERDDY(session:AsyncSession,limit:int=3000):
    
    DEDUPED_CAMPAIGN_RAW_SQL = """
                    SELECT i.id, fore_name, last_name, i.cell
                    FROM info_tbl i, campaign_dedupe c
                    WHERE EXISTS (
                            SELECT 1
                            FROM campaign_dedupe
                            WHERE campaign_dedupe.cell = i.cell
                          )
                      AND c.status = 'R'
                      AND c.campaign_name =\"TEBBDY/TERDDY/TEUAPIDY/TLEBHQD/TLED3HQD/TLEAHQD\" 
                      AND (i.id IS NOT NULL)
                    ORDER BY random()
                    LIMIT :limit
                """
    
    if limit<0:
        return None
    
    
    stmt=text(DEDUPED_CAMPAIGN_RAW_SQL,{"limit":limit})
    results=await session.execute(stmt)
    leads=results.fetchall()

    return leads


#this method should get gender,limit,
async def get_leads_from_db_for_dedupe_campaign_gender_derived_income_limit(session:AsyncSession,gender:str,limit:int=10000):

    return True


#derived income should be provide but gender is optional

async def get_leads_from_db_for_dedupe_campaign_derived_income_limit(session:AsyncSession,derived_income:int,gender:Optional[str]=None,limit:int=10000):
  
    RAW_SQL = """
    SELECT id, cell 
    FROM info_tbl i 
    WHERE (derived_income >= :min_income) 
      AND (:gender IS NULL OR gender = :gender)
      AND (id IS NOT NULL) 
    ORDER BY RANDOM() 
    LIMIT :limit
    """
    if limit < 0:
        return None
    results=await session.execute(text(RAW_SQL),{"gender":gender.lower() if gender else None,"min_income":derived_income,"limit":limit})
    
    leads=results.fetchall()

    return leads


async def get_leads_for_OMLIFE(session:AsyncSession,derived_income:int,limit:int):

    RAW_QUERY= text("""
        SELECT id, cell 
        FROM info_tbl i 
        WHERE (
            DATE_PART('day', NOW()::timestamp - date_of_birth::timestamp) >= 10220 
            AND DATE_PART('day', NOW()::timestamp - date_of_birth::timestamp) <= 18250
          )
          AND (derived_income >= :derived_income)
          AND (extra_info IS NULL)
        ORDER BY RANDOM()
        LIMIT :limit
    """)

    
    results=await session.execute(RAW_QUERY,{"derived_income":derived_income,"limit":limit}) 
    
    return results.fetchall()



async def get_leads_for_MIWAYHKT(session:AsyncSession,derived_income:int,limit:int):

    RAW_QUERY= text("""
        SELECT id, cell 
        FROM info_tbl i 
        WHERE derived_income >= :derived_income
          AND extra_info IS NULL
          AND (
            last_used IS NULL 
            OR DATE_PART('day', NOW()::timestamp - last_used::timestamp) > 29
          )
        ORDER BY RANDOM()
        LIMIT :limit
    """)

    results=await session.execute(RAW_QUERY,{"derived_income":derived_income,"limit":limit})

    return results.fetchall()


async def get_leads_for_DIFFWT(session:AsyncSession,salary:int,gender:str,limit:int,year:Optional[int]=2020):
    
    RAW_QUERY= text("""
        SELECT id, cell 
        FROM info_tbl i 
        WHERE (salary >= :salary OR salary IS NULL)
          AND DATE_PART('year', created_at) = :year
          AND gender = :gender
        ORDER BY RANDOM()
        LIMIT :limit
    """)
    
    results=await session.execute(RAW_QUERY,{"salary":salary,"gender":gender,"limit":limit,"year":year})
    
    return results.fetchall()

async def get_leads_for_DIAGTE(session:AsyncSession,salary:int,limit:int,year:Optional[int]=2019):
    
    RAW_QUERY = text("""
        SELECT id, cell 
        FROM info_tbl i 
        WHERE (salary >= :salary OR salary IS NULL)
          AND DATE_PART('year', created_at) = :year
        ORDER BY RANDOM()
        LIMIT :limit
    """)

    results=await session.execute(RAW_QUERY,{"salary":salary,"year":year,"limit":limit})

    return results.fetchall()

#These following four have the same rules
async def get_leads_for_AGTEDI(session:AsyncSession,salary:int,limit:int,year:Optional[int]=2019):

    RAW_QUERY= text("""
        SELECT id, cell 
        FROM info_tbl i 
        WHERE (salary >= :salary OR salary IS NULL)
          AND DATE_PART('year', created_at) = :year
        ORDER BY RANDOM()
        LIMIT :limit
    """)

    results=await session.execute(RAW_QUERY,{"salary":salary,"limit":limit,"year":year})

    return results.fetchall()


async def get_leads_for_TelAGW(session:AsyncSession,salary:int,limit:int,year:Optional[int]=2019):

    RAW_QUERY= text("""
        SELECT id, cell 
        FROM info_tbl i 
        WHERE (salary >= :salary OR salary IS NULL)
          AND DATE_PART('year', created_at) = :year
        ORDER BY RANDOM()
        LIMIT :limit
    """)

    results=await session.execute(RAW_QUERY,{"salary":salary,"year":year,"limit":limit})

    return results.fetchall()

async def get_leads_for_TeleBudg(session:AsyncSession,salary:int,limit:int,year:Optional[int]=2019):
    
    RAW_QUERY=text("""
        SELECT id, cell 
        FROM info_tbl i 
        WHERE (salary >= :salary OR salary IS NULL)
          AND DATE_PART('year', created_at) = :year
        ORDER BY RANDOM()
        LIMIT :limit
    """)

    result=await session.execute(RAW_QUERY,{"salary":salary,"year":year,"limit":limit})
    return result.fetchall()

async def get_leads_for_TeleDial(session:AsyncSession,salary:int,limit:int,year:Optional[int]=2019):
    
    RAW_QUERY= text("""
        SELECT id, cell 
        FROM info_tbl i 
        WHERE (salary >= :salary OR salary IS NULL)
          AND DATE_PART('year', created_at) = :year
        ORDER BY RANDOM()
        LIMIT :limit
    """)

    results=await session.execute(RAW_QUERY,{"salary":salary,"year":year,"limit":limit})

    return results.fetchall()

#before this everything is the same

async def get_leads_for_DITFCS(session:AsyncSession,salary:int,limit:int,gender:str,year:Optional[int]=2020):

    RAW_QUERY = text("""
        SELECT id, cell 
        FROM info_tbl i 
        WHERE (salary >= :salary OR salary IS NULL)
          AND DATE_PART('year', created_at) = :year
          AND gender = :gender
        ORDER BY RANDOM()
        LIMIT :limit
    """)

    results=await session.execute(RAW_QUERY,{"salary":salary,"year":year,"gender":gender,"limit":limit})

    return results.fetchall()


async def get_leads_for_CRISPIP3(session:AsyncSession,derived_income:int,limit:int):

    RAW_QUERY= text("""
        SELECT id, cell 
        FROM info_tbl i 
        WHERE derived_income >= :derived_income
          AND extra_info IS NULL
          AND (
            last_used IS NULL 
            OR DATE_PART('day', NOW()::timestamp - last_used::timestamp) > 29
          )
        ORDER BY RANDOM()
        LIMIT :limit
    """)
    
    results=await session.execute(RAW_QUERY,{"derived_income":derived_income,"limit":limit})

    return results.fetchall()

 #"DIAGTE/AGTEDI/TelAGW/TeleBudg/TeleDial/TELEAGNI/TELEBDNI/TELEDDNI"
 #"DIAGTE/AGTEDI/TelAGW/TeleBudg/TeleDial/TELEAGNI/TELEBDNI/TELEDDNI":
 #salary cannot be null


async def get_leads_for_campaigns_list(session:AsyncSession,salary:int,limit:int):

    RAW_QUERY= text("""
        SELECT id, cell 
        FROM info_tbl i 
        WHERE salary >= :salary 
          AND id IS NOT NULL 
        ORDER BY RANDOM()
        LIMIT :limit
    """)
    if limit<0:
        return None
    
    result=await session.execute(RAW_QUERY,{"salary":salary,"limit":limit})

    return result.fetchall()

# "DITFCS/DIFFWT/TELEFFWN":


async def get_leads_for_DITFCS_DIFFWT_TELEFFWN(session:AsyncSession,salary:int,gender:str,limit:int):
    
    RAW_QUERY= text("""
        SELECT id, cell 
        FROM info_tbl i 
        WHERE salary >= :salary 
          AND gender = :gender 
          AND id IS NOT NULL 
        ORDER BY RANDOM()
        LIMIT :limit
    """
    )
    if limit<0:
        return None

    results=await session.execute(RAW_QUERY,{"salary":salary,"gender":gender,"limit":limit})

    return results.fetchall()

#   "TELEAGNI/TELEBDNI/TELEDDNI"
async def get_leads_for_TELEAGNI_TELEBDNI_TELEDDNI_with_derived_income_and_limit(session:AsyncSession,derived_income:int,limit:int):
   
    RAW_SQL=text(
        """
            SELECT id, cell
            FROM info_tbl i
            WHERE derived_income >= :derived_income
              AND id IS NOT NULL
            ORDER BY RANDOM()             
            LIMIT :limit
        """
    )

    result=await session.execute(RAW_SQL,{"derived_income":derived_income,"limit":limit})

    return result.fetchall()


#TELEFFWN

async def get_leads_for_TELEFFWN_with_gender_derived_income_and_limit(session:AsyncSession,derived_income:int,gender:str,limit):

    RAW_QUERY= text("""
        SELECT id, cell 
        FROM info_tbl i 
        WHERE derived_income >= :derived_income 
          AND gender = :gender 
          AND id IS NOT NULL 
        ORDER BY RANDOM()
        LIMIT :limit
    """
    )
    results=await session.execute(RAW_QUERY,{"derived_income":derived_income,"gender":gender,"limit":limit})

    return results.fetchall()


#fetch active dedupe campaign

#change campaign code 

#update campaign name


async def bulk_update_campaign_dedupe(session:AsyncSession,db_list:List,status:str,camp_code:str):

    for code_camp,db_list in db_list:
        sql = text(
                    """
                    UPDATE campaign_dedupe
                    SET status = 'R'
                    WHERE code = :code
                      AND id = ANY(:ids)
                    """
                )
    return True



async def bulk_upsert_update_info_tbl_in_batches(session:AsyncSession,data:Sequence[Dict[str,str]],batch_size:int=1000):
    
    RAW_QUERY="""
                INSERT INTO info_tbl(cell,extra_info)
                VALUES(:cell,:extra_info)
                ON CONFLICT(cell)
                DO UPDATE SET extra_info=EXCLUDED.extra_info
                WHERE info_tbl.cell = EXCLUDED.cell
             """
    total=len(data)
    for i in range(0,total,batch_size):
        batch=data[i:i + batch_size]
        for record in batch:
            await session.execute(RAW_QUERY,record)
        await session.commit()

    return 

async def bulk_insert_campaign_dedupe_tbl_in_batches():
    return True



async def select_code_from_campaign_dedupe_table(session:AsyncSession,code:str)->str | None:

    sql_query = text("SELECT code FROM campaign_dedupe WHERE code = :code")

    result=await session.execute(sql_query,{"code":code})

    row=result.fetchone()
    return row[0] if row else None



#campaigns rule where clause builder to query the campaigns correctly

def build_where_clause(filters:dict)->Tuple[str,dict]:

    DEDUPE_JOIN_CAMPAIGNS = {
    "TEBBDY/TERDDY/TEUAPIDY/TLEBHQD/TLED3HQD/TLEAHQD",
    "TEFWFDY/TLEFHQD"
     }
    conditions=[]

    params={}
    camp_code=filters.get('camp_code','')
    
    if any(group in camp_code for group in DEDUPE_JOIN_CAMPAIGNS):
        group_name= next(g for g in DEDUPE_JOIN_CAMPAIGNS if g in camp_code)

        where_clause = """
            EXISTS (
                SELECT 1 FROM campaign_dedupe cd 
                WHERE cd.cell = i.cell 
                  AND cd.status = 'R' 
                  AND cd.campaign_name = :group_name
            )
            AND i.id IS NOT NULL
        """
        params={"group_name": group_name}

        return where_clause,params
    
    #Salary or null
    if filters.get('min_salary') is not None:
        if filters.get('salary_or_NULL') is not None:
            conditions.append("(salary>=:min_salary or salary IS NULL)")
        else:
            conditions.append("salary>=:salary")
        params['min_salary']=filters['min_salary']
    
    # other filters, derived_income

    if filters.get('min_derived_income') is not None:
        conditions.append("derived_income>=:min_derived_income")
        params['min_derived_income']=filters['min_derived_income']
    # gender
    if filters.get('gender') is not None:
        conditions.append("gender=:gender")
        params["gender"]=filters["gender"]

    if filters.get('min_age') is not None or filters.get('max_age') is not None:
        return True
    
    if filters.get('last_used') is not None:
        conditions.append("""
            (last_used IS NULL OR 
             DATE_PART('day', NOW()::timestamp - last_used::timestamp) > :last_used_days)
        """)
        params['last_used']=filters['last_used']
    
    if filters.get('exclude_processed') is True:
        conditions.append("extra_info IS NULL")
    
    #created at year

    if filters.get('created_at_year') is not None:
        conditions.append("DATE_PART('year', created_at) = :created_at_year")
        params['created_at_year'] = filters['created_at_year']

    if filters.get('require_id') is True:
        conditions.append("i.id IS NOT NULL")

    if filters.get('dedupe_join_required') is True:
        conditions.append("""
            EXISTS (
                SELECT 1 FROM campaign_dedupe cd 
                WHERE cd.cell = i.cell 
                  AND cd.status = 'R' 
                  AND cd.campaign_name = :camp_code
            )
        """)
        params['camp_code'] = filters['camp_code']

    where_sql = " AND ".join(conditions) if conditions else "TRUE"


    return where_sql, params



async def create_manual_dedupe_key(session:AsyncSession,rule_name:str,dedupe_key:str,number_of_leads:int,user):
    try:
        record=manual_dedupe_key_tbl(rule_name=rule_name,dedupe_key=dedupe_key,number_of_leads=number_of_leads)
        session.add(record)
        await session.commit()
        await session.refresh(record)
        dedupe_logger.info(f"manual dedupe key:{dedupe_key} committed to the db by user:{user.id} with email:{user.email}")
        return record
    
    except HTTPException:
        raise
    except Exception as e:
        dedupe_logger.exception(f"An exception occurred while committing dedupe key to the table:{e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An exception occurred while creating a dedupe key")



async def get_dedupe_campaigns_aggregated_count_db(page:int,page_size:int,session:AsyncSession,user):

    try:
        #calculate the offset for pagination
        offset=(page - 1)*page_size
        #query building the query to get all campaign names and the count of associated records
        query=select(Dedupe_History_Tracker.campaign_name,func.count(Dedupe_History_Tracker.pk).label("record_count")).group_by(Dedupe_History_Tracker.campaign_name)
        #Apply pagination by limiting the number of results
        query=query.limit(page_size).offset(offset)
        #execute the query
        result=await session.execute(query)
        records=result.all()
        if not records:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"No records found")
        count_query=select([func.count()]).select_from(Dedupe_History_Tracker)
        total_count=await session.execute(count_query)
        total_count=total_count.scalar()
        #calculate total pages
        total_pages=(total_count + page_size -1) // page_size
        dedupe_logger.info(f"user:{user.id} with email:{user.email} fetch an aggregated count for dedupe campaigns")
        return PaginatedAggregatedDedupeResult(page=page,page_size=page_size,total=total_count,total_pages=total_pages,records=records)
    
    except HTTPException:
        raise
    except Exception as e:
        dedupe_logger.exception(f"an exception occurred while fetching dedupe campaigns aggregated count:{e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while fetching an aggregated count for dedupe campaigns")
    


async def search_cell_number_history_db(cell:str,page:int,page_size:int,session:AsyncSession,user,campaign_name:Optional[str]=None):
    try:

        if not re.match(r'^\d{10}$',cell):

            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"cell number:{cell} is invalid, cell number must have 10-digit")
        offset=(page - 1)*page_size
        query_count=select([func.count()]).select_from(Dedupe_History_Tracker).where(Dedupe_History_Tracker.cell==cell)
        #if the campaign name is provided add it as a filter to the query
        if campaign_name:
            query_count=query_count.where(Dedupe_History_Tracker.campaign_name==campaign_name)
        total_count=await session.execute(query_count)
        total_count=total_count.scalar()
        total_pages=(total_count + page_size -1) // page_size
        query=select(Dedupe_History_Tracker).where(Dedupe_History_Tracker.cell==cell)
        if campaign_name:
            query=query.where(Dedupe_History_Tracker.campaign_name==campaign_name)
        query=query.limit(page_size).offset(offset)
        result=await session.execute(query)
        records=result.scalars().all()

        if not records:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Cell number:{cell} has no history")
        dedupe_logger.info(f"user:{user.id} with email:{user.email} fetch an aggregated count for dedupe campaigns")
        return PaginatedResultsResponse(page=page,page_size=page_size,total=total_pages,records=records)
    
    except HTTPException:
        raise

    except Exception as e:
        dedupe_logger.exception(f"an exception occurred while searching for the history of cell number:{cell},{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while search for cell number:{cell} history")
    

async def search_id_number_history_db(id_number:str,page:int,page_size:int,session:AsyncSession,user):

    try:
        if not re.match(r'^\d{13}$',id_number):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"Invalid South African ID Number. It must be a 13-digit numeric string")
        
        offset=(page-1)*page_size

        query_count=select([func.count()]).select_from(Dedupe_History_Tracker).where(Dedupe_History_Tracker.id==id_number)
        total_count=await session.execute(query_count)
        total_count=total_count.scalar()
        total_pages=(total_count + page_size - 1) // page_size 
        query=select(Dedupe_History_Tracker).where(Dedupe_History_Tracker.id==id_number)
        query = query.limit(page_size).offset(offset)
        result = await session.execute(query)
        records = result.scalars().all()

        if not records:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"No records found for this id number:{id_number}")  
        
        dedupe_logger.info(f"user:{user.id} with email:{user.email} retrieved id number history:{id_number}")
        
        return PaginatedResultsResponse(
            page=page,
            page_size=page_size,
            total=total_count,
            total_pages=total_pages,
            records=records
        )
    
    except HTTPException:
        raise
    except Exception as e:
        dedupe_logger.exception(f"an exception occurred while searching for the history of cell number:{id_number},{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while search for cell number:{id_number} history")



async def search_dedupe_campaign_by_campaign_name_db(campaign_name:str,page:int,page_size:int,session:AsyncSession)->PaginatedResultsResponse:
    
    try:
        #calculate the offset based on the page number and page size
        offset=(page-1)*page_size
        #search the number the number of campaigns available

        #Query to count the total number of records
        query_count=select([func.count()]).select_from(Dedupe_History_Tracker).where(Dedupe_History_Tracker.campaign_name.ilike(f"%{campaign_name}%"))

        total_count=await session.execute(query_count)
        total_count=total_count.scalar()
        #calculate total pages based on total count and page_size
        total_pages=(total_count + page_size -1) // page_size
        #query to fetch paginated results
        query=select(Dedupe_History_Tracker)

        if campaign_name:
            query=query.where(Dedupe_History_Tracker.campaign_name.ilike(f"%{campaign_name}%"))

        query=query.limit(page_size).offset(offset)
        result=await session.execute(query)
        records=result.scalars().all()
        if not records:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"No records found for campaign:{campaign_name}")
        
        return PaginatedResultsResponse(
            page=page,
            page_size=page_size,
            total=total_pages,
            total_pages=total_pages,
            records=records
        )
    

    
    except Exception as e:
        dedupe_logger.exception(f"an exception occurred while fetching all records associated:{campaign_name}:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while fetch records for campaign:{campaign_name}")
    

