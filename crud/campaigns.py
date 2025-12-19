from fastapi import HTTPException,status
from sqlmodel import select
from sqlalchemy import func,or_
from sqlmodel.ext.asyncio.session import AsyncSession
from typing import List,Annotated,Tuple,Dict,Optional
from models.campaigns_table import campaign_tbl
from schemas.campaigns import CreateCampaign,UpdateCampaignName,CreateCampaignResponse,InfiniteResponseSchema,CampaignsTotal
from utils.logger import define_logger
from models.campaigns_table import campaign_tbl
from schemas.campaigns import CampaignSpecLevelResponse
from models.rules_table import rules_tbl
from crud.rule_engine_db import get_rule_by_name_db
from utils.check_spec_levels_helper import spec_level_query_builder
#create campaign on the master db

campaigns_logger=define_logger("als campaign logs","logs/campaigns_route.log")

#create campaign

async def create_campaign_db(campaign:CreateCampaign,session:AsyncSession,user)->CreateCampaignResponse:
    
    exists=await get_campaign_by_code_db(campaign.camp_code,session,user)
    if exists is not None:
        return False
    db_campaign=campaign_tbl(**campaign.model_dump())
    session.add(db_campaign)
    await session.commit()
    await session.refresh(db_campaign)
    
    return CreateCampaignResponse.model_validate(db_campaign)

#update campaign name
async def update_campaign_name_db(campaign_new_name:UpdateCampaignName,camp_code:str,session:AsyncSession,user)->CreateCampaignResponse|None:
    #get campaign by code
    result=await get_campaign_by_code_db(camp_code,session,user)
    if result==None:
        return None
    result.campaign_name=campaign_new_name.campaign_name
    try:
        session.add(result)
        await session.commit()
        await session.refresh(result)
        campaigns_logger.info(f"user {user.id} with email:{user.email} updated campaign:{campaign_new_name.campaign_name} to {campaign_new_name}")
        return CreateCampaignResponse.model_validate(result)
    except HTTPException:
        raise
    except Exception as e:
        await session.rollback()
        campaigns_logger.exception(f"an internal server error occurred while updating campaign:{camp_code}:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"Internal server error occurred")


#get campaign by name
async def get_campaign_by_name_db(campaign_name:str,session:AsyncSession)->CreateCampaignResponse:
    try:
        campaign_query=select(campaign_tbl).where(campaign_tbl.campaign_name==campaign_name)
        campaign=await session.exec(campaign_query)
        result=campaign.first()
        if not result:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Campaign:{campaign_name} does not exist")
        return CreateCampaignResponse.model_validate(result)
    
    except HTTPException:
        raise

    except Exception as e:
        campaigns_logger.exception(f"An exception occurred:{e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred")

#get campaign by campaign code
async def get_campaign_by_code_db(camp_code:str,session:AsyncSession,user)->CreateCampaignResponse| None:
    campaign=await session.exec(select(campaign_tbl).where(campaign_tbl.camp_code==camp_code))
    result=campaign.first()
    if result is None:
        return None
    return CreateCampaignResponse.model_validate(result)

#get all active campaigns
async def get_all_campaigns_by_branch_db(branch:str,session:AsyncSession,user,page:int,page_size:int)->List[CreateCampaignResponse]:
    campaigns_query=await session.exec(select(campaign_tbl).where(campaign_tbl.branch==branch))
    campaigns=campaigns_query.all()
    total=len(campaigns)
    #pagination calculations
    start=(page - 1)*page_size
    end=start + page_size
    paginated_campaigns=campaigns[start:end]
    results=[CreateCampaignResponse.model_validate(c) for c in paginated_campaigns]
    campaigns_logger.info(f"user:{user.id} with email:{user.email} retrieve campaigns for branch:{branch}")
    return {
        "total":total,
        "page":page,
        "page_size":page_size,
        "results":results
    }

async def get_all_campaigns_db(session:AsyncSession,page:int,page_size:int,user)->List[CreateCampaignResponse]:
    campaigns_query=await session.exec(select(campaign_tbl))
    campaigns=campaigns_query.all()
    total=len(campaigns)
    #pagination calculations
    start=(page - 1)*page_size
    end=start + page_size
    paginated_campaigns=campaigns[start:end]
    results=[CreateCampaignResponse.model_validate(c) for c in paginated_campaigns]

    campaigns_logger.info(f"user:{user.id} with email:{user.email} retrieved {len(results)} campaigns")

    return {
        "total":total,
        "page":page,
        "page_size":page_size,
        "results":results
    }


async def get_all_campaigns_infinite_scroll_db(
    session: AsyncSession,
    page: int,
    page_size: int,
    search: Optional[str],
    user
) -> Dict:
    
   

    # Build the base query 
    base_query = select(campaign_tbl.camp_code, campaign_tbl.campaign_name)
    if search:
        pattern = f"%{search}%"
        base_query = base_query.where(
            or_(
                campaign_tbl.camp_code.ilike(pattern),
                campaign_tbl.campaign_name.ilike(pattern),
            )
        )

    # Build count statement from the filtered base query (use subquery)
    count_stmt = select(func.count()).select_from(base_query.subquery())

    # Execute count and extract scalar in a robust way
    total = 0
    try:
        total_result = await session.exec(count_stmt)
        # Try modern API first
        try:
            # scalar_one() exists in many SQLAlchemy versions and returns the scalar directly.
            total = int(total_result.scalar_one())
        except AttributeError:
            # Older Result API fallback: try scalar() then one()/first()
            if hasattr(total_result, "scalar"):
                val = total_result.scalar()
                total = int(val) if val is not None else 0
            else:
                # fallback to .one() or .first()
                try:
                    one_row = total_result.one()
                    # one_row may be a tuple like (count,) or an int
                    total = int(one_row[0]) if isinstance(one_row, (list, tuple)) else int(one_row)
                except Exception:
                    first_row = total_result.first()
                    if first_row is None:
                        total = 0
                    else:
                        total = int(first_row[0]) if isinstance(first_row, (list, tuple)) else int(first_row)
    except Exception as e:
        campaigns_logger.exception("Failed to compute total count for infinite scroll")
        # Re-raise or convert to a handled error upstream
        raise

    # Paginated fetch (stable ordering is important)
    stmt = (
        base_query
        .order_by(campaign_tbl.camp_code)
        .offset((page - 1) * page_size)
        .limit(page_size)
    )

    try:
        result = await session.exec(stmt)
        rows = result.all()  # list of tuples: (camp_code, campaign_name)
    except Exception:
        campaigns_logger.exception("Failed to fetch paginated campaign rows")
        raise

    # Convert rows to Pydantic schemas (or dicts)
    results = [
        InfiniteResponseSchema(camp_code=camp_code, campaign_name=campaign_name)
        for camp_code, campaign_name in rows
    ]

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "results": results,
    }


async def get_active_campaign_to_load(camp_code:str,session:AsyncSession):
    try:
        stmt=select(rules_tbl.rule_name).where(rules_tbl.rule_name==camp_code).where(rules_tbl.is_active==True)
        result=await session.scalar(stmt)
        if result is None:
            return None
        return result
    except Exception as e:
        campaigns_logger.exception(f"An internal server error occurred:{e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred")
    

async def get_total_campaigns_on_the_db(session:AsyncSession,user)->CampaignsTotal:
    try:
        result=await session.exec(select(func.count()).select_from(campaign_tbl))
        campaigns_total=result.one()
        campaigns_logger.info(f"user:{user.id} with email:{user.email} fetched a total of:{campaigns_total} campaigns")
        return CampaignsTotal(total_number_of_campaigns=campaigns_total)
    
    except Exception as e:
        campaigns_logger.exception(f"exception occurred while fetching the total number of campaigns:{e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while fetching the total number of campaigns")


async def get_spec_level_campaign_name_db(rule_name:str,session:AsyncSession,user)->CampaignSpecLevelResponse:
    try:
        result=await get_rule_by_name_db(rule_name,session)

        if result==None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"campaign rule:{rule_name} does not exist")
        #builder the sql query and return it
        stmt,params=spec_level_query_builder(result[0].rule_json)
        #This returns a list of dictionaries
        rows=await session.execute(stmt,params)
        #count the number of entries
        number_of_row_entries=len(rows.mappings().all())
        campaigns_logger.info(f"user:{user.id} with email:{user.email} check the specific level for rule name:{rule_name}")
        
        return CampaignSpecLevelResponse(rule_name=rule_name,number_of_leads_available=number_of_row_entries)
    
    except Exception as e:
        campaigns_logger.exception(f"An exception occurred while fetching the spec level for campaign rule name:{rule_name}, {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while checking the specification level for campaign rule:{rule_name}")