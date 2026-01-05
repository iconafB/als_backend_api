from fastapi import APIRouter,Depends,HTTPException,status,Path
from sqlmodel import select
from sqlalchemy.ext.asyncio.session import AsyncSession
from utils.auth import get_current_active_user
from database.master_database_test import get_async_master_test_session

from database.master_db_connect import get_async_session
from models.rules_table import rules_tbl
from schemas.rules_schema import CreateRule,ResponseRuleSchema,RuleSchema,RuleResponseModel,NumericConditionResponse,AgeConditionResponse,LastUsedConditionResponse,RecordsLoadedConditionResponse
from utils.dynamic_sql_rule_function import build_dynamic_rule_engine
from crud.rule_engine_db import (create_person_db,get_rule_by_name_db)
from schemas.person import PersonCreate,PersonCreateResponse

practice_router=APIRouter(prefix="/master_db_test",tags=["Master Database Test"])

@practice_router.get("/persons_from_database/{rule_name}")
async def get_persons_by_rule_name(rule_name:str,master_session:AsyncSession=Depends(get_async_master_test_session),session:AsyncSession=Depends(get_async_session)):
    result=await get_rule_by_name_db(rule_name,session)
    if result==None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"The requested rule does not exist")
    stmt,params=build_dynamic_rule_engine(result[0].rule_json)
    rows=await master_session.execute(stmt,params)
    result=rows.mappings().all()
    return result


    
@practice_router.post("/create-person",status_code=status.HTTP_201_CREATED,response_model=PersonCreateResponse)
async def create_person(person:PersonCreate,session:AsyncSession=Depends(get_async_session)):
    return await create_person_db(person,session)

