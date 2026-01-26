from fastapi import APIRouter,Depends,HTTPException,status,Path
from sqlmodel import select
from sqlalchemy.ext.asyncio.session import AsyncSession
from utils.auth import get_current_active_user
from database.master_database_prod import get_async_master_prod_session
from database.master_db_connect import get_async_session
from models.rules_table import new_rules_tbl,rules_tbl
from schemas.rules_schema import CreateRule,ResponseRuleSchema,RuleSchema,RuleResponseModel,NumericConditionResponse,AgeConditionResponse,LastUsedConditionResponse,RecordsLoadedConditionResponse
from utils.dynamic_sql_rule_function import build_dynamic_rule_engine
from crud.rule_engine_db import (create_person_db,get_rule_by_name_db)
from schemas.person import PersonCreate,PersonCreateResponse
from utils.logger import define_logger
from utils.dynamic_sql_rule_function import remove_order_by_random,build_left_anti_join_sql,fetch_rule_sql,execute_built_sql_query,fix_typedata_double_quotes,replace_double_quotes_with_single



master_db_logger=define_logger("als_practice_logger","logs/practice_route.log")

practice_router=APIRouter(prefix="/master_db_test",tags=["Master Database Test"])


@practice_router.get("/persons_from_database/{rule_name}")
async def get_persons_by_rule_name(rule_name:str,master_session:AsyncSession=Depends(get_async_master_prod_session),session:AsyncSession=Depends(get_async_session)):
    print("enter the main route")
    result=await get_rule_by_name_db(rule_name,master_session)
    if result==None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"The requested rule does not exist")
    
    print("build sql query")
    print(result)
    stmt,params=build_dynamic_rule_engine(result[0].rule_json)
    rows=await master_session.execute(stmt,params)
    result=rows.mappings().all()
    return result



   
@practice_router.post("/create-person",status_code=status.HTTP_201_CREATED,response_model=PersonCreateResponse)
async def create_person(person:PersonCreate,session:AsyncSession=Depends(get_async_master_prod_session)):
    return await create_person_db(person,session)



@practice_router.get("/practice/{rule_name}",status_code=status.HTTP_200_OK,description="Get all the leads by rule name")

async def get_practice_leads(rule_name:str,session:AsyncSession=Depends(get_async_master_prod_session)):
    try:
        print("enter the route")
        print(f"enter the route:{rule_name}")
        fetched_rule=await fetch_rule_sql(session,rule_name)
        print("print the rule sql")
        print()
        print(fetched_rule)
        print("print the fetched rule without random method")
        random_removed=remove_order_by_random(fetched_rule)
        print("print the random removed sql query")
        print("")
        print(random_removed)
        print("print the built sql query")
        print("")
        print(build_left_anti_join_sql(random_removed))

        built_query=build_left_anti_join_sql(random_removed)
        print("")
        print("print the the fetched result after executing the query")
        cleaned_query=replace_double_quotes_with_single(built_query)
        
        result=await execute_built_sql_query(session,cleaned_query)
        print(result)
        print("print the length of the leads")
        print(len(result))
        return True
    
    except Exception as e:
        master_db_logger.exception(f"an internal server while fetching practice leads:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while fetching practice leads")