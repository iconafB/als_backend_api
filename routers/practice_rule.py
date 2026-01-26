from fastapi import APIRouter,Depends,HTTPException,status,Path
from sqlmodel import select
from sqlalchemy.ext.asyncio.session import AsyncSession
from utils.auth import get_current_active_user
from database.master_db_connect import get_async_session
from database.master_db_connect import get_async_session
from models.rules_table import new_rules_tbl
from schemas.rules_schema import CreateRule,ResponseRuleSchema,RuleSchema,RuleResponseModel,NumericConditionResponse,AgeConditionResponse,LastUsedConditionResponse,RecordsLoadedConditionResponse
from utils.dynamic_sql_rule_function import build_dynamic_rule_engine
from crud.rule_engine_db import (create_person_db,get_rule_by_name_db)
from schemas.person import PersonCreate,PersonCreateResponse

practice_rule_router=APIRouter(prefix="/practice-rule",tags=["Practice Rule"])

@practice_rule_router.post("/rules",status_code=status.HTTP_200_OK,description="Create Rule",response_model=RuleResponseModel)

async def create_rule(campaign_code:str,rule:RuleSchema,session:AsyncSession=Depends(get_async_session)):
    db_rule=new_rules_tbl(rule_name=campaign_code,status=status,rule_json=rule.model_dump())
    session.add(db_rule)
    await session.commit()
    await session.refresh(db_rule)
    rule_json=db_rule.rule_json
    response=RuleResponseModel(
        id=db_rule.id,
        rule_name=db_rule.rule_name,
        salary=NumericConditionResponse.from_condition(rule_json["salary"]),
        derived_income=NumericConditionResponse.from_condition(rule_json["derived_income"]),
        gender=rule_json["gender"]["value"],
        typedata=rule_json["typedata"]["value"],
        is_active=rule_json["is_active"]["value"],
        age=AgeConditionResponse.from_condition(rule_json["age"]),
        records_loaded=RecordsLoadedConditionResponse.from_condition(rule_json["number_of_records"])
        if rule_json.get("number_of_records") else None
        ,
        last_used=LastUsedConditionResponse.from_condition(rule_json["last_used"])
        if rule_json.get("last_used") else None
    )

    return response

@practice_rule_router.put("/rules/{name}",status_code=status.HTTP_200_OK,description="Update the rule name")

async def update_rule(name:str,updated:RuleSchema,session:AsyncSession=Depends(get_async_session)):
    result=await get_rule_by_name_db(name,session)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"No rule found")
    print("print the rule again")
    print(result)
    return True

@practice_rule_router.get("/persons/{rule_name}")
async def get_persons_by_rule_name(rule_name:str,session:AsyncSession=Depends(get_async_session)):
    result=await get_rule_by_name_db(rule_name,session)
    
    if result==None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"The requested rule does not exist")
    stmt,params=build_dynamic_rule_engine(result[0].rule_json)
    rows=(await session.execute(stmt,params)).all()
    print("print the leads fetched from the master database")
    print(rows)
    return rows


@practice_rule_router.post("/create-person",status_code=status.HTTP_201_CREATED,response_model=PersonCreateResponse)
async def create_person(person:PersonCreate,session:AsyncSession=Depends(get_async_session)):
    return await create_person_db(person,session)



@practice_rule_router.get("/get-rule/{rule_name}",status_code=status.HTTP_200_OK,description="Get all the leads associated with a rule code")
async def get_leads_by_rule_name():
    try:
        return True
    except Exception as e:
        
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while fetching the leads")
