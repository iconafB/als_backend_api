from fastapi import HTTPException,status,Depends
from sqlmodel import select,update,delete
from typing import Annotated
from datetime import datetime
from sqlalchemy import update, func, cast
from sqlalchemy import text,func,or_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import ARRAY, TEXT
from sqlalchemy.exc import IntegrityError
from schemas.campaign_rules import CreateCampaignRule
from utils.auth import get_current_active_user
from utils.logger import define_logger
from sqlmodel.ext.asyncio.session import AsyncSession
from typing import List,Optional
from sqlalchemy.exc import IntegrityError
from models.rules_table import rules_tbl
from models.campaign_rules_table import campaign_rule_tbl

from crud.campaigns import (get_campaign_by_code_db)
from models.campaigns_table import campaign_tbl
from models.campaign_rules_table import campaign_rule_tbl

from schemas.campaign_rules import RuleCreate,AssignCampaignRuleToCampaign,AssignCampaignRuleResponse,CreateCampaignRuleResponse,PaginatedCampaignRules
from schemas.rules_schema import RuleSchema,ResponseRuleSchema,RuleSchema,RuleResponseModel,NumericConditionResponse,AgeConditionResponse,LastUsedConditionResponse,RecordsLoadedConditionResponse,DeactivateRuleResponseModel,ActivateCampaignRuleResponse,GetCampaignRuleResponse,UpdateCampaignRule,UpdatingSalarySchema,UpdatingDerivedIncomeSchema,UpdateAgeSchema,GetAllCampaignRulesResponse,ActivateRuleResponseModel,UpdateNumberOfLeads,DeleteCampaignRuleResponse,CampaignRulesTotal
from utils.campaign_rules_helper import extract_numeric_rule

#rules logger
campaign_rules_logger=define_logger("als campaign rules","logs/campaign_rules_logs")

#create campaign rule 
async def create_campaign_rule_db(campaign_code:str,rule:RuleSchema,session:AsyncSession,user)->RuleResponseModel:
    
    try:
        db_rule=rules_tbl(rule_name=campaign_code,rule_json=rule.model_dump(),created_by=user.id,is_active=True)
        session.add(db_rule)
        await session.commit()
        await session.refresh(db_rule)
        rule_json=db_rule.rule_json

        response=RuleResponseModel(
            rule_code=db_rule.rule_code,
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
    
    except HTTPException:
        raise
    except Exception as e:
        campaign_rules_logger.exception(f"user id:{user.id} with email:{user.email} created a campaign rule:{db_rule.rule_name} and an exception occurred:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occured while creating a campaign rule:{db_rule.rule_name}")

#search rule_code by rule_name
async def get_campaign_rule_by_rule_name_db(rule_name:str,session:AsyncSession,user):
    rule_query=await session.exec(select(rules_tbl).where(rules_tbl.rule_name==rule_name))
    rule=rule_query.first()
    if not rule:
        campaign_rules_logger.info(f"user with user id:{user.id} with email:{user.email} requested campaign rule with rule code:{rule_name} but it does not exist")
        return None
    
    return rule


#update campaign rule name
async def update_campaign_name_db(rule_code:str,update_name:str,session:AsyncSession,user)->str:
    try:
        rule=await session.get(rules_tbl,rule_code)
        if not rule:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Campaign rule with rule_code:{rule_code} does not exist")
        rule.rule_name=update_name
        session.add(rule)
        await session.commit()
        await session.refresh(rule)
        campaign_rules_logger.info(f"user:{user.id} with email:{user.email} updated campaign rule with rule code:{rule_code}")
        
        print(f"the updated rule name:{rule.rule_name}")
        return rule.rule_name,rule.rule_code
    
    except HTTPException:
        raise
    except Exception as e:
        campaign_rules_logger.exception(f"an exception while updating campaign rule:{rule_code} by user:{user.id} with email:{user.email}:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while updating the name of campaign rule with rule code:{rule_code}")


async def deactivate_campaign_db(rule_code,session:AsyncSession,user)->DeactivateRuleResponseModel:
    try:
        rule=await session.get(rules_tbl,rule_code)

        if not rule:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"The requested campaign rule does not exist on the campaign rules table")
        
        campaign_rule_tbl_query=await session.exec(select(campaign_rule_tbl).where(campaign_rule_tbl.rule_code==rule_code))
        campaign_rule_tbl_result=campaign_rule_tbl_query.one_or_none()

        if campaign_rule_tbl_result==None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"rule:{rule_code} does not exist on the campaign rule table")
        campaign_rule_tbl_result.is_active=False
        rule.is_active=False
        await session.commit()
        await session.refresh(rule)
        campaign_rules_logger.info(f"user:{user.id} with email:{user.email} deactivaed campaign rule:{rule.rule_code}")
        return DeactivateRuleResponseModel(rule_code=rule_code,rule_name=rule.rule_name,message=f"Campaign rule with rule code:{rule_code} has been deactivated")
    
    
    except HTTPException:
        raise
    except Exception as e:

        campaign_rules_logger.exception(f"an internal server error occurred while updating campaign rule:{rule_code},{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while updating campaign rule:{rule_code}")


async def activate_campaign_db(rule_code,session:AsyncSession,user)->ActivateRuleResponseModel:
    try:
        print("enter the crud method")
        rule=await session.get(rules_tbl,rule_code)
        if not rule:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Camapign rule:{rule_code} does not exist")
        campaign_rule_tbl_query=await session.exec(select(campaign_rule_tbl).where(campaign_rule_tbl.rule_code==rule_code))
        campaign_rule_tbl_result=campaign_rule_tbl_query.one_or_none()

        if not campaign_rule_tbl_result:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"Campaign rule is alraedy active, or it is not assigned to anything")
        
        campaign_rule_tbl_result.is_active=True
        rule.is_active=True
        await session.commit()
        await session.refresh(rule)
        return ActivateRuleResponseModel(rule_code=rule_code,rule_name=rule.rule_name,message=f"campaign rule:{rule_code} has been activated",is_active=True)
    
    except HTTPException:
        raise

    except Exception as e:
        campaign_rules_logger.exception(f"an internal server error occurred while user:{user.id} with email:{user.email} activating campaign rule:{rule_code} with this exception:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while activating campaign rule:{rule_code}")



async def get_rule_by_rule_code_db(rule_code:int,session:AsyncSession,user)->RuleResponseModel:
    try:
        rule_query=await session.exec(select(rules_tbl).where(rules_tbl.rule_code==rule_code))
        rule=rule_query.first()
        if not rule:
            return None
        return rule
    
    except HTTPException:
        raise

    except Exception as e:
        campaign_rules_logger.exception(f"user id:{user.id} with email:{user.email} created a campaign rule:{rule_code} and an exception occurred:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occured while creating a campaign rule:{rule_code}")
    




#change rule name 

#assign campaign rule to an existing campaign
async def assign_campaign_rule_to_campaign_db(rule:AssignCampaignRuleToCampaign,session:AsyncSession,user=Depends(get_current_active_user))->AssignCampaignRuleResponse:
    try:
        #find campaign, exit and raise an exception if it's does not exist
        rule_code=rule.rule_code
        camp_code=rule.camp_code
        print("print the incoming payload")
        print(rule)

        campaign=await get_campaign_by_code_db(rule.camp_code,session)
        print()
        if campaign==None:
            campaign_rules_logger.info(f"user with user id:{user.id} with email:{user.email} requested campaign with code:{rule.rule_code} and it does not exist")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Campaign:{rule.camp_code} does not exist, create it and than assign to rule code:{rule.rule_code}")
        
        query= text("""
                SELECT r.rule_code
                FROM campaign_rule_tbl AS c
                JOIN rules_tbl AS r
                ON c.rule_code = r.rule_code
                WHERE c.camp_code = :camp_code
                  AND c.is_active = TRUE
                """
        )

        print("print the sql query")
        print(query)

        result=await session.execute(query,{"camp_code":camp_code})
        rows=result.fetchall()
        rule_codes=[row.rule_code for row in rows]

        if rule_code in rule_codes:
            rule_code_message=f"rule number:{rule_code} was found active, is now deactivated and {rule_code} is now active for campaign:{camp_code}"
            
            query=text("""
                    UPDATE campaign_rule_tbl
                    SET is_active = FALSE
                    WHERE camp_code = :camp_code
                    """)
            
            await session.execute(query,{"camp_code":camp_code})
            await session.commit()
            campaign_rules_logger.info(f"update campaign code:{camp_code} on table campaign_rule_tbl")
            todays_date=datetime.today().strftime('%Y-%m-%d')

            insert_campaign_rule_tbl_query=text("""
                                INSERT INTO campaign_rule_tbl (camp_code, rule_code, date_rule_created, is_active)
                                VALUES (:camp_code, :rule_code, :todays_date, TRUE)
                                """
                            )
            
            await session.execute(insert_campaign_rule_tbl_query,{
                "camp_code":camp_code,
                "rule_code":rule_code,
                "todays_date":todays_date
            })
            await session.commit()
            campaign_rules_logger.info(f"inserted camp code:{camp_code}, rule code:{rule_code} in table campaign_rule_tbl on:{todays_date}")
       
        else:
            rule_code_message=f'No active rule was found active for campaign {camp_code} but rule {rule_code} was made active for it'
            insert_query= text("""
                INSERT INTO campaign_rule_tbl (camp_code, rule_code, date_rule_created, is_active)
                VALUES (:camp_code, :rule_code, :todays_date, TRUE)
                    """
                )
            
            await session.execute(insert_query,{"camp_code":camp_code,"rule_code":rule_code,"todays_date":todays_date})
            #commit changes
            await session.commit()
            campaign_rules_logger.info(f"inserted camp code:{camp_code}, rule code:{rule_code} in table campaign_rule_tbl on:{todays_date}")  

        return AssignCampaignRuleResponse(message=rule_code_message,Success=True)
    
    except HTTPException:
        raise
    
    except Exception as e:
       campaign_rules_logger.exception(f"user with user id:{user.id} with email:{user.email} caused an exception:{e}")
       raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while assigning campaign:{rule.rule_code} to campaign:{rule.camp_code}")

#delete campaign rule by making the is_active field to false
async def delete_campaign_rule_db(rule_name:str,session:AsyncSession,user=Depends(get_current_active_user)):
    try:
        #find the campaign rule
        campaign_rule_query=select(rules_tbl.is_active).where(rules_tbl.rule_name==rule_name)
        campaign_rule=await session.exec(campaign_rule_query)
        result=campaign_rule.first()
        if not result:
            return False
        result=False
        session.add(result)
        await session.commit()
        return True
    except HTTPException:
        raise
    except Exception as e:
        campaign_rules_logger.exception(f"an exception occurred for user:{user.id} with email:{user.email} while deleting campaign:{rule_name}:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"an exception occurred for user:{user.id} with email:{user.email} while deleting campaign:{rule_name}")


#get all campaign rules
async def get_all_campaign_rules_db(page:int,page_size:int,session:AsyncSession,user)->GetAllCampaignRulesResponse:

    try:
        total=await session.scalar(select(func.count()).select_from(rules_tbl))

        offset=(page - 1)*page_size
        #always sort by the latest record created 
        sort_column=rules_tbl.created_at.desc()

        result=await session.exec(select(rules_tbl).order_by(sort_column).offset(offset).limit(page_size))
        
        results=result.all()

        campaign_rules=[]

        for r in results:
            
            data = r.rule_json or {}
            campaign_rules.append(
                GetCampaignRuleResponse(
                    rule_code=r.rule_code,
                    rule_name=r.rule_name,
                    is_active=r.is_active,
                    salary=extract_numeric_rule(data, "salary"),
                    derived_income=extract_numeric_rule(data, "derived_income"),
                    age=extract_numeric_rule(data, "age"),
                    gender=data.get("gender", {}).get("value") if data.get("gender") else None,
                    typedata=data.get("typedata", {}).get("value") if data.get("typedata") else None,
                    last_used=data.get("last_used", {}).get("value") if data.get("last_used") else None,
                    records_loaded=data.get("number_of_records", {}).get("value") if data.get("number_of_records") else None
                )
            )

        campaign_rules_logger.info(f"user:{user.id} with email:{user.email} retrieved records for campaign rules:{len(campaign_rules)}")
        
        return GetAllCampaignRulesResponse(total=total or 0,page=page,page_size=page_size,rules=campaign_rules)
    
    except Exception as e:
        campaign_rules_logger.exception(f"an exception occurred while fetching all campaign rules by user {user.id} with email:{user.email}:{str(e)}")

        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while fetching all campaign rules")
    



# safe list to prevent SQL injection
ALLOWED_SORT_FIELDS = {
    "rule_code": rules_tbl.rule_code,
    "rule_name": rules_tbl.rule_name,
    "created_at": rules_tbl.created_at,
    "is_active": rules_tbl.is_active,
}




#search for campaign rule
async def search_for_a_campaign_rule_db(page: int,page_size: int,session: AsyncSession,user,rule_name: Optional[str] = None,salary: Optional[int] = None,derived_income: Optional[int] = None,sort_by: str = "created_at",sort_order: str = "desc"):
    try:
        query = select(rules_tbl)
        if rule_name:
            query = query.where(rules_tbl.rule_name.ilike(f"%{rule_name}%"))

        if salary is not None:
            query = query.where(
                or_(
                    rules_tbl.rule_json["salary"]["value"].as_float() == salary,
                    rules_tbl.rule_json["salary"]["lower"].as_float() == salary,
                    rules_tbl.rule_json["salary"]["upper"].as_float() == salary,
                )
            )

        if derived_income is not None:
            query = query.where(
                or_(
                    rules_tbl.rule_json["derived_income"]["value"].as_float() == derived_income,
                    rules_tbl.rule_json["derived_income"]["lower"].as_float() == derived_income,
                    rules_tbl.rule_json["derived_income"]["upper"].as_float() == derived_income,
                )
            )

  
        sort_col = ALLOWED_SORT_FIELDS.get(sort_by)

        if not sort_col:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"sort_by must be one of {list(ALLOWED_SORT_FIELDS.keys())}")

        if sort_order.lower() == "asc":

            query = query.order_by(sort_col.asc())
        else:
            query = query.order_by(sort_col.desc())

        total = await session.scalar(select(func.count()).select_from(query.subquery()))
        offset = (page - 1) * page_size
        query = query.offset(offset).limit(page_size)
        result = await session.exec(query)
        rows = result.all()
        rules_list = []
        for r in rows:
            data = r.rule_json or {}

            rules_list.append(
                GetCampaignRuleResponse(
                    rule_code=r.rule_code,
                    rule_name=r.rule_name,
                    is_active=r.is_active,
                    salary=extract_numeric_rule(data, "salary"),
                    derived_income=extract_numeric_rule(data, "derived_income"),
                    age=extract_numeric_rule(data, "age"),
                    gender=data.get("gender", {}).get("value"),
                    typedata=data.get("typedata", {}).get("value"),
                    last_used=data.get("last_used", {}).get("value"),
                    records_loaded=data.get("number_of_records", {}).get("value")
                )
            )

        return GetAllCampaignRulesResponse(total=total or 0,page=page,page_size=page_size,rules=rules_list)

    except Exception as e:
        campaign_rules_logger.exception(f"error while searching rules by user {user.id}, email:{user.email}: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Internal server error while searching campaign rules")


#get rules within a salary range

async def update_salary_for_campaign_rule_db(rule_code:int,salary:UpdatingSalarySchema,session:AsyncSession,user):
    try:
        result=await session.exec(select(rules_tbl).where(rules_tbl.rule_code==rule_code))
        rule=result.one_or_none()
        if rule is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Campaign rule:{rule_code} does not exist")
        #move this thing when you are done
        campaign_rules_logger.info(f"user:{user.id} with email:{user.email} updated campaign rule:{rule_code}")
        salary_object=rule.rule_json.get("salary")
        if salary_object is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="Salary object could not be extracted from rules table")
        
        if salary_object['operator']=="between":

            if salary.lower_limit_salary >= salary.upper_limit_salary:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"upper limit:{salary.upper_limit_salary} should be greater than the lower limit:{salary.lower_limit_salary}")
            
            if salary.upper_limit_salary!=0:

                salary_object['upper']=salary.upper_limit_salary
            if salary.lower_limit_salary!=0:
                salary_object['lower']=salary.lower_limit_salary
            
        else:
            salary_object['value']=salary.salary
        rule.rule_json["salary"]=salary_object

        session.add(rule)
        await session.commit()
        await session.refresh(rule)
        return rule
    
    except HTTPException:
        raise
    except Exception as e:
        campaign_rules_logger.exception(f"an exception occurred while updating salary for campaign rule:{rule_code}:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while updating the salary for campaign rule:{rule_code}")



#update derived income 
async def update_derived_income_for_campaign_rule_db(rule_code:int,income:UpdatingDerivedIncomeSchema,session:AsyncSession,user):
    try:
        result=await session.exec(select(rules_tbl).where(rules_tbl.rule_code==rule_code))
        rule=result.one_or_none()
        if rule is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Campaign rule:{rule_code} does not exist")
        
        derived_income_json=rule.rule_json.get("derived_income")
        if derived_income_json is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="Salary object could not be extracted from rules table")
        
        if derived_income_json["operator"]=="between":

            if income.lower_limit_derived_income>=income.upper_limit_derived_income:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"upper limit:{income.upper_limit_derived_income} for derived income should be greater than lower limit:{income.lower_limit_derived_income}")
            
            if income.lower_limit_derived_income!=0:
                derived_income_json['upper']=income.lower_limit_derived_income
            if income.upper_limit_derived_income!=0:
                derived_income_json['lower']=income.upper_limit_derived_income
        else:
            derived_income_json["value"]=income.derived_income_value

        rule.rule_json['derived_income']=derived_income_json
        session.add(rule)
        await session.commit()
        await session.refresh(rule)
        campaign_rules_logger.info(f"user:{user.id} with email:{user.email} updated campaign rule:{rule_code}")
        return rule
    except HTTPException:
        raise
    except Exception as e:
        campaign_rules_logger.exception(f"an internal server error occurred while updating derived income:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while updating the derived income")


async def update_campaign_rule_age_db(rule_code:int,age_schema:UpdateAgeSchema,session:AsyncSession,user):
    try:
        result=await session.exec(select(rules_tbl).where(rules_tbl.rule_code==rule_code))
        rule=result.one_or_none()
        if rule is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Campaign rule:{rule_code} does not exist")
        age_json=rule.rule_json.get("age")

        if age_json is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"Age json field could not be extracted")
        
        if age_json["operator"]=="between":
            if age_schema.age_lower_limit>=age_schema.age_upper_limit:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"upper limit value:{age_schema.age_upper_limit} must be greater than the lower limit:{age_schema.age_lower_limit}")
            if age_schema.age_lower_limit!=0:
                age_json['lower']=age_schema.age_lower_limit
            if age_schema.age_upper_limit!=0:
                age_json['upper']=age_schema.age_upper_limit
        else:
            age_json['value']=age_schema.age_value
        
        rule.rule_json['age']=age_json
        session.add(rule)
        await session.commit()
        await session.refresh(rule)
        campaign_rules_logger.info(f"user:{user.id} with email:{user.email} update the age for campaign rule:{rule.rule_code}")
        return rule
    
    except HTTPException:
        raise
    except Exception as e:
        campaign_rules_logger.exception(f"An internal server error occurred while updating the campaign rule age:{str(e)}") 
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while updating the age")



#update the number of leads to load

async def update_number_of_leads_db(session: AsyncSession, rule_code: int, number_of_leads: int, user):
    try:
        rule_query=await session.exec(select(rules_tbl).where(rules_tbl.rule_code==rule_code))
        result=rule_query.one_or_none()
        if result==None:
            return None
        result.rule_json['number_of_records']={"value":number_of_leads}
        session.add(result)
        await session.commit()
        await session.refresh(result)
        return result.rule_json['number_of_records']['value']
    except Exception as e:
        campaign_rules_logger.exception(f"an exception occurred while updating total leads number")
        raise


#update campaign rules name

#update campaign rules status

async def fetch_campaign_code_from_campaign_tbl_db(camp_code:str,session:AsyncSession):

    print(f"the campaign code:{camp_code}")

    print("enter the first crud method")

    stmt_campaign = text("""
        SELECT camp_code 
        FROM campaign_tbl 
        WHERE camp_code = :camp_code
    """)
    campaign_query=await session.exec(select(campaign_tbl).where(campaign_tbl.camp_code==camp_code))
    campaign_result=campaign_query.first()
    if campaign_result==None:
        return None
    # params={"camp_code": camp_code}
    # result_campaign = await session.execute(stmt_campaign,params)
    # campaign = result_campaign.scalar_one_or_none()
    print("print what is returned from the database")
    print(campaign_result)
    return campaign_result.camp_code



async def fetch_rule_code_from_rules_tbl_and_campaign_rules_tbl_db(camp_code:str,session:AsyncSession):
        stmt_campaign=text("""
            SELECT r.rule_code
            FROM campaign_rule_tbl c
            JOIN rules_tbl r ON c.rule_code = r.rule_code
            WHERE c.camp_code = :camp_code
              AND c.is_active = TRUE
        """)
        result=await session.execute(stmt_campaign,{"camp_code":camp_code})
        rule_code=result.scalars().first()
        return rule_code
    

async def update_campaign_rule_and_insert_rule_code_db(
    rule_code: int,
    camp_code: str,
    session: AsyncSession
):
   
    todaysdate = datetime.utcnow().strftime('%Y-%m-%d')

    # deactivate_stmt = text("""
    #     UPDATE campaign_rule_tbl
    #     SET is_active = FALSE
    #     WHERE camp_code = :camp_code
    #       AND is_active = TRUE
    # """)
    

    # insert_stmt = text("""
    #     INSERT INTO campaign_rule_tbl (camp_code, rule_code, date_rule_created, is_active)
    #     VALUES (:camp_code, :rule_code, :date_rule_created, TRUE)
    #     RETURNING camp_code, rule_code, date_rule_created, is_active
    # """)



    try:
        # # Deactivate current active rule (if any)
        # await session.execute(deactivate_stmt, {"camp_code": camp_code})

        # # Insert the new active rule
        # result = await session.execute(
        #     insert_stmt,
        #     {
        #         "camp_code": camp_code,
        #         "rule_code": rule_code,
        #         "date_rule_created": todaysdate
        #     }
        # )

        # row=result.fetchone()
        # await session.commit()
        # # Return the newly inserted record
        print(f"print inside update AND insert rule code:{rule_code}")
        stmt_deactivate=(select(campaign_rule_tbl).where(campaign_rule_tbl.camp_code==camp_code,campaign_rule_tbl.is_active==True))
        result=await session.exec(stmt_deactivate)
        active_rules=result.all()
        for rule in active_rules:
            rule.is_active=False
            session.add(rule) # Mark for update


        new_rule=campaign_rule_tbl(camp_code=camp_code,rule_code=rule_code,is_active=True)
        session.add(new_rule)
        await session.commit()
        await session.refresh(new_rule)
        return new_rule.camp_code
    
    except IntegrityError as e:
        # Handles duplicate rule insertion attempts
        await session.rollback()
        raise ValueError(f"Duplicate entry: (camp_code={camp_code}, rule_code={rule_code}) must be unique.") from e

    except Exception as e:
        # Catch any other SQL/database error
        await session.rollback()
        raise


async def insert_new_campaign_rule_on_campaign_rule_tbl_db(camp_code:str,rule_code:int,session:AsyncSession):
    try:
        todaysdate = datetime.strptime("2025-12-08", "%Y-%m-%d").date()

        # insert_stmt = text("""
        #     INSERT INTO campaign_rule_tbl (camp_code, rule_code,is_active)
        #     VALUES (:camp_code, :rule_code,TRUE)
        #     RETURNING camp_code, rule_code, date_rule_created, is_active
        # """
        # )

        # params={
        #     "camp_code":camp_code,
        #     "rule_code":rule_code
        # }

        new_rule=campaign_rule_tbl(camp_code=camp_code,rule_code=rule_code,is_active=True)
        session.add(new_rule)
        await session.commit()
        await session.refresh(new_rule)
        # await session.execute(insert_stmt,params)
        # await session.commit()
        #return camp_code for confirmation
        return new_rule.camp_code
    
    
    except IntegrityError:
          raise ValueError(
            f"Duplicate entry: (camp_code={camp_code}, rule_code={rule_code})"
        )
    except Exception:
        campaign_rules_logger.exception(f"an exception occurred insert a new rule on campaign_rule_tbl")
        raise


async def remove_campaign_rule_db(rule_code:int,session:AsyncSession,user)->DeleteCampaignRuleResponse:
    try:
        stmt_rule=select(rules_tbl).where(rules_tbl.rule_code==rule_code)
        result_obj=await session.exec(stmt_rule)
        result=result_obj.one_or_none()
        if not result:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"No rule found with rule_code:{rule_code}")
        if result.is_active==True:
            print("print the test before raising an exception")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"The request rule cannot be deleted since it is still active and potentially assigned to a campaign")
        await session.delete(result)
        await session.commit()
        campaign_rules_logger.info(f"user:{user.id} with email {user.email} deleted rule with rule code:{rule_code}")
        return DeleteCampaignRuleResponse(message=f"all resources with with rule code:{rule_code} have been deleted from the database",success=True)
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        campaign_rules_logger.exception(f"an exception occurred while deleting resource with rule code:{rule_code}:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"Database error while deleting rule with rule code:{rule_code}")
    except Exception:
        campaign_rules_logger.exception("an exception occurred while deleting a rule")
        raise



async def total_campaign_rules_db(session:AsyncSession,user)->CampaignRulesTotal:
    try:
        result=await session.exec(select(func.count()).select_from(rules_tbl))
        campaign_rules_total=result.one()
        campaign_rules_logger.info(f"user:{user.id} with email:{user.email} retrieved a total of {campaign_rules_total} rules")
        
        return CampaignRulesTotal(total_number_of_rules=campaign_rules_total)
    except Exception as e:
        campaign_rules_logger.exception(f"an exception occurred while retrieving campaign rules total:{e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while fetching the total number of campaign rules on the system")