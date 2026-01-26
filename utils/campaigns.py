from models.rules_table import new_rules_tbl
from models.information_table import info_tbl
from sqlalchemy import text
from datetime import date
from typing import Dict,Any,Tuple,Optional
from textwrap import dedent


def build_dynamic_query(rule:new_rules_tbl)->tuple[str,dict]:

    base_select_clause="""
        SELECT id,fore_name,last_name,cell 
        FROM info_tbl
        WHERE 1=1
    """
    params={}
    conditions=[]
    #Salary condition

    if rule.minimum_salary is not None:
        conditions.append("(salary>=:min_salary OR salary IS NULL)")
        params["min_salary"]=rule.minimum_salary
    
    #conditions.append("typedata='Status'")
    #last_used condition
    #rule.typedata
    typedata=rule.typedata

    if rule.typedata is None:
        conditions.append("(typedate=:typedata)")
        params['typedata']=typedata
    
    # 3. last_used
    if rule.last_used is not None:
        conditions.append(
            "(last_used IS NULL OR "
            "DATE_PART('day', NOW()::timestamp - last_used::timestamp) > :last_used_days)"
        )
        params["last_used_days"] = rule.last_used

    # Start birth year range
    if rule.birth_year_start is not None:
        yy=rule.birth_year_start % 100
        conditions.append("CAST(SUBSTRING(id, 1, 2) AS INTEGER) >= :birth_year_start")
        params["birth_year_start"]=yy

    # End birth year upper range 
    if rule.birth_year_end is not None:
        yy=rule.birth_year_end % 100
        conditions.append("CAST(SUBSTRING(id, 1, 2) AS INTEGER) <= :birth_year_end")
        params["birth_year_end"]=yy
    
    # Dynamic Limit
    limit_clause=""

    if rule.number_of_records is not None:

        if rule.number_of_records<=0:
            return None
        limit_clause="LIMIT:limit_count"

        params["limit_count"]=rule.number_of_records

    else:
        limit_clause="LIMIT 2500"
        params["limit_count"]=2500
    #join all the conditions using the AND operator
    #  
    where_clause="AND".join(f"({c})" for c in conditions)

    final_query=f"""
            {base_select_clause}
            AND {where_clause}
            ORDER BY random()
            {limit_clause}
        """.strip()
    
    return final_query,params

def build_dynamic_dedupe_main_query(campaign_name:str,status:str,limit:int)->tuple[str,dict]:
   
    #basic query for deduping
    params={}
    dedupe_query=text(
        """
        SELECT i.id,i.fore_name,i.last_name,i.cell 
        FROM info_tbl i
        WHERE EXISTS(
            SELECT 1
            FROM campaign_dedupe c
            WHERE c.cell = i.cell
            AND c.campaign_name=:campaign_name
            AND c.status =:status
        )
        LIMIT :limit
        """
    )

    params["campaign_name"]=campaign_name
    params["status"]=status
    params["limit"]=limit

    # params:Dict[str,Any]={
    #     "campaign_name":campaign_name,
    #     "status":status,
    #     "limit":limit
    # }

    return dedupe_query,params

#dynamic query that involve information table and the finance table

def build_dynamic_query_finance_tbl(rule:new_rules_tbl)->tuple[str,dict]:

    #First where statement
    params={}
    base_where = dedent("""
        ping_status IS NOT NULL
        AND typedata = :typedata
    """).strip()
    params['typedata']=rule.typedata

    second_where = dedent("""
        (salary >= :min_salary
         OR (salary IS NULL AND status = :status))
        AND (last_used IS NULL
             OR DATE_PART('day', now()::timestamp - last_used::timestamp) > :last_used_days)
        AND CAST(SUBSTRING(id,1,2) AS INTEGER) BETWEEN :age_min AND :age_max
    """).strip()
    params['salary']=rule.minimum_salary
    params['status']=rule.status
    params['last_used_days']=rule.last_used
    params['age_max']=rule.birth_year_end % 100
    params['age_min']=rule.birth_year_start % 100

    gender_filter = ""

    if rule.gender is not None:
        gender_filter = "AND gender = :gender"
        params['gender']=rule.gender

    
    sql = dedent(f"""
        (
            SELECT id, fore_name, last_name, cell
            FROM   info_tbl
            WHERE  {base_where}
            ORDER BY random()
            LIMIT :limit
        )
        UNION
        (
            SELECT id, fore_name, last_name, cell
            FROM   info_tbl
            WHERE  {second_where}
                   {gender_filter}
            ORDER BY random()
            LIMIT :limit
        )
    """).strip()

    params_dict = {
        "age_min": params["age_min"],
        "age_max": params["age_max"],
        "last_used_days": params["last_used_days"],
        "limit": params["limit_count"],
        "min_salary": params["min_salary"],
        "status": params["status"],
        "typedata":params['typedata']
    }

    if params.get("gender"):
        params_dict["gender"] = params["gender"]
    
    return sql,params_dict



#dynamically loads campaign query builder

def load_campaign_query_builder(rule:new_rules_tbl):
    #where conditions to filters leads fetched
    conditions=[]
    #filtering parameters
    params={}

    if rule.minimum_salary is not None:
        conditions.append("(salary >= :salary OR salary IS NULL)")
        params["salary"]=rule.minimum_salary
    

    if rule.typedata is not None:
        conditions.append("typedata = :typedata")
        params["typedata"]=rule.typedata
    
    
    if rule.last_used is not None:
        conditions.append(  "(last_used IS NULL OR DATE_PART('day', now()::timestamp - last_used::timestamp) > :last_used_days)")
        params["last_used"]=rule.last_used
    
    else:
         
         conditions.append(
            "(last_used IS NULL OR DATE_PART('day', now()::timestamp - last_used::timestamp) > 29)"
        )
    
    #Age Calculation

    age_clause = """
        DATE_PART('year', age(
            TO_DATE(
                CASE 
                    WHEN CAST(SUBSTRING(id, 1, 2) AS INTEGER) > CAST(TO_CHAR(NOW(), 'YY') AS INTEGER)
                    THEN '19' || SUBSTRING(id, 1, 6)
                    ELSE '20' || SUBSTRING(id, 1, 6)
                END, 'YYYYMMDD')
        )) BETWEEN :age_lower AND :age_upper
    """


    conditions.append(age_clause)

    params["age_lower"]=rule.age_lower_limit
    params["age_upper"]=rule.age_upper_limit

    # params["age_lower"]=date.today().year - rule.birth_year_end
    # params["age_upper"]=date.today().year - rule.birth_year_start

    #where clause 
    where_clause="AND".join(conditions)


    load_sql_query = text(f"""
        SELECT 
            id,
            fore_name,
            last_name,
            cell,
            DATE_PART('year', age(
                TO_DATE(
                    CASE 
                        WHEN CAST(SUBSTRING(id, 1, 2) AS INTEGER) > CAST(TO_CHAR(NOW(), 'YY') AS INTEGER)
                        THEN '19' || SUBSTRING(id, 1, 6)
                        ELSE '20' || SUBSTRING(id, 1, 6)
                    END, 'YYYYMMDD')
            )) AS age
        FROM info_tbl
        WHERE {where_clause}
        ORDER BY random()
        LIMIT :limit
    """)


    params["limit"]=rule.number_of_records

    return load_sql_query,params
