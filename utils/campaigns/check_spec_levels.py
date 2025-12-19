from schemas.rules_schema import RuleSchema
from sqlalchemy.sql import text
from datetime import timedelta

def apply_numeric_condition(sql_field_name:str,rule_obj,sql:str,params:dict):
    
    op_map = {
        "equal": "=",
        "not_equal": "!=",
        "less_than": "<",
        "less_than_equal": "<=",
        "greater_than": ">",
        "greater_than_equal": ">="
    }
    #BETWEEN

    if rule_obj.operator =="between".lower():

        sql += f" AND {sql_field_name} BETWEEN :{sql_field_name}_lower AND :{sql_field_name}_upper"
        params[f"{sql_field_name}_lower"] = rule_obj.lower
        params[f"{sql_field_name}_upper"] = rule_obj.upper
        return sql, params
    
    #single value operator

    sql += f" AND {sql_field_name} {op_map[rule_obj.operator]} :{sql_field_name}_value"
    params[f"{sql_field_name}_value"] = rule_obj.value
    return sql, params


def spec_level_query_builder(rule: RuleSchema):

    sql = """
    SELECT id, fore_name, last_name, cell
    FROM info_tbl
    WHERE 1=1

    """
    
    params = {}
    # SALARY (always include IS NULL)
    salary_rule = rule['salary']
    salary_op = salary_rule['operator']
    salary_conditions = ["salary IS NULL"]

    if salary_op == "between":

        salary_conditions.append("salary BETWEEN :salary_lower AND :salary_upper")

        params["salary_lower"] = salary_rule['lower']
        params["salary_upper"] = salary_rule['upper']
    else:
        op_map = {
            "equal": "=",
            "not_equal": "!=",
            "less_than": "<",
            "less_than_equal": "<=",
            "greater_than": ">",
            "greater_than_equal": ">="
        }
        salary_conditions.append(f"salary {op_map[salary_op]} :salary_value")

        params["salary_value"] = salary_rule['value']

    sql += " AND (" + " OR ".join(salary_conditions) + ")"

    # GENDER optional

    if rule['gender']['value']!="NULL":

        op_map_gender = {"equal": "=", "not_equal": "!="}
        gender_op = op_map_gender[rule['gender']['operator']]
        sql += f" AND (gender {gender_op} :gender_value)"
        params["gender_value"] = rule['gender']['value']
    
    # TYPEDATA
    typedata_rule = rule["typedata"]

    typedata_value=rule["typedata"]["value"]

    op_map_typedata = {"equal": "=", "not_equal": "!="}
    typedata_op = op_map_typedata[typedata_rule["operator"]]

    sql += f" AND (typedata {typedata_op} :typedata_value)"

    params["typedata_value"]=typedata_value

    # # DERIVED INCOME

    if  rule["derived_income"]['value']!=0.0 or rule['derived_income']['upper']!=0.0 or rule['derived_income']['lower']!=0.0:

        income_rule = rule["derived_income"]
        
        income_op = income_rule["operator"]
        if income_op == "between":
            sql += " AND (derived_income BETWEEN :income_lower AND :income_upper)"
            params["income_lower"] = income_rule["lower"]
            params["income_upper"] = income_rule["upper"]

        else:
            op_map_income = {"equal": "=", "not_equal": "!=","less_than": "<","less_than_equal": "<=","greater_than": ">","greater_than_equal": ">="}
            sql += f" AND (derived_income {op_map_income[income_op]} :income_value)"
            print("print the income value from the sql query builder")
            print(income_rule['value'])
            params["income_value"] = income_rule["value"]


    # #AGE calculated from SA ID number
    if rule["age"]["value"] is not None or rule["age"]["lower"] is not None or rule["age"]["upper"] is not None:

        age_rule = rule["age"]
        age_op = age_rule["operator"]
        # Base age calculation with SA ID validation
          # Expression to calculate age from SA ID
        # age_expr = f"""
        # EXTRACT(YEAR FROM AGE(
        #     CURRENT_DATE,
        #     MAKE_DATE(
        #         CASE
        #             WHEN CAST(SUBSTRING(id,1,2) AS INT) <= CAST(TO_CHAR(CURRENT_DATE,'YY') AS INT)
        #                 THEN 2000 + CAST(SUBSTRING(id,1,2) AS INT)
        #             ELSE 1900 + CAST(SUBSTRING(id,1,2) AS INT)
        #         END,
        #         CAST(SUBSTRING(id,3,2) AS INT),
        #         CAST(SUBSTRING(id,5,2) AS INT)
        #     )
        # ))
        # """

        age_expr = f"""
                EXTRACT(YEAR FROM AGE(
                    CURRENT_DATE,
                    MAKE_DATE(
                        CASE
                            WHEN CAST(SUBSTRING(id,1,2) AS INT) <= CAST(TO_CHAR(CURRENT_DATE,'YY') AS INT)
                                THEN 2000 + CAST(SUBSTRING(id,1,2) AS INT)
                            ELSE 1900 + CAST(SUBSTRING(id,1,2) AS INT)
                        END,
                        CAST(SUBSTRING(id,3,2) AS INT),
                        LEAST(CAST(SUBSTRING(id,5,2) AS INT),
                              CASE CAST(SUBSTRING(id,3,2) AS INT)
                                  WHEN 1 THEN 31
                                  WHEN 2 THEN CASE WHEN EXTRACT(YEAR FROM CURRENT_DATE)::int % 4 = 0 THEN 29 ELSE 28 END
                                  WHEN 3 THEN 31
                                  WHEN 4 THEN 30
                                  WHEN 5 THEN 31
                                  WHEN 6 THEN 30
                                  WHEN 7 THEN 31
                                  WHEN 8 THEN 31
                                  WHEN 9 THEN 30
                                  WHEN 10 THEN 31
                                  WHEN 11 THEN 30
                                  WHEN 12 THEN 31
                              END
                        )
                    )
                ))
        """
        
        # Always validate month/day of SA ID
        #sa_id_valid = "(SUBSTRING(id,3,2)::int BETWEEN 1 AND 12 AND SUBSTRING(id,5,2)::int BETWEEN 1 AND 31)"

        # Wrap age_expr with month/day validation

        # op_map_age = {
        #     "equal": "=",
        #     "less_than": "<",
        #     "less_than_equal": "<=",
        #     "greater_than": ">",
        #     "greater_than_equal": ">=",
        #     "between": "BETWEEN"
        # }
    
    sa_id_valid = "(SUBSTRING(id,3,2)::int BETWEEN 1 AND 12 AND SUBSTRING(id,5,2)::int BETWEEN 1 AND 31)"

    op_map_age = {
        "equal": "=",
        "less_than": "<",
        "less_than_equal": "<=",
        "greater_than": ">",
        "greater_than_equal": ">=",
        "between": "BETWEEN"
    }


    if age_op == "between":

        sql += f" AND ({sa_id_valid} AND ({age_expr} BETWEEN :age_lower AND :age_upper))"
        params["age_lower"] = age_rule["lower"]
        params["age_upper"] = age_rule["upper"]
    
    else:
        sql += f" AND ({sa_id_valid} AND ({age_expr} {op_map_age[age_op]} :age_value))"
        params["age_value"] = age_rule["value"]
        
    # # date last used records
    if rule["last_used"]["value"] is not None:
        last_used_days = rule["last_used"]["value"]
        sql += " AND (last_used IS NULL OR DATE_PART('day', now() - last_used) > :last_used)"
        params["last_used"] = last_used_days

    print("print the final query")
    print(sql)

    return text(sql), params


