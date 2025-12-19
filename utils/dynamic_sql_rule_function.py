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



def build_dynamic_rule_engine(rule: dict):

    SQL = "SELECT id, fore_name, last_name, cell FROM info_tbl WHERE 1=1"
    params = {}
    salary_rule = rule.get('salary', {})
    salary_op = salary_rule.get('operator')
    salary_conditions = ["salary IS NULL"]

    if salary_op == "between":
        lower = salary_rule.get('lower')
        upper = salary_rule.get('upper')
        if lower is not None and upper is not None:
            salary_conditions.append("salary BETWEEN :salary_lower AND :salary_upper")
            params["salary_lower"] = lower
            params["salary_upper"] = upper
    else:
        value = salary_rule.get('value')
        if value is not None:
            op_map = {
                "equal": "=", "not_equal": "!=", "less_than": "<",
                "less_than_equal": "<=", "greater_than": ">", "greater_than_equal": ">="
            }
            salary_conditions.append(f"salary {op_map.get(salary_op, '=')} :salary_value")
            params["salary_value"] = value

    SQL += " AND (" + " OR ".join(salary_conditions) + ")"

   
    if 'derived_income' in rule:
        income_rule = rule["derived_income"]
        income_op = income_rule.get("operator")

        if income_op == "between":
            lower = income_rule.get("lower")
            upper = income_rule.get("upper")
            if lower is not None and upper is not None:
                SQL += " AND (derived_income BETWEEN :income_lower AND :income_upper)"
                params["income_lower"] = lower
                params["income_upper"] = upper
        elif income_rule.get("value") not in (None, 0.0):
            value = income_rule.get("value")
            if value is not None:
                op_map = {
                    "equal": "=", "not_equal": "!=", "less_than": "<",
                    "less_than_equal": "<=", "greater_than": ">", "greater_than_equal": ">="
                }
                SQL += f" AND (derived_income {op_map.get(income_op, '=')} :income_value)"
                params["income_value"] = value

  
    if 'gender' in rule:
        gender_rule = rule["gender"]
        gender_value = gender_rule.get("value")
        if gender_value not in (None, "", "NULL"):
            gender_str = str(gender_value).strip().upper()
            if gender_str != "BOTH":
                op_map = {"equal": "=", "not_equal": "!="}
                op = op_map.get(gender_rule.get("operator"), "=")
                SQL += f" AND (gender {op} :gender_value)"
                params["gender_value"] = str(gender_value).strip()

  

    last_used_value = rule.get("last_used", {}).get("value")
    if last_used_value is not None:
        SQL += " AND (last_used IS NULL OR DATE_PART('day', NOW() - last_used) > :last_used)"
        params["last_used"] = last_used_value

   
    age_rule = rule.get("age", {})
    age_op = age_rule.get("operator")

    if age_op and (
        age_rule.get("value") is not None or
        (age_rule.get("lower") is not None and age_rule.get("upper") is not None)
    ):
        yy = "CAST(SUBSTRING(id, 1, 2) AS INTEGER)"
        mm = "CAST(SUBSTRING(id, 3, 2) AS INTEGER)"
        dd = "CAST(SUBSTRING(id, 5, 2) AS INTEGER)"
        current_yy = "EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER % 100"

        birth_year = f"""
            CASE 
                WHEN {yy} <= {current_yy} THEN 2000 + {yy}
                ELSE 1900 + {yy}
            END
        """

        is_leap = f"""
            ({birth_year} % 4 = 0 AND ({birth_year} % 100 != 0 OR {birth_year} % 400 = 0))
        """

        max_days = f"""
            CASE {mm}
                WHEN 1 THEN 31 WHEN 3 THEN 31 WHEN 5 THEN 31 WHEN 7 THEN 31
                WHEN 8 THEN 31 WHEN 10 THEN 31 WHEN 12 THEN 31
                WHEN 4 THEN 30 WHEN 6 THEN 30 WHEN 9 THEN 30 WHEN 11 THEN 30
                WHEN 2 THEN CASE WHEN {is_leap} THEN 29 ELSE 28 END
                ELSE 0
            END
        """

        sa_id_valid = f"{mm} BETWEEN 1 AND 12 AND {dd} BETWEEN 1 AND {max_days}"

        age_expr = f"""
            EXTRACT(YEAR FROM AGE(
                CURRENT_DATE,
                MAKE_DATE({birth_year}, {mm}, {dd})
            ))
        """

       

        if age_op == "between":
            lower = age_rule.get("lower")
            upper = age_rule.get("upper")
            if lower is not None and upper is not None:
                SQL += f" AND ({sa_id_valid} AND {age_expr} BETWEEN :age_lower AND :age_upper)"
                params["age_lower"] = lower
                params["age_upper"] = upper
        else:
            value = age_rule.get("value")
            if value is not None:
                op_map = {
                    "equal": "=", "less_than": "<", "less_than_equal": "<=",
                    "greater_than": ">", "greater_than_equal": ">="
                }
                op = op_map.get(age_op, '=')
                SQL += f" AND ({sa_id_valid} AND {age_expr} {op} :age_value)"
                params["age_value"] = value
    
    num_records = rule.get("number_of_records", {}).get("value")
    if num_records is not None:
        SQL += " LIMIT :number_of_records"
        params["number_of_records"] = num_records

   
    return text(SQL), params

