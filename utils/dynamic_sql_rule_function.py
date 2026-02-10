from __future__ import annotations
from schemas.rules_schema import RuleSchema
from sqlalchemy.sql import text
from datetime import timedelta
import re
from typing import Optional
from typing import Optional,Any,List,Dict
from sqlmodel.ext.asyncio.session import AsyncSession
from utils.logger import define_logger

legacy_rules_table_logger=define_logger("als_legacy_rules_table","logs/legacy_rules_table_logs")

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



def build_dynamic_rule_engine(rule: dict,rule_name:Optional[str]=None):
    # LEFT ANTI JOIN: keep only info_tbl rows that do NOT match gdnc on cell
    print("enter the left join method")
    is_deduped=rule.get('is_deduped',False)
    
    if not is_deduped:

        SQL = """
        SELECT i.id,i.fore_name, i.last_name, i.cell
        FROM info_tbl i
        LEFT JOIN global_dnc_numbers gdnc
        ON i.cell = gdnc.cell
        WHERE 1=1
        AND gdnc.cell IS NULL
        """
        params = {}

    # Salary rules 
        salary_rule = rule.get('salary', {})
        salary_op = salary_rule.get('operator')
        salary_conditions = ["i.salary IS NULL"]

        if salary_op == "between":
            lower = salary_rule.get('lower')
            upper = salary_rule.get('upper')
            if lower is not None and upper is not None:
                salary_conditions.append("i.salary BETWEEN :salary_lower AND :salary_upper")
                params["salary_lower"] = lower
                params["salary_upper"] = upper
        
        else:
            value = salary_rule.get('value')
            if value is not None:
                op_map = {
                    "equal": "=", "not_equal": "!=", "less_than": "<",
                    "less_than_equal": "<=", "greater_than": ">", "greater_than_equal": ">="
                }
                salary_conditions.append(f"i.salary {op_map.get(salary_op, '=')} :salary_value")
                params["salary_value"] = value

        SQL += " AND (" + " OR ".join(salary_conditions) + ")"

        # Derived income rules
        if 'derived_income' in rule:
            income_rule = rule["derived_income"]
            income_op = income_rule.get("operator")

            if income_op == "between":
                lower = income_rule.get("lower")
                upper = income_rule.get("upper")
                if lower is not None and upper is not None:
                    SQL += " AND (i.derived_income BETWEEN :income_lower AND :income_upper)"
                    params["income_lower"] = lower
                    params["income_upper"] = upper
            elif income_rule.get("value") not in (None, 0.0):
                value = income_rule.get("value")
                if value is not None:
                    op_map = {
                        "equal": "=", "not_equal": "!=", "less_than": "<",
                        "less_than_equal": "<=", "greater_than": ">", "greater_than_equal": ">="
                    }
                    SQL += f" AND (i.derived_income {op_map.get(income_op, '=')} :income_value)"
                    params["income_value"] = value

        # Gender rules 
        if 'gender' in rule:
            gender_rule = rule["gender"]
            gender_value = gender_rule.get("value")
            if gender_value not in (None, "", "NULL"):
                gender_str = str(gender_value).strip().upper()
                if gender_str != "BOTH":
                    op_map = {"equal": "=", "not_equal": "!="}
                    op = op_map.get(gender_rule.get("operator"), "=")
                    SQL += f" AND (i.gender {op} :gender_value)"
                    params["gender_value"] = str(gender_value).strip()

        # Last used
        last_used_value = rule.get("last_used", {}).get("value")

        if last_used_value is not None:
            SQL += " AND (i.last_used IS NULL OR DATE_PART('day', NOW() - i.last_used) > :last_used)"
            params["last_used"] = last_used_value

        # Age rules
        age_rule = rule.get("age", {})
        age_op = age_rule.get("operator")

        if age_op and (
            age_rule.get("value") is not None or
            (age_rule.get("lower") is not None and age_rule.get("upper") is not None)
        ):
            yy = "CAST(SUBSTRING(i.id, 1, 2) AS INTEGER)"
            mm = "CAST(SUBSTRING(i.id, 3, 2) AS INTEGER)"
            dd = "CAST(SUBSTRING(i.id, 5, 2) AS INTEGER)"
            current_yy = "EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER % 100"

            birth_year = f"""
                CASE 
                    WHEN {yy} <= {current_yy} THEN 2000 + {yy}
                    ELSE 1900 + {yy}
                END
            """

            is_leap = f"({birth_year} % 4 = 0 AND ({birth_year} % 100 != 0 OR {birth_year} % 400 = 0))"

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
                    op = op_map.get(age_op, "=")
                    SQL += f" AND ({sa_id_valid} AND {age_expr} {op} :age_value)"
                    params["age_value"] = value

        # Limit
        num_records = rule.get("number_of_records", {}).get("value")

        if num_records is not None:
            SQL += " LIMIT :number_of_records"
            params["number_of_records"] = num_records

        return text(SQL), params
    #search leads for a dedupe campaign
    else:

        SQL="""
        SELECT i.info_pk,i.id, i.fore_name, i.last_name, i.cell
        FROM info_tbl i
        JOIN campaign_dedupe c
        ON i.cell=c.cell
        WHERE c.status="R"
        AND c.campaign_name=:campaign_name
        LIMIT 5000
        """
        
        print("print the final sql query")
        print(SQL)
        params={"campaign_name":rule_name}

        return text(SQL),params
    



def remove_order_by_random(sql) -> str:
    """
    Remove ORDER BY RANDOM() (and RANDOM() within ORDER BY lists)
    while preserving WHERE, LIMIT, OFFSET, etc.
    """

    print("enter the remove order by random helper")

    # Handle SQLAlchemy Row / tuple
    if hasattr(sql, "__getitem__") and not isinstance(sql, str):
        sql = sql[0]

    if not sql or not sql.strip():
        return sql

    s = sql.strip().rstrip(";").strip()

    # Remove ORDER BY RANDOM()
    s = re.sub(
        r"""
        \border\s+by\s+random\s*\(\s*\)
        \s*
        (?=\blimit\b|\boffset\b|\bfetch\b|\bfor\b|\)|$)
        """,
        "",
        s,
        flags=re.IGNORECASE | re.VERBOSE,
    )

    # RANDOM() first in ORDER BY list
    s = re.sub(
        r"\border\s+by\s+random\s*\(\s*\)\s*,\s*",
        "ORDER BY ",
        s,
        flags=re.IGNORECASE,
    )

    # RANDOM() later in list
    s = re.sub(
        r"\s*,\s*random\s*\(\s*\)",
        "",
        s,
        flags=re.IGNORECASE,
    )

    # Remove dangling ORDER BY
    s = re.sub(
        r"\border\s+by\s*(?=\blimit\b|\boffset\b|\bfetch\b|\bfor\b|\)|$)",
        "",
        s,
        flags=re.IGNORECASE,
    )

    s = re.sub(r"[ \t]+", " ", s).strip()

    return s


SQL_KEYWORDS = {
    "where", "join", "left", "right", "inner", "outer", "cross", "full",
    "on", "group", "order", "limit", "offset", "fetch", "for", "having",
    "union", "intersect", "except"
}

def ensure_info_pk_selected(
    base_sql: str,
    *,
    table: str = "info_tbl",
    pk_col: str = "info_pk",
) -> str:
    """
    Ensure info_pk (or alias.info_pk) is in the SELECT list when selecting from `table`.

    Fixes the bug where `WHERE` was incorrectly captured as an alias.
    """

    if not base_sql or not base_sql.strip():
        return base_sql

    sql = base_sql.strip()

    # Match: FROM info_tbl
    # Optionally: FROM info_tbl i
    # Optionally: FROM info_tbl AS i
    # But do NOT treat SQL keywords (WHERE/JOIN/ORDER/...) as aliases.
    m = re.search(
        rf"""
        \bfrom\s+{re.escape(table)}\b
        (?:\s+(?:as\s+)?(?P<alias>[a-zA-Z_]\w*))?
        """,
        sql,
        flags=re.IGNORECASE | re.VERBOSE,
    )

    if not m:
        return sql

    alias = m.group("alias")
    if alias and alias.lower() in SQL_KEYWORDS:
        alias = None  # keyword accidentally captured -> treat as no alias

    pk_ref = f"{alias}.{pk_col}" if alias else pk_col

    # Already selected?
    if re.search(rf"\b{re.escape(pk_ref)}\b", sql, flags=re.IGNORECASE) or \
       re.search(rf"\b{re.escape(pk_col)}\b", sql, flags=re.IGNORECASE):
        return sql

    # Inject right after SELECT / SELECT DISTINCT
    def _inject(match: re.Match) -> str:
        return f"{match.group(0)}{pk_ref}, "

    return re.sub(
        r"(?i)\bselect\s+(?:distinct\s+)?",
        _inject,
        sql,
        count=1,
    )





def build_left_anti_join_sql(base_sql: str,*,dnc_table: str = "global_dnc_numbers",join_col: str = "cell",left_alias: str = "l",dnc_alias: str = "dnc") -> str:
    """
    Returns a SQL string:
      SELECT l.*
      FROM ( <base_sql with ORDER BY RANDOM removed> ) AS l
      LEFT JOIN global_dnc_numbers AS dnc ON l.cell = dnc.cell
      WHERE dnc.cell IS NULL
    """

    print("enter the build left join method")


    if not base_sql or not base_sql.strip():

        raise ValueError("base_sql is empty")

    cleaned = remove_order_by_random(base_sql.strip())

    return f"""
    SELECT {left_alias}.*
    FROM (
        {cleaned}
    ) AS {left_alias}
    LEFT JOIN {dnc_table} AS {dnc_alias}
        ON {left_alias}.{join_col} = {dnc_alias}.{join_col}
    WHERE {dnc_alias}.{join_col} IS NULL
    """.strip()




async def fetch_rule_sql(session: AsyncSession, rule_name: str) -> str:
    """
    Fetch the rule_sql string for a given rule_name from rules_tbl.
    """
    q = text("""
        SELECT rule_sql
        FROM rules_tbl
        WHERE rule_name = :rule_name
        LIMIT 1
    """)

    print("enter the fetch rule helper")

    res = await session.execute(q, {"rule_name": rule_name})
    rule_sql = res.scalar_one_or_none()

    if rule_sql is None or not str(rule_sql).strip():
        legacy_rules_table_logger.info(f"legacy table exception")
        return None
    
    return str(rule_sql)


def fix_typedata_double_quotes(sql: str) -> str:
    """
    Replace typedata = "VALUE" with typedata = 'VALUE'
    to avoid PostgreSQL interpreting "VALUE" as a column.
    """

    if not sql or not isinstance(sql, str):
        return sql

    return re.sub(
        r'(typedata\s*=\s*)"([^"]+)"',
        r"\1'\2'",
        sql,
        flags=re.IGNORECASE,
    )



def replace_double_quotes_with_single(sql: str) -> str:
    """
    Replace double-quoted string literals with single-quoted ones.
    Example:
        typedata = "Status"  -> typedata = 'Status'
    """

    if not sql or not isinstance(sql, str):
        return sql

    return re.sub(
        r'"([^"]*)"',
        r"'\1'",
        sql
    )


async def execute_built_sql_query(session:AsyncSession,sql:str,params:Dict[str,Any]|None=None)->List[Dict[str,Any]]:
    """
    Execute the built sql query to return leads from the legacy table
    """
    print("enter the where the rule is executed")
    stmt=text(sql)
    result = await session.execute(stmt, params or {})
    rows = result.mappings().all()  
    return [dict(row) for row in rows]



def build_dynamic_rule_query_for_legacy_rules_table(base_query:str)->str:

    sql=base_query.strip().rstrip(";")
    limit_match=re.search(r"\bLIMIT\s+(\d+)\b",sql,flags=re.IGNORECASE)
    if not limit_match:
        raise ValueError("base_sql must contain a LIMIT clause")
    limit_value=int(limit_match.group(1))
    #remove ORDER BY RANDOM() if present
    sql=re.sub(r"\bORDER\s+BY\s+RANDOM\s*\(\s*\)\s*", " ",sql,flags=re.IGNORECASE)

    # Remove original LIMIT clause
    sql=re.sub(r"\bLIMIT\s+\d+\b", " ", sql, flags=re.IGNORECASE)

    #inject LEFT JOIN dnc table after from info_tbl i
    from_pattern = r"\bFROM\s+info_tbl\s+i\b"
    if not re.search(from_pattern, sql, flags=re.IGNORECASE):
        raise ValueError("Expected 'FROM info_tbl i' in base_sql")
    
    sql = re.sub(from_pattern,"FROM info_tbl i\nLEFT JOIN global_table g ON g.cell = i.cell",sql,flags=re.IGNORECASE,count=1)
    # add anti-join condition
    if re.search(r"\bWHERE\b", sql, flags=re.IGNORECASE):
        sql = re.sub(r"\bWHERE\b","WHERE g.cell IS NULL AND",sql,flags=re.IGNORECASE,count=1)

    else:
        sql = sql.strip() + "\nWHERE g.cell IS NULL"
    # cleanup whitespaces and re-append LIMIT at the end of the sqll string
    sql = re.sub(r"\s{2,}", " ", sql).strip()

    sql = f"{sql}\nLIMIT {limit_value};"

    return sql

