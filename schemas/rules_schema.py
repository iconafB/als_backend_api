from fastapi import HTTPException,status
from typing import Optional,Any,Literal,Union
from pydantic import BaseModel,model_validator,Field as PydField
from enum import Enum


class Operator(str,Enum):
    equal="equal"
    not_equal="not_equal"
    less_than="less_than"
    greater_than="greater_than"
    between="between"


 #lower' and 'upper' must not be provided for operator 'equal'

class NumericCondition(BaseModel):

    operator: Literal[
        "equal", "not_equal",
        "less_than", "less_than_equal",
        "greater_than", "greater_than_equal",
        "between"
    ]="equal"
    
    value: Optional[float] = None
    lower: Optional[float] = None
    upper: Optional[float] = None

    @model_validator(mode="before")
    def validate_numeric_condition(cls, values):

        op = values.get("operator")
        value = values.get("value")
        lower = values.get("lower")
        upper = values.get("upper")

        if op == "between":
            if lower is None or upper is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f"Operator requires both lower and upper values")
            if upper < lower:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail="upper limit value must be greater than lower")
            if value!=0 and op == 'between':
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail="'value' must not be provided when using 'between'.")
            
        elif op in ["less_than","less_than_equal"]:
            if upper is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f"Operator '{op}' requires 'upper'")
            
        elif op in ["greater_than", "greater_than_equal"]:
            if lower is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f"Operator '{op}' requires 'lower'")
        else:
            if value is None:
               raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f"Unknown operator '{op}'")
        return values


#checked

class LastUsedCondition(BaseModel):
    operator:Literal["less_than","less_than_equal","equal","greater_than","greater_than_equal"]
    value:int
    @model_validator(mode="before")
    def validate_value_type(cls, values):
        value = values.get("value")
        if not isinstance(value, int):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f"Invalid type for 'value'. Expected 'int', got '{type(value).__name__}'.")
        return values

class RecordsLoadedCondition(BaseModel):
    Operator:Literal["equal"]
    value:int

    @model_validator(mode="before")
    def validate_value_type(cls, values):
        value = values.get("value")
        if not isinstance(value, int):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f"Invalid type for 'value'. Expected 'int', got '{type(value).__name__}'.")
        return values
    

    @classmethod
    def from_condition(cls,condition:dict):
        return cls.model_validate(condition)


#checked

class TypeDataCondition(BaseModel):

    operator: Literal["equal","not_equal"]

    value: Literal["Status","Enriched","None"]

    @model_validator(mode="before")
    def validate_typedata(cls, values):
        # Allowed keys
        allowed_keys = {"operator", "value"}
        extra_keys = set(values.keys()) - allowed_keys
        if extra_keys:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f"Unexpected fields for TypeDataCondition: {extra_keys}. Only 'operator' and 'value' are allowed.")
        # Validate value case-insensitively
        value = values.get("value")
        if value is None:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail="Value must be provided for TypeDataCondition.")

        allowed_values = {"STATUS", "ENRICHED", "NONE"}
        if value.upper() not in allowed_values:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f"Invalid value '{value}' for TypeDataCondition. Must be one of 'Status', 'Enriched', 'None'.")

        # Normalize value to canonical format (optional)
        values["value"] = value.capitalize() if value.upper() != "NONE" else "None"
        return values


#checked
class GenderCondition(BaseModel):
    operator: Literal["equal","not_equal"]
    value: Literal["MALE","FEMALE","BOTH"]

    @model_validator(mode="before")
    def validate_gender(cls,values):
        #Allowed keys
        allowed_keys={"operator","value"}
        #check for extra keys on the payload
        extra_keys = set(values.keys()) - allowed_keys

        if extra_keys:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Unexpected fields for gender condition: {extra_keys}. Only 'operator' and 'value' are allowed."
            )
         # Validate that value is one of allowed literals

        value = values.get("value")
        if value not in {"MALE", "FEMALE", "BOTH"}:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid value '{value}' for gender condition. Must be one of 'MALE', 'FEMALE', 'BOTH'."
            )

        return values

#checked
class IsActiveCondition(BaseModel):

    operator: Literal["equal"]
    value: Literal[True]


#Checked
class AgeCondition(BaseModel):

    operator: Literal[
        "equal", "less_than", "less_than_equal",
        "greater_than", "greater_than_equal", "between"
    ]
    value: Optional[int] = None
    lower: Optional[int] = None
    upper: Optional[int] = None

    @model_validator(mode="before")

    def check_age_range(cls, values):
        
        op = values.get("operator")
        value = values.get("value")
        lower = values.get("lower")
        upper = values.get("upper")

        if op == "between":
            if lower is None or upper is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail="Both lower and upper must be provided for 'between'")
            if upper < lower:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail="Age upper must be >= lower")
            if value!=0 and op == 'between':
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail="'value' must not be provided for 'between'")
            
        elif op in ["less_than", "less_than_equal"]:

            if upper is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f"Age Condition '{op}' requires 'upper'")
        elif op in ["greater_than", "greater_than_equal"]:
            if upper is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f"Age Condition '{op}' requires 'upper'")
            
        elif op in ["equal", "not_equal"]:
            if value is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f"Age Condition '{op}' requires 'value'")   
        else:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f"Unknown operator '{op}'")
        return values


class RuleSchema(BaseModel):

    salary:Optional[NumericCondition]=None
    gender:Optional[GenderCondition]=None
    typedata:Optional[TypeDataCondition]=None
    is_active:IsActiveCondition=IsActiveCondition(operator="equal",value=True)
    age:Optional[AgeCondition]=None
    derived_income:Optional[NumericCondition]=None
    is_deduped:Optional[bool]=False
    last_used:Optional[LastUsedCondition]=None
    number_of_records:RecordsLoadedCondition

    
#Flattend Numeric Response Condition

class NumericConditionResponse(BaseModel):
    operator: str
    value: Optional[float] = None
    lower: Optional[float] = None
    upper: Optional[float] = None
    @classmethod
    def from_condition(cls, condition: Optional[dict]):
        if condition is None:
            return None
        if condition["operator"] == "between":
            return cls(operator=condition["operator"], lower=condition.get("lower"), upper=condition.get("upper"))
        else:
            return cls(operator=condition["operator"], value=condition.get("value"))


# Flattened Age response

class AgeConditionResponse(BaseModel):
    operator: str
    value: Optional[int] = None
    lower: Optional[int] = None
    upper: Optional[int] = None
    @classmethod
    def from_condition(cls, condition: dict):
        if condition["operator"] == "between":
            return cls(operator=condition["operator"], lower=condition.get("lower"), upper=condition.get("upper"))
        else:
            return cls(operator=condition["operator"], value=condition.get("value"))


class LastUsedConditionResponse(BaseModel):
    value: int
    @classmethod
    def from_condition(cls, condition: dict):
        return cls(value=condition.get("value"))
    
class RecordsLoadedConditionResponse(BaseModel):
    
    Operator: Literal["equal"]="equal"
    value:int=5000

    @model_validator(mode="before")
    def validate_value_type(cls,values):
        value = values.get("value", 5000)
        if not isinstance(value, int):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f"Invalid type for 'value'. Expected 'int', got '{type(value).__name__}'.")
        return values
    @classmethod
    def from_condition(cls,condition:dict):
        return cls.model_validate(condition)
    
# Top-level response model


class RuleResponseModel(BaseModel):
    rule_code: int
    rule_name: str
    salary: NumericConditionResponse
    derived_income: Optional[NumericConditionResponse]=None
    gender: str
    typedata: str
    is_active: bool
    age: AgeConditionResponse
    last_used: Optional[LastUsedConditionResponse] = None
    records_loaded:Optional[RecordsLoadedConditionResponse]=None
    is_active:bool

class DeactivateRuleResponseModel(BaseModel):
    rule_code:int
    rule_name:str
    message:str
    class Config:
        from_attributes = True

class ActivateRuleResponseModel(BaseModel):
    rule_code:int
    rule_name:str
    message:str
    class Config:
        from_attributes = True

class UpdatingSalarySchema(BaseModel):
    salary:Optional[int]=None
    lower_limit_salary:Optional[int]=None
    upper_limit_salary:Optional[int]=None


class UpdatingDerivedIncomeSchema(BaseModel):
    derived_income_value:Optional[int]=None
    lower_limit_derived_income:Optional[int]=None
    upper_limit_derived_income:Optional[int]=None

class UpdateAgeSchema(BaseModel):
    age_value:Optional[int]=None
    age_lower_limit:Optional[int]=None
    age_upper_limit:Optional[int]=None

class ActivateCampaignRuleResponse(BaseModel):
    rule_code:int
    rule_name:str
    status:str
    is_active:bool
    class Config:
        from_attributes = True


class DeleteCampaignRuleResponse(BaseModel):
    message:str
    success:bool
    class Config:
        from_attributes=True


class GetCampaignRuleByNameResponse(BaseModel):
    status:str
    created_by:int
    pinged_data:bool
    rule_code:int
    rule_name:str
    is_active:bool


class OperatorBase(BaseModel):
    operator:str
    value:Optional[int]=None

class BetweenOperator(OperatorBase):
    operator:Literal["between"]
    lower:int
    upper:int

class SingleValueOperator(OperatorBase):
    operator:Literal["equal","less_than","greater_than","not_equal","greater_than_equal","less_than_equal"]

NumericRule=Union[BetweenOperator,SingleValueOperator]

class GetCampaignRuleResponse(BaseModel):
    rule_code:int
    rule_name:str
    salary:Optional[NumericRule]=None
    derived_income:Optional[NumericRule]=None
    age:Optional[NumericRule]=None
    gender:Optional[str]=None
    typedata:Optional[str]=None
    is_active:bool
    last_used:Optional[int]=None
    records_loaded:Optional[int]=None

    class Config:
        from_attributes = True

class GetAllCampaignRulesResponse(BaseModel):
    total:int
    page:int
    page_size:int
    rules:list[GetCampaignRuleResponse]
    class Config:
        from_attributes = True

class UpdateCampaignRule(BaseModel):
    new_campaign_rule_name:str

class ChangeRuleResponse(BaseModel):
    success:bool
    message:str

class UpdatingCampaignRuleResponse(BaseModel):
    rule_code:int
    new_rule_name:str


class UpdateNumberOfLeads(BaseModel):
    number_of_leads:int
class UpdateNumberOfLeadsResponse(BaseModel):
    numer_of_leads:int
    class Config:
        from_attributes = True


#Request schema for rule creation

class CreateRule(BaseModel):
    name:bool
    status:bool
    definition:RuleSchema



class ResponseRuleSchema(BaseModel):
    id:int
    name:str
    status:bool
    definition:RuleSchema
    class Config:
        from_attributes=True



class ComparisonRule(BaseModel):
    operator:str
    value:Optional[Any]=None
    lower:Optional[Any]=None
    upper:Optional[Any]=None

class GenderRule(BaseModel):
    operator:Literal["equal"]
    value:Literal["MALE","FEMALE","NULL"]


class CampaignRulesTotal(BaseModel):
    total_number_of_rules:int


