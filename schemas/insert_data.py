from pydantic import BaseModel,field_validator,model_validator,Field
from typing import Optional,Union,Dict
from datetime import datetime
import re


class StatusedData(BaseModel):

    idnum: Optional[str] = None
    cell: Optional[str] = None
    created_at: Optional[str] = None
    salary: Optional[float] = None
    name: Optional[str] = None
    surname: Optional[str] = None
    address1: Optional[str] = None
    address2: Optional[str] = None
    suburb: Optional[str] = None
    city: Optional[str] = None
    postal: Optional[str] = None
    email: Optional[str] = None
    status: Optional[str] = None
    dob: Optional[str] = None
    gender: Optional[str] = None
    company: Optional[str] = None
    job: Optional[str] = None
    make: Optional[str] = None
    model: Optional[str] = None
    bank: Optional[str] = None
    bal: Optional[float] = None

    
    # Global cleaning for all fields
  
    @field_validator("*", mode="before")
    @classmethod
    def clean_empty_and_null(cls, value):
        if isinstance(value, str) and value.strip().lower() in {"", "null", "nan"}:
            return None
        return value

  
    # Field-specific validators
  
    @field_validator("idnum")
    @classmethod
    def validate_idnum(cls, value):
        if value and re.match(r"^\d{13}$", value):
            return value
        return None

    @field_validator("postal")
    @classmethod
    def validate_postal(cls, value):
        if value and isinstance(value, str):
            value = value.split(".")[0] if "." in value else value
            if value.isdigit() and 4 <= len(value) <= 5:
                return value
        return None

    @field_validator("dob", mode="before")
    @classmethod
    def validate_dob(cls, value):
        if not value or not isinstance(value, str):
            return None
        try:
            if re.match(r"^\d{13}$", value):
                prefix = "20" if int(value[:2]) <= 30 else "19"
                year = prefix + value[:2]
                month = value[2:4]
                day = value[4:6]
                datetime(int(year), int(month), int(day))  # validate
                return f"{year}-{month}-{day}"
        except:
            return None
        return None

    @field_validator("created_at", mode="before")
    @classmethod
    def validate_date_created(cls, value):
        if not value or not isinstance(value, str):
            return None
        try:
            if "/" in value:
                day, month, year_time = value.split("/")
                year_parts = re.split(r"\D+", year_time.strip())
                year = year_parts[0]
                dt = datetime(int(year), int(month), int(day))
                return dt.strftime("%Y-%m-%d")
            
            else:
                dt = datetime.strptime(value, "%Y-%m-%d")
                return dt.strftime("%Y-%m-%d")
        except:
            return None

    @field_validator("gender", mode="after")
    @classmethod
    def extract_gender_from_id(cls, value, info):
        idnum = info.data.get("idnum")
        if idnum and re.match(r"^\d{13}$", idnum):
            gender_digit = int(idnum[6])
            return "FEMALE" if gender_digit <= 4 else "MALE"
        
        return None




class EnrichedData(BaseModel):
    Title: str
    forename: str
    lastname: str
    IDNo: Optional[str] = None
    Race: str
    gender: str
    Marital_Status: str
    line1: str
    line2: str
    line3: str
    line4: str
    PCode: str
    Province: str
    Home_number: Optional[str] = None
    Work_number: Optional[str] = None
    mobile_Number: Optional[str] = None
    mobile_Number2: Optional[str] = None
    mobile_Number3: Optional[str] = None
    mobile_Number4: Optional[str] = None
    mobile_Number5: Optional[str] = None
    mobile_Number6: Optional[str] = None
    derived_income: Optional[str] = None
    cipro_reg: str
    Deed_office_reg: str
    vehicle_owner: str
    cr_score_tu: str
    monthly_expenditure: str
    owns_cr_card: str
    cr_card_rem_bal: str
    owns_st_card: str
    st_card_rem_bal: str
    has_loan_acc: str
    loan_acc_rem_bal: str
    has_st_loan: str
    st_loan_bal: str
    has_1mth_loan: str
    onemth_loan_bal: str
    sti_insurance: str
    has_sequestration: str
    has_admin_order: str
    under_debt_review: str
    deceased_status: str
    has_judgements: str
    make: Optional[str] = None
    model: Optional[str] = None
    year: Optional[str] = None
    birth_date: Optional[str] = None

    # Pydantic v2 validators
  
    @model_validator(mode='before')
    def replace_nan_and_format(cls, data):
        """Replace 'nan' strings with None and normalize phone numbers."""
        for key, value in data.items():
            if value == 'nan':
                data[key] = None

            if key.startswith("mobile_Number") or key in ("Home_number", "Work_number"):
                if value is not None:
                    val_str = str(value)
                    if len(val_str) == 11:
                        data[key] = '0' + val_str[2:]
                    elif '.' in val_str:
                        data[key] = '0' + val_str[2:11]
        return data

    @model_validator(mode='after')

    def validate_fields(cls, data):
        # Validate Title
        if data.Title not in ("ADV", "DR", "MEJ", "MEV", "MISS", "MNR", "MR", "MRS", "MS", "PROF"):
            raise ValueError(f"Invalid Title: {data.Title}")

        # Validate Race
        if data.Race not in ("BLACK", "COLOURED", "INDIAN", "MIXED", "WHITE", "UNKNOWN"):
            raise ValueError(f"Invalid Race: {data.Race}")

        # Validate gender
        if data.gender not in ("MALE", "FEMALE"):
            raise ValueError(f"Invalid gender: {data.gender}")

        # Validate Marital_Status
        if data.Marital_Status not in ("MARRIED", "SINGLE"):
            raise ValueError(f"Invalid Marital_Status: {data.Marital_Status}")

        # Map Y/N fields to T/F
        yn_fields = [
            "cipro_reg", "Deed_office_reg", "vehicle_owner",
            "owns_cr_card", "owns_st_card", "has_loan_acc", "has_st_loan",
            "has_1mth_loan", "sti_insurance", "has_sequestration", "has_admin_order",
            "under_debt_review", "deceased_status", "has_judgements"
        ]
        for f in yn_fields:
            if getattr(data, f) in ("Y", "N"):
                setattr(data, f, "T" if getattr(data, f) == "Y" else "F")

        # Validate birth_date format if IDNo is provided

        if data.birth_date and re.match(r'^\d{13}$', data.IDNo or ''):
            IDNo = data.IDNo
            month = IDNo[2:4]
            day = IDNo[4:6]
            year = '20' + IDNo[0:2] if IDNo[0] == '0' else '19' + IDNo[0:2]
            data.birth_date = f"{year}-{month}-{day}"

        # Validate year field
        if data.year:
            if len(data.year) == 4:
                data.year = data.year
            elif '.' in str(data.year):
                data.year = str(data.year).split('.')[0]

        return data


class InsertEnrichedDataResponseModel(BaseModel):
    status: str
    elapsed_seconds: float


from pydantic import BaseModel, Field


class InsertStatusDataResponse(BaseModel):

    success: bool = Field(..., description="Whether the ingestion completed successfully")
    file: str = Field(..., description="The filename that was processed")
    rows_seen: int = Field(..., ge=0, description="Total number of rows read from the CSV")
    rows_valid: int = Field(..., ge=0, description="Rows that passed validation and were processed")
    seconds: float = Field(..., ge=0, description="Total runtime in seconds")
    rows_per_second: float = Field(..., ge=0, description="rows_valid / seconds")

    

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "file": "status_dump.csv",
                "rows_seen": 90000,
                "rows_valid": 87234,
                "seconds": 92.417,
                "rows_per_second": 943.7,
            }
        }




class TableResult(BaseModel):
    inserted_records: int
    insert_status:str
    error:Optional[str]=None

class InsertStatusDataResponseModel(BaseModel):
    success: bool
    details: Dict[str, TableResult]
    processing_time_sec:float=Field(...,description="Time taken to process the entire operation")

class TableInsertCount(BaseModel):
    inserted:int
    updated:int

class UploadStatusResponse(BaseModel):
    status:str
    total_rows_processed:int
    table_counts:Dict[str,TableInsertCount]
    
    model_config={
        "from_attributes":True
    }



class TableInsertStatusSummary(BaseModel):
    table_num:int
    total_rows:int
    total_batches:int

class BulkStatusResponse(BaseModel):
    status:str
    message:str
    summary:Dict[int,TableInsertStatusSummary]


class TableInsertEnrichedSummary(BaseModel):
    table_num:int
    table_rows:int
    table_batches:int

class BulkEnrichedResponse(BaseModel):
    status:str
    message:str
    summary:Dict[int,TableInsertEnrichedSummary]




# class EnrichedData(BaseModel):
#     Title: str
#     forename: str
#     lastname: str
#     IDNo: Union[str, None] = None
#     Race: str
#     gender: str
#     Marital_Status: str
#     line1: str
#     line2: str
#     line3: str
#     line4: str
#     PCode: str
#     Province: str
#     Home_number: Union[str, None] = None
#     Work_number: Union[str, None] = None
#     mobile_Number: Union[str, None] = None
#     mobile_Number2: Union[str, None] = None
#     mobile_Number3: Union[str, None] = None
#     mobile_Number4: Union[str, None] = None
#     mobile_Number5: Union[str, None] = None
#     mobile_Number6: Union[str, None] = None
#     derived_income: Union[str, None] = None
#     cipro_reg: str
#     Deed_office_reg: str
#     vehicle_owner: str
#     cr_score_tu: str
#     monthly_expenditure: str
#     owns_cr_card: str
#     cr_card_rem_bal: str
#     owns_st_card: str
#     st_card_rem_bal: str
#     has_loan_acc: str
#     loan_acc_rem_bal: str
#     has_st_loan: str
#     st_loan_bal: str
#     has_1mth_loan: str
#     onemth_loan_bal: str
#     sti_insurance: str
#     has_sequestration: str
#     has_admin_order: str
#     under_debt_review: str
#     deceased_status: str
#     has_judgements: str
#     make: Union[str, None] = None
#     model: Union[str, None] = None
#     year: Union[str, None] = None
#     birth_date: Union[str, None] = None

#     @validator("birth_date")  # to make sure variable is in a given range
#     def check_birthdate(cls, value):
#         IDNo = value
#         if re.match('^(\d{13})?$', IDNo):
#             month = IDNo[2:4]

#             day = IDNo[4:6]

#             if (0 <= int(IDNo[0]) <= 9) and (1 <= int(month) <= 12) and (1 <= int(day) <= 31):
#                 if IDNo[0] == '0':
#                     year = '20' + IDNo[0:2]
#                 else:
#                     year = '19' + IDNo[0:2]

#                 strdate = year + '-' + month + '-' + day

#                 return strdate
#         else:
#             return None

#     @validator("*")  # to make sure variable is in a given range

#     def replace_nan(cls, value):
#         if value == 'nan':
#             return None
#         else:
#             return value

#     @validator("Title")  # to make sure variable is in a given range
#     def check_title(cls, value):
#         if value in ("ADV", "DR", "MEJ", "MEV", "MISS", "MNR", "MR", "MRS", "MS", "PROF"):
#             return value

#     @validator("forename")  # make sure int field is not empty
#     def check_forename(cls, value):
#         if value != '':
#             return value

#     @validator("lastname")  # make sure int field is not empty
#     def check_lastname(cls, value):
#         if value != '':
#             return value

#     @validator("IDNo")  # make sure field is said digits
#     def check_IDNo(cls, value):
#         if (value == None):
#             return None
#         if len(value) == 13:
#             return value

#     @validator("Race")  # to make sure variable is in a given range
#     def check_Race(cls, value):
#         if value in ("BLACK", "COLOURED", "INDIAN", "MIXED", "WHITE", "UNKNOWN"):
#             return value

#     @validator("gender")  # to make sure variable is in a given range
#     def check_gender(cls, value):
#         if value in ("MALE", "FEMALE"):
#             return value

#     @validator("Marital_Status")  # to make sure variable is in a given range
#     def check_Marital_Status(cls, value):
#         if value in ("MARRIED", "SINGLE"):
#             return value

#     @validator("line1")  # make sure int field is not empty
#     def check_line1(cls, value):
#         if value != '':
#             return value

#     @validator("line2")  # make sure int field is not empty
#     def check_line2(cls, value):
#         if value != '':
#             return value

#     @validator("line3")  # make sure int field is not empty
#     def check_line3(cls, value):
#         if value != '':
#             return value

#     @validator("line4")  # make sure int field is not empty
#     def check_line4(cls, value):
#         if value != '':
#             return value

#     @validator("PCode")  # make sure field is said digits
#     def check_PCode(cls, value):
#         if len(value) == 4 or len(value) == 5:
#             return value

#     @validator("Province")  # make sure int field is not empty
#     def check_Province(cls, value):
#         if value != '':
#             return value

#     @validator("Home_number")  # validate phone number
#     def check_Home_number(cls, value):
#         if(value == None):
#             return None
#         if len(value) == 11:
#             new_value = '0' + value[2:]
#             return new_value
#         if '.' in str(value):
#             new_value = '0' + value[2:11]
#             return new_value

#     @validator("Work_number")  # validate phone number
#     def check_Work_number(cls, value):
#         if (value == None):
#             return None
#         if len(value) == 11:
#             new_value = '0' + value[2:]
#             return new_value
#         if '.' in str(value):
#             new_value = '0' + value[2:11]
#             return new_value

#     @validator("mobile_Number")  # validate phone number
#     def check_mobile_Number(cls, value):
#         if (value == None):
#             return None
#         if len(value) == 11:
#             new_value = '0' + value[2:]
#             return new_value
#         if '.' in str(value):
#             new_value = '0' + value[2:11]
#             return new_value

#     @validator("mobile_Number2")  # validate phone number
#     def check_mobile_Number2(cls, value):
#         if (value == None):
#             return None
#         if len(value) == 11:
#             new_value = '0' + value[2:]
#             return new_value
#         if '.' in str(value):
#             new_value = '0' + value[2:11]
#             return new_value

#     @validator("mobile_Number3")  # validate phone number
#     def check_mobile_Number3(cls, value):
#         if (value == None):
#             return None
#         if len(value) == 11:
#             new_value = '0' + value[2:]
#             return new_value
#         if '.' in str(value):
#             new_value = '0' + value[2:11]
#             return new_value

#     @validator("mobile_Number4")  # validate phone number
#     def check_mobile_Number4(cls, value):
#         if (value == None):
#             return None
#         if len(value) == 11:
#             new_value = '0' + value[2:]
#             return new_value
#         if '.' in str(value):
#             new_value = '0' + value[2:11]
#             return new_value

#     @validator("mobile_Number5")  # validate phone number
#     def check_mobile_Number5(cls, value):
#         if (value == None):
#             return None
#         if len(value) == 11:
#             new_value = '0' + value[2:]
#             return new_value
#         if '.' in str(value):
#             new_value = '0' + value[2:11]
#             return new_value

#     @validator("mobile_Number6")  # validate phone number
#     def check_mobile_Number6(cls, value):
#         if (value == None):
#             return None
#         if len(value) == 11:
#             new_value = '0' + value[2:]
#             return new_value
#         if '.' in str(value):
#             new_value = '0' + value[2:11]
#             return new_value

#     @validator("derived_income")  # make sure int field is not empty
#     def check_derived_income(cls, value):
#         if value != '':
#             return value

#     @validator("cipro_reg")  # to make sure variable is in a given range
#     def check_cipro_reg(cls, value):
#         if value in ("Y", "N"):
#             if value == "Y":
#                 return "T"
#             else:
#                 if value == "N":
#                     return "F"

#     @validator("Deed_office_reg")  # to make sure variable is in a given range
#     def check_Deed_office_reg(cls, value):
#         if value in ("Y", "N"):
#             if value == "Y":
#                 return "T"
#             else:
#                 if value == "N":
#                     return "F"

#     @validator("vehicle_owner")  # to make sure variable is in a given range
#     def check_vehicle_owner(cls, value):
#         if value in ("Y", "N"):
#             if value == "Y":
#                 return "T"
#             else:
#                 if value == "N":
#                     return "F"

#     @validator("cr_score_tu")  # make sure int field is not empty
#     def check_cr_score_tu(cls, value):
#         if value != '':
#             return value

#     @validator("monthly_expenditure")  # make sure int field is not empty
#     def check_monthly_expenditure(cls, value):
#         if value != '':
#             return value

#     @validator("owns_cr_card")  # to make sure variable is in a given range
#     def check_owns_cr_card(cls, value):
#         if value in ("Y", "N"):
#             if value == "Y":
#                 return "T"
#             else:
#                 if value == "N":
#                     return "F"

#     @validator("cr_card_rem_bal")  # make sure int field is not empty
#     def check_cr_card_rem_bal(cls, value):
#         if value != '':
#             return value

#     @validator("owns_st_card")  # to make sure variable is in a given range
#     def check_owns_st_card(cls, value):
#         if value in ("Y", "N"):
#             if value == "Y":
#                 return "T"
#             else:
#                 if value == "N":
#                     return "F"

#     @validator("st_card_rem_bal")  # make sure int field is not empty
#     def check_st_card_rem_bal(cls, value):
#         if value != '':
#             return value

#     @validator("has_loan_acc")  # to make sure variable is in a given range
#     def check_has_loan_acc(cls, value):
#         if value in ("Y", "N"):
#             if value == "Y":
#                 return "T"
#             else:
#                 if value == "N":
#                     return "F"

#     @validator("loan_acc_rem_bal")  # make sure int field is not empty
#     def check_loan_acc_rem_bal(cls, value):
#         if value != '':
#             return value

#     @validator("has_st_loan")  # to make sure variable is in a given range
#     def check_has_st_loan(cls, value):
#         if value in ("Y", "N"):
#             if value == "Y":
#                 return "T"
#             else:
#                 if value == "N":
#                     return "F"

#     @validator("st_loan_bal")  # make sure int field is not empty
#     def check_st_loan_bal(cls, value):
#         if value != '':
#             return value

#     @validator("has_1mth_loan")  # to make sure variable is in a given range
#     def check_has_1mth_loan(cls, value):
#         if value in ("Y", "N"):
#             if value == "Y":
#                 return "T"
#             else:
#                 if value == "N":
#                     return "F"

#     @validator("onemth_loan_bal")  # make sure int field is not empty
#     def check_onemth_loan_bal(cls, value):
#         if value != '':
#             return value

#     @validator("sti_insurance")  # to make sure variable is in a given range
#     def check_sti_insurance(cls, value):
#         if value in ("Y", "N"):
#             if value == "Y":
#                 return "T"
#             else:
#                 if value == "N":
#                     return "F"

#     @validator("has_sequestration")  # to make sure variable is in a given range
#     def check_has_sequestration(cls, value):
#         if value in ("Y", "N"):
#             if value == "Y":
#                 return "T"
#             else:
#                 if value == "N":
#                     return "F"

#     @validator("has_admin_order")  # to make sure variable is in a given range
#     def check_has_admin_order(cls, value):
#         if value in ("Y", "N"):
#             if value == "Y":
#                 return "T"
#             else:
#                 if value == "N":
#                     return "F"

#     @validator("under_debt_review")  # to make sure variable is in a given range
#     def check_under_debt_review(cls, value):
#         if value in ("Y", "N"):
#             if value == "Y":
#                 return "T"
#             else:
#                 if value == "N":
#                     return "F"

#     @validator("deceased_status")  # to make sure variable is in a given range
#     def check_deceased_status(cls, value):
#         if value in ("Y", "N"):
#             if value == "Y":
#                 return "T"
#             else:
#                 if value == "N":
#                     return "F"

#     @validator("has_judgements")  # to make sure variable is in a given range
#     def check_has_judgements(cls, value):
#         if value in ("Y", "N"):
#             if value == "Y":
#                 return "T"
#             else:
#                 if value == "N":
#                     return "F"

#     @validator("make")  # make sure int field is not empty
#     def check_make(cls, value):
#         if value != '':
#             return value

#     @validator("model")  # make sure int field is not empty
#     def check_model(cls, value):
#         if value != '':
#             return value

#     @validator("year")  # validate phone number
#     def check_year(cls, value):
#         if len(value) == 4:
#             return value
#         if '.' in str(value):
#             new_value = str(value).split('.')[0]
#             return new_value


