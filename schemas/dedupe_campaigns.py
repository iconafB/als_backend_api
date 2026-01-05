from pydantic import BaseModel,field_validator,validator
from typing import Optional,Union
import re
from datetime import datetime

class CreateDedupeCampaign(BaseModel):
    branch:str
    camp_code:str
    campaign_name:str

class UpdateDedupeCampaign(BaseModel):
    branch:str
    camp_code:str
    camp_name:str


class DeleteCamapignSchema(BaseModel):
    campaign_code:str
    message:str

class SubmitDedupeReturnSchema(BaseModel):
    camp_code:str
    dedupe_code:str

class ManualDedupeListReturn(BaseModel):
    Success:bool
    Information_Table:str
    Campaign_Dedupe_Table:str
    Key:str


