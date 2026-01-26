from pydantic import BaseModel
from typing import List

class CreateCampaignResponse(BaseModel):
    camp_code:str
    campaign_name:str
    branch:str
    is_new:bool
    model_config={
        "from_attributes":True
    }

class InfiniteResponseSchema(BaseModel):
    camp_code:str
    campaign_name:str
    model_config={
        "from_attributes":True
    }

class PaginatedInfiniteResponse(BaseModel):
    total:int
    page:int
    page_size:int
    results:List[InfiniteResponseSchema]

class PaginatedCampaigResponse(BaseModel):
    total:int
    page:int
    page_size:int
    results:List[CreateCampaignResponse]

    
class CreateCampaign(BaseModel):
    branch:str
    camp_code:str
    campaign_name:str
    is_new:bool=True



class GetCampaignResponse(CreateCampaignResponse):
    pass

class LoadCampaign(BaseModel):
    branch:str
    camp_code:str

class UpdateCampaignName(BaseModel):
    campaign_name:str


class CampaignSpec(BaseModel):
    id_number:str
    fore_name:str
    last_name:str
    cell_number:str

class LoadCampaignResponse(BaseModel):
    campaign_code:str
    branch:str
    list_name:str
    audit_id:str
    records_processed:int
    
    model_config={
        "from_attributes":True
    }



class CampaignSpecLevelResponse(BaseModel):
    rule_name:str
    number_of_leads_available:int
    model_config={
        "from_attributes":True
    }


class CampaignsTotal(BaseModel):
    total_number_of_campaigns:int
