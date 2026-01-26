from utils.load_data_to_als_service import LoadDataToALSService
from typing import Optional
from sqlalchemy.ext.asyncio.session import AsyncSession
from utils.leads_cleaner_load_campaign import clean_and_process_results

async def send_leads_to_dedago_helper(load_data_to_dedago:LoadDataToALSService,session:AsyncSession,branch:Optional[str]=None,rule_code:Optional[int]=None):
    
    try:

        return True
    except Exception as e:
        return False