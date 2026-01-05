from typing import List, Optional
import httpx
import asyncio
from settings.Settings import get_settings
from typing import Annotated
from fastapi import Depends
from utils.logger import define_logger

dedago_logger=define_logger("als_load_campaign_logs","logs/dedago_logger_route.log")

class LoadALSClass:

    STATUS_LIST = ["BLACKL", "DEC", "INV", "LB", "NDNE", "NEW", "NI", "PTH", "QTR", "SALE", "SENT"]
    BRANCH_LISTS = {
        "INVTNTDBN": [100, 108],
        "P3": [106, 108],
        "HQ": [100, 108],
        "default": [100, 112]
    }

    def __init__(self,http_client: Optional[httpx.AsyncClient] = None):
        """
        settings: object providing tokens and dedago_url
        http_client: optionally inject an httpx.AsyncClient
        """
        self.settings=get_settings()
        self.http_client = http_client or httpx.AsyncClient(timeout=20)

    def get_token(self, branch: str) -> str:

        if branch == "INVTNTDBN":
            return self.settings.INVTNTDBN_TOKEN
        elif branch == "P3":
            return self.settings.P3_TOKEN
        elif branch == "HQ":
            return self.settings.HQ_TOKEN
        else:
            return self.settings.YORK_TOKEN
    

    def set_payload(self, branch: str, leads: List[dict], camp_code: str, list_name: str) -> dict:

        payload = {
            "campaign_id": camp_code,
            "dedup": "dupcamp",
            "list_method": "NEW",
            "status": self.STATUS_LIST,
            "active": False,
            "days": "30",
            "list_name": list_name,
            "leads": leads,
            "custom_list_id": self.BRANCH_LISTS.get(branch, self.BRANCH_LISTS["default"])
        }

        return payload

    async def send_data_to_dedago(self, token: str, payload: dict, retries: int = 3, backoff_factor: float = 0.5) -> dict:
        """
        Send the payload to Dedago asynchronously with automatic retries.
        """
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": token
        }

        for attempt in range(1, retries + 1):
            try:
                response = await self.http_client.post(
                    url=self.settings.dedago_url,
                    json=payload,
                    headers=headers
                )
                response.raise_for_status()

                return {"status_code": response.status_code, "list_id": response.json().get("list_id")}
            
            except (httpx.RequestError, httpx.HTTPStatusError) as e:
                if attempt == retries:
                    dedago_logger.info(f"status code:{response.status_code},response message:{response.json()}")
                    dedago_logger.exception(f"maximum number of retries reached for the dedago service:{e}")
                    #this is crazy

                    return {"status_code":response.status_code,"list_id":None}
                
                # exponential backoff
                await asyncio.sleep(backoff_factor * (2 ** (attempt - 1)))

    async def close(self):
        """Close the HTTP client if it was internally created."""

        await self.http_client.aclose()


async def get_als_service():

    service=LoadALSClass()
    try:
        yield service
    finally:
        await service.close()


LoadDataToALSService=Annotated[LoadALSClass,Depends(get_als_service)]
