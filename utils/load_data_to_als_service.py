from typing import List, Optional,Dict,Any
import httpx
import json
import base64
import requests
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

        print("print the expected leads")
        print(leads)
        print()
        print(f"list names:{list_name}")

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

    async def send_data_to_dedago2(self,token:str,payload:dict):

        API_ALS_LOADER_URL = "https://ss.dedago.com/api/lead_loader2"
        headers = {
        "Accept": "application/json",
        "Authorization": token
         }
        
        timeout = httpx.Timeout(54000.0)

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
            url=API_ALS_LOADER_URL,
            json=payload,
            headers=headers
            )

        print(response.text)
        print(response.content)

        list_id = None
        if response.status_code == 200:
            payload = response.json()
            list_id = payload.get("list_id")

        return {
            "status": response.status_code,
            "list_id": list_id
        }


    def send_data_to_dedago3(self,token:str,payload:List[dict]) -> Dict[str, Any]:

        API_ALS_LOADER_URL = "https://ss.dedago.com/api/lead_loader2"

        API_ALS_HEADERS = {
            "Content-type": "application/json",
            "Accept": "application/json"
            # "Authorization": "Basic aW52dGRibkFQSTpCaFBTc2V3dXM5cnpVcUZt"  # Durban
        }

        print("print the payload inside the send method")
        print(payload)

        res = requests.post(
            url=API_ALS_LOADER_URL,
            data=json.dumps(payload),
            headers=API_ALS_HEADERS,
            timeout=54000
        )

        print(res.text)
        print(res.status_code)

        try:
            r = res.json()
        except ValueError:
            r = {}

    # list_id is the key for the list name provided by the endpoint.
        if res.status_code == 200:
            list_id = r.get("list_id")
        else:
            list_id = "None"

        als_response = {
            "status": res.status_code,
            "list_id": list_id
        }

        return als_response



    async def send_data_to_dedago(
    self,
    token: str,
    payload: dict,
    retries: int = 2,
    backoff_factor: float = 0.5
) -> dict:
        
        schema,credentials=token.split(" ",1)
        print(schema)
        print(credentials)
        decoded=base64.b64decode(credentials).decode("utf-8")
        
        username,password=decoded.split(":",1)

        print(username)
        print(password)

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        print("enter the send to dedago method")
        print(headers)

        for attempt in range(1,retries+1):

            try:
                response = await self.http_client.get(
                url=self.settings.dedago_url,
                auth=(username,password),
                json=payload,
                headers=headers,
                )
                response.raise_for_status()
                print(f"print the dedago status code")
                print(response.status_code)

                try:
                    data=response.json()
                except ValueError():
                    dedago_logger.warning(
                        "Dedago returned non-JSON on success. status=%s body=%r",
                        response.status_code,
                        response.text,
                        )
                    return {"status_code": response.status_code, "list_id": None}
                
                return {"status_code": response.status_code,"list_id": data.get("list_id")}
            
            except httpx.HTTPStatusError as e:
                response=e.response
                try:
                    body_for_log=response.json()
                except ValueError:
                    body_for_log=response.text
                dedago_logger.error(
                "Dedago HTTP error. attempt=%s/%s status=%s body=%r",
                attempt,
                retries,
                response.status_code,
                body_for_log,
                )
            
            except httpx.RequestError as e:
                dedago_logger.error(
                "Dedago request failed (no HTTP response). attempt=%s/%s error=%r",
                attempt,
                retries,
                e,
                )
                if attempt == retries:
                    return {"status_code": 0, "list_id": None}
            
            # exponential backoff
            await asyncio.sleep(backoff_factor * (2 ** (attempt - 1)))

    
 


    def load_data_to_als(self,
        branch: str,
        camp_code: str,
        leads: List[Dict[str, Any]],
        token: str,
        list_name: str,
        timeout: int = 54000,

    ) -> Dict[str, Any]:
        """
    Send leads to ALS lead loader endpoint.

    Params:
        branch: e.g. "HQ", "P3", "INVNTDBN", "YORK"
        camp_code: string e.g "KP_DBN"
        leads: list of dicts (json objects)
        token: Authorization header value
        list_name: list name to create/associate

    Returns:
        {'status': <http_status_code>, 'list_id': <list_id_or_'None'>}
    """

        url= "https://ss.dedago.com/api/lead_loader2"
        print()
        print("enter the loading method")
        print()

        # Decide custom_list_id by rules (camp_code override wins)
        custom_list_id: List[int] = []

        if branch == "YORK":
            custom_list_id = [100, 112]
        elif branch == "DENEXIS":
            custom_list_id = [100, 112]

        elif branch == "INVNTDBN":
            custom_list_id = [100, 108]
        elif branch == "HQ":
            custom_list_id = [100,112]
        elif branch == "P3":
            custom_list_id = [106, 108]

        # Special campaign override
        if camp_code == "WAR1STM":
            custom_list_id = [100, 106, 108]


        print("print the custom list id")
        print(custom_list_id)

        payload=json.dumps({
            "campaign_id": camp_code,
            "list_name":list_name,
            "dedup": "dupcamp",
            "list_method":"NEW",
            "active": "False",
            "status": ["BLACKL", "DEC", "INV", "LB", "NDNE", "NEW", "NI", "PTH", "QTR", "SALE", "SENT"],
            "custom_list_id": custom_list_id,
            "days":30,
            "leads": leads
        })

        headers = {
        "Content-Type": "application/json",
        "Accept":"application/json"
        }
        
        print()
        print("decode the token")
        schema,credentials=token.split(" ",1)
        
        print(f"schema:{schema}")
        print(f"credentials:{credentials}")

        decoded=base64.b64decode(credentials).decode("utf-8")
        username,password=decoded.split(":",1)
        
        try:
            # GET OR POST??
            res = requests.request("GET",url,headers=headers,auth=(username,password),data=payload)
            print()
            print()
            print("print the text DEDAGO response")
            print(res.text)
            print("print the response json")
            print("print the status code from DEDAGO")
            print(res.status_code)
            print("print the response with headers")
            print(res.headers)
            print("print the url")
            print(res.url)
            print()

        except requests.RequestException:
            # Network/DNS/timeout/etc
            return {"status": 0, "list_id": "None"}

        # Try to parse JSON safely
        list_id = "None"
        if res.status_code == 200:
            try:
                body = res.json()
                list_id = body.get("list_id", "None")
            except ValueError:
                list_id = "None"

        return {"status_code": res.status_code, "list_id": list_id}

async def get_als_service():
    service=LoadALSClass()
    yield service


LoadDataToALSService=Annotated[LoadALSClass,Depends(get_als_service)]
