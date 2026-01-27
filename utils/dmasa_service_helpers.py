from __future__ import annotations
import asyncio
import json
from typing import Any, Dict, List, Optional, Tuple, Annotated, Union
import httpx
import urllib3
from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select
from settings.Settings import get_settings
from models.dma_service import dma_audit_id_tbl
from utils.logger import define_logger
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

dma_logger = define_logger("dmasa_logs", "logs/dma.log")


class DMAClassHelper:

    READY_STATUSES = ("Download Ready", "Dedupe Complete")

    def __init__(self):
        s = get_settings()
        self.dmasa_api_key = s.DMASA_API_KEY
        self.dmasa_member_id = s.DMASA_MEMBER_ID
        self.check_credits_dmasa_url = s.CHECK_CREDITS_DMASA_URL
        self.notification_email = s.NOTIFICATION_EMAIL
        self.submit_dedupes_dmasa_url = s.UPLOAD_DMASA_URL
        self.read_dmasa_dedupe_status = s.READ_DMASA_DEDUPE_STATUS
        self.read_dedupe_output_url = s.READ_DMASA_OUTPUT_URL
        self.client = httpx.AsyncClient(verify=False,timeout=httpx.Timeout(30.0, connect=10.0))

    async def close(self):
        await self.client.aclose()


    async def check_credits(self) -> int:
        print("enter the credits calculation method")

        params = {"API_Key": self.dmasa_api_key, "MemberID": self.dmasa_member_id}

        resp = await self.client.get(self.check_credits_dmasa_url, params=params)
        print("print the whole thing")
        print(resp.json())
        resp.raise_for_status()
        print(type(resp.json()['Credits']))
        return resp.json()['Credits']
    
    async def upload_data_for_dedupe(self,data: str,session: AsyncSession,camp_code: str,data_type: str = "C") -> str:

        payload = {"API_Key": self.dmasa_api_key,"Data": data,  "DataType": data_type,"MemberID": self.dmasa_member_id,"NotificationEmail": self.notification_email}
        resp = await self.client.post(self.submit_dedupes_dmasa_url,data=json.dumps(payload),timeout=540.0)
        resp.raise_for_status()
        result = resp.json()
        print("print the result inside the upload to dmasa method")
        print(result)

        if result.get("Errors"):

            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"DMASA rejected upload: {result['Errors']}")
        audit_id = str(result["DedupeAuditId"])
        record=dma_audit_id_tbl(audit_id=audit_id,number_of_records=int(result.get("RecordsProcessed", 0)),notification_email=self.notification_email,camp_code=camp_code,dedupe_status="Submitted",is_processed=False)

        session.add(record)
        await session.commit()
        dma_logger.info(f"DMASA upload successful | AuditID: {audit_id}")
        return audit_id
    

    async def check_dedupe_status(self, audit_id: str, session: AsyncSession) -> str:

        record = (await session.exec(select(dma_audit_id_tbl).where(dma_audit_id_tbl.audit_id == audit_id))).first()
        if not record:
            raise HTTPException(status_code=404, detail="Invalid audit id")
        params = {"API_Key": self.dmasa_api_key,"MemberID": self.dmasa_member_id,"DedupeAuditId": audit_id,}
        resp = await self.client.get(self.read_dmasa_dedupe_status, params=params)
        resp.raise_for_status()
        data = resp.json()
        status_value = (data.get("Status") or "").strip()

        if status_value in self.READY_STATUSES:

            record.is_processed = True
            record.dedupe_status = status_value
            total = data.get("TotalRecords")

            if total not in (None, ""):
                try:
                    record.number_of_records = int(total)
                #What the fuck is this
                except ValueError:
                    pass

            session.add(record)
            await session.commit()

        return status_value 
    
    async def wait_for_download_to_be_ready(self,session: AsyncSession,audit_id: str,retries: int = 10,delay: int = 5) -> bool:
        try:
            retries = int(retries)
            delay = int(delay)
        except (TypeError, ValueError):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="retries and delay must be integers")

        for _ in range(retries):
            status_value = await self.check_dedupe_status(audit_id, session)
            if status_value in self.READY_STATUSES:
                return True
            await asyncio.sleep(delay)
        return False


    async def read_dedupe_output(self,dma_audit_id: str,include_date: bool = False)->Union[bool, List[Dict[str, Any]]]:
        
        """
        Return:
          - True  -> when DMASA ReadOutput is empty (no rows yet)
          - List[Dict] -> when DMASA ReadOutput has rows.
                        Each dict contains: DataEntry (cell), DateAdded, OptedOut (bool)
        """
        params = {"MemberID": self.dmasa_member_id,"API_Key": self.dmasa_api_key,"AuditId": dma_audit_id,"Include": "1" if include_date else ""}
        resp = await self.client.get(self.read_dedupe_output_url, params=params, timeout=300)
        resp.raise_for_status()

        if "application/json" not in resp.headers.get("content-type", "").lower():
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY,detail=f"Non-JSON response from DMASA: {resp.text[:300]}")
        
        result = resp.json()
        if result.get("Errors"):
            dma_logger.exception(f"dma errors occurred while fetch dma records:{result.get("Errors")}")
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,detail=f"DMASA ReadOutput errors: {result['Errors']}")
        rows=result.get("ReadOutput", [])
        print("print the rows from dmasa")
        print(rows)

        if len(rows) == 0:
            return True
        normalized: List[Dict[str, Any]] = []
        for r in rows:
            value = (r.get("DataEntry") or "").strip()
            raw_opted=r.get("OptedOut")
            if isinstance(raw_opted,bool):
                opted=raw_opted
            elif isinstance(raw_opted,str):
                opted=raw_opted.strip().lower()=="true"
            else:
                opted=False
            normalized.append({"DataEntry": value,"DateAdded": r.get("DateAdded", ""),"OptedOut": opted})

        return normalized

    async def wait_for_readoutput_non_empty(self,dma_audit_id: str,session:AsyncSession,attempts: int = 5,delay: int = 3,include_date: bool = False) ->Union[bool,Tuple[List[Dict],List[Dict]]]:
        try:
            attempts = int(attempts)
            delay = int(delay)
        except (TypeError, ValueError):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="attempts and delay must be integers")
        
        for _ in range(attempts):
            #check the dedupe status again
            status_value=await self.check_dedupe_status(audit_id=dma_audit_id,session=session)
            print()
            print("print the dedupe status")
            print(status_value)
            print()
            output = await self.read_dedupe_output(dma_audit_id, include_date)

            print("print the response after reading dedupe")
            print(output)

            # output is True return immediately since there are no opted ins
            if output:
                print("print return from dedupe assuming that the response is True")
                #indicate the number of records returned
                return True
            
            opted_in:List[Dict]=[]
            opted_out:List[Dict]=[]

            for item in output:
                number=item.get("DataEntry")
                if not number:
                    continue
                normalized_item={**item,"DataEntry":f"0{number}"}
                if normalized_item.get("OptedOut") is True:
                    opted_in.append(normalized_item)
                else:
                    opted_out.append(normalized_item)
            return opted_out,opted_in
        # Safe fallback but will be rarely hit
        return True
    

    async def reconcile_dedupe_outputs(self,session: AsyncSession,audit_ids: Optional[List[str]] = None,limit: int = 200,include_date: bool = False) -> Dict[str, List[Dict[str, Any]]]:
        """
        Scan dma_audit_id_tbl by audit_id and reconcile state using DMASA ReadOutput:

        - If ReadOutput empty (read_dedupe_output returns True):
            is_processed = False
            dedupe_status = "Dedupe Incomplete"

        - If ReadOutput has rows (read_dedupe_output returns list[dict]):
            is_processed = True
            dedupe_status = "Dedupe Complete"
            and return the documents (list[dict])

        Returns:
            dict[audit_id] = list_of_dicts ONLY for audits with non-empty output.
        """
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="limit must be an integer",
            )

        query = select(dma_audit_id_tbl)

        if audit_ids:
            query = query.where(dma_audit_id_tbl.audit_id.in_(audit_ids))
        else:
            query = query.where(dma_audit_id_tbl.is_processed == False)  # noqa: E712

        query = query.limit(limit)

        records = (await session.exec(query)).all()

        completed: Dict[str, List[Dict[str, Any]]] = {}

        for rec in records:
            audit_id = str(rec.audit_id)

            try:
                output = await self.read_dedupe_output(dma_audit_id=audit_id, include_date=include_date)

                # output True => empty
                if output is True:
                    rec.is_processed = False
                    rec.dedupe_status = "Dedupe Incomplete"
                    session.add(rec)
                    continue

                # output list => complete
                rec.is_processed = True
                rec.dedupe_status = "Dedupe Complete"
                session.add(rec)

                completed[audit_id] = output

            except HTTPException as e:
                rec.is_processed = False
                rec.dedupe_status = "Dedupe Incomplete"
                session.add(rec)
                dma_logger.error(f"Reconcile failed for audit_id={audit_id}: {e.detail}")

            except Exception as e:
                rec.is_processed = False
                rec.dedupe_status = "Dedupe Incomplete"
                session.add(rec)
                dma_logger.exception(f"Unexpected reconcile error for audit_id={audit_id}: {str(e)}")

        await session.commit()
        return completed


async def get_dmasa_service_helper():
    
    service = DMAClassHelper()
    try:
        yield service
    finally:
        await service.close()


DMAService = Annotated[DMAClassHelper, Depends(get_dmasa_service_helper)]
