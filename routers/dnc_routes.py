from fastapi import APIRouter,status,Depends,HTTPException,BackgroundTasks,UploadFile,File,Query
import pandas as pd
import re
import io
from utils.auth import get_current_user
from utils.logger import define_logger
from schemas.dnc_schemas import DNCNumberResponse
from utils.dnc_util import send_dnc_list_to_db

dnc_logger=define_logger("als dnc logs","logs/dnc_route.log")
SMALL_FILE_THRESHOLD = 5 * 1024 * 1024 #file threshold
TEN_DIGIT_PATTERN = re.compile(r'^\d{10}$')
dnc_router=APIRouter(tags=["DNC Enpoints"],prefix="/dnc")

#helper validate and extract 10-digit numbers from an iterable

def _extract_numbers(iterable):
    for raw in iterable:
        val=str(raw).strip()
        if TEN_DIGIT_PATTERN.match(val):
            yield val

@dnc_router.post("/add-blacklist",description="Add a list of numbers to blacklist locally",response_model=DNCNumberResponse)

async def add_to_dnc(bg_tasks:BackgroundTasks,camp_code:str=Query(...,description="Campaign Code"),file:UploadFile=File(...,description="Add File With numbers to blacklist"),user=Depends(get_current_user)):
    
    try:
        filename=(file.filename or "").strip().lower()

        if not filename:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="Empty file names are not allowed")
        contents:bytes=await file.read()
        file_size=len(contents)
        #list_contents=pd.DataFrame(contents.splitlines()).values.tolist()
        #dnc_list=[str(item[0]) for item in list_contents if re.match('^\d{10}$',str(item[0]))]

        dnc_list=[]

        if file_size<=SMALL_FILE_THRESHOLD:

            if filename.endswith(".txt"):
                text=contents.decode("utf-8")
                dnc_list=[num for num in _extract_numbers(text.splitlines())]
            
            elif filename.endswith(".csv"):

                df=pd.read_csv(io.StringIO(contents.decode("utf-8")),header=None,dtype=str)
                dnc_list=list(_extract_numbers(df.iloc[:,0]))

            elif filename.endswith((".xls",".xlsx")):
                excel_file=pd.ExcelFile(io.BytesIO(contents))
                for sheet in excel_file.sheet_names:
                    df=pd.read_excel(excel_file,sheet_name=sheet,header=None,dtype=str)
                    dnc_list.extend(_extract_numbers(df.iloc[:,0]))
            else:

                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"Unsupported file type.Allowed file type .txt,.csv, .xls, and .xlsx")

        #Larger files   
        else:
            if filename.endswith(".txt"):
                text=contents.decode("utf-8")
                dnc_list=[num for num in _extract_numbers(text.splitlines())]

            elif filename.endswith(".csv"):
                csv_io=io.BytesIO(contents)
                #ensure that pointer is at the start
                csv_io.seek(0)

                try:
                    for chunk in pl.read_csv(csv_io,has_header=False,separator=",",dtypes=[pl.Utf8],batch_size=100_000):
                        column_name=chunk.columns[0]
                        dnc_list.extend(_extract_numbers(chunk.get_column(column_name)))    
                except Exception as e:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=f"Invalid CSV format:{str(e)}")
            elif filename.endswith((".xls", ".xlsx")):

                bio=io.BytesIO(contents)
                try:
                    with pd.ExcelFile(bio,engine="openpyxl") as excel_file:
                        for sheet_name in excel_file.sheet_names:
                            for chunk_df in pd.read_excel(excel_file,sheet_name=sheet_name,header=None,dtype=str,engine="openpyxl",chunksize=100_000):
                                dnc_list.extend(_extract_numbers(chunk_df.iloc[:,0]))
                except ImportError:
                    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"openpyxl not installed. Required for.xlsx files")
                
            else:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="Unsupported file type. Use TXT, CSV, or Excel.")
            
        
        if len(dnc_list)>0:

            process_status=True
            result=str(len(dnc_list)) + 'records added to the dnc'
            
            bg_tasks.add_task(send_dnc_list_to_db,dnc_list,camp_code)

        
        else:
            process_status=False
            result='No valid 10-digit records were added to the dnc'
        dnc_logger.info(f"user:{user.id} with email:{user.email} added {len(dnc_list)} numbers to the dnc list")
        
        
        return DNCNumberResponse(status=process_status,message=result)
    
    except HTTPException:
        raise

    except Exception as e:
        dnc_logger.exception(f"exception occurred while adding number to dnc:{str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"error reading file")
