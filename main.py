from fastapi import FastAPI,Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.openapi.docs import get_redoc_html
from fastapi.security import HTTPBasicCredentials
import pytz 
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from routers.authentication import auth_router
from routers.campaigns import campaigns_router
from routers.dnc_routes import dnc_router
from routers.dedupes import dedupe_routes
from routers.campaign_rules import campaign_rule_router
from routers.black_list import black_router
from routers.pings import ping_router
from routers.dma_services_route import dma_service_router
from routers.leads_route import leads_router
from routers.insert_data import insert_data_router
from routers.practice_rule import practice_rule_router
from routers.master_db_test_route import practice_router
from routers.data_extraction import data_extraction_router
from database.master_db_connect import init_db,master_async_engine
from routers.leads_route import leads_router
from utils.security_helper import get_current_admin
from utils.pings import send_pings_to_dedago,send_pings_to_kuda,send_pings_to_troy,classify_model_type,send_numbers_6am

#from database.database import create_db_and_tables



@asynccontextmanager
async def lifespan(app:FastAPI):
    await init_db()
    yield
    #Engine disposal on shutdown
    await master_async_engine.dispose()


app=FastAPI(lifespan=lifespan,title="ALS BACKEND API",
            description="The ALS API receives the request from frontend to load a list for a specific campaign and ALS checks the data spec that needs to be used for a campaign",
            version="0.2.0",
            # docs_url=None,
            # redoc_url=None,
            # openapi_url="/openapi.json"
            )


scheduler=BackgroundScheduler(timezone=pytz.timezone("Africa/Johannesburg"))



#Not best practice you need to filter the correct domain


origins=["http://localhost:5173","http://127.0.0.1:8000/auth/login","http://127.0.0.1:8000/auth/register"]

app.add_middleware(CORSMiddleware,allow_origins=origins,allow_credentials=True,allow_methods=["*"],allow_headers=["*"])

#add cors middleware chief


#add shutdown and startup events

# @app.on_event("startup")
# def start_scheduler():

#     scheduler.add_job(send_numbers_6am,"cron",hour=8,minute=0)
#     scheduler.add_job(read_kuda_ping,"cron",hour=19,minute=0)
#     scheduler.add_job(classify_model_type,"cron",hour=20,minute=30)
#     scheduler.add_job(read_log_7pm,"cron",hour=20,minute=00)
#     scheduler.add_job(update_extract_date2,"cron",hour=21,minute=45)
#     return True

# @app.on_event("shutdown")
# def shutdown_scheduler():

#     return True



# @app.get("/docs",include_in_schema=False)
# def custom_swagger_ui(credentials:HTTPBasicCredentials=Depends(get_current_admin)):
#     return get_swagger_ui_html(openapi_url="/openapi.json",title="Secure Docs")


# @app.get("/redoc",include_in_schema=False)
# def redoc_ui(username:str=Depends(get_current_admin)):
#     return get_redoc_html(openapi_url=app.openapi_url,title="Secure Redoc")


# @app.get("/openapi.json",include_in_schema=False)
# def openapi(usrename:str=Depends(get_current_admin)):
#     return app.openapi()



@app.get("/")
async def health_check():
    return {"main":"test the als endpoints"}

app.include_router(auth_router)
app.include_router(campaigns_router)
app.include_router(dedupe_routes)
app.include_router(campaign_rule_router)
app.include_router(dma_service_router)
app.include_router(insert_data_router)
app.include_router(data_extraction_router)
#this should go baba loading should be automatic
app.include_router(black_router)
app.include_router(ping_router)
app.include_router(leads_router)
app.include_router(dnc_router)
app.include_router(practice_rule_router)
app.include_router(practice_router)



""" 
@app.on_event("startup")
    def on_start():
        create_db_and_tables()
        print("Table Created")

if __name__=="__main__":
    @app.on_event("startup")
    def on_start():
        create_db_and_tables()
        print("Table Created")

        
 """
