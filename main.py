from fastapi import FastAPI,status,Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html,get_redoc_html
from fastapi.openapi.utils import get_openapi
from starlette.responses import JSONResponse
from contextlib import asynccontextmanager
from routers.authentication import auth_router
from routers.health_check import health_router
from routers.campaigns import campaigns_router
from routers.dnc_routes import dnc_router
from routers.dedupes import dedupe_routes
from routers.campaign_rules import campaign_rule_router
from routers.pings import ping_router
from routers.dma_services_route import dma_service_router
from routers.insert_data import insert_data_router
from routers.practice_rule import practice_rule_router
from routers.master_db_test_route import practice_router
from routers.data_extraction import data_extraction_router
from database.master_database_prod import master_async_engine
from sqlalchemy import text
from utils.auth import require_docs_auth
from schedulers.dmasa_reconcile_job import start_dmasa_scheduler
#from database.database import create_db_and_tables

@asynccontextmanager
async def lifespan(app:FastAPI):
    #---startup---
    #verify DB is reachable
    async with master_async_engine.begin() as conn:
        await conn.execute(text("SELECT 1"))
    yield
    #Engine disposal on shutdown
    await master_async_engine.dispose()


app=FastAPI(lifespan=lifespan,description="ALS API, ADMINISTRATORS CAN CREATE CAMPAIGNS AND CAMPAIGNS RULES. ADMIN AND USERS ARE FULLY AUTHENTICATED, MANAGE GENERIC AND DEDUPE CAMAPIGNS,DMA RECORDS AND DATA INSERTIONS",docs_url=None,redoc_url=None,openapi_url=None)



#Not best practice you need to filter the correct domain
#add cors middleware chief
origins=["http://localhost:8006","http://127.0.0.1:8005/auth/login","http://127.0.0.1:8005/auth/register"]

app.add_middleware(CORSMiddleware,allow_origins=origins,allow_credentials=True,allow_methods=["*"],allow_headers=["*"])
@app.get("/openapi.json",include_in_schema=False)
def openapi_json(_:bool=Depends(require_docs_auth)):
    return JSONResponse(get_openapi(title="ALS BACKEND API",version="1.0.0",routes=app.routes))

@app.get("/docs",include_in_schema=False)
def swagger_docs(_:bool=Depends(require_docs_auth)):
    return get_swagger_ui_html(openapi_url="/openapi.json",title="ALS BACKEND API DOCS")

@app.get("/redoc",include_in_schema=False)
def redoc(_:bool=Depends(require_docs_auth)):
    return get_redoc_html(openapi_url="/openapi.json", title="My API ReDoc")

#add shutdown and startup events
app.include_router(health_router)
app.include_router(auth_router)
app.include_router(campaigns_router)
app.include_router(dedupe_routes)
app.include_router(dnc_router)
app.include_router(campaign_rule_router)
app.include_router(dma_service_router)
app.include_router(insert_data_router)
app.include_router(practice_router)

