from fastapi import status,APIRouter
from datetime import datetime
health_router=APIRouter(prefix="/health",tags=["Health Check"])

@health_router.get("/",status_code=status.HTTP_200_OK)

async def service_health():
    return {
        "service":"service is healthy",
        "status":"ok",
        "timestamp":datetime.utcnow().isoformat()
        }