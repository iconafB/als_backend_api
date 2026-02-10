from typing import AsyncGenerator
from sqlmodel.ext.asyncio.session import AsyncSession
from settings.Settings import get_settings
from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker

DATABASE_URL=f"postgresql+asyncpg://{get_settings().MASTER_DB_OWNER}:{get_settings().MASTER_DB_PASSWORD}@{get_settings().MASTER_DB_HOST_NAME}:{get_settings().MASTER_DB_PORT}/{get_settings().MASTER_DB_NAME}"

master_async_engine=create_async_engine(DATABASE_URL,echo=False,future=True,pool_timeout=30,pool_recycle=1800,pool_size=10,max_overflow=20)
#session factory
async_session_maker=async_sessionmaker(bind=master_async_engine,class_=AsyncSession,expire_on_commit=False)

#dependency injection

async def get_async_master_prod_session()->AsyncGenerator[AsyncSession,None]:

    async with async_session_maker() as session:
        try:
            yield session

        #ensures session closes after session

        finally:
            await session.close()



