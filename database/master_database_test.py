
from typing import AsyncGenerator
from sqlmodel.ext.asyncio.session import AsyncSession
from settings.Settings import get_settings
from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker

DATABASE_URL=f"postgresql+asyncpg://{get_settings().master_db_name}:{get_settings().master_db_password}@{get_settings().master_db_host_name}:{get_settings().master_db_port_number}/{get_settings().master_db_name}"

master_async_engine=create_async_engine(DATABASE_URL,echo=False,future=True,pool_timeout=30,pool_recycle=1800,pool_size=10,max_overflow=20)

#session factory
async_session_maker=async_sessionmaker(
    bind=master_async_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

#dependency injection
async def get_async_master_test_session()->AsyncGenerator[AsyncSession,None]:

    async with async_session_maker() as session:
        try:
            yield session 
        #ensure session closes after session
        finally:
            await session.close()




