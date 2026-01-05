from sqlmodel import SQLModel
from typing import AsyncGenerator
from sqlmodel.ext.asyncio.session import AsyncSession
from settings.Settings import get_settings
from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker

DATABASE_URL=f"postgresql+asyncpg://{get_settings().database_owner}:{get_settings().database_password}@{get_settings().database_host_name}:{get_settings().database_port}/{get_settings().database_name}"


#create the connection engine with the master db
#master_db_engine=create_engine(MASTER_DB_URL,echo=True)

master_async_engine=create_async_engine(DATABASE_URL,echo=False,pool_timeout=30,pool_recycle=1800,pool_size=10,max_overflow=20,pool_pre_ping=True)

#session factory
async_session_maker=async_sessionmaker(bind=master_async_engine,class_=AsyncSession,expire_on_commit=False)

#dependency injection

async def get_async_session()->AsyncGenerator[AsyncSession,None]:

    async with async_session_maker() as session:
        try:
            yield session 
        #ensure session closes after session
        finally:
            await session.close()



#initialize the db with new tables
async def init_db():
    async with master_async_engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)



