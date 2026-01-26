from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from contextlib import asynccontextmanager
from utils.dmasa_service_helpers import DMAClassHelper
from database.master_database_prod import async_sessionmaker
from sqlalchemy import text
from sqlalchemy.ext.asyncio.session import AsyncSession
from utils.logger import define_logger

scheduler_logger = define_logger("dmasa_scheduler", "logs/dmasa_scheduler.log")

scheduler=AsyncIOScheduler()

async def _try_advisory_locker(session:AsyncSession,lock_key:int=987654321)->bool:
    """
    Returns True only for the instance that acquires the lock.
    Others should exit the job immediately.
    """

    res = await session.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": lock_key})

    return bool(res.scalar())

#Here we are making sure that only one instance can start the job

async def reconcile_dmasa_job():

    scheduler_logger.info("DMASA reconcile job started")
    dma_helper=DMAClassHelper()
    try:
        async with async_sessionmaker() as session:
            if not await _try_advisory_locker():
                scheduler_logger.info("DMASA reconcile job skipped (locked held by another instance)")
                return
            
            completed=await dma_helper.reconcile_dedupe_outputs(session)
            scheduler_logger.info(f"DMASA reconcile job complete:Completed audits={len(completed)}")
    
    except Exception as e:
        scheduler_logger.exception(f"an exception occurred while processing a scheduled job:{str(e)}")
    
    finally:
        await dma_helper.close()


def start_dmasa_scheduler():
    """
    Start APScheduler and register 30-minute interval job
    """
    scheduler.add_job(reconcile_dmasa_job,trigger=IntervalTrigger(minutes=30),id="dmasa_reconcile_job",replace_existing=True,max_instances=1,coalesce=True)
    scheduler.start()
    scheduler_logger.info(f"DMASA schedule job starts every 30 minutes")
