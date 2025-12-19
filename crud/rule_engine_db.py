from sqlmodel import select
from sqlalchemy.ext.asyncio.session import AsyncSession
from models.person_table import Person
from models.rules_table import rules_tbl
from schemas.person import PersonCreate
#create the test person
async def create_person_db(person:PersonCreate,session:AsyncSession):
    db_person=Person(**person.model_dump())
    session.add(db_person)
    await session.commit()
    await session.refresh(db_person)
    return db_person

async def get_rule_by_name_db(name:str,session:AsyncSession):
    db_person_query=select(rules_tbl).where(rules_tbl.rule_name==name)
    db_person=await session.execute(db_person_query)
    result=db_person.first()
    if result==None:
        return None
    return result
