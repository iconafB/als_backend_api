from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import or_, and_


def chunked(lst, size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]



def build_info_upsert_stmt(info_table):
   
    t = info_table.__table__ if hasattr(info_table, "__table__") else info_table
    stmt = insert(t)
    excluded = stmt.excluded

    where_clause = and_(
        t.c.cell == excluded.cell,
        or_(
            t.c.id == excluded.id,
            t.c.fore_name == excluded.fore_name,
            t.c.last_name == excluded.last_name,
        ),
    )

    return stmt.on_conflict_do_update(
        index_elements=[t.c.cell],
        set_={
            "created_at": excluded.created_at,
            "salary": excluded.salary,
            "status": excluded.status,
        },
        where=where_clause,
    )


def build_contact_upsert_stmt(contact_table):

    t = contact_table.__table__ if hasattr(contact_table, "__table__") else contact_table
    stmt = insert(t)
    excluded = stmt.excluded

    return stmt.on_conflict_do_update(
        index_elements=[t.c.cell],
        set_={"email": excluded.email},
        where=(t.c.cell == excluded.cell),
    )


def build_do_nothing_stmt(table, conflict_col_name="cell"):
    t = table.__table__ if hasattr(table, "__table__") else table
    stmt = insert(t)
    return stmt.on_conflict_do_nothing(index_elements=[getattr(t.c, conflict_col_name)])


