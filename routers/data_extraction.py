from fastapi import APIRouter,Depends,status
from fastapi.responses import StreamingResponse
from sqlalchemy import text
import anyio
from io import StringIO
import csv
import datetime
from sqlmodel.ext.asyncio.session import AsyncSession
from database.master_database_prod import get_async_master_prod_session

data_extraction_router=APIRouter(prefix="/export",tags=["Export Data"])

chunk_size = 1000  # number of rows per flush

@data_extraction_router.get("/data/txt",status_code=status.HTTP_200_OK,description="Download data in text files")
async def download_data_into_text_file(session:AsyncSession=Depends(get_async_master_prod_session)):
    filename = f"data_export_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"
    async def txt_stream():
        buffer=StringIO()
        # Header
        header = "|".join([
            "id", "title", "fore_name", "last_name", "cell", "email", "date_of_birth",
            "race", "gender", "marital_status", "salary", "status", "derived_income",
            "typedata", "extra_info", "created_at",
            "job", "occupation", "company",
            "line_one", "line_two", "line_three", "line_four",
            "postal_code", "province", "suburb", "city"
        ]) + "\n"
        yield header.encode('utf-8')

        query = text("""
            SELECT DISTINCT ON (i.id)
                i.*,
                COALESCE(et.job, ''), COALESCE(et.occupation, ''), COALESCE(et.company, ''),
                COALESCE(lt.line_one, ''), COALESCE(lt.line_two, ''), COALESCE(lt.line_three, ''),
                COALESCE(lt.line_four, ''), COALESCE(lt.postal_code, ''), 
                COALESCE(lt.province, ''), COALESCE(lt.suburb, ''), COALESCE(lt.city, '')
            FROM info_tbl i
            LEFT JOIN employment_tbl et ON i.cell = et.cell
            LEFT JOIN location_tbl lt   ON i.cell = lt.cell
            ORDER BY i.id
        """)

        async with session.stream(query) as stream:

            async for row in stream:
                line = "|".join(str(v) if v is not None else "" for v in row) + "\n"
                buffer.write(line)
                if stream._row_number % 25000 == 0:
                    yield buffer.getvalue().encode('utf-8')
                    buffer.seek(0); buffer.truncate(0)
                    await anyio.sleep(0)
            if buffer.tell():
                yield buffer.getvalue().encode('utf-8')

     
    return StreamingResponse(
        txt_stream(),
        media_type="text/plain",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Type": "text/plain; charset=utf-8"
        }
    )

@data_extraction_router.get("/data/csv",status_code=status.HTTP_200_OK,description="Download full data as CSV file")
async def download_data_into_csv_file(session: AsyncSession = Depends(get_async_master_prod_session)):

    filename = f"data_export_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    async def csv_stream():
        buffer = StringIO()
        writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL)

        # Header
        writer.writerow([
            "id", "title", "fore_name", "last_name", "cell", "email", "date_of_birth",
            "race", "gender", "marital_status", "salary", "status", "derived_income",
            "typedata", "extra_info", "created_at",
            "job", "occupation", "company",
            "line_one", "line_two", "line_three", "line_four",
            "postal_code", "province", "suburb", "city"
        ])

        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

        query = text("""
            SELECT DISTINCT ON (i.id)
                i.*,
                COALESCE(et.job, ''), COALESCE(et.occupation, ''), COALESCE(et.company, ''),
                COALESCE(lt.line_one, ''), COALESCE(lt.line_two, ''), COALESCE(lt.line_three, ''),
                COALESCE(lt.line_four, ''), COALESCE(lt.postal_code, ''),
                COALESCE(lt.province, ''), COALESCE(lt.suburb, ''), COALESCE(lt.city, '')
            FROM info_tbl i
            LEFT JOIN employment_tbl et ON i.cell = et.cell
            LEFT JOIN location_tbl lt ON i.cell = lt.cell
            ORDER BY i.id
        """)

        async with session.stream(query) as stream:
            async for row in stream:
                writer.writerow(row)

                if getattr(stream, "_row_number", 0) % 20_000 == 0:
                    yield buffer.getvalue()
                    buffer.seek(0)
                    buffer.truncate(0)
                    await anyio.sleep(0)

            # Final chunk
            if buffer.tell() > 0:
                yield buffer.getvalue()

    return StreamingResponse(
        csv_stream(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }
    )

