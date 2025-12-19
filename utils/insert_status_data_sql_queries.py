INFO_STATUS_SQL= """"
    INSERT INTO info_tbl(cell, id, fore_name, last_name, date_of_birth,created_at, gender, salary, status, typedata) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
    ON CONFLICT (cell) DO UPDATE SET created_at = EXCLUDED.created_at, salary = EXCLUDED.salary, status = EXCLUDED.status WHERE info_tbl.cell = EXCLUDED.cell and ((info_tbl.id = EXCLUDED.id) OR (info_tbl.fore_name = EXCLUDED.fore_name or info_tbl.last_name = EXCLUDED.last_name));
    """



LOCATION_STATUS_SQL= """ 
        INSERT INTO location_tbl(cell, line_one, line_two, suburb, city, postal_code) 
        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (cell) DO NOTHING
        """


CONTACT_STATUS_SQL= """"
    INSERT INTO contact_tbl(cell, email) 
    VALUES (%s, %s) 
    ON CONFLICT(cell) DO UPDATE SET email = EXCLUDED.email WHERE contact_tbl.cell = EXCLUDED.cell
    """

EMPLOYMENT_STATUS_SQL=""""
INSERT INTO employment_tbl(cell, company, job) 
VALUES (%s, %s, %s) 
ON CONFLICT (cell) DO NOTHING
"""


CAR_STATUS_SQL="""
INSERT INTO car_tbl(cell, make, model) 
VALUES (%s, %s, %s) 
ON CONFLICT (cell) DO NOTHING
"""


FINANCE_STATUS_SQL="""
    INSERT INTO finance_tbl(cell, bank, bal) 
    VALUES (%s, %s, %s) 
    ON CONFLICT (cell) DO NOTHING
    """

