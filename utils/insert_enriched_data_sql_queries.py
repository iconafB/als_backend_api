from sqlalchemy import text


INFO_TBL_ENRICHED = text("""
INSERT INTO info_tbl (
    cell,
    id,
    title,
    fore_name,
    last_name,
    date_of_birth,
    created_at,
    race,
    gender,
    marital_status,
    salary,
    status,
    derived_income,
    typedata,
    extra_info
)
VALUES (
    :cell,
    :id,
    :title,
    :fore_name,
    :last_name,
    :date_of_birth,
    :created_at,
    :race,
    :gender,
    :marital_status,
    :salary,
    :status,
    :derived_income,
    :typedata,
    :extra_info
)
ON CONFLICT (cell)
DO UPDATE SET
    title = EXCLUDED.title,
    race = EXCLUDED.race,
    id = EXCLUDED.id,
    gender = EXCLUDED.gender,
    marital_status = EXCLUDED.marital_status,
    derived_income = EXCLUDED.derived_income
WHERE info_tbl.cell = EXCLUDED.cell
  AND (
        info_tbl.id = EXCLUDED.id
        OR info_tbl.fore_name = EXCLUDED.fore_name
        OR info_tbl.last_name = EXCLUDED.last_name
      )
RETURNING cell
"""
)

CONTACT_TBL_SQL = text("""
INSERT INTO contact_tbl (
    cell,
    home_number,
    work_number,
    mobile_number_one,
    mobile_number_two,
    mobile_number_three,
    mobile_number_four,
    mobile_number_five,
    mobile_number_six,
    email
)
VALUES (
    :cell,
    :home_number,
    :work_number,
    :mobile_number_one,
    :mobile_number_two,
    :mobile_number_three,
    :mobile_number_four,
    :mobile_number_five,
    :mobile_number_six,
    :email
)
ON CONFLICT (cell) DO NOTHING
RETURNING cell;
"""
)


FINANCE_TBL_SQL = text("""
INSERT INTO finance_tbl (
    cell,
    cipro_reg,
    deed_office_reg,
    vehicle_owner,
    credit_score,
    monthly_expenditure,
    owns_credit_card,
    credit_card_bal,
    owns_st_card,
    st_card_rem_bal,
    has_loan_acc,
    loan_acc_rem_bal,
    has_st_loan,
    st_loan_bal,
    has1mth_loan_bal,
    bal_1mth_load,
    sti_insurance,
    has_sequestration,
    has_admin_order,
    under_debt_review,
    has_judgements
)
VALUES (
    :cell,
    :cipro_reg,
    :deed_office_reg,
    :vehicle_owner,
    :credit_score,
    :monthly_expenditure,
    :owns_credit_card,
    :credit_card_bal,
    :owns_st_card,
    :st_card_rem_bal,
    :has_loan_acc,
    :loan_acc_rem_bal,
    :has_st_loan,
    :st_loan_bal,
    :has1mth_loan_bal,
    :bal_1mth_load,
    :sti_insurance,
    :has_sequestration,
    :has_admin_order,
    :under_debt_review,
    :has_judgements
)
ON CONFLICT (cell) DO NOTHING
RETURNING cell
"""
)


CAR_TBL_SQL = text("""
INSERT INTO car_tbl (
    cell,
    make,
    model,
    year
)
VALUES (
    :cell,
    :make,
    :model,
    :year
)
ON CONFLICT (cell) DO NOTHING
RETURNING cell;
"""
)


EMPLOYMENT_TBL_SQL = text("""
INSERT INTO employment_tbl (
    cell,
    job,
    occupation,
    company
)
VALUES (
    :cell,
    :job,
    :occupation,
    :company
)
ON CONFLICT (cell) DO NOTHING
RETURNING cell
""")


LOCATION_TBL_SQL = text("""
INSERT INTO location_tbl (
    cell,
    line_one,
    line_two,
    line_three,
    line_four,
    postal_code,
    province,
    city
)
VALUES (
    :cell,
    :line_one,
    :line_two,
    :line_three,
    :line_four,
    :postal_code,
    :province,
    :city
)
ON CONFLICT (cell) DO NOTHING
RETURNING cell
""")

