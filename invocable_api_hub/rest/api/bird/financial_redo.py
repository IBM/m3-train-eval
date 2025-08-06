

from fastapi import APIRouter, Query
import sqlite3

app = APIRouter()

# Database connection
conn = sqlite3.connect('invocable_api_hub/db/bird/test/financial.sqlite', check_same_thread=False)
cursor = conn.cursor()

# Endpoint to get count of account_id based on district and frequency
@app.get("/v1/bird/financial/count_accounts_by_district_frequency", summary="Get count of account_id based on district and frequency")
async def get_count_accounts_by_district_frequency(district: str = Query(..., description="District name"), frequency: str = Query(..., description="Frequency type")):
    query = f"""
    SELECT COUNT(T2.account_id)
    FROM district AS T1
    INNER JOIN account AS T2 ON T1.district_id = T2.district_id
    WHERE T1.A3 = ? AND T2.frequency = ?
    """
    cursor.execute(query, (district, frequency))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of account_id based on district
@app.get("/v1/bird/financial/count_accounts_by_district", summary="Get count of account_id based on district")
async def get_count_accounts_by_district(district: str = Query(..., description="District name")):
    query = f"""
    SELECT COUNT(T1.account_id)
    FROM account AS T1
    INNER JOIN loan AS T2 ON T1.account_id = T2.account_id
    INNER JOIN district AS T3 ON T1.district_id = T3.district_id
    WHERE T3.A3 = ?
    """
    cursor.execute(query, (district,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get distinct year based on average values
@app.get("/v1/bird/financial/get_distinct_year", summary="Get distinct year based on average values")
async def get_distinct_year():
    query = """
    SELECT DISTINCT IIF(AVG(A13) > AVG(A12), '1996', '1995') FROM district
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"year": result[0]}

# Endpoint to get count of distinct district_id based on gender and A11 range
@app.get("/v1/bird/financial/count_distinct_districts_by_gender_a11", summary="Get count of distinct district_id based on gender and A11 range")
async def get_count_distinct_districts_by_gender_a11(gender: str = Query(..., description="Gender"), a11_min: int = Query(..., description="Minimum A11 value"), a11_max: int = Query(..., description="Maximum A11 value")):
    query = """
    SELECT COUNT(DISTINCT T2.district_id)
    FROM client AS T1
    INNER JOIN district AS T2 ON T1.district_id = T2.district_id
    WHERE T1.gender = ? AND T2.A11 BETWEEN ? AND ?
    """
    cursor.execute(query, (gender, a11_min, a11_max))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of client_id based on gender, district, and A11 value
@app.get("/v1/bird/financial/count_clients_by_gender_district_a11", summary="Get count of client_id based on gender, district, and A11 value")
async def get_count_clients_by_gender_district_a11(gender: str = Query(..., description="Gender"), district: str = Query(..., description="District name"), a11_value: int = Query(..., description="A11 value")):
    query = """
    SELECT COUNT(T1.client_id)
    FROM client AS T1
    INNER JOIN district AS T2 ON T1.district_id = T2.district_id
    WHERE T1.gender = ? AND T2.A3 = ? AND T2.A11 > ?
    """
    cursor.execute(query, (gender, district, a11_value))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get account_id and A11 range based on gender and district
@app.get("/v1/bird/financial/get_account_id_a11_range", summary="Get account_id and A11 range based on gender and district")
async def get_account_id_a11_range(gender: str = Query(..., description="Gender")):
    query = """
    SELECT T1.account_id, (SELECT MAX(A11) - MIN(A11) FROM district)
    FROM account AS T1
    INNER JOIN district AS T2 ON T1.district_id = T2.district_id
    INNER JOIN disp AS T3 ON T1.account_id = T3.account_id
    INNER JOIN client AS T4 ON T3.client_id = T4.client_id
    WHERE T2.district_id = (SELECT district_id FROM client WHERE gender = ? ORDER BY birth_date ASC LIMIT 1)
    ORDER BY T2.A11 DESC LIMIT 1
    """
    cursor.execute(query, (gender,))
    result = cursor.fetchone()
    return {"account_id": result[0], "a11_range": result[1]}

# Endpoint to get account_id based on client_id
@app.get("/v1/bird/financial/get_account_id_by_client_id", summary="Get account_id based on client_id")
async def get_account_id_by_client_id():
    query = """
    SELECT T1.account_id
    FROM account AS T1
    INNER JOIN disp AS T2 ON T1.account_id = T2.account_id
    INNER JOIN client AS T3 ON T2.client_id = T3.client_id
    INNER JOIN district AS T4 on T4.district_id = T1.district_id
    WHERE T2.client_id = (SELECT client_id FROM client ORDER BY birth_date DESC LIMIT 1)
    GROUP BY T4.A11, T1.account_id
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"account_id": result[0]}

# Endpoint to get count of account_id based on type and frequency
@app.get("/v1/bird/financial/count_accounts_by_type_frequency", summary="Get count of account_id based on type and frequency")
async def get_count_accounts_by_type_frequency(type: str = Query(..., description="Type"), frequency: str = Query(..., description="Frequency")):
    query = """
    SELECT COUNT(T1.account_id)
    FROM account AS T1
    INNER JOIN disp AS T2 ON T1.account_id = T2.account_id
    WHERE T2.type = ? AND T1.frequency = ?
    """
    cursor.execute(query, (type, frequency))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get client_id based on frequency and type
@app.get("/v1/bird/financial/get_client_id_by_frequency_type", summary="Get client_id based on frequency and type")
async def get_client_id_by_frequency_type(frequency: str = Query(..., description="Frequency"), type: str = Query(..., description="Type")):
    query = """
    SELECT T2.client_id
    FROM account AS T1
    INNER JOIN disp AS T2 ON T1.account_id = T2.account_id
    WHERE T1.frequency = ? AND T2.type = ?
    """
    cursor.execute(query, (frequency, type))
    result = cursor.fetchone()
    return {"client_id": result[0]}

# Endpoint to get account_id based on year and frequency
@app.get("/v1/bird/financial/get_account_id_by_year_frequency", summary="Get account_id based on year and frequency")
async def get_account_id_by_year_frequency(year: str = Query(..., description="Year"), frequency: str = Query(..., description="Frequency")):
    query = """
    SELECT T2.account_id
    FROM loan AS T1
    INNER JOIN account AS T2 ON T1.account_id = T2.account_id
    WHERE STRFTIME('%Y', T1.date) = ? AND T2.frequency = ?
    ORDER BY T1.amount LIMIT 1
    """
    cursor.execute(query, (year, frequency))
    result = cursor.fetchone()
    return {"account_id": result[0]}

# Endpoint to get account_id based on year and duration
@app.get("/v1/bird/financial/get_account_id_by_year_duration", summary="Get account_id based on year and duration")
async def get_account_id_by_year_duration(year: str = Query(..., description="Year"), duration: int = Query(..., description="Duration")):
    query = """
    SELECT T1.account_id
    FROM loan AS T1
    INNER JOIN account AS T2 ON T1.account_id = T2.account_id
    WHERE STRFTIME('%Y', T2.date) = ? AND T1.duration > ?
    ORDER BY T1.amount DESC LIMIT 1
    """
    cursor.execute(query, (year, duration))
    result = cursor.fetchone()
    return {"account_id": result[0]}

# Endpoint to get count of client_id based on gender, birth year, and district
@app.get("/v1/bird/financial/count_clients_by_gender_birth_year_district", summary="Get count of client_id based on gender, birth year, and district")
async def get_count_clients_by_gender_birth_year_district(gender: str = Query(..., description="Gender"), birth_year: str = Query(..., description="Birth year"), district: str = Query(..., description="District name")):
    query = """
    SELECT COUNT(T2.client_id)
    FROM district AS T1
    INNER JOIN client AS T2 ON T1.district_id = T2.district_id
    WHERE T2.gender = ? AND STRFTIME('%Y', T2.birth_date) < ? AND T1.A2 = ?
    """
    cursor.execute(query, (gender, birth_year, district))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get account_id based on year
@app.get("/v1/bird/financial/get_account_id_by_year", summary="Get account_id based on year")
async def get_account_id_by_year(year: str = Query(..., description="Year")):
    query = """
    SELECT account_id
    FROM trans
    WHERE STRFTIME('%Y', date) = ?
    ORDER BY date ASC LIMIT 1
    """
    cursor.execute(query, (year,))
    result = cursor.fetchone()
    return {"account_id": result[0]}

# Endpoint to get distinct account_id based on year and amount
@app.get("/v1/bird/financial/get_distinct_account_id_by_year_amount", summary="Get distinct account_id based on year and amount")
async def get_distinct_account_id_by_year_amount(year: str = Query(..., description="Year"), amount: int = Query(..., description="Amount")):
    query = """
    SELECT DISTINCT T2.account_id
    FROM trans AS T1
    INNER JOIN account AS T2 ON T1.account_id = T2.account_id
    WHERE STRFTIME('%Y', T2.date) < ? AND T1.amount > ?
    """
    cursor.execute(query, (year, amount))
    result = cursor.fetchone()
    return {"account_id": result[0]}

# Endpoint to get client_id based on issued date
@app.get("/v1/bird/financial/get_client_id_by_issued_date", summary="Get client_id based on issued date")
async def get_client_id_by_issued_date(issued_date: str = Query(..., description="Issued date")):
    query = """
    SELECT T2.client_id
    FROM client AS T1
    INNER JOIN disp AS T2 ON T1.client_id = T2.client_id
    INNER JOIN card AS T3 ON T2.disp_id = T3.disp_id
    WHERE T3.issued = ?
    """
    cursor.execute(query, (issued_date,))
    result = cursor.fetchone()
    return {"client_id": result[0]}

# Endpoint to get date based on amount and date
@app.get("/v1/bird/financial/get_date_by_amount_date", summary="Get date based on amount and date")
async def get_date_by_amount_date(amount: int = Query(..., description="Amount"), date: str = Query(..., description="Date")):
    query = """
    SELECT T1.date
    FROM account AS T1
    INNER JOIN trans AS T2 ON T1.account_id = T2.account_id
    WHERE T2.amount = ? AND T2.date = ?
    """
    cursor.execute(query, (amount, date))
    result = cursor.fetchone()
    return {"date": result[0]}

# Endpoint to get district_id based on date
@app.get("/v1/bird/financial/get_district_id_by_date", summary="Get district_id based on date")
async def get_district_id_by_date(date: str = Query(..., description="Date")):
    query = """
    SELECT T1.district_id
    FROM account AS T1
    INNER JOIN loan AS T2 ON T1.account_id = T2.account_id
    WHERE T2.date = ?
    """
    cursor.execute(query, (date,))
    result = cursor.fetchone()
    return {"district_id": result[0]}

# Endpoint to get amount based on issued date
@app.get("/v1/bird/financial/get_amount_by_issued_date", summary="Get amount based on issued date")
async def get_amount_by_issued_date(issued_date: str = Query(..., description="Issued date")):
    query = """
    SELECT T4.amount
    FROM card AS T1
    JOIN disp AS T2 ON T1.disp_id = T2.disp_id
    JOIN account AS T3 on T2.account_id = T3.account_id
    JOIN trans AS T4 on T3.account_id = T4.account_id
    WHERE T1.issued = ?
    ORDER BY T4.amount DESC LIMIT 1
    """
    cursor.execute(query, (issued_date,))
    result = cursor.fetchone()
    return {"amount": result[0]}

# Endpoint to get gender based on district
@app.get("/v1/bird/financial/get_gender_by_district", summary="Get gender based on district")
async def get_gender_by_district():
    query = """
    SELECT T2.gender
    FROM district AS T1
    INNER JOIN client AS T2 ON T1.district_id = T2.district_id
    ORDER BY T1.A11 DESC, T2.birth_date ASC LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"gender": result[0]}

# Endpoint to get amount based on loan amount and date
@app.get("/v1/bird/financial/get_amount_by_loan_amount_date", summary="Get amount based on loan amount and date")
async def get_amount_by_loan_amount_date():
    query = """
    SELECT T3.amount
    FROM loan AS T1
    INNER JOIN account AS T2 ON T1.account_id = T2.account_id
    INNER JOIN trans AS T3 ON T2.account_id = T3.account_id
    ORDER BY T1.amount DESC, T3.date ASC LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"amount": result[0]}

# Endpoint to get count of clients by gender and district
@app.get("/v1/bird/financial/client_count", summary="Get count of clients by gender and district")
async def get_client_count(gender: str = Query(..., description="Gender of the client"), district: str = Query(..., description="District name")):
    query = f"SELECT COUNT(T1.client_id) FROM client AS T1 INNER JOIN district AS T2 ON T1.district_id = T2.district_id WHERE T1.gender = ? AND T2.A2 = ?"
    cursor.execute(query, (gender, district))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get disp_id by date and amount
@app.get("/v1/bird/financial/disp_id", summary="Get disp_id by date and amount")
async def get_disp_id(date: str = Query(..., description="Date of the transaction"), amount: int = Query(..., description="Amount of the transaction")):
    query = f"SELECT T1.disp_id FROM disp AS T1 INNER JOIN account AS T2 ON T1.account_id = T2.account_id INNER JOIN trans AS T3 ON T2.account_id = T3.account_id WHERE T3.date = ? AND T3.amount = ?"
    cursor.execute(query, (date, amount))
    result = cursor.fetchall()
    return {"disp_id": [row[0] for row in result]}

# Endpoint to get count of accounts by year and district
@app.get("/v1/bird/financial/account_count", summary="Get count of accounts by year and district")
async def get_account_count(year: str = Query(..., description="Year of the account"), district: str = Query(..., description="District name")):
    query = f"SELECT COUNT(T2.account_id) FROM district AS T1 INNER JOIN account AS T2 ON T1.district_id = T2.district_id WHERE STRFTIME('%Y', T2.date) = ? AND T1.A2 = ?"
    cursor.execute(query, (year, district))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get district by birth_date and gender
@app.get("/v1/bird/financial/district_by_birth_date", summary="Get district by birth_date and gender")
async def get_district_by_birth_date(birth_date: str = Query(..., description="Birth date of the client"), gender: str = Query(..., description="Gender of the client")):
    query = f"SELECT T1.A2 FROM district AS T1 INNER JOIN client AS T2 ON T1.district_id = T2.district_id WHERE T2.birth_date = ? AND T2.gender = ?"
    cursor.execute(query, (birth_date, gender))
    result = cursor.fetchall()
    return {"district": [row[0] for row in result]}

# Endpoint to get birth_date by loan date and amount
@app.get("/v1/bird/financial/birth_date_by_loan", summary="Get birth_date by loan date and amount")
async def get_birth_date_by_loan(loan_date: str = Query(..., description="Date of the loan"), loan_amount: int = Query(..., description="Amount of the loan")):
    query = f"SELECT T4.birth_date FROM loan AS T1 INNER JOIN account AS T2 ON T1.account_id = T2.account_id INNER JOIN disp AS T3 ON T2.account_id = T3.account_id INNER JOIN client AS T4 ON T3.client_id = T4.client_id WHERE T1.date = ? AND T1.amount = ?"
    cursor.execute(query, (loan_date, loan_amount))
    result = cursor.fetchall()
    return {"birth_date": [row[0] for row in result]}

# Endpoint to get account_id by district and order by date
@app.get("/v1/bird/financial/account_id_by_district", summary="Get account_id by district and order by date")
async def get_account_id_by_district(district: str = Query(..., description="District name")):
    query = f"SELECT T1.account_id FROM account AS T1 INNER JOIN district AS T2 ON T1.district_id = T2.district_id WHERE T2.A3 = ? ORDER BY T1.date ASC LIMIT 1"
    cursor.execute(query, (district,))
    result = cursor.fetchone()
    return {"account_id": result[0]}

# Endpoint to get percentage of male clients by district
@app.get("/v1/bird/financial/percentage_male_clients", summary="Get percentage of male clients by district")
async def get_percentage_male_clients(district: str = Query(..., description="District name")):
    query = f"SELECT CAST(SUM(T1.gender = 'M') AS REAL) * 100 / COUNT(T1.client_id) FROM client AS T1 INNER JOIN district AS T2 ON T1.district_id = T2.district_id WHERE T2.A3 = ? GROUP BY T2.A4 ORDER BY T2.A4 DESC LIMIT 1"
    cursor.execute(query, (district,))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get balance change percentage by loan date
@app.get("/v1/bird/financial/balance_change_percentage", summary="Get balance change percentage by loan date")
async def get_balance_change_percentage(loan_date: str = Query(..., description="Date of the loan")):
    query = f"SELECT CAST((SUM(IIF(T3.date = '1998-12-27', T3.balance, 0)) - SUM(IIF(T3.date = '1993-03-22', T3.balance, 0))) AS REAL) * 100 / SUM(IIF(T3.date = '1993-03-22', T3.balance, 0)) FROM loan AS T1 INNER JOIN account AS T2 ON T1.account_id = T2.account_id INNER JOIN trans AS T3 ON T3.account_id = T2.account_id WHERE T1.date = ?"
    cursor.execute(query, (loan_date,))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get percentage of active loans
@app.get("/v1/bird/financial/percentage_active_loans", summary="Get percentage of active loans")
async def get_percentage_active_loans():
    query = f"SELECT (CAST(SUM(CASE WHEN status = 'A' THEN amount ELSE 0 END) AS REAL) * 100) / SUM(amount) FROM loan"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get percentage of loans with amount less than a threshold
@app.get("/v1/bird/financial/percentage_loans_less_than", summary="Get percentage of loans with amount less than a threshold")
async def get_percentage_loans_less_than(threshold: int = Query(..., description="Threshold amount")):
    query = f"SELECT CAST(SUM(status = 'C') AS REAL) * 100 / COUNT(account_id) FROM loan WHERE amount < ?"
    cursor.execute(query, (threshold,))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get account details by frequency and year
@app.get("/v1/bird/financial/account_details_by_frequency", summary="Get account details by frequency and year")
async def get_account_details_by_frequency(frequency: str = Query(..., description="Frequency of the account"), year: str = Query(..., description="Year of the account")):
    query = f"SELECT T1.account_id, T2.A2, T2.A3 FROM account AS T1 INNER JOIN district AS T2 ON T1.district_id = T2.district_id WHERE T1.frequency = ? AND STRFTIME('%Y', T1.date) = ?"
    cursor.execute(query, (frequency, year))
    result = cursor.fetchall()
    return {"account_details": [{"account_id": row[0], "A2": row[1], "A3": row[2]} for row in result]}

# Endpoint to get account details by district and year range
@app.get("/v1/bird/financial/account_details_by_district_year_range", summary="Get account details by district and year range")
async def get_account_details_by_district_year_range(district: str = Query(..., description="District name"), start_year: str = Query(..., description="Start year"), end_year: str = Query(..., description="End year")):
    query = f"SELECT T1.account_id, T1.frequency FROM account AS T1 INNER JOIN district AS T2 ON T1.district_id = T2.district_id WHERE T2.A3 = ? AND STRFTIME('%Y', T1.date) BETWEEN ? AND ?"
    cursor.execute(query, (district, start_year, end_year))
    result = cursor.fetchall()
    return {"account_details": [{"account_id": row[0], "frequency": row[1]} for row in result]}

# Endpoint to get account details by district
@app.get("/v1/bird/financial/account_details_by_district", summary="Get account details by district")
async def get_account_details_by_district(district: str = Query(..., description="District name")):
    query = f"SELECT T1.account_id, T1.date FROM account AS T1 INNER JOIN district AS T2 ON T1.district_id = T2.district_id WHERE T2.A2 = ?"
    cursor.execute(query, (district,))
    result = cursor.fetchall()
    return {"account_details": [{"account_id": row[0], "date": row[1]} for row in result]}

# Endpoint to get district details by loan_id
@app.get("/v1/bird/financial/district_details_by_loan_id", summary="Get district details by loan_id")
async def get_district_details_by_loan_id(loan_id: int = Query(..., description="Loan ID")):
    query = f"SELECT T2.A2, T2.A3 FROM account AS T1 INNER JOIN district AS T2 ON T1.district_id = T2.district_id INNER JOIN loan AS T3 ON T1.account_id = T3.account_id WHERE T3.loan_id = ?"
    cursor.execute(query, (loan_id,))
    result = cursor.fetchall()
    return {"district_details": [{"A2": row[0], "A3": row[1]} for row in result]}

# Endpoint to get account details by loan amount
@app.get("/v1/bird/financial/account_details_by_loan_amount", summary="Get account details by loan amount")
async def get_account_details_by_loan_amount(loan_amount: int = Query(..., description="Loan amount")):
    query = f"SELECT T1.account_id, T2.A2, T2.A3 FROM account AS T1 INNER JOIN district AS T2 ON T1.district_id = T2.district_id INNER JOIN loan AS T3 ON T1.account_id = T3.account_id WHERE T3.amount > ?"
    cursor.execute(query, (loan_amount,))
    result = cursor.fetchall()
    return {"account_details": [{"account_id": row[0], "A2": row[1], "A3": row[2]} for row in result]}

# Endpoint to get loan details by duration
@app.get("/v1/bird/financial/loan_details_by_duration", summary="Get loan details by duration")
async def get_loan_details_by_duration(duration: int = Query(..., description="Loan duration")):
    query = f"SELECT T3.loan_id, T2.A2, T2.A11 FROM account AS T1 INNER JOIN district AS T2 ON T1.district_id = T2.district_id INNER JOIN loan AS T3 ON T1.account_id = T3.account_id WHERE T3.duration = ?"
    cursor.execute(query, (duration,))
    result = cursor.fetchall()
    return {"loan_details": [{"loan_id": row[0], "A2": row[1], "A11": row[2]} for row in result]}

# Endpoint to get balance change percentage by loan status
@app.get("/v1/bird/financial/balance_change_percentage_by_status", summary="Get balance change percentage by loan status")
async def get_balance_change_percentage_by_status(status: str = Query(..., description="Loan status")):
    query = f"SELECT CAST((T3.A13 - T3.A12) AS REAL) * 100 / T3.A12 FROM loan AS T1 INNER JOIN account AS T2 ON T1.account_id = T2.account_id INNER JOIN district AS T3 ON T2.district_id = T3.district_id WHERE T1.status = ?"
    cursor.execute(query, (status,))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get percentage of accounts in a district by year
@app.get("/v1/bird/financial/percentage_accounts_by_district_year", summary="Get percentage of accounts in a district by year")
async def get_percentage_accounts_by_district_year(district: str = Query(..., description="District name"), year: str = Query(..., description="Year of the account")):
    query = f"SELECT CAST(SUM(T1.A2 = ?) AS REAL) * 100 / COUNT(account_id) FROM district AS T1 INNER JOIN account AS T2 ON T1.district_id = T2.district_id WHERE STRFTIME('%Y', T2.date) = ?"
    cursor.execute(query, (district, year))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get account_id by frequency
@app.get("/v1/bird/financial/account_id_by_frequency", summary="Get account_id by frequency")
async def get_account_id_by_frequency(frequency: str = Query(..., description="Frequency of the account")):
    query = f"SELECT account_id FROM account WHERE Frequency = ?"
    cursor.execute(query, (frequency,))
    result = cursor.fetchall()
    return {"account_id": [row[0] for row in result]}

# Endpoint to get count of clients by gender and district
@app.get("/v1/bird/financial/client_count_by_gender_district", summary="Get count of clients by gender and district")
async def get_client_count_by_gender_district(gender: str = Query(..., description="Gender of the client")):
    query = f"SELECT T2.A2, COUNT(T1.client_id) FROM client AS T1 INNER JOIN district AS T2 ON T1.district_id = T2.district_id WHERE T1.gender = ? GROUP BY T2.district_id, T2.A2 ORDER BY COUNT(T1.client_id) DESC LIMIT 9"
    cursor.execute(query, (gender,))
    result = cursor.fetchall()
    return {"client_count": [{"district": row[0], "count": row[1]} for row in result]}

# Endpoint to get distinct A2 from district with specific conditions
@app.get("/v1/bird/financial/distinct_a2", summary="Get distinct A2 from district with specific conditions")
async def get_distinct_a2(type: str = Query(..., description="Type of transaction"), date: str = Query(..., description="Date in YYYY-MM format")):
    query = f"""
    SELECT DISTINCT T1.A2
    FROM district AS T1
    INNER JOIN account AS T2 ON T1.district_id = T2.district_id
    INNER JOIN trans AS T3 ON T2.account_id = T3.account_id
    WHERE T3.type = ? AND T3.date LIKE ?
    ORDER BY A2 ASC
    LIMIT 10
    """
    cursor.execute(query, (type, date + '%'))
    result = cursor.fetchall()
    return result

# Endpoint to get count of account_id from district with specific conditions
@app.get("/v1/bird/financial/count_account_id_district", summary="Get count of account_id from district with specific conditions")
async def get_count_account_id_district(a3: str = Query(..., description="A3 value"), type: str = Query(..., description="Type of transaction")):
    query = f"""
    SELECT COUNT(T3.account_id)
    FROM district AS T1
    INNER JOIN client AS T2 ON T1.district_id = T2.district_id
    INNER JOIN disp AS T3 ON T2.client_id = T3.client_id
    WHERE T1.A3 = ? AND T3.type != ?
    """
    cursor.execute(query, (a3, type))
    result = cursor.fetchall()
    return result

# Endpoint to get A3 from account with specific conditions
@app.get("/v1/bird/financial/a3_from_account", summary="Get A3 from account with specific conditions")
async def get_a3_from_account(status: str = Query(..., description="Status of loan")):
    query = f"""
    SELECT T2.A3
    FROM account AS T1
    INNER JOIN district AS T2 ON T1.district_id = T2.district_id
    INNER JOIN loan AS T3 ON T1.account_id = T3.account_id
    WHERE T3.status IN ({','.join(['?']*len(status.split(',')))})
    GROUP BY T2.A3
    ORDER BY SUM(T3.amount) DESC
    LIMIT 1
    """
    cursor.execute(query, status.split(','))
    result = cursor.fetchall()
    return result

# Endpoint to get average amount from client with specific conditions
@app.get("/v1/bird/financial/avg_amount_client", summary="Get average amount from client with specific conditions")
async def get_avg_amount_client(gender: str = Query(..., description="Gender of client")):
    query = f"""
    SELECT AVG(T4.amount)
    FROM client AS T1
    INNER JOIN disp AS T2 ON T1.client_id = T2.client_id
    INNER JOIN account AS T3 ON T2.account_id = T3.account_id
    INNER JOIN loan AS T4 ON T3.account_id = T4.account_id
    WHERE T1.gender = ?
    """
    cursor.execute(query, (gender,))
    result = cursor.fetchall()
    return result

# Endpoint to get district_id and A2 from district
@app.get("/v1/bird/financial/district_id_a2", summary="Get district_id and A2 from district")
async def get_district_id_a2():
    query = f"""
    SELECT district_id, A2
    FROM district
    ORDER BY A13 DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get count of account_id from district grouped by A16
@app.get("/v1/bird/financial/count_account_id_grouped", summary="Get count of account_id from district grouped by A16")
async def get_count_account_id_grouped():
    query = f"""
    SELECT COUNT(T2.account_id)
    FROM district AS T1
    INNER JOIN account AS T2 ON T1.district_id = T2.district_id
    GROUP BY T1.A16
    ORDER BY T1.A16 DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get count of account_id from trans with specific conditions
@app.get("/v1/bird/financial/count_account_id_trans", summary="Get count of account_id from trans with specific conditions")
async def get_count_account_id_trans(balance: float = Query(..., description="Balance value"), operation: str = Query(..., description="Operation type"), frequency: str = Query(..., description="Frequency type")):
    query = f"""
    SELECT COUNT(T1.account_id)
    FROM trans AS T1
    INNER JOIN account AS T2 ON T1.account_id = T2.account_id
    WHERE T1.balance < ? AND T1.operation = ? AND T2.frequency = ?
    """
    cursor.execute(query, (balance, operation, frequency))
    result = cursor.fetchall()
    return result

# Endpoint to get count of account_id from account with specific conditions
@app.get("/v1/bird/financial/count_account_id_account", summary="Get count of account_id from account with specific conditions")
async def get_count_account_id_account(start_date: str = Query(..., description="Start date in YYYY-MM-DD format"), end_date: str = Query(..., description="End date in YYYY-MM-DD format"), frequency: str = Query(..., description="Frequency type"), amount: float = Query(..., description="Amount value")):
    query = f"""
    SELECT COUNT(T1.account_id)
    FROM account AS T1
    INNER JOIN loan AS T2 ON T1.account_id = T2.account_id
    WHERE T2.date BETWEEN ? AND ? AND T1.frequency = ? AND T2.amount >= ?
    """
    cursor.execute(query, (start_date, end_date, frequency, amount))
    result = cursor.fetchall()
    return result

# Endpoint to get count of account_id from account with specific conditions
@app.get("/v1/bird/financial/count_account_id_district_loan", summary="Get count of account_id from account with specific conditions")
async def get_count_account_id_district_loan(district_id: int = Query(..., description="District ID"), status: str = Query(..., description="Status of loan")):
    query = f"""
    SELECT COUNT(T1.account_id)
    FROM account AS T1
    INNER JOIN district AS T2 ON T1.district_id = T2.district_id
    INNER JOIN loan AS T3 ON T1.account_id = T3.account_id
    WHERE T1.district_id = ? AND (T3.status = ? OR T3.status = ?)
    """
    cursor.execute(query, (district_id, status, status))
    result = cursor.fetchall()
    return result

# Endpoint to get count of client_id from client with specific conditions
@app.get("/v1/bird/financial/count_client_id_district", summary="Get count of client_id from client with specific conditions")
async def get_count_client_id_district(gender: str = Query(..., description="Gender of client"), a15: str = Query(..., description="A15 value")):
    query = f"""
    SELECT COUNT(T1.client_id)
    FROM client AS T1
    INNER JOIN district AS T2 ON T1.district_id = T2.district_id
    WHERE T1.gender = ? AND T2.A15 = (SELECT T3.A15 FROM district AS T3 ORDER BY T3.A15 DESC LIMIT 1, 1)
    """
    cursor.execute(query, (gender,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of card_id from card with specific conditions
@app.get("/v1/bird/financial/count_card_id_disp", summary="Get count of card_id from card with specific conditions")
async def get_count_card_id_disp(card_type: str = Query(..., description="Type of card"), disp_type: str = Query(..., description="Type of disp")):
    query = f"""
    SELECT COUNT(T1.card_id)
    FROM card AS T1
    INNER JOIN disp AS T2 ON T1.disp_id = T2.disp_id
    WHERE T1.type = ? AND T2.type = ?
    """
    cursor.execute(query, (card_type, disp_type))
    result = cursor.fetchall()
    return result

# Endpoint to get count of account_id from account with specific conditions
@app.get("/v1/bird/financial/count_account_id_district_a2", summary="Get count of account_id from account with specific conditions")
async def get_count_account_id_district_a2(a2: str = Query(..., description="A2 value")):
    query = f"""
    SELECT COUNT(T1.account_id)
    FROM account AS T1
    INNER JOIN district AS T2 ON T1.district_id = T2.district_id
    WHERE T2.A2 = ?
    """
    cursor.execute(query, (a2,))
    result = cursor.fetchall()
    return result

# Endpoint to get district_id from account with specific conditions
@app.get("/v1/bird/financial/district_id_from_account", summary="Get district_id from account with specific conditions")
async def get_district_id_from_account(year: str = Query(..., description="Year in YYYY format"), amount: float = Query(..., description="Amount value")):
    query = f"""
    SELECT T1.district_id
    FROM account AS T1
    INNER JOIN district AS T2 ON T1.district_id = T2.district_id
    INNER JOIN trans AS T3 ON T1.account_id = T3.account_id
    WHERE STRFTIME('%Y', T3.date) = ?
    GROUP BY T1.district_id
    HAVING SUM(T3.amount) > ?
    """
    cursor.execute(query, (year, amount))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct account_id from trans with specific conditions
@app.get("/v1/bird/financial/distinct_account_id_trans", summary="Get distinct account_id from trans with specific conditions")
async def get_distinct_account_id_trans(k_symbol: str = Query(..., description="K symbol value"), a2: str = Query(..., description="A2 value")):
    query = f"""
    SELECT DISTINCT T2.account_id
    FROM trans AS T1
    INNER JOIN account AS T2 ON T1.account_id = T2.account_id
    INNER JOIN district AS T3 ON T2.district_id = T3.district_id
    WHERE T1.k_symbol = ? AND T3.A2 = ?
    """
    cursor.execute(query, (k_symbol, a2))
    result = cursor.fetchall()
    return result

# Endpoint to get account_id from disp with specific conditions
@app.get("/v1/bird/financial/account_id_from_disp", summary="Get account_id from disp with specific conditions")
async def get_account_id_from_disp(card_type: str = Query(..., description="Type of card")):
    query = f"""
    SELECT T2.account_id
    FROM disp AS T2
    INNER JOIN card AS T1 ON T1.disp_id = T2.disp_id
    WHERE T1.type = ?
    """
    cursor.execute(query, (card_type,))
    result = cursor.fetchall()
    return result

# Endpoint to get average amount from card with specific conditions
@app.get("/v1/bird/financial/avg_amount_card", summary="Get average amount from card with specific conditions")
async def get_avg_amount_card(year: str = Query(..., description="Year in YYYY format"), operation: str = Query(..., description="Operation type")):
    query = f"""
    SELECT AVG(T4.amount)
    FROM card AS T1
    INNER JOIN disp AS T2 ON T1.disp_id = T2.disp_id
    INNER JOIN account AS T3 ON T2.account_id = T3.account_id
    INNER JOIN trans AS T4 ON T3.account_id = T4.account_id
    WHERE STRFTIME('%Y', T4.date) = ? AND T4.operation = ?
    """
    cursor.execute(query, (year, operation))
    result = cursor.fetchall()
    return result

# Endpoint to get account_id from trans with specific conditions
@app.get("/v1/bird/financial/account_id_from_trans", summary="Get account_id from trans with specific conditions")
async def get_account_id_from_trans(year: str = Query(..., description="Year in YYYY format"), operation: str = Query(..., description="Operation type")):
    query = f"""
    SELECT T1.account_id
    FROM trans AS T1
    INNER JOIN account AS T2 ON T1.account_id = T2.account_id
    WHERE STRFTIME('%Y', T1.date) = ? AND T1.operation = ? AND T1.amount < (SELECT AVG(amount) FROM trans WHERE STRFTIME('%Y', date) = ?)
    """
    cursor.execute(query, (year, operation, year))
    result = cursor.fetchall()
    return result

# Endpoint to get client_id from client with specific conditions
@app.get("/v1/bird/financial/client_id_from_client", summary="Get client_id from client with specific conditions")
async def get_client_id_from_client(gender: str = Query(..., description="Gender of client")):
    query = f"""
    SELECT T1.client_id
    FROM client AS T1
    INNER JOIN disp AS T2 ON T1.client_id = T2.client_id
    INNER JOIN account AS T5 ON T2.account_id = T5.account_id
    INNER JOIN loan AS T3 ON T5.account_id = T3.account_id
    INNER JOIN card AS T4 ON T2.disp_id = T4.disp_id
    WHERE T1.gender = ?
    """
    cursor.execute(query, (gender,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of client_id from client with specific conditions
@app.get("/v1/bird/financial/count_client_id_district_a3", summary="Get count of client_id from client with specific conditions")
async def get_count_client_id_district_a3(gender: str = Query(..., description="Gender of client"), a3: str = Query(..., description="A3 value")):
    query = f"""
    SELECT COUNT(T1.client_id)
    FROM client AS T1
    INNER JOIN district AS T2 ON T1.district_id = T2.district_id
    WHERE T1.gender = ? AND T2.A3 = ?
    """
    cursor.execute(query, (gender, a3))
    result = cursor.fetchall()
    return result

# Endpoint to get account_id from district with specific conditions
@app.get("/v1/bird/financial/account_id_from_district", summary="Get account_id from district with specific conditions")
async def get_account_id_from_district(a2: str = Query(..., description="A2 value"), disp_type: str = Query(..., description="Type of disp")):
    query = f"""
    SELECT T2.account_id
    FROM district AS T1
    INNER JOIN account AS T2 ON T1.district_id = T2.district_id
    INNER JOIN disp AS T3 ON T2.account_id = T3.account_id
    WHERE T3.type = ? AND T1.A2 = ?
    """
    cursor.execute(query, (disp_type, a2))
    result = cursor.fetchall()
    return result

# Endpoint to get type from disp table
@app.get("/v1/bird/financial/disp_type", summary="Get type from disp table")
async def get_disp_type(min_a11: int = Query(..., description="Minimum value for A11"),
                        max_a11: int = Query(..., description="Maximum value for A11")):
    query = """
    SELECT T3.type FROM district AS T1
    INNER JOIN account AS T2 ON T1.district_id = T2.district_id
    INNER JOIN disp AS T3 ON T2.account_id = T3.account_id
    WHERE T3.type != 'OWNER' AND T1.A11 BETWEEN ? AND ?
    """
    cursor.execute(query, (min_a11, max_a11))
    result = cursor.fetchall()
    return result

# Endpoint to get count of account_id from trans table
@app.get("/v1/bird/financial/trans_account_count", summary="Get count of account_id from trans table")
async def get_trans_account_count(bank: str = Query(..., description="Bank name"),
                                  a3: str = Query(..., description="Value for A3")):
    query = """
    SELECT COUNT(T2.account_id) FROM district AS T1
    INNER JOIN account AS T2 ON T1.district_id = T2.district_id
    INNER JOIN trans AS T3 ON T2.account_id = T3.account_id
    WHERE T3.bank = ? AND T1.A3 = ?
    """
    cursor.execute(query, (bank, a3))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct A2 from district table
@app.get("/v1/bird/financial/distinct_a2", summary="Get distinct A2 from district table")
async def get_distinct_a2(type: str = Query(..., description="Type value")):
    query = """
    SELECT DISTINCT T1.A2 FROM district AS T1
    INNER JOIN account AS T2 ON T1.district_id = T2.district_id
    INNER JOIN trans AS T3 ON T2.account_id = T3.account_id
    WHERE T3.type = ?
    """
    cursor.execute(query, (type,))
    result = cursor.fetchall()
    return result

# Endpoint to get average A15 from district table
@app.get("/v1/bird/financial/avg_a15", summary="Get average A15 from district table")
async def get_avg_a15(year: int = Query(..., description="Year value"),
                      min_a15: int = Query(..., description="Minimum value for A15")):
    query = """
    SELECT AVG(T1.A15) FROM district AS T1
    INNER JOIN account AS T2 ON T1.district_id = T2.district_id
    WHERE STRFTIME('%Y', T2.date) >= ? AND T1.A15 > ?
    """
    cursor.execute(query, (str(year), min_a15))
    result = cursor.fetchall()
    return result

# Endpoint to get count of card_id from card table
@app.get("/v1/bird/financial/card_count", summary="Get count of card_id from card table")
async def get_card_count(card_type: str = Query(..., description="Card type"),
                         disp_type: str = Query(..., description="Disp type")):
    query = """
    SELECT COUNT(T1.card_id) FROM card AS T1
    INNER JOIN disp AS T2 ON T1.disp_id = T2.disp_id
    WHERE T1.type = ? AND T2.type = ?
    """
    cursor.execute(query, (card_type, disp_type))
    result = cursor.fetchall()
    return result

# Endpoint to get count of client_id from client table
@app.get("/v1/bird/financial/client_count", summary="Get count of client_id from client table")
async def get_client_count(gender: str = Query(..., description="Gender value"),
                           a2: str = Query(..., description="Value for A2")):
    query = """
    SELECT COUNT(T1.client_id) FROM client AS T1
    INNER JOIN district AS T2 ON T1.district_id = T2.district_id
    WHERE T1.gender = ? AND T2.A2 = ?
    """
    cursor.execute(query, (gender, a2))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of gold cards issued before 1998
@app.get("/v1/bird/financial/gold_card_percentage", summary="Get percentage of gold cards issued before 1998")
async def get_gold_card_percentage():
    query = """
    SELECT CAST(SUM(type = 'gold' AND STRFTIME('%Y', issued) < '1998') AS REAL) * 100 / COUNT(card_id) FROM card
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get client_id from disp table
@app.get("/v1/bird/financial/disp_client_id", summary="Get client_id from disp table")
async def get_disp_client_id(disp_type: str = Query(..., description="Disp type")):
    query = """
    SELECT T1.client_id FROM disp AS T1
    INNER JOIN account AS T3 ON T1.account_id = T3.account_id
    INNER JOIN loan AS T2 ON T3.account_id = T2.account_id
    WHERE T1.type = ?
    ORDER BY T2.amount DESC LIMIT 1
    """
    cursor.execute(query, (disp_type,))
    result = cursor.fetchall()
    return result

# Endpoint to get A15 from district table
@app.get("/v1/bird/financial/district_a15", summary="Get A15 from district table")
async def get_district_a15(account_id: int = Query(..., description="Account ID")):
    query = """
    SELECT T1.A15 FROM district AS T1
    INNER JOIN account AS T2 ON T1.district_id = T2.district_id
    WHERE T2.account_id = ?
    """
    cursor.execute(query, (account_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get district_id from order table
@app.get("/v1/bird/financial/order_district_id", summary="Get district_id from order table")
async def get_order_district_id(order_id: int = Query(..., description="Order ID")):
    query = """
    SELECT T3.district_id FROM `order` AS T1
    INNER JOIN account AS T2 ON T1.account_id = T2.account_id
    INNER JOIN district AS T3 ON T2.district_id = T3.district_id
    WHERE T1.order_id = ?
    """
    cursor.execute(query, (order_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get trans_id from client table
@app.get("/v1/bird/financial/client_trans_id", summary="Get trans_id from client table")
async def get_client_trans_id(client_id: int = Query(..., description="Client ID"),
                              operation: str = Query(..., description="Operation value")):
    query = """
    SELECT T4.trans_id FROM client AS T1
    INNER JOIN disp AS T2 ON T1.client_id = T2.client_id
    INNER JOIN account AS T3 ON T2.account_id = T3.account_id
    INNER JOIN trans AS T4 ON T3.account_id = T4.account_id
    WHERE T1.client_id = ? AND T4.operation = ?
    """
    cursor.execute(query, (client_id, operation))
    result = cursor.fetchall()
    return result

# Endpoint to get count of account_id from loan table
@app.get("/v1/bird/financial/loan_account_count", summary="Get count of account_id from loan table")
async def get_loan_account_count(frequency: str = Query(..., description="Frequency value"),
                                 max_amount: int = Query(..., description="Maximum amount")):
    query = """
    SELECT COUNT(T1.account_id) FROM loan AS T1
    INNER JOIN account AS T2 ON T1.account_id = T2.account_id
    WHERE T2.frequency = ? AND T1.amount < ?
    """
    cursor.execute(query, (frequency, max_amount))
    result = cursor.fetchall()
    return result

# Endpoint to get type from disp table
@app.get("/v1/bird/financial/disp_type_by_client", summary="Get type from disp table by client ID")
async def get_disp_type_by_client(client_id: int = Query(..., description="Client ID")):
    query = """
    SELECT T3.type FROM disp AS T1
    INNER JOIN client AS T2 ON T1.client_id = T2.client_id
    INNER JOIN card AS T3 ON T1.disp_id = T3.disp_id
    WHERE T2.client_id = ?
    """
    cursor.execute(query, (client_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get A3 from district table
@app.get("/v1/bird/financial/district_a3", summary="Get A3 from district table")
async def get_district_a3(client_id: int = Query(..., description="Client ID")):
    query = """
    SELECT T1.A3 FROM district AS T1
    INNER JOIN client AS T2 ON T1.district_id = T2.district_id
    WHERE T2.client_id = ?
    """
    cursor.execute(query, (client_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get A2 from district table
@app.get("/v1/bird/financial/district_a2_by_loan_status", summary="Get A2 from district table by loan status")
async def get_district_a2_by_loan_status(status: str = Query(..., description="Loan status")):
    query = """
    SELECT T1.A2 FROM district AS T1
    INNER JOIN account AS T2 ON T1.district_id = T2.district_id
    INNER JOIN loan AS T3 ON T2.account_id = T3.account_id
    WHERE T3.status = ?
    GROUP BY T1.district_id
    ORDER BY COUNT(T2.account_id) DESC LIMIT 1
    """
    cursor.execute(query, (status,))
    result = cursor.fetchall()
    return result

# Endpoint to get client_id from order table
@app.get("/v1/bird/financial/order_client_id", summary="Get client_id from order table")
async def get_order_client_id(order_id: int = Query(..., description="Order ID")):
    query = """
    SELECT T3.client_id FROM `order` AS T1
    INNER JOIN account AS T2 ON T1.account_id = T2.account_id
    INNER JOIN disp AS T4 ON T4.account_id = T2.account_id
    INNER JOIN client AS T3 ON T4.client_id = T3.client_id
    WHERE T1.order_id = ?
    """
    cursor.execute(query, (order_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get trans_id from district table
@app.get("/v1/bird/financial/district_trans_id", summary="Get trans_id from district table")
async def get_district_trans_id(district_id: int = Query(..., description="District ID")):
    query = """
    SELECT T3.trans_id FROM district AS T1
    INNER JOIN account AS T2 ON T1.district_id = T2.district_id
    INNER JOIN trans AS T3 ON T2.account_id = T3.account_id
    WHERE T1.district_id = ?
    """
    cursor.execute(query, (district_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of account_id from district table
@app.get("/v1/bird/financial/district_account_count", summary="Get count of account_id from district table")
async def get_district_account_count(a2: str = Query(..., description="Value for A2")):
    query = """
    SELECT COUNT(T2.account_id) FROM district AS T1
    INNER JOIN account AS T2 ON T1.district_id = T2.district_id
    WHERE T1.A2 = ?
    """
    cursor.execute(query, (a2,))
    result = cursor.fetchall()
    return result

# Endpoint to get client_id from card table
@app.get("/v1/bird/financial/card_client_id", summary="Get client_id from card table")
async def get_card_client_id(card_type: str = Query(..., description="Card type"),
                             issued_date: str = Query(..., description="Issued date")):
    query = """
    SELECT T2.client_id FROM card AS T1
    INNER JOIN disp AS T2 ON T1.disp_id = T2.disp_id
    WHERE T1.type = ? AND T1.issued >= ?
    """
    cursor.execute(query, (card_type, issued_date))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of female clients in a district
@app.get("/v1/bird/financial/female_client_percentage", summary="Get percentage of female clients in a district")
async def get_female_client_percentage(min_a11: int = Query(..., description="Minimum value for A11")):
    query = """
    SELECT CAST(SUM(T2.gender = 'F') AS REAL) * 100 / COUNT(T2.client_id) FROM district AS T1
    INNER JOIN client AS T2 ON T1.district_id = T2.district_id
    WHERE T1.A11 > ?
    """
    cursor.execute(query, (min_a11,))
    result = cursor.fetchall()
    return result

# Endpoint to get loan growth percentage
@app.get("/v1/bird/financial/loan_growth_percentage", summary="Get loan growth percentage for a given gender and type")
async def get_loan_growth_percentage(gender: str = Query(..., description="Gender of the client"), type: str = Query(..., description="Type of the disp")):
    query = """
    SELECT CAST((SUM(CASE WHEN STRFTIME('%Y', T1.date) = '1997' THEN T1.amount ELSE 0 END) - SUM(CASE WHEN STRFTIME('%Y', T1.date) = '1996' THEN T1.amount ELSE 0 END)) AS REAL) * 100 / SUM(CASE WHEN STRFTIME('%Y', T1.date) = '1996' THEN T1.amount ELSE 0 END)
    FROM loan AS T1
    INNER JOIN account AS T2 ON T1.account_id = T2.account_id
    INNER JOIN disp AS T3 ON T3.account_id = T2.account_id
    INNER JOIN client AS T4 ON T4.client_id = T3.client_id
    WHERE T4.gender = ? AND T3.type = ?
    """
    cursor.execute(query, (gender, type))
    result = cursor.fetchall()
    return result

# Endpoint to get transaction count
@app.get("/v1/bird/financial/transaction_count", summary="Get transaction count for a given year and operation")
async def get_transaction_count(year: int = Query(..., description="Year of the transaction"), operation: str = Query(..., description="Operation type")):
    query = """
    SELECT COUNT(account_id)
    FROM trans
    WHERE STRFTIME('%Y', date) > ? AND operation = ?
    """
    cursor.execute(query, (year, operation))
    result = cursor.fetchall()
    return result

# Endpoint to get district difference
@app.get("/v1/bird/financial/district_difference", summary="Get district difference for given regions")
async def get_district_difference(region1: str = Query(..., description="First region"), region2: str = Query(..., description="Second region")):
    query = """
    SELECT SUM(IIF(A3 = ?, A16, 0)) - SUM(IIF(A3 = ?, A16, 0))
    FROM district
    """
    cursor.execute(query, (region1, region2))
    result = cursor.fetchall()
    return result

# Endpoint to get disp type counts
@app.get("/v1/bird/financial/disp_type_counts", summary="Get disp type counts for a given account ID range")
async def get_disp_type_counts(start_id: int = Query(..., description="Start account ID"), end_id: int = Query(..., description="End account ID")):
    query = """
    SELECT SUM(type = 'OWNER') , SUM(type = 'DISPONENT')
    FROM disp
    WHERE account_id BETWEEN ? AND ?
    """
    cursor.execute(query, (start_id, end_id))
    result = cursor.fetchall()
    return result

# Endpoint to get account frequency and k_symbol
@app.get("/v1/bird/financial/account_frequency_k_symbol", summary="Get account frequency and k_symbol for a given account ID and total amount")
async def get_account_frequency_k_symbol(account_id: int = Query(..., description="Account ID"), total_amount: float = Query(..., description="Total amount")):
    query = """
    SELECT T1.frequency, T2.k_symbol
    FROM account AS T1
    INNER JOIN (
        SELECT account_id, k_symbol, SUM(amount) AS total_amount
        FROM `order`
        GROUP BY account_id, k_symbol
    ) AS T2 ON T1.account_id = T2.account_id
    WHERE T1.account_id = ? AND T2.total_amount = ?
    """
    cursor.execute(query, (account_id, total_amount))
    result = cursor.fetchall()
    return result

# Endpoint to get client birth year
@app.get("/v1/bird/financial/client_birth_year", summary="Get client birth year for a given account ID")
async def get_client_birth_year(account_id: int = Query(..., description="Account ID")):
    query = """
    SELECT STRFTIME('%Y', T1.birth_date)
    FROM client AS T1
    INNER JOIN disp AS T3 ON T1.client_id = T3.client_id
    INNER JOIN account AS T2 ON T3.account_id = T2.account_id
    WHERE T2.account_id = ?
    """
    cursor.execute(query, (account_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get account count for owner and frequency
@app.get("/v1/bird/financial/account_count_owner_frequency", summary="Get account count for owner and frequency")
async def get_account_count_owner_frequency(frequency: str = Query(..., description="Frequency type")):
    query = """
    SELECT COUNT(T1.account_id)
    FROM account AS T1
    INNER JOIN disp AS T2 ON T1.account_id = T2.account_id
    WHERE T2.type = 'OWNER' AND T1.frequency = ?
    """
    cursor.execute(query, (frequency,))
    result = cursor.fetchall()
    return result

# Endpoint to get loan amount and status for a client
@app.get("/v1/bird/financial/loan_amount_status", summary="Get loan amount and status for a given client ID")
async def get_loan_amount_status(client_id: int = Query(..., description="Client ID")):
    query = """
    SELECT T4.amount, T4.status
    FROM client AS T1
    INNER JOIN disp AS T2 ON T1.client_id = T2.client_id
    INNER JOIN account AS T3 on T2.account_id = T3.account_id
    INNER JOIN loan AS T4 ON T3.account_id = T4.account_id
    WHERE T1.client_id = ?
    """
    cursor.execute(query, (client_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get transaction balance and client gender
@app.get("/v1/bird/financial/transaction_balance_gender", summary="Get transaction balance and client gender for a given client ID and transaction ID")
async def get_transaction_balance_gender(client_id: int = Query(..., description="Client ID"), trans_id: int = Query(..., description="Transaction ID")):
    query = """
    SELECT T4.balance, T1.gender
    FROM client AS T1
    INNER JOIN disp AS T2 ON T1.client_id = T2.client_id
    INNER JOIN account AS T3 ON T2.account_id =T3.account_id
    INNER JOIN trans AS T4 ON T3.account_id = T4.account_id
    WHERE T1.client_id = ? AND T4.trans_id = ?
    """
    cursor.execute(query, (client_id, trans_id))
    result = cursor.fetchall()
    return result

# Endpoint to get card type for a client
@app.get("/v1/bird/financial/card_type", summary="Get card type for a given client ID")
async def get_card_type(client_id: int = Query(..., description="Client ID")):
    query = """
    SELECT T3.type
    FROM client AS T1
    INNER JOIN disp AS T2 ON T1.client_id = T2.client_id
    INNER JOIN card AS T3 ON T2.disp_id = T3.disp_id
    WHERE T1.client_id = ?
    """
    cursor.execute(query, (client_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get transaction sum for a client
@app.get("/v1/bird/financial/transaction_sum", summary="Get transaction sum for a given client ID and year")
async def get_transaction_sum(client_id: int = Query(..., description="Client ID"), year: int = Query(..., description="Year")):
    query = """
    SELECT SUM(T3.amount)
    FROM client AS T1
    INNER JOIN disp AS T4 ON T1.client_id = T4.client_id
    INNER JOIN account AS T2 ON T4.account_id = T2.account_id
    INNER JOIN trans AS T3 ON T2.account_id = T3.account_id
    WHERE STRFTIME('%Y', T3.date)= ? AND T1.client_id = ?
    """
    cursor.execute(query, (year, client_id))
    result = cursor.fetchall()
    return result

# Endpoint to get client and account IDs for a district and birth year range
@app.get("/v1/bird/financial/client_account_ids", summary="Get client and account IDs for a given district and birth year range")
async def get_client_account_ids(district: str = Query(..., description="District"), start_year: int = Query(..., description="Start birth year"), end_year: int = Query(..., description="End birth year")):
    query = """
    SELECT T1.client_id, T3.account_id
    FROM client AS T1
    INNER JOIN district AS T2 ON T1.district_id = T2.district_id
    INNER JOIN disp AS T4 ON T1.client_id = T4.client_id
    INNER JOIN account AS T3 ON T2.district_id = T3.district_id and T4.account_id = T3.account_id
    WHERE T2.A3 = ? AND STRFTIME('%Y', T1.birth_date) BETWEEN ? AND ?
    """
    cursor.execute(query, (district, start_year, end_year))
    result = cursor.fetchall()
    return result

# Endpoint to get top female clients by loan amount
@app.get("/v1/bird/financial/top_female_clients", summary="Get top female clients by loan amount")
async def get_top_female_clients(limit: int = Query(..., description="Limit of top clients")):
    query = """
    SELECT T1.client_id
    FROM client AS T1
    INNER JOIN disp AS T4 on T1.client_id= T4.client_id
    INNER JOIN account AS T2 ON T4.account_id = T2.account_id
    INNER JOIN loan AS T3 ON T2.account_id = T3.account_id and T4.account_id = T3.account_id
    WHERE T1.gender = 'F'
    ORDER BY T3.amount DESC
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    result = cursor.fetchall()
    return result

# Endpoint to get transaction count for a given birth year range, gender, amount, and k_symbol
@app.get("/v1/bird/financial/transaction_count_filtered", summary="Get transaction count for a given birth year range, gender, amount, and k_symbol")
async def get_transaction_count_filtered(start_year: int = Query(..., description="Start birth year"), end_year: int = Query(..., description="End birth year"), gender: str = Query(..., description="Gender"), amount: float = Query(..., description="Amount"), k_symbol: str = Query(..., description="K symbol")):
    query = """
    SELECT COUNT(T1.account_id)
    FROM trans AS T1
    INNER JOIN account AS T2 ON T1.account_id = T2.account_id
    INNER JOIN disp AS T4 ON T2.account_id = T4.account_id
    INNER JOIN client AS T3 ON T4.client_id = T3.client_id
    WHERE STRFTIME('%Y', T3.birth_date) BETWEEN ? AND ? AND T3.gender = ? AND T1.amount > ? AND T1.k_symbol = ?
    """
    cursor.execute(query, (start_year, end_year, gender, amount, k_symbol))
    result = cursor.fetchall()
    return result

# Endpoint to get account count for a given district and year
@app.get("/v1/bird/financial/account_count_district_year", summary="Get account count for a given district and year")
async def get_account_count_district_year(district: str = Query(..., description="District"), year: int = Query(..., description="Year")):
    query = """
    SELECT COUNT(T1.account_id)
    FROM account AS T1
    INNER JOIN district AS T2 ON T1.district_id = T2.district_id
    WHERE STRFTIME('%Y', T1.date) > ? AND T2.A2 = ?
    """
    cursor.execute(query, (year, district))
    result = cursor.fetchall()
    return result

# Endpoint to get client count for a given gender and card type
@app.get("/v1/bird/financial/client_count_gender_card_type", summary="Get client count for a given gender and card type")
async def get_client_count_gender_card_type(gender: str = Query(..., description="Gender"), card_type: str = Query(..., description="Card type")):
    query = """
    SELECT COUNT(T1.client_id)
    FROM client AS T1
    INNER JOIN disp AS T2 ON T1.client_id = T2.client_id
    INNER JOIN card AS T3 ON T2.disp_id = T3.disp_id
    WHERE T1.gender = ? AND T3.type = ?
    """
    cursor.execute(query, (gender, card_type))
    result = cursor.fetchall()
    return result

# Endpoint to get female client percentage for a given district
@app.get("/v1/bird/financial/female_client_percentage", summary="Get female client percentage for a given district")
async def get_female_client_percentage(district: str = Query(..., description="District")):
    query = """
    SELECT CAST(SUM(T2.gender = 'F') AS REAL) / COUNT(T2.client_id) * 100
    FROM district AS T1
    INNER JOIN client AS T2 ON T1.district_id = T2.district_id
    WHERE T1.A3 = ?
    """
    cursor.execute(query, (district,))
    result = cursor.fetchall()
    return result

# Endpoint to get male client percentage for a given frequency
@app.get("/v1/bird/financial/male_client_percentage", summary="Get male client percentage for a given frequency")
async def get_male_client_percentage(frequency: str = Query(..., description="Frequency")):
    query = """
    SELECT CAST(SUM(T1.gender = 'M') AS REAL) * 100 / COUNT(T1.client_id)
    FROM client AS T1
    INNER JOIN district AS T3 ON T1.district_id = T3.district_id
    INNER JOIN account AS T2 ON T2.district_id = T3.district_id
    INNER JOIN disp as T4 on T1.client_id = T4.client_id AND T2.account_id = T4.account_id
    WHERE T2.frequency = ?
    """
    cursor.execute(query, (frequency,))
    result = cursor.fetchall()
    return result

# Endpoint to get account count for a given frequency and type
@app.get("/v1/bird/financial/account_count_frequency_type", summary="Get account count for a given frequency and type")
async def get_account_count_frequency_type(frequency: str = Query(..., description="Frequency"), type: str = Query(..., description="Type")):
    query = """
    SELECT COUNT(T2.account_id)
    FROM account AS T1
    INNER JOIN disp AS T2 ON T2.account_id = T1.account_id
    WHERE T1.frequency = ? AND T2.type = ?
    """
    cursor.execute(query, (frequency, type))
    result = cursor.fetchall()
    return result

# Endpoint to get account ID for a given duration and year
@app.get("/v1/bird/financial/account_id_duration_year", summary="Get account ID for a given duration and year")
async def get_account_id_duration_year(duration: int = Query(..., description="Duration"), year: int = Query(..., description="Year")):
    query = """
    SELECT T1.account_id
    FROM loan AS T1
    INNER JOIN account AS T2 ON T1.account_id = T2.account_id
    WHERE T1.duration > ? AND STRFTIME('%Y', T2.date) < ?
    ORDER BY T1.amount ASC
    LIMIT 1
    """
    cursor.execute(query, (duration, year))
    result = cursor.fetchall()
    return result

# Endpoint to get account_id for a given gender
@app.get("/v1/bird/financial/account_id_by_gender", summary="Get account_id for a given gender")
async def get_account_id_by_gender(gender: str = Query(..., description="Gender of the client")):
    query = """
    SELECT T3.account_id
    FROM client AS T1
    INNER JOIN district AS T2 ON T1.district_id = T2.district_id
    INNER JOIN account AS T3 ON T2.district_id = T3.district_id
    INNER JOIN disp AS T4 ON T1.client_id = T4.client_id AND T4.account_id = T3.account_id
    WHERE T1.gender = ?
    ORDER BY T1.birth_date ASC, T2.A11 ASC
    LIMIT 1
    """
    cursor.execute(query, (gender,))
    result = cursor.fetchone()
    return {"account_id": result[0]}

# Endpoint to get count of clients born in a specific year and region
@app.get("/v1/bird/financial/client_count_by_year_and_region", summary="Get count of clients born in a specific year and region")
async def get_client_count_by_year_and_region(year: int = Query(..., description="Year of birth"), region: str = Query(..., description="Region of the client")):
    query = """
    SELECT COUNT(T1.client_id)
    FROM client AS T1
    INNER JOIN district AS T2 ON T1.district_id = T2.district_id
    WHERE STRFTIME('%Y', T1.birth_date) = ? AND T2.A3 = ?
    """
    cursor.execute(query, (year, region))
    result = cursor.fetchone()
    return {"client_count": result[0]}

# Endpoint to get count of accounts with specific loan duration and frequency
@app.get("/v1/bird/financial/account_count_by_loan_duration_and_frequency", summary="Get count of accounts with specific loan duration and frequency")
async def get_account_count_by_loan_duration_and_frequency(duration: int = Query(..., description="Loan duration"), frequency: str = Query(..., description="Frequency of the account")):
    query = """
    SELECT COUNT(T2.account_id)
    FROM account AS T1
    INNER JOIN loan AS T2 ON T1.account_id = T2.account_id
    WHERE T2.duration = ? AND T1.frequency = ?
    """
    cursor.execute(query, (duration, frequency))
    result = cursor.fetchone()
    return {"account_count": result[0]}

# Endpoint to get average loan amount for specific status and frequency
@app.get("/v1/bird/financial/average_loan_amount_by_status_and_frequency", summary="Get average loan amount for specific status and frequency")
async def get_average_loan_amount_by_status_and_frequency(status: str = Query(..., description="Loan status"), frequency: str = Query(..., description="Frequency of the account")):
    query = """
    SELECT AVG(T2.amount)
    FROM account AS T1
    INNER JOIN loan AS T2 ON T1.account_id = T2.account_id
    WHERE T2.status IN (?) AND T1.frequency = ?
    """
    cursor.execute(query, (status, frequency))
    result = cursor.fetchone()
    return {"average_loan_amount": result[0]}

# Endpoint to get client details for a specific type
@app.get("/v1/bird/financial/client_details_by_type", summary="Get client details for a specific type")
async def get_client_details_by_type(type: str = Query(..., description="Type of the client")):
    query = """
    SELECT T3.client_id, T2.district_id, T2.A2
    FROM account AS T1
    INNER JOIN district AS T2 ON T1.district_id = T2.district_id
    INNER JOIN disp AS T3 ON T1.account_id = T3.account_id
    WHERE T3.type = ?
    """
    cursor.execute(query, (type,))
    result = cursor.fetchall()
    return {"client_details": result}

# Endpoint to get client age for specific card and disp type
@app.get("/v1/bird/financial/client_age_by_card_and_disp_type", summary="Get client age for specific card and disp type")
async def get_client_age_by_card_and_disp_type(card_type: str = Query(..., description="Type of the card"), disp_type: str = Query(..., description="Type of the disp")):
    query = """
    SELECT T1.client_id, STRFTIME('%Y', CURRENT_TIMESTAMP) - STRFTIME('%Y', T3.birth_date)
    FROM disp AS T1
    INNER JOIN card AS T2 ON T2.disp_id = T1.disp_id
    INNER JOIN client AS T3 ON T1.client_id = T3.client_id
    WHERE T2.type = ? AND T1.type = ?
    """
    cursor.execute(query, (card_type, disp_type))
    result = cursor.fetchall()
    return {"client_age": result}
