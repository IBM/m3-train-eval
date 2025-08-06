

from fastapi import APIRouter, Query
import sqlite3

app = APIRouter()

# Database connection
conn = sqlite3.connect('invocable_api_hub/db/bird/test/debit_card_specializing.sqlite', check_same_thread=False)
cursor = conn.cursor()

# Endpoint to get count of gas stations
@app.get("/v1/bird/debit_card_specializing/gasstations_count", summary="Get count of gas stations for a given country and segment")
async def get_gasstations_count(country: str = Query(..., description="Country code"), segment: str = Query(..., description="Segment type")):
    query = f"SELECT COUNT(GasStationID) FROM gasstations WHERE Country = '{country}' AND Segment = '{segment}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get currency ratio
@app.get("/v1/bird/debit_card_specializing/currency_ratio", summary="Get currency ratio between EUR and CZK")
async def get_currency_ratio():
    query = "SELECT CAST(SUM(IIF(Currency = 'EUR', 1, 0)) AS FLOAT) / SUM(IIF(Currency = 'CZK', 1, 0)) AS ratio FROM customers"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"ratio": result[0]}

# Endpoint to get customer with minimum consumption
@app.get("/v1/bird/debit_card_specializing/min_consumption_customer", summary="Get customer with minimum consumption for a given year and segment")
async def get_min_consumption_customer(year: int = Query(..., description="Year"), segment: str = Query(..., description="Segment type")):
    query = f"""
    SELECT T1.CustomerID FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T1.Segment = '{segment}' AND SUBSTR(T2.Date, 1, 4) = '{year}'
    GROUP BY T1.CustomerID
    ORDER BY SUM(T2.Consumption) ASC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"customer_id": result[0]}

# Endpoint to get average consumption
@app.get("/v1/bird/debit_card_specializing/avg_consumption", summary="Get average consumption for a given year and segment")
async def get_avg_consumption(year: int = Query(..., description="Year"), segment: str = Query(..., description="Segment type")):
    query = f"""
    SELECT AVG(T2.Consumption) / 12 FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE SUBSTR(T2.Date, 1, 4) = '{year}' AND T1.Segment = '{segment}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"avg_consumption": result[0]}

# Endpoint to get customer with maximum consumption
@app.get("/v1/bird/debit_card_specializing/max_consumption_customer", summary="Get customer with maximum consumption for a given year and currency")
async def get_max_consumption_customer(year: int = Query(..., description="Year"), currency: str = Query(..., description="Currency type")):
    query = f"""
    SELECT T1.CustomerID FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T1.Currency = '{currency}' AND T2.Date BETWEEN '{year}01' AND '{year}12'
    GROUP BY T1.CustomerID
    ORDER BY SUM(T2.Consumption) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"customer_id": result[0]}

# Endpoint to get count of customers with consumption below a threshold
@app.get("/v1/bird/debit_card_specializing/customers_below_threshold", summary="Get count of customers with consumption below a threshold for a given year and segment")
async def get_customers_below_threshold(year: int = Query(..., description="Year"), segment: str = Query(..., description="Segment type"), threshold: int = Query(..., description="Consumption threshold")):
    query = f"""
    SELECT COUNT(*) FROM (
        SELECT T2.CustomerID FROM customers AS T1
        INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
        WHERE T1.Segment = '{segment}' AND SUBSTRING(T2.Date, 1, 4) = '{year}'
        GROUP BY T2.CustomerID
        HAVING SUM(T2.Consumption) < {threshold}
    ) AS t1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get consumption difference between currencies
@app.get("/v1/bird/debit_card_specializing/consumption_difference", summary="Get consumption difference between CZK and EUR for a given year")
async def get_consumption_difference(year: int = Query(..., description="Year")):
    query = f"""
    SELECT SUM(IIF(T1.Currency = 'CZK', T2.Consumption, 0)) - SUM(IIF(T1.Currency = 'EUR', T2.Consumption, 0))
    FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE SUBSTR(T2.Date, 1, 4) = '{year}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"difference": result[0]}

# Endpoint to get year with maximum consumption for a given currency
@app.get("/v1/bird/debit_card_specializing/max_consumption_year", summary="Get year with maximum consumption for a given currency")
async def get_max_consumption_year(currency: str = Query(..., description="Currency type")):
    query = f"""
    SELECT SUBSTRING(T2.Date, 1, 4) FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T1.Currency = '{currency}'
    GROUP BY SUBSTRING(T2.Date, 1, 4)
    ORDER BY SUM(T2.Consumption) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"year": result[0]}

# Endpoint to get segment with minimum consumption
@app.get("/v1/bird/debit_card_specializing/min_consumption_segment", summary="Get segment with minimum consumption")
async def get_min_consumption_segment():
    query = """
    SELECT T1.Segment FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    GROUP BY T1.Segment
    ORDER BY SUM(T2.Consumption) ASC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"segment": result[0]}

# Endpoint to get year with maximum consumption for a given currency
@app.get("/v1/bird/debit_card_specializing/max_consumption_year_currency", summary="Get year with maximum consumption for a given currency")
async def get_max_consumption_year_currency(currency: str = Query(..., description="Currency type")):
    query = f"""
    SELECT SUBSTR(T2.Date, 1, 4) FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T1.Currency = '{currency}'
    GROUP BY SUBSTR(T2.Date, 1, 4)
    ORDER BY SUM(T2.Consumption) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"year": result[0]}

# Endpoint to get month with maximum consumption for a given year and segment
@app.get("/v1/bird/debit_card_specializing/max_consumption_month", summary="Get month with maximum consumption for a given year and segment")
async def get_max_consumption_month(year: int = Query(..., description="Year"), segment: str = Query(..., description="Segment type")):
    query = f"""
    SELECT SUBSTR(T2.Date, 5, 2) FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE SUBSTR(T2.Date, 1, 4) = '{year}' AND T1.Segment = '{segment}'
    GROUP BY SUBSTR(T2.Date, 5, 2)
    ORDER BY SUM(T2.Consumption) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"month": result[0]}

# Endpoint to get consumption difference between segments
@app.get("/v1/bird/debit_card_specializing/segment_consumption_difference", summary="Get consumption difference between segments for a given year and currency")
async def get_segment_consumption_difference(year: int = Query(..., description="Year"), currency: str = Query(..., description="Currency type")):
    query = f"""
    SELECT
        CAST(SUM(IIF(T1.Segment = 'SME', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID) -
        CAST(SUM(IIF(T1.Segment = 'LAM', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID),
        CAST(SUM(IIF(T1.Segment = 'LAM', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID) -
        CAST(SUM(IIF(T1.Segment = 'KAM', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID),
        CAST(SUM(IIF(T1.Segment = 'KAM', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID) -
        CAST(SUM(IIF(T1.Segment = 'SME', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID)
    FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T1.Currency = '{currency}' AND T2.Consumption = (
        SELECT MIN(Consumption) FROM yearmonth
    ) AND T2.Date BETWEEN '{year}01' AND '{year}12'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"difference_sme_lam": result[0], "difference_lam_kam": result[1], "difference_kam_sme": result[2]}

# Endpoint to get consumption growth rate
@app.get("/v1/bird/debit_card_specializing/consumption_growth_rate", summary="Get consumption growth rate for a given segment")
async def get_consumption_growth_rate(segment: str = Query(..., description="Segment type")):
    query = f"""
    SELECT
        CAST((SUM(IIF(T1.Segment = '{segment}' AND T2.Date LIKE '2013%', T2.Consumption, 0)) -
              SUM(IIF(T1.Segment = '{segment}' AND T2.Date LIKE '2012%', T2.Consumption, 0))) AS FLOAT) * 100 /
        SUM(IIF(T1.Segment = '{segment}' AND T2.Date LIKE '2012%', T2.Consumption, 0))
    FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"growth_rate": result[0]}

# Endpoint to get total consumption for a given customer and date range
@app.get("/v1/bird/debit_card_specializing/total_consumption", summary="Get total consumption for a given customer and date range")
async def get_total_consumption(customer_id: int = Query(..., description="Customer ID"), start_date: str = Query(..., description="Start date (YYYYMM)"), end_date: str = Query(..., description="End date (YYYYMM)")):
    query = f"""
    SELECT SUM(Consumption) FROM yearmonth
    WHERE CustomerID = {customer_id} AND Date BETWEEN '{start_date}' AND '{end_date}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"total_consumption": result[0]}

# Endpoint to get gas station count difference between countries
@app.get("/v1/bird/debit_card_specializing/gasstation_count_difference", summary="Get gas station count difference between countries for a given segment")
async def get_gasstation_count_difference(segment: str = Query(..., description="Segment type")):
    query = f"""
    SELECT SUM(IIF(Country = 'CZE', 1, 0)) - SUM(IIF(Country = 'SVK', 1, 0))
    FROM gasstations
    WHERE Segment = '{segment}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"difference": result[0]}

# Endpoint to get consumption difference between customers
@app.get("/v1/bird/debit_card_specializing/customer_consumption_difference", summary="Get consumption difference between customers for a given date")
async def get_customer_consumption_difference(customer_id1: int = Query(..., description="Customer ID 1"), customer_id2: int = Query(..., description="Customer ID 2"), date: str = Query(..., description="Date (YYYYMM)")):
    query = f"""
    SELECT SUM(IIF(CustomerID = {customer_id1}, Consumption, 0)) - SUM(IIF(CustomerID = {customer_id2}, Consumption, 0))
    FROM yearmonth
    WHERE Date = '{date}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"difference": result[0]}

# Endpoint to get currency count difference
@app.get("/v1/bird/debit_card_specializing/currency_count_difference", summary="Get currency count difference for a given segment")
async def get_currency_count_difference(segment: str = Query(..., description="Segment type")):
    query = f"""
    SELECT SUM(Currency = 'CZK') - SUM(Currency = 'EUR')
    FROM customers
    WHERE Segment = '{segment}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"difference": result[0]}

# Endpoint to get customer with maximum consumption for a given date, segment, and currency
@app.get("/v1/bird/debit_card_specializing/max_consumption_customer_date", summary="Get customer with maximum consumption for a given date, segment, and currency")
async def get_max_consumption_customer_date(date: str = Query(..., description="Date (YYYYMM)"), segment: str = Query(..., description="Segment type"), currency: str = Query(..., description="Currency type")):
    query = f"""
    SELECT T1.CustomerID FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T1.Segment = '{segment}' AND T2.Date = '{date}' AND T1.Currency = '{currency}'
    GROUP BY T1.CustomerID
    ORDER BY SUM(T2.Consumption) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"customer_id": result[0]}

# Endpoint to get customer with maximum consumption for a given segment
@app.get("/v1/bird/debit_card_specializing/max_consumption_customer_segment", summary="Get customer with maximum consumption for a given segment")
async def get_max_consumption_customer_segment(segment: str = Query(..., description="Segment type")):
    query = f"""
    SELECT T2.CustomerID, SUM(T2.Consumption) FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T1.Segment = '{segment}'
    GROUP BY T2.CustomerID
    ORDER BY SUM(T2.Consumption) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"customer_id": result[0], "total_consumption": result[1]}

# Endpoint to get total consumption for a given date and segment
@app.get("/v1/bird/debit_card_specializing/total_consumption_date_segment", summary="Get total consumption for a given date and segment")
async def get_total_consumption_date_segment(date: str = Query(..., description="Date (YYYYMM)"), segment: str = Query(..., description="Segment type")):
    query = f"""
    SELECT SUM(T2.Consumption) FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T2.Date = '{date}' AND T1.Segment = '{segment}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"total_consumption": result[0]}

# Endpoint to get percentage of customers with consumption greater than a threshold
@app.get("/v1/bird/debit_card_specializing/consumption_percentage", summary="Get percentage of customers with consumption greater than a threshold")
async def get_consumption_percentage(segment: str = Query(..., description="Segment of the customer"), threshold: float = Query(..., description="Consumption threshold")):
    query = f"""
    SELECT CAST(SUM(IIF(T2.Consumption > {threshold}, 1, 0)) AS FLOAT) * 100 / COUNT(T1.CustomerID)
    FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T1.Segment = '{segment}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get country with the highest number of gas stations in a specific segment
@app.get("/v1/bird/debit_card_specializing/top_country_gas_stations", summary="Get country with the highest number of gas stations in a specific segment")
async def get_top_country_gas_stations(segment: str = Query(..., description="Segment of the gas station")):
    query = f"""
    SELECT Country, (SELECT COUNT(GasStationID) FROM gasstations WHERE Segment = '{segment}')
    FROM gasstations
    WHERE Segment = '{segment}'
    GROUP BY Country
    ORDER BY COUNT(GasStationID) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"country": result[0], "count": result[1]}

# Endpoint to get percentage of customers with a specific currency in a segment
@app.get("/v1/bird/debit_card_specializing/currency_percentage", summary="Get percentage of customers with a specific currency in a segment")
async def get_currency_percentage(segment: str = Query(..., description="Segment of the customer"), currency: str = Query(..., description="Currency of the customer")):
    query = f"""
    SELECT CAST(SUM(Currency = '{currency}') AS FLOAT) * 100 / COUNT(CustomerID)
    FROM customers
    WHERE Segment = '{segment}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get percentage of customers with consumption greater than a threshold on a specific date
@app.get("/v1/bird/debit_card_specializing/consumption_percentage_date", summary="Get percentage of customers with consumption greater than a threshold on a specific date")
async def get_consumption_percentage_date(date: str = Query(..., description="Date in YYYYMM format"), threshold: float = Query(..., description="Consumption threshold")):
    query = f"""
    SELECT CAST(SUM(IIF(Consumption > {threshold}, 1, 0)) AS FLOAT) * 100 / COUNT(CustomerID)
    FROM yearmonth
    WHERE Date = '{date}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get percentage of gas stations in a specific segment in a country
@app.get("/v1/bird/debit_card_specializing/gas_station_percentage", summary="Get percentage of gas stations in a specific segment in a country")
async def get_gas_station_percentage(country: str = Query(..., description="Country of the gas station"), segment: str = Query(..., description="Segment of the gas station")):
    query = f"""
    SELECT CAST(SUM(IIF(Segment = '{segment}', 1, 0)) AS FLOAT) * 100 / COUNT(GasStationID)
    FROM gasstations
    WHERE Country = '{country}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get customer with the highest consumption on a specific date
@app.get("/v1/bird/debit_card_specializing/top_customer_consumption", summary="Get customer with the highest consumption on a specific date")
async def get_top_customer_consumption(date: str = Query(..., description="Date in YYYYMM format")):
    query = f"""
    SELECT T1.CustomerID
    FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T2.Date = '{date}'
    GROUP BY T1.CustomerID
    ORDER BY SUM(T2.Consumption) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"customer_id": result[0]}

# Endpoint to get segment of customer with the lowest consumption on a specific date
@app.get("/v1/bird/debit_card_specializing/lowest_consumption_segment", summary="Get segment of customer with the lowest consumption on a specific date")
async def get_lowest_consumption_segment(date: str = Query(..., description="Date in YYYYMM format")):
    query = f"""
    SELECT T1.Segment
    FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T2.Date = '{date}'
    GROUP BY T1.CustomerID
    ORDER BY SUM(T2.Consumption) ASC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"segment": result[0]}

# Endpoint to get customer with the lowest consumption in a specific segment on a specific date
@app.get("/v1/bird/debit_card_specializing/lowest_consumption_customer", summary="Get customer with the lowest consumption in a specific segment on a specific date")
async def get_lowest_consumption_customer(date: str = Query(..., description="Date in YYYYMM format"), segment: str = Query(..., description="Segment of the customer")):
    query = f"""
    SELECT T1.CustomerID
    FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T2.Date = '{date}' AND T1.Segment = '{segment}'
    GROUP BY T1.CustomerID
    ORDER BY SUM(T2.Consumption) ASC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"customer_id": result[0]}

# Endpoint to get month with the highest consumption in a specific year
@app.get("/v1/bird/debit_card_specializing/highest_consumption_month", summary="Get month with the highest consumption in a specific year")
async def get_highest_consumption_month(year: str = Query(..., description="Year in YYYY format")):
    query = f"""
    SELECT SUM(Consumption)
    FROM yearmonth
    WHERE SUBSTR(Date, 1, 4) = '{year}'
    GROUP BY SUBSTR(Date, 5, 2)
    ORDER BY SUM(Consumption) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"consumption": result[0]}

# Endpoint to get customer with the highest monthly consumption in a specific currency
@app.get("/v1/bird/debit_card_specializing/highest_monthly_consumption", summary="Get customer with the highest monthly consumption in a specific currency")
async def get_highest_monthly_consumption(currency: str = Query(..., description="Currency of the customer")):
    query = f"""
    SELECT SUM(T2.Consumption) / 12 AS MonthlyConsumption
    FROM customers AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T1.Currency = '{currency}'
    GROUP BY T1.CustomerID
    ORDER BY MonthlyConsumption DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"monthly_consumption": result[0]}

# Endpoint to get product descriptions for transactions on a specific date
@app.get("/v1/bird/debit_card_specializing/product_descriptions", summary="Get product descriptions for transactions on a specific date")
async def get_product_descriptions(date: str = Query(..., description="Date in YYYYMM format")):
    query = f"""
    SELECT T3.Description
    FROM transactions_1k AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    INNER JOIN products AS T3 ON T1.ProductID = T3.ProductID
    WHERE T2.Date = '{date}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"descriptions": [row[0] for row in result]}

# Endpoint to get distinct countries for transactions on a specific date
@app.get("/v1/bird/debit_card_specializing/distinct_countries", summary="Get distinct countries for transactions on a specific date")
async def get_distinct_countries(date: str = Query(..., description="Date in YYYYMM format")):
    query = f"""
    SELECT DISTINCT T2.Country
    FROM transactions_1k AS T1
    INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
    INNER JOIN yearmonth AS T3 ON T1.CustomerID = T3.CustomerID
    WHERE T3.Date = '{date}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"countries": [row[0] for row in result]}

# Endpoint to get distinct chain IDs for transactions in a specific currency
@app.get("/v1/bird/debit_card_specializing/distinct_chain_ids", summary="Get distinct chain IDs for transactions in a specific currency")
async def get_distinct_chain_ids(currency: str = Query(..., description="Currency of the customer")):
    query = f"""
    SELECT DISTINCT T3.ChainID
    FROM transactions_1k AS T1
    INNER JOIN customers AS T2 ON T1.CustomerID = T2.CustomerID
    INNER JOIN gasstations AS T3 ON T1.GasStationID = T3.GasStationID
    WHERE T2.Currency = '{currency}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"chain_ids": [row[0] for row in result]}

# Endpoint to get distinct product IDs and descriptions for transactions in a specific currency
@app.get("/v1/bird/debit_card_specializing/distinct_product_ids", summary="Get distinct product IDs and descriptions for transactions in a specific currency")
async def get_distinct_product_ids(currency: str = Query(..., description="Currency of the customer")):
    query = f"""
    SELECT DISTINCT T1.ProductID, T3.Description
    FROM transactions_1k AS T1
    INNER JOIN customers AS T2 ON T1.CustomerID = T2.CustomerID
    INNER JOIN products AS T3 ON T1.ProductID = T3.ProductID
    WHERE T2.Currency = '{currency}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"products": [{"product_id": row[0], "description": row[1]} for row in result]}

# Endpoint to get average amount for transactions in a specific month
@app.get("/v1/bird/debit_card_specializing/average_amount", summary="Get average amount for transactions in a specific month")
async def get_average_amount(month: str = Query(..., description="Month in YYYY-MM format")):
    query = f"""
    SELECT AVG(Amount)
    FROM transactions_1k
    WHERE Date LIKE '{month}%'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"average_amount": result[0]}

# Endpoint to get count of customers with consumption greater than a threshold in a specific currency
@app.get("/v1/bird/debit_card_specializing/high_consumption_count", summary="Get count of customers with consumption greater than a threshold in a specific currency")
async def get_high_consumption_count(currency: str = Query(..., description="Currency of the customer"), threshold: float = Query(..., description="Consumption threshold")):
    query = f"""
    SELECT COUNT(*)
    FROM yearmonth AS T1
    INNER JOIN customers AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T2.Currency = '{currency}' AND T1.Consumption > {threshold}
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get distinct product descriptions for transactions in a specific country
@app.get("/v1/bird/debit_card_specializing/distinct_product_descriptions", summary="Get distinct product descriptions for transactions in a specific country")
async def get_distinct_product_descriptions(country: str = Query(..., description="Country of the gas station")):
    query = f"""
    SELECT DISTINCT T3.Description
    FROM transactions_1k AS T1
    INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
    INNER JOIN products AS T3 ON T1.ProductID = T3.ProductID
    WHERE T2.Country = '{country}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"descriptions": [row[0] for row in result]}

# Endpoint to get distinct times for transactions in a specific chain
@app.get("/v1/bird/debit_card_specializing/distinct_times", summary="Get distinct times for transactions in a specific chain")
async def get_distinct_times(chain_id: int = Query(..., description="Chain ID of the gas station")):
    query = f"""
    SELECT DISTINCT T1.Time
    FROM transactions_1k AS T1
    INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
    WHERE T2.ChainID = {chain_id}
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"times": [row[0] for row in result]}

# Endpoint to get count of transactions with price greater than a threshold in a specific country
@app.get("/v1/bird/debit_card_specializing/high_price_transaction_count", summary="Get count of transactions with price greater than a threshold in a specific country")
async def get_high_price_transaction_count(country: str = Query(..., description="Country of the gas station"), threshold: float = Query(..., description="Price threshold")):
    query = f"""
    SELECT COUNT(T1.TransactionID)
    FROM transactions_1k AS T1
    INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
    WHERE T2.Country = '{country}' AND T1.Price > {threshold}
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of transactions in a specific country and year
@app.get("/v1/bird/debit_card_specializing/transaction_count", summary="Get count of transactions in a specific country and year")
async def get_transaction_count(country: str = Query(..., description="Country of the gas station"), year: int = Query(..., description="Year of the transaction")):
    query = f"""
    SELECT COUNT(T1.TransactionID)
    FROM transactions_1k AS T1
    INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
    WHERE T2.Country = '{country}' AND STRFTIME('%Y', T1.Date) >= '{year}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count": result[0]}
# Endpoint to get average price for a given country
@app.get("/v1/bird/debit_card_specializing/average_price_by_country", summary="Get average price for a given country")
async def get_average_price_by_country(country: str = Query(..., description="Country code")):
    query = """
    SELECT AVG(T1.Price) FROM transactions_1k AS T1
    INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
    WHERE T2.Country = ?
    """
    cursor.execute(query, (country,))
    result = cursor.fetchone()
    return {"average_price": result[0]}

# Endpoint to get average price for a given currency
@app.get("/v1/bird/debit_card_specializing/average_price_by_currency", summary="Get average price for a given currency")
async def get_average_price_by_currency(currency: str = Query(..., description="Currency code")):
    query = """
    SELECT AVG(T1.Price) FROM transactions_1k AS T1
    INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
    INNER JOIN customers AS T3 ON T1.CustomerID = T3.CustomerID
    WHERE T3.Currency = ?
    """
    cursor.execute(query, (currency,))
    result = cursor.fetchone()
    return {"average_price": result[0]}

# Endpoint to get top customer by price on a given date
@app.get("/v1/bird/debit_card_specializing/top_customer_by_date", summary="Get top customer by price on a given date")
async def get_top_customer_by_date(date: str = Query(..., description="Date in YYYY-MM-DD format")):
    query = """
    SELECT CustomerID FROM transactions_1k
    WHERE Date = ?
    GROUP BY CustomerID
    ORDER BY SUM(Price) DESC
    LIMIT 1
    """
    cursor.execute(query, (date,))
    result = cursor.fetchone()
    return {"customer_id": result[0]}

# Endpoint to get country of the last transaction on a given date
@app.get("/v1/bird/debit_card_specializing/last_transaction_country_by_date", summary="Get country of the last transaction on a given date")
async def get_last_transaction_country_by_date(date: str = Query(..., description="Date in YYYY-MM-DD format")):
    query = """
    SELECT T2.Country FROM transactions_1k AS T1
    INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
    WHERE T1.Date = ?
    ORDER BY T1.Time DESC
    LIMIT 1
    """
    cursor.execute(query, (date,))
    result = cursor.fetchone()
    return {"country": result[0]}

# Endpoint to get distinct currencies for a given date and time
@app.get("/v1/bird/debit_card_specializing/distinct_currencies_by_date_time", summary="Get distinct currencies for a given date and time")
async def get_distinct_currencies_by_date_time(date: str = Query(..., description="Date in YYYY-MM-DD format"), time: str = Query(..., description="Time in HH:MM:SS format")):
    query = """
    SELECT DISTINCT T3.Currency FROM transactions_1k AS T1
    INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
    INNER JOIN customers AS T3 ON T1.CustomerID = T3.CustomerID
    WHERE T1.Date = ? AND T1.Time = ?
    """
    cursor.execute(query, (date, time))
    result = cursor.fetchall()
    return {"currencies": [row[0] for row in result]}

# Endpoint to get customer segment for a given date and time
@app.get("/v1/bird/debit_card_specializing/customer_segment_by_date_time", summary="Get customer segment for a given date and time")
async def get_customer_segment_by_date_time(date: str = Query(..., description="Date in YYYY-MM-DD format"), time: str = Query(..., description="Time in HH:MM:SS format")):
    query = """
    SELECT T2.Segment FROM transactions_1k AS T1
    INNER JOIN customers AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T1.Date = ? AND T1.Time = ?
    """
    cursor.execute(query, (date, time))
    result = cursor.fetchall()
    return {"segments": [row[0] for row in result]}

# Endpoint to get transaction count for a given date, time, and currency
@app.get("/v1/bird/debit_card_specializing/transaction_count_by_date_time_currency", summary="Get transaction count for a given date, time, and currency")
async def get_transaction_count_by_date_time_currency(date: str = Query(..., description="Date in YYYY-MM-DD format"), time: str = Query(..., description="Time in HH:MM:SS format"), currency: str = Query(..., description="Currency code")):
    query = """
    SELECT COUNT(T1.TransactionID) FROM transactions_1k AS T1
    INNER JOIN customers AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T1.Date = ? AND T1.Time < ? AND T2.Currency = ?
    """
    cursor.execute(query, (date, time, currency))
    result = cursor.fetchone()
    return {"transaction_count": result[0]}

# Endpoint to get customer segment for the earliest transaction
@app.get("/v1/bird/debit_card_specializing/earliest_customer_segment", summary="Get customer segment for the earliest transaction")
async def get_earliest_customer_segment():
    query = """
    SELECT T2.Segment FROM transactions_1k AS T1
    INNER JOIN customers AS T2 ON T1.CustomerID = T2.CustomerID
    ORDER BY Date ASC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"segment": result[0]}

# Endpoint to get country of transaction for a given date and time
@app.get("/v1/bird/debit_card_specializing/transaction_country_by_date_time", summary="Get country of transaction for a given date and time")
async def get_transaction_country_by_date_time(date: str = Query(..., description="Date in YYYY-MM-DD format"), time: str = Query(..., description="Time in HH:MM:SS format")):
    query = """
    SELECT T2.Country FROM transactions_1k AS T1
    INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
    WHERE T1.Date = ? AND T1.Time = ?
    """
    cursor.execute(query, (date, time))
    result = cursor.fetchone()
    return {"country": result[0]}

# Endpoint to get product IDs for a given date and time
@app.get("/v1/bird/debit_card_specializing/product_ids_by_date_time", summary="Get product IDs for a given date and time")
async def get_product_ids_by_date_time(date: str = Query(..., description="Date in YYYY-MM-DD format"), time: str = Query(..., description="Time in HH:MM:SS format")):
    query = """
    SELECT T1.ProductID FROM transactions_1k AS T1
    INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
    WHERE T1.Date = ? AND T1.Time = ?
    """
    cursor.execute(query, (date, time))
    result = cursor.fetchall()
    return {"product_ids": [row[0] for row in result]}

# Endpoint to get customer details for a given date, price, and year-month
@app.get("/v1/bird/debit_card_specializing/customer_details_by_date_price_yearmonth", summary="Get customer details for a given date, price, and year-month")
async def get_customer_details_by_date_price_yearmonth(date: str = Query(..., description="Date in YYYY-MM-DD format"), price: float = Query(..., description="Price"), yearmonth: str = Query(..., description="Year-month in YYYYMM format")):
    query = """
    SELECT T1.CustomerID, T2.Date, T2.Consumption FROM transactions_1k AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T1.Date = ? AND T1.Price = ? AND T2.Date = ?
    """
    cursor.execute(query, (date, price, yearmonth))
    result = cursor.fetchall()
    return {"customer_details": [{"customer_id": row[0], "date": row[1], "consumption": row[2]} for row in result]}

# Endpoint to get transaction count for a given date, time range, and country
@app.get("/v1/bird/debit_card_specializing/transaction_count_by_date_time_range_country", summary="Get transaction count for a given date, time range, and country")
async def get_transaction_count_by_date_time_range_country(date: str = Query(..., description="Date in YYYY-MM-DD format"), start_time: str = Query(..., description="Start time in HH:MM:SS format"), end_time: str = Query(..., description="End time in HH:MM:SS format"), country: str = Query(..., description="Country code")):
    query = """
    SELECT COUNT(T1.TransactionID) FROM transactions_1k AS T1
    INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
    WHERE T1.Date = ? AND T1.Time BETWEEN ? AND ? AND T2.Country = ?
    """
    cursor.execute(query, (date, start_time, end_time, country))
    result = cursor.fetchone()
    return {"transaction_count": result[0]}

# Endpoint to get currency for a given year-month and consumption
@app.get("/v1/bird/debit_card_specializing/currency_by_yearmonth_consumption", summary="Get currency for a given year-month and consumption")
async def get_currency_by_yearmonth_consumption(yearmonth: str = Query(..., description="Year-month in YYYYMM format"), consumption: float = Query(..., description="Consumption")):
    query = """
    SELECT T2.Currency FROM yearmonth AS T1
    INNER JOIN customers AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T1.Date = ? AND T1.Consumption = ?
    """
    cursor.execute(query, (yearmonth, consumption))
    result = cursor.fetchone()
    return {"currency": result[0]}

# Endpoint to get country for a given card ID
@app.get("/v1/bird/debit_card_specializing/country_by_card_id", summary="Get country for a given card ID")
async def get_country_by_card_id(card_id: str = Query(..., description="Card ID")):
    query = """
    SELECT T2.Country FROM transactions_1k AS T1
    INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
    WHERE T1.CardID = ?
    """
    cursor.execute(query, (card_id,))
    result = cursor.fetchone()
    return {"country": result[0]}

# Endpoint to get country for a given date and price
@app.get("/v1/bird/debit_card_specializing/country_by_date_price", summary="Get country for a given date and price")
async def get_country_by_date_price(date: str = Query(..., description="Date in YYYY-MM-DD format"), price: float = Query(..., description="Price")):
    query = """
    SELECT T2.Country FROM transactions_1k AS T1
    INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
    WHERE T1.Date = ? AND T1.Price = ?
    """
    cursor.execute(query, (date, price))
    result = cursor.fetchone()
    return {"country": result[0]}

# Endpoint to get percentage of customers using EUR on a given date
@app.get("/v1/bird/debit_card_specializing/percentage_customers_using_eur_by_date", summary="Get percentage of customers using EUR on a given date")
async def get_percentage_customers_using_eur_by_date(date: str = Query(..., description="Date in YYYY-MM-DD format")):
    query = """
    SELECT CAST(SUM(IIF(T2.Currency = 'EUR', 1, 0)) AS FLOAT) * 100 / COUNT(T1.CustomerID) FROM transactions_1k AS T1
    INNER JOIN customers AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T1.Date = ?
    """
    cursor.execute(query, (date,))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get consumption change between two years for a given customer
@app.get("/v1/bird/debit_card_specializing/consumption_change_by_customer", summary="Get consumption change between two years for a given customer")
async def get_consumption_change_by_customer(date: str = Query(..., description="Date in YYYY-MM-DD format"), price: float = Query(..., description="Price")):
    query = """
    SELECT CAST(SUM(IIF(SUBSTR(Date, 1, 4) = '2012', Consumption, 0)) - SUM(IIF(SUBSTR(Date, 1, 4) = '2013', Consumption, 0)) AS FLOAT) / SUM(IIF(SUBSTR(Date, 1, 4) = '2012', Consumption, 0)) FROM yearmonth
    WHERE CustomerID = (
        SELECT T1.CustomerID FROM transactions_1k AS T1
        INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
        WHERE T1.Date = ? AND T1.Price = ?
    )
    """
    cursor.execute(query, (date, price))
    result = cursor.fetchone()
    return {"consumption_change": result[0]}

# Endpoint to get top gas station by total price
@app.get("/v1/bird/debit_card_specializing/top_gas_station_by_price", summary="Get top gas station by total price")
async def get_top_gas_station_by_price():
    query = """
    SELECT GasStationID FROM transactions_1k
    GROUP BY GasStationID
    ORDER BY SUM(Price) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"gas_station_id": result[0]}

# Endpoint to get percentage of premium gas stations in a given country
@app.get("/v1/bird/debit_card_specializing/percentage_premium_gas_stations_by_country", summary="Get percentage of premium gas stations in a given country")
async def get_percentage_premium_gas_stations_by_country(country: str = Query(..., description="Country code")):
    query = """
    SELECT CAST(SUM(IIF(Country = ? AND Segment = 'Premium', 1, 0)) AS FLOAT) * 100 / SUM(IIF(Country = ?, 1, 0)) FROM gasstations
    """
    cursor.execute(query, (country, country))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get total and specific year-month price for a given customer
@app.get("/v1/bird/debit_card_specializing/total_and_specific_yearmonth_price_by_customer", summary="Get total and specific year-month price for a given customer")
async def get_total_and_specific_yearmonth_price_by_customer(customer_id: str = Query(..., description="Customer ID"), yearmonth: str = Query(..., description="Year-month in YYYYMM format")):
    query = """
    SELECT SUM(T1.Price), SUM(IIF(T3.Date = ?, T1.Price, 0)) FROM transactions_1k AS T1
    INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
    INNER JOIN yearmonth AS T3 ON T1.CustomerID = T3.CustomerID
    WHERE T1.CustomerID = ?
    """
    cursor.execute(query, (yearmonth, customer_id))
    result = cursor.fetchone()
    return {"total_price": result[0], "specific_yearmonth_price": result[1]}

# Endpoint to get product descriptions for top transactions
@app.get("/v1/bird/debit_card_specializing/top_product_descriptions", summary="Get top product descriptions based on transaction amount")
async def get_top_product_descriptions(limit: int = Query(5, description="Number of top products to retrieve")):
    query = """
    SELECT T2.Description
    FROM transactions_1k AS T1
    INNER JOIN products AS T2 ON T1.ProductID = T2.ProductID
    ORDER BY T1.Amount DESC
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    results = cursor.fetchall()
    return {"descriptions": [row[0] for row in results]}

# Endpoint to get customer consumption details
@app.get("/v1/bird/debit_card_specializing/customer_consumption", summary="Get customer consumption details")
async def get_customer_consumption(customer_id: int = Query(..., description="Customer ID")):
    query = """
    SELECT T2.CustomerID, SUM(T2.Price / T2.Amount), T1.Currency
    FROM customers AS T1
    INNER JOIN transactions_1k AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T2.CustomerID = ?
    GROUP BY T2.CustomerID, T1.Currency
    """
    cursor.execute(query, (customer_id,))
    results = cursor.fetchall()
    return {"consumption_details": results}

# Endpoint to get country of the most expensive product
@app.get("/v1/bird/debit_card_specializing/most_expensive_product_country", summary="Get country of the most expensive product")
async def get_most_expensive_product_country(product_id: int = Query(..., description="Product ID")):
    query = """
    SELECT T2.Country
    FROM transactions_1k AS T1
    INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID
    WHERE T1.ProductID = ?
    ORDER BY T1.Price DESC
    LIMIT 1
    """
    cursor.execute(query, (product_id,))
    result = cursor.fetchone()
    return {"country": result[0] if result else None}

# Endpoint to get consumption details for a specific date and product
@app.get("/v1/bird/debit_card_specializing/consumption_details", summary="Get consumption details for a specific date and product")
async def get_consumption_details(date: str = Query(..., description="Date in YYYYMM format"), product_id: int = Query(..., description="Product ID")):
    query = """
    SELECT T2.Consumption
    FROM transactions_1k AS T1
    INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
    WHERE T1.Price / T1.Amount > 29.00 AND T1.ProductID = ? AND T2.Date = ?
    """
    cursor.execute(query, (product_id, date))
    results = cursor.fetchall()
    return {"consumption_details": [row[0] for row in results]}
