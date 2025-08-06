

from fastapi import APIRouter, Query
import sqlite3

app = APIRouter()

def get_db_connection():
    return sqlite3.connect('invocable_api_hub/db/bird/test/formula_1.sqlite', check_same_thread=False)

# Database connection
conn = sqlite3.connect('invocable_api_hub/db/bird/test/formula_1.sqlite', check_same_thread=False)
cursor = conn.cursor()

# Endpoint to get driverRef for a given raceId
@app.get("/v1/bird/formula_1/driverRef", summary="Get driverRef for a given raceId")
async def get_driver_ref(race_id: int = Query(..., description="ID of the race"), limit: int = Query(5, description="Limit the number of results")):
    query = """
    SELECT T2.driverRef FROM qualifying AS T1
    INNER JOIN drivers AS T2 ON T2.driverId = T1.driverId
    WHERE T1.raceId = ?
    ORDER BY T1.q1 DESC
    LIMIT ?
    """
    cursor.execute(query, (race_id, limit))
    results = cursor.fetchall()
    return {"driverRef": [result[0] for result in results]}

# Endpoint to get surname for a given raceId
@app.get("/v1/bird/formula_1/surname", summary="Get surname for a given raceId")
async def get_surname(race_id: int = Query(..., description="ID of the race"), limit: int = Query(1, description="Limit the number of results")):
    query = """
    SELECT T2.surname FROM qualifying AS T1
    INNER JOIN drivers AS T2 ON T2.driverId = T1.driverId
    WHERE T1.raceId = ?
    ORDER BY T1.q2 ASC
    LIMIT ?
    """
    cursor.execute(query, (race_id, limit))
    results = cursor.fetchall()
    return {"surname": [result[0] for result in results]}

# Endpoint to get year for a given location
@app.get("/v1/bird/formula_1/year", summary="Get year for a given location")
async def get_year(location: str = Query(..., description="Location of the circuit")):
    query = """
    SELECT T2.year FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE T1.location = ?
    """
    cursor.execute(query, (location,))
    results = cursor.fetchall()
    return {"year": [result[0] for result in results]}

# Endpoint to get url for a given circuit name
@app.get("/v1/bird/formula_1/circuit_url", summary="Get url for a given circuit name")
async def get_circuit_url(name: str = Query(..., description="Name of the circuit")):
    query = """
    SELECT DISTINCT T1.url FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE T1.name = ?
    """
    cursor.execute(query, (name,))
    results = cursor.fetchall()
    return {"url": [result[0] for result in results]}

# Endpoint to get race names for a given country
@app.get("/v1/bird/formula_1/race_names", summary="Get race names for a given country")
async def get_race_names(country: str = Query(..., description="Country of the circuit")):
    query = """
    SELECT DISTINCT T2.name FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE T1.country = ?
    """
    cursor.execute(query, (country,))
    results = cursor.fetchall()
    return {"race_names": [result[0] for result in results]}

# Endpoint to get position for a given constructor name
@app.get("/v1/bird/formula_1/constructor_position", summary="Get position for a given constructor name")
async def get_constructor_position(name: str = Query(..., description="Name of the constructor")):
    query = """
    SELECT DISTINCT T1.position FROM constructorStandings AS T1
    INNER JOIN constructors AS T2 ON T2.constructorId = T1.constructorId
    WHERE T2.name = ?
    """
    cursor.execute(query, (name,))
    results = cursor.fetchall()
    return {"position": [result[0] for result in results]}

# Endpoint to get race count for a given year and excluded countries
@app.get("/v1/bird/formula_1/race_count", summary="Get race count for a given year and excluded countries")
async def get_race_count(year: int = Query(..., description="Year of the race"), excluded_countries: str = Query(..., description="Comma-separated list of countries to exclude")):
    excluded_countries_list = excluded_countries.split(',')
    placeholders = ','.join('?' for _ in excluded_countries_list)
    query = f"""
    SELECT COUNT(T3.raceId) FROM circuits AS T1
    INNER JOIN races AS T3 ON T3.circuitID = T1.circuitId
    WHERE T1.country NOT IN ({placeholders}) AND T3.year = ?
    """
    cursor.execute(query, (*excluded_countries_list, year))
    results = cursor.fetchall()
    return {"race_count": results[0][0]}

# Endpoint to get race names for a given country
@app.get("/v1/bird/formula_1/race_names_by_country", summary="Get race names for a given country")
async def get_race_names_by_country(country: str = Query(..., description="Country of the circuit")):
    query = """
    SELECT DISTINCT T2.name FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE T1.country = ?
    """
    cursor.execute(query, (country,))
    results = cursor.fetchall()
    return {"race_names": [result[0] for result in results]}

# Endpoint to get lat and lng for a given race name
@app.get("/v1/bird/formula_1/race_coordinates", summary="Get lat and lng for a given race name")
async def get_race_coordinates(name: str = Query(..., description="Name of the race")):
    query = """
    SELECT DISTINCT T1.lat, T1.lng FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE T2.name = ?
    """
    cursor.execute(query, (name,))
    results = cursor.fetchall()
    return {"coordinates": [{"lat": result[0], "lng": result[1]} for result in results]}

# Endpoint to get url for a given circuit name
@app.get("/v1/bird/formula_1/circuit_url_by_name", summary="Get url for a given circuit name")
async def get_circuit_url_by_name(name: str = Query(..., description="Name of the circuit")):
    query = """
    SELECT DISTINCT T1.url FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE T1.name = ?
    """
    cursor.execute(query, (name,))
    results = cursor.fetchall()
    return {"url": [result[0] for result in results]}
# Endpoint to get distinct times for a given circuit
@app.get("/v1/bird/formula_1/circuit_times", summary="Get distinct times for a given circuit")
async def get_circuit_times(circuit_name: str = Query(..., description="Name of the circuit")):
    query = """
    SELECT DISTINCT T2.time
    FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE T1.name = ?
    """
    cursor.execute(query, (circuit_name,))
    results = cursor.fetchall()
    return results

# Endpoint to get latitude and longitude for a given race
@app.get("/v1/bird/formula_1/race_coordinates", summary="Get latitude and longitude for a given race")
async def get_race_coordinates(race_name: str = Query(..., description="Name of the race")):
    query = """
    SELECT DISTINCT T1.lat, T1.lng
    FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE T2.name = ?
    """
    cursor.execute(query, (race_name,))
    results = cursor.fetchall()
    return results

# Endpoint to get nationality of constructor for a given race and points
@app.get("/v1/bird/formula_1/constructor_nationality", summary="Get nationality of constructor for a given race and points")
async def get_constructor_nationality(race_id: int = Query(..., description="ID of the race"), points: int = Query(..., description="Points scored")):
    query = """
    SELECT T2.nationality
    FROM constructorResults AS T1
    INNER JOIN constructors AS T2 ON T2.constructorId = T1.constructorId
    WHERE T1.raceId = ? AND T1.points = ?
    """
    cursor.execute(query, (race_id, points))
    results = cursor.fetchall()
    return results

# Endpoint to get qualifying time for a given race and driver
@app.get("/v1/bird/formula_1/qualifying_time", summary="Get qualifying time for a given race and driver")
async def get_qualifying_time(race_id: int = Query(..., description="ID of the race"), forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver")):
    query = """
    SELECT T1.q1
    FROM qualifying AS T1
    INNER JOIN drivers AS T2 ON T2.driverId = T1.driverId
    WHERE T1.raceId = ? AND T2.forename = ? AND T2.surname = ?
    """
    cursor.execute(query, (race_id, forename, surname))
    results = cursor.fetchall()
    return results

# Endpoint to get distinct nationalities for a given race and qualifying time
@app.get("/v1/bird/formula_1/qualifying_nationalities", summary="Get distinct nationalities for a given race and qualifying time")
async def get_qualifying_nationalities(race_id: int = Query(..., description="ID of the race"), q2_time: str = Query(..., description="Qualifying time (q2)")):
    query = """
    SELECT DISTINCT T2.nationality
    FROM qualifying AS T1
    INNER JOIN drivers AS T2 ON T2.driverId = T1.driverId
    WHERE T1.raceId = ? AND T1.q2 LIKE ?
    """
    cursor.execute(query, (race_id, q2_time))
    results = cursor.fetchall()
    return results

# Endpoint to get driver number for a given race and qualifying time
@app.get("/v1/bird/formula_1/qualifying_driver_number", summary="Get driver number for a given race and qualifying time")
async def get_qualifying_driver_number(race_id: int = Query(..., description="ID of the race"), q3_time: str = Query(..., description="Qualifying time (q3)")):
    query = """
    SELECT T2.number
    FROM qualifying AS T1
    INNER JOIN drivers AS T2 ON T2.driverId = T1.driverId
    WHERE T1.raceId = ? AND T1.q3 LIKE ?
    """
    cursor.execute(query, (race_id, q3_time))
    results = cursor.fetchall()
    return results

# Endpoint to get count of drivers with null time for a given year and race
@app.get("/v1/bird/formula_1/null_time_drivers_count", summary="Get count of drivers with null time for a given year and race")
async def get_null_time_drivers_count(year: int = Query(..., description="Year of the race"), race_name: str = Query(..., description="Name of the race")):
    query = """
    SELECT COUNT(T3.driverId)
    FROM races AS T1
    INNER JOIN results AS T2 ON T2.raceId = T1.raceId
    INNER JOIN drivers AS T3 ON T3.driverId = T2.driverId
    WHERE T1.year = ? AND T1.name = ? AND T2.time IS NULL
    """
    cursor.execute(query, (year, race_name))
    results = cursor.fetchall()
    return results

# Endpoint to get season URL for a given race
@app.get("/v1/bird/formula_1/season_url", summary="Get season URL for a given race")
async def get_season_url(race_id: int = Query(..., description="ID of the race")):
    query = """
    SELECT T2.url
    FROM races AS T1
    INNER JOIN seasons AS T2 ON T2.year = T1.year
    WHERE T1.raceId = ?
    """
    cursor.execute(query, (race_id,))
    results = cursor.fetchall()
    return results

# Endpoint to get count of drivers with non-null time for a given date
@app.get("/v1/bird/formula_1/non_null_time_drivers_count", summary="Get count of drivers with non-null time for a given date")
async def get_non_null_time_drivers_count(date: str = Query(..., description="Date of the race")):
    query = """
    SELECT COUNT(T2.driverId)
    FROM races AS T1
    INNER JOIN results AS T2 ON T2.raceId = T1.raceId
    WHERE T1.date = ? AND T2.time IS NOT NULL
    """
    cursor.execute(query, (date,))
    results = cursor.fetchall()
    return results

# Endpoint to get driver details for a given race with non-null time and dob
@app.get("/v1/bird/formula_1/driver_details", summary="Get driver details for a given race with non-null time and dob")
async def get_driver_details(race_id: int = Query(..., description="ID of the race")):
    query = """
    SELECT T1.forename, T1.surname
    FROM drivers AS T1
    INNER JOIN results AS T2 ON T2.driverId = T1.driverId
    WHERE T2.raceId = ? AND T2.time IS NOT NULL AND T1.dob IS NOT NULL
    ORDER BY T1.dob ASC
    LIMIT 1
    """
    cursor.execute(query, (race_id,))
    results = cursor.fetchall()
    return results
# Endpoint to get distinct driver details for a given raceId and time
@app.get("/v1/bird/formula_1/driver_details", summary="Get distinct driver details for a given raceId and time")
async def get_driver_details(race_id: int = Query(..., description="ID of the race"), time: str = Query(..., description="Time in the format '1:27%'")):
    query = """
    SELECT DISTINCT T2.forename, T2.surname, T2.url
    FROM lapTimes AS T1
    INNER JOIN drivers AS T2 ON T2.driverId = T1.driverId
    WHERE T1.raceId = ? AND T1.time LIKE ?
    """
    cursor.execute(query, (race_id, time))
    result = cursor.fetchall()
    return result

# Endpoint to get nationality of the driver with the fastest lap time for a given raceId
@app.get("/v1/bird/formula_1/fastest_lap_nationality", summary="Get nationality of the driver with the fastest lap time for a given raceId")
async def get_fastest_lap_nationality(race_id: int = Query(..., description="ID of the race")):
    query = """
    SELECT T1.nationality
    FROM drivers AS T1
    INNER JOIN results AS T2 ON T2.driverId = T1.driverId
    WHERE T2.raceId = ? AND T2.fastestLapTime IS NOT NULL
    ORDER BY T2.fastestLapSpeed DESC
    LIMIT 1
    """
    cursor.execute(query, (race_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct circuit coordinates for a given race name
@app.get("/v1/bird/formula_1/circuit_coordinates", summary="Get distinct circuit coordinates for a given race name")
async def get_circuit_coordinates(race_name: str = Query(..., description="Name of the race")):
    query = """
    SELECT DISTINCT T1.lat, T1.lng
    FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE T2.name = ?
    """
    cursor.execute(query, (race_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get constructor URL for a given raceId
@app.get("/v1/bird/formula_1/constructor_url", summary="Get constructor URL for a given raceId")
async def get_constructor_url(race_id: int = Query(..., description="ID of the race")):
    query = """
    SELECT T2.url
    FROM constructorResults AS T1
    INNER JOIN constructors AS T2 ON T2.constructorId = T1.constructorId
    WHERE T1.raceId = ?
    ORDER BY T1.points DESC
    LIMIT 1
    """
    cursor.execute(query, (race_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get qualifying time for a given raceId, forename, and surname
@app.get("/v1/bird/formula_1/qualifying_time", summary="Get qualifying time for a given raceId, forename, and surname")
async def get_qualifying_time(race_id: int = Query(..., description="ID of the race"), forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver")):
    query = """
    SELECT T1.q1
    FROM qualifying AS T1
    INNER JOIN drivers AS T2 ON T2.driverId = T1.driverId
    WHERE T1.raceId = ? AND T2.forename = ? AND T2.surname = ?
    """
    cursor.execute(query, (race_id, forename, surname))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct nationality of drivers for a given raceId and qualifying time
@app.get("/v1/bird/formula_1/qualifying_nationality", summary="Get distinct nationality of drivers for a given raceId and qualifying time")
async def get_qualifying_nationality(race_id: int = Query(..., description="ID of the race"), qualifying_time: str = Query(..., description="Qualifying time in the format '1:15%'")):
    query = """
    SELECT DISTINCT T2.nationality
    FROM qualifying AS T1
    INNER JOIN drivers AS T2 ON T2.driverId = T1.driverId
    WHERE T1.raceId = ? AND T1.q2 LIKE ?
    """
    cursor.execute(query, (race_id, qualifying_time))
    result = cursor.fetchall()
    return result

# Endpoint to get driver code for a given raceId and qualifying time
@app.get("/v1/bird/formula_1/qualifying_code", summary="Get driver code for a given raceId and qualifying time")
async def get_qualifying_code(race_id: int = Query(..., description="ID of the race"), qualifying_time: str = Query(..., description="Qualifying time in the format '1:33%'")):
    query = """
    SELECT T2.code
    FROM qualifying AS T1
    INNER JOIN drivers AS T2 ON T2.driverId = T1.driverId
    WHERE T1.raceId = ? AND T1.q3 LIKE ?
    """
    cursor.execute(query, (race_id, qualifying_time))
    result = cursor.fetchall()
    return result

# Endpoint to get race time for a given raceId, forename, and surname
@app.get("/v1/bird/formula_1/race_time", summary="Get race time for a given raceId, forename, and surname")
async def get_race_time(race_id: int = Query(..., description="ID of the race"), forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver")):
    query = """
    SELECT T2.time
    FROM drivers AS T1
    INNER JOIN results AS T2 ON T2.driverId = T1.driverId
    WHERE T2.raceId = ? AND T1.forename = ? AND T1.surname = ?
    """
    cursor.execute(query, (race_id, forename, surname))
    result = cursor.fetchall()
    return result

# Endpoint to get driver details for a given year, race name, and position
@app.get("/v1/bird/formula_1/driver_details_by_position", summary="Get driver details for a given year, race name, and position")
async def get_driver_details_by_position(year: int = Query(..., description="Year of the race"), race_name: str = Query(..., description="Name of the race"), position: int = Query(..., description="Position of the driver")):
    query = """
    SELECT T3.forename, T3.surname
    FROM races AS T1
    INNER JOIN results AS T2 ON T2.raceId = T1.raceId
    INNER JOIN drivers AS T3 ON T3.driverId = T2.driverId
    WHERE T1.year = ? AND T1.name = ? AND T2.position = ?
    """
    cursor.execute(query, (year, race_name, position))
    result = cursor.fetchall()
    return result

# Endpoint to get season URL for a given raceId
@app.get("/v1/bird/formula_1/season_url", summary="Get season URL for a given raceId")
async def get_season_url(race_id: int = Query(..., description="ID of the race")):
    query = """
    SELECT T2.url
    FROM races AS T1
    INNER JOIN seasons AS T2 ON T2.year = T1.year
    WHERE T1.raceId = ?
    """
    cursor.execute(query, (race_id,))
    result = cursor.fetchall()
    return result
# Endpoint to get count of drivers with null time for a given date
@app.get("/v1/bird/formula_1/count_drivers_null_time", summary="Get count of drivers with null time for a given date")
async def get_count_drivers_null_time(date: str = Query(..., description="Date of the race")):
    query = """
    SELECT COUNT(T2.driverId) FROM races AS T1
    INNER JOIN results AS T2 ON T2.raceId = T1.raceId
    WHERE T1.date = ? AND T2.time IS NULL
    """
    cursor.execute(query, (date,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get driver details for a given raceId
@app.get("/v1/bird/formula_1/driver_details", summary="Get driver details for a given raceId")
async def get_driver_details(race_id: int = Query(..., description="ID of the race")):
    query = """
    SELECT T1.forename, T1.surname FROM drivers AS T1
    INNER JOIN results AS T2 ON T2.driverId = T1.driverId
    WHERE T2.raceId = ? AND T2.time IS NOT NULL
    ORDER BY T1.dob DESC LIMIT 1
    """
    cursor.execute(query, (race_id,))
    result = cursor.fetchone()
    return {"forename": result[0], "surname": result[1]}

# Endpoint to get fastest driver for a given raceId
@app.get("/v1/bird/formula_1/fastest_driver", summary="Get fastest driver for a given raceId")
async def get_fastest_driver(race_id: int = Query(..., description="ID of the race")):
    query = """
    SELECT T2.forename, T2.surname FROM lapTimes AS T1
    INNER JOIN drivers AS T2 ON T2.driverId = T1.driverId
    WHERE T1.raceId = ?
    ORDER BY T1.time ASC LIMIT 1
    """
    cursor.execute(query, (race_id,))
    result = cursor.fetchone()
    return {"forename": result[0], "surname": result[1]}

# Endpoint to get nationality of the driver with the fastest lap speed
@app.get("/v1/bird/formula_1/fastest_lap_nationality", summary="Get nationality of the driver with the fastest lap speed")
async def get_fastest_lap_nationality():
    query = """
    SELECT T1.nationality FROM drivers AS T1
    INNER JOIN results AS T2 ON T2.driverId = T1.driverId
    ORDER BY T2.fastestLapSpeed DESC LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"nationality": result[0]}

# Endpoint to get percentage difference in fastest lap speed for a given driver
@app.get("/v1/bird/formula_1/fastest_lap_speed_difference", summary="Get percentage difference in fastest lap speed for a given driver")
async def get_fastest_lap_speed_difference(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver")):
    query = """
    SELECT (SUM(IIF(T2.raceId = 853, T2.fastestLapSpeed, 0)) - SUM(IIF(T2.raceId = 854, T2.fastestLapSpeed, 0))) * 100 / SUM(IIF(T2.raceId = 853, T2.fastestLapSpeed, 0))
    FROM drivers AS T1
    INNER JOIN results AS T2 ON T2.driverId = T1.driverId
    WHERE T1.forename = ? AND T1.surname = ?
    """
    cursor.execute(query, (forename, surname))
    result = cursor.fetchone()
    return {"percentage_difference": result[0]}

# Endpoint to get percentage of drivers with non-null time for a given date
@app.get("/v1/bird/formula_1/percentage_drivers_non_null_time", summary="Get percentage of drivers with non-null time for a given date")
async def get_percentage_drivers_non_null_time(date: str = Query(..., description="Date of the race")):
    query = """
    SELECT CAST(COUNT(CASE WHEN T2.time IS NOT NULL THEN T2.driverId END) AS REAL) * 100 / COUNT(T2.driverId)
    FROM races AS T1
    INNER JOIN results AS T2 ON T2.raceId = T1.raceId
    WHERE T1.date = ?
    """
    cursor.execute(query, (date,))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get the year of the first Singapore Grand Prix
@app.get("/v1/bird/formula_1/first_singapore_grand_prix_year", summary="Get the year of the first Singapore Grand Prix")
async def get_first_singapore_grand_prix_year():
    query = """
    SELECT year FROM races WHERE name = 'Singapore Grand Prix'
    ORDER BY year ASC LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"year": result[0]}

# Endpoint to get race names for a given year
@app.get("/v1/bird/formula_1/race_names_by_year", summary="Get race names for a given year")
async def get_race_names_by_year(year: int = Query(..., description="Year of the races")):
    query = """
    SELECT name FROM races WHERE year = ?
    ORDER BY name DESC
    """
    cursor.execute(query, (year,))
    result = cursor.fetchall()
    return {"race_names": [row[0] for row in result]}

# Endpoint to get the first race of the year
@app.get("/v1/bird/formula_1/first_race_of_the_year", summary="Get the first race of the year")
async def get_first_race_of_the_year():
    query = """
    SELECT name FROM races
    WHERE STRFTIME('%Y', date) = (SELECT STRFTIME('%Y', date) FROM races ORDER BY date ASC LIMIT 1)
    AND STRFTIME('%m', date) = (SELECT STRFTIME('%m', date) FROM races ORDER BY date ASC LIMIT 1)
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"race_name": result[0]}

# Endpoint to get the last race of a given year
@app.get("/v1/bird/formula_1/last_race_of_the_year", summary="Get the last race of a given year")
async def get_last_race_of_the_year(year: int = Query(..., description="Year of the races")):
    query = """
    SELECT name, date FROM races WHERE year = ?
    ORDER BY round DESC LIMIT 1
    """
    cursor.execute(query, (year,))
    result = cursor.fetchone()
    return {"name": result[0], "date": result[1]}

# Endpoint to get the year with the most rounds
@app.get("/v1/bird/formula_1/most_rounds_year", summary="Get the year with the most rounds")
async def get_most_rounds_year():
    query = "SELECT year FROM races GROUP BY year ORDER BY COUNT(round) DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"year": result[0]}

# Endpoint to get race names for a given year excluding another year
@app.get("/v1/bird/formula_1/race_names", summary="Get race names for a given year excluding another year")
async def get_race_names(year: int = Query(..., description="Year to include"), exclude_year: int = Query(..., description="Year to exclude")):
    query = f"SELECT name FROM races WHERE year = {year} AND name NOT IN (SELECT name FROM races WHERE year = {exclude_year})"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"race_names": [row[0] for row in result]}

# Endpoint to get circuit details for a specific race
@app.get("/v1/bird/formula_1/circuit_details", summary="Get circuit details for a specific race")
async def get_circuit_details(race_name: str = Query(..., description="Name of the race")):
    query = f"""
    SELECT T1.country, T1.location
    FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE T2.name = '{race_name}'
    ORDER BY T2.year ASC LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"country": result[0], "location": result[1]}

# Endpoint to get the date of a specific race at a specific circuit
@app.get("/v1/bird/formula_1/race_date", summary="Get the date of a specific race at a specific circuit")
async def get_race_date(circuit_name: str = Query(..., description="Name of the circuit"), race_name: str = Query(..., description="Name of the race")):
    query = f"""
    SELECT T2.date
    FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE T1.name = '{circuit_name}' AND T2.name = '{race_name}'
    ORDER BY T2.year DESC LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"date": result[0]}

# Endpoint to get the count of races at a specific circuit for a specific race
@app.get("/v1/bird/formula_1/race_count", summary="Get the count of races at a specific circuit for a specific race")
async def get_race_count(circuit_name: str = Query(..., description="Name of the circuit"), race_name: str = Query(..., description="Name of the race")):
    query = f"""
    SELECT COUNT(T2.circuitid)
    FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE T1.name = '{circuit_name}' AND T2.name = '{race_name}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get driver details for a specific race and year
@app.get("/v1/bird/formula_1/driver_details", summary="Get driver details for a specific race and year")
async def get_driver_details(race_name: str = Query(..., description="Name of the race"), year: int = Query(..., description="Year of the race")):
    query = f"""
    SELECT T3.forename, T3.surname
    FROM races AS T1
    INNER JOIN driverStandings AS T2 ON T2.raceId = T1.raceId
    INNER JOIN drivers AS T3 ON T3.driverId = T2.driverId
    WHERE T1.name = '{race_name}' AND T1.year = {year}
    ORDER BY T2.position ASC
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"drivers": [{"forename": row[0], "surname": row[1]} for row in result]}

# Endpoint to get the top driver by points
@app.get("/v1/bird/formula_1/top_driver", summary="Get the top driver by points")
async def get_top_driver():
    query = """
    SELECT T3.forename, T3.surname, T2.points
    FROM races AS T1
    INNER JOIN driverStandings AS T2 ON T2.raceId = T1.raceId
    INNER JOIN drivers AS T3 ON T3.driverId = T2.driverId
    ORDER BY T2.points DESC LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"forename": result[0], "surname": result[1], "points": result[2]}

# Endpoint to get top drivers by points for a specific race and year
@app.get("/v1/bird/formula_1/top_drivers", summary="Get top drivers by points for a specific race and year")
async def get_top_drivers(race_name: str = Query(..., description="Name of the race"), year: int = Query(..., description="Year of the race"), limit: int = Query(..., description="Number of top drivers to return")):
    query = f"""
    SELECT T3.forename, T3.surname, T2.points
    FROM races AS T1
    INNER JOIN driverStandings AS T2 ON T2.raceId = T1.raceId
    INNER JOIN drivers AS T3 ON T3.driverId = T2.driverId
    WHERE T1.name = '{race_name}' AND T1.year = {year}
    ORDER BY T2.points DESC LIMIT {limit}
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"drivers": [{"forename": row[0], "surname": row[1], "points": row[2]} for row in result]}

# Endpoint to get the fastest lap details
@app.get("/v1/bird/formula_1/fastest_lap", summary="Get the fastest lap details")
async def get_fastest_lap():
    query = """
    SELECT T2.milliseconds, T1.forename, T1.surname, T3.name
    FROM drivers AS T1
    INNER JOIN lapTimes AS T2 ON T1.driverId = T2.driverId
    INNER JOIN races AS T3 ON T2.raceId = T3.raceId
    ORDER BY T2.milliseconds ASC LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"milliseconds": result[0], "forename": result[1], "surname": result[2], "race_name": result[3]}

# Endpoint to get the average lap time for a specific driver in a specific race and year
@app.get("/v1/bird/formula_1/average_lap_time", summary="Get the average lap time for a specific driver in a specific race and year")
async def get_average_lap_time(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver"), year: int = Query(..., description="Year of the race"), race_name: str = Query(..., description="Name of the race")):
    query = f"""
    SELECT AVG(T2.milliseconds)
    FROM races AS T1
    INNER JOIN lapTimes AS T2 ON T2.raceId = T1.raceId
    INNER JOIN drivers AS T3 ON T3.driverId = T2.driverId
    WHERE T3.forename = '{forename}' AND T3.surname = '{surname}' AND T1.year = {year} AND T1.name = '{race_name}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"average_milliseconds": result[0]}
# Endpoint to get the percentage of races where a driver finished in a position other than 1
@app.get("/v1/bird/formula_1/driver_race_percentage", summary="Get the percentage of races where a driver finished in a position other than 1")
async def get_driver_race_percentage(surname: str = Query(..., description="Surname of the driver"), year: int = Query(..., description="Year to filter races")):
    query = """
    SELECT CAST(COUNT(CASE WHEN T2.position <> 1 THEN T2.position END) AS REAL) * 100 / COUNT(T2.driverStandingsId)
    FROM races AS T1
    INNER JOIN driverStandings AS T2 ON T2.raceId = T1.raceId
    INNER JOIN drivers AS T3 ON T3.driverId = T2.driverId
    WHERE T3.surname = ? AND T1.year >= ?
    """
    cursor.execute(query, (surname, year))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get the driver with the most wins
@app.get("/v1/bird/formula_1/top_driver_by_wins", summary="Get the driver with the most wins")
async def get_top_driver_by_wins(wins: int = Query(..., description="Minimum number of wins")):
    query = """
    SELECT T1.forename, T1.surname, T1.nationality, MAX(T2.points)
    FROM drivers AS T1
    INNER JOIN driverStandings AS T2 ON T2.driverId = T1.driverId
    WHERE T2.wins >= ?
    GROUP BY T1.forename, T1.surname, T1.nationality
    ORDER BY COUNT(T2.wins) DESC
    LIMIT 1
    """
    cursor.execute(query, (wins,))
    result = cursor.fetchone()
    return {"forename": result[0], "surname": result[1], "nationality": result[2], "max_points": result[3]}

# Endpoint to get the oldest Japanese driver
@app.get("/v1/bird/formula_1/oldest_japanese_driver", summary="Get the oldest Japanese driver")
async def get_oldest_japanese_driver(nationality: str = Query(..., description="Nationality of the driver")):
    query = """
    SELECT STRFTIME('%Y', CURRENT_TIMESTAMP) - STRFTIME('%Y', dob), forename, surname
    FROM drivers
    WHERE nationality = ?
    ORDER BY dob DESC
    LIMIT 1
    """
    cursor.execute(query, (nationality,))
    result = cursor.fetchone()
    return {"age": result[0], "forename": result[1], "surname": result[2]}

# Endpoint to get circuits with exactly 4 races between 1990 and 2000
@app.get("/v1/bird/formula_1/circuits_with_four_races", summary="Get circuits with exactly 4 races between 1990 and 2000")
async def get_circuits_with_four_races(start_year: int = Query(..., description="Start year"), end_year: int = Query(..., description="End year")):
    query = """
    SELECT DISTINCT T1.name
    FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE STRFTIME('%Y', T2.date) BETWEEN ? AND ?
    GROUP BY T1.name
    HAVING COUNT(T2.raceId) = 4
    """
    cursor.execute(query, (start_year, end_year))
    result = cursor.fetchall()
    return {"circuits": [row[0] for row in result]}

# Endpoint to get circuits in the USA for a specific year
@app.get("/v1/bird/formula_1/usa_circuits_by_year", summary="Get circuits in the USA for a specific year")
async def get_usa_circuits_by_year(country: str = Query(..., description="Country of the circuit"), year: int = Query(..., description="Year of the race")):
    query = """
    SELECT T1.name, T1.location, T2.name
    FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE T1.country = ? AND T2.year = ?
    """
    cursor.execute(query, (country, year))
    result = cursor.fetchall()
    return {"circuits": [{"name": row[0], "location": row[1], "race_name": row[2]} for row in result]}

# Endpoint to get circuits with races in a specific month and year
@app.get("/v1/bird/formula_1/circuits_by_month_year", summary="Get circuits with races in a specific month and year")
async def get_circuits_by_month_year(year: int = Query(..., description="Year of the race"), month: str = Query(..., description="Month of the race")):
    query = """
    SELECT DISTINCT T2.name, T1.name, T1.location
    FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE T2.year = ? AND STRFTIME('%m', T2.date) = ?
    """
    cursor.execute(query, (year, month))
    result = cursor.fetchall()
    return {"circuits": [{"race_name": row[0], "circuit_name": row[1], "location": row[2]} for row in result]}

# Endpoint to get races where a specific driver finished in a position less than 20
@app.get("/v1/bird/formula_1/driver_races_by_position", summary="Get races where a specific driver finished in a position less than 20")
async def get_driver_races_by_position(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver"), position: int = Query(..., description="Maximum position")):
    query = """
    SELECT T1.name
    FROM races AS T1
    INNER JOIN driverStandings AS T2 ON T2.raceId = T1.raceId
    INNER JOIN drivers AS T3 ON T3.driverId = T2.driverId
    WHERE T3.forename = ? AND T3.surname = ? AND T2.position < ?
    """
    cursor.execute(query, (forename, surname, position))
    result = cursor.fetchall()
    return {"races": [row[0] for row in result]}

# Endpoint to get the total wins of a specific driver at a specific circuit
@app.get("/v1/bird/formula_1/driver_wins_at_circuit", summary="Get the total wins of a specific driver at a specific circuit")
async def get_driver_wins_at_circuit(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver"), circuit_name: str = Query(..., description="Name of the circuit")):
    query = """
    SELECT SUM(T2.wins)
    FROM drivers AS T1
    INNER JOIN driverStandings AS T2 ON T2.driverId = T1.driverId
    INNER JOIN races AS T3 ON T3.raceId = T2.raceId
    INNER JOIN circuits AS T4 ON T4.circuitId = T3.circuitId
    WHERE T1.forename = ? AND T1.surname = ? AND T4.name = ?
    """
    cursor.execute(query, (forename, surname, circuit_name))
    result = cursor.fetchone()
    return {"total_wins": result[0]}

# Endpoint to get the fastest race of a specific driver
@app.get("/v1/bird/formula_1/fastest_race_by_driver", summary="Get the fastest race of a specific driver")
async def get_fastest_race_by_driver(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver")):
    query = """
    SELECT T1.name, T1.year
    FROM races AS T1
    INNER JOIN lapTimes AS T2 ON T2.raceId = T1.raceId
    INNER JOIN drivers AS T3 ON T3.driverId = T2.driverId
    WHERE T3.forename = ? AND T3.surname = ?
    ORDER BY T2.milliseconds ASC
    LIMIT 1
    """
    cursor.execute(query, (forename, surname))
    result = cursor.fetchone()
    return {"race_name": result[0], "year": result[1]}

# Endpoint to get the average points of a specific driver in a specific year
@app.get("/v1/bird/formula_1/average_points_by_driver_year", summary="Get the average points of a specific driver in a specific year")
async def get_average_points_by_driver_year(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver"), year: int = Query(..., description="Year of the race")):
    query = """
    SELECT AVG(T2.points)
    FROM drivers AS T1
    INNER JOIN driverStandings AS T2 ON T2.driverId = T1.driverId
    INNER JOIN races AS T3 ON T3.raceId = T2.raceId
    WHERE T1.forename = ? AND T1.surname = ? AND T3.year = ?
    """
    cursor.execute(query, (forename, surname, year))
    result = cursor.fetchone()
    return {"average_points": result[0]}

# Endpoint to get race details for a given driver
@app.get("/v1/bird/formula_1/race_details", summary="Get race details for a given driver")
async def get_race_details(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT T1.name, T2.points
    FROM races AS T1
    INNER JOIN driverStandings AS T2 ON T2.raceId = T1.raceId
    INNER JOIN drivers AS T3 ON T3.driverId = T2.driverId
    WHERE T3.forename = ? AND T3.surname = ?
    ORDER BY T1.year ASC
    LIMIT 1
    """
    cursor.execute(query, (forename, surname))
    result = cursor.fetchall()
    conn.close()
    return result

# Endpoint to get distinct race details for a given year
@app.get("/v1/bird/formula_1/race_details_by_year", summary="Get distinct race details for a given year")
async def get_race_details_by_year(year: int = Query(..., description="Year of the race")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT DISTINCT T2.name, T1.country
    FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE T2.year = ?
    ORDER BY T2.date ASC
    """
    cursor.execute(query, (year,))
    result = cursor.fetchall()
    conn.close()
    return result

# Endpoint to get lap details
@app.get("/v1/bird/formula_1/lap_details", summary="Get lap details")
async def get_lap_details():
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT T3.lap, T2.name, T2.year, T1.location
    FROM circuits AS T1
    INNER JOIN races AS T2 ON T1.circuitId = T2.circuitId
    INNER JOIN lapTimes AS T3 ON T3.raceId = T2.raceId
    ORDER BY T3.lap DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return result

# Endpoint to get percentage of races in a specific country
@app.get("/v1/bird/formula_1/race_percentage", summary="Get percentage of races in a specific country")
async def get_race_percentage(country: str = Query(..., description="Country name")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT CAST(COUNT(CASE WHEN T1.country = ? THEN T2.circuitID END) AS REAL) * 100 / COUNT(T2.circuitId)
    FROM circuits AS T1
    INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId
    WHERE T2.name = 'European Grand Prix'
    """
    cursor.execute(query, (country,))
    result = cursor.fetchall()
    conn.close()
    return result

# Endpoint to get circuit coordinates
@app.get("/v1/bird/formula_1/circuit_coordinates", summary="Get circuit coordinates")
async def get_circuit_coordinates(name: str = Query(..., description="Circuit name")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT lat, lng
    FROM circuits
    WHERE name = ?
    """
    cursor.execute(query, (name,))
    result = cursor.fetchall()
    conn.close()
    return result

# Endpoint to get circuit names by list of names
@app.get("/v1/bird/formula_1/circuit_names", summary="Get circuit names by list of names")
async def get_circuit_names(names: str = Query(..., description="Comma-separated list of circuit names")):
    conn = get_db_connection()
    cursor = conn.cursor()
    names_list = names.split(',')
    placeholders = ','.join('?' for _ in names_list)
    query = f"""
    SELECT name
    FROM circuits
    WHERE name IN ({placeholders})
    ORDER BY lat DESC
    LIMIT 1
    """
    cursor.execute(query, names_list)
    result = cursor.fetchall()
    conn.close()
    return result

# Endpoint to get circuit reference
@app.get("/v1/bird/formula_1/circuit_reference", summary="Get circuit reference")
async def get_circuit_reference(name: str = Query(..., description="Circuit name")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT circuitRef
    FROM circuits
    WHERE name = ?
    """
    cursor.execute(query, (name,))
    result = cursor.fetchall()
    conn.close()
    return result

# Endpoint to get country with highest altitude
@app.get("/v1/bird/formula_1/highest_altitude_country", summary="Get country with highest altitude")
async def get_highest_altitude_country():
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT country
    FROM circuits
    ORDER BY alt DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return result

# Endpoint to get count of drivers without code
@app.get("/v1/bird/formula_1/driver_count_without_code", summary="Get count of drivers without code")
async def get_driver_count_without_code():
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT COUNT(driverId) - COUNT(CASE WHEN code IS NOT NULL THEN code END)
    FROM drivers
    """
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return result

# Endpoint to get nationality of the youngest driver
@app.get("/v1/bird/formula_1/youngest_driver_nationality", summary="Get nationality of the youngest driver")
async def get_youngest_driver_nationality():
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT nationality
    FROM drivers
    WHERE dob IS NOT NULL
    ORDER BY dob ASC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return result
# Endpoint to get driver surname by nationality
@app.get("/v1/bird/formula_1/drivers/surname", summary="Get driver surname by nationality")
async def get_driver_surname(nationality: str = Query(..., description="Nationality of the driver")):
    query = "SELECT surname FROM drivers WHERE nationality = ?"
    cursor.execute(query, (nationality,))
    result = cursor.fetchall()
    return {"surname": result}

# Endpoint to get driver URL by forename and surname
@app.get("/v1/bird/formula_1/drivers/url", summary="Get driver URL by forename and surname")
async def get_driver_url(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver")):
    query = "SELECT url FROM drivers WHERE forename = ? AND surname = ?"
    cursor.execute(query, (forename, surname))
    result = cursor.fetchall()
    return {"url": result}

# Endpoint to get driver reference by forename and surname
@app.get("/v1/bird/formula_1/drivers/driverRef", summary="Get driver reference by forename and surname")
async def get_driver_reference(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver")):
    query = "SELECT driverRef FROM drivers WHERE forename = ? AND surname = ?"
    cursor.execute(query, (forename, surname))
    result = cursor.fetchall()
    return {"driverRef": result}

# Endpoint to get circuit name by year and race name
@app.get("/v1/bird/formula_1/circuits/name", summary="Get circuit name by year and race name")
async def get_circuit_name(year: int = Query(..., description="Year of the race"), race_name: str = Query(..., description="Name of the race")):
    query = "SELECT T1.name FROM circuits AS T1 INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId WHERE T2.year = ? AND T2.name = ?"
    cursor.execute(query, (year, race_name))
    result = cursor.fetchall()
    return {"name": result}

# Endpoint to get distinct years for a circuit
@app.get("/v1/bird/formula_1/circuits/years", summary="Get distinct years for a circuit")
async def get_circuit_years(circuit_name: str = Query(..., description="Name of the circuit")):
    query = "SELECT DISTINCT T2.year FROM circuits AS T1 INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId WHERE T1.name = ?"
    cursor.execute(query, (circuit_name,))
    result = cursor.fetchall()
    return {"years": result}

# Endpoint to get distinct URLs for a circuit
@app.get("/v1/bird/formula_1/circuits/urls", summary="Get distinct URLs for a circuit")
async def get_circuit_urls(circuit_name: str = Query(..., description="Name of the circuit")):
    query = "SELECT DISTINCT T1.url FROM circuits AS T1 INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId WHERE T1.name = ?"
    cursor.execute(query, (circuit_name,))
    result = cursor.fetchall()
    return {"urls": result}

# Endpoint to get race date and time by year and race name
@app.get("/v1/bird/formula_1/races/datetime", summary="Get race date and time by year and race name")
async def get_race_datetime(year: int = Query(..., description="Year of the race"), race_name: str = Query(..., description="Name of the race")):
    query = "SELECT T2.date, T2.time FROM circuits AS T1 INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId WHERE T2.year = ? AND T2.name = ?"
    cursor.execute(query, (year, race_name))
    result = cursor.fetchall()
    return {"datetime": result}

# Endpoint to get count of races for a country
@app.get("/v1/bird/formula_1/races/count", summary="Get count of races for a country")
async def get_race_count(country: str = Query(..., description="Country of the circuit")):
    query = "SELECT COUNT(T2.circuitId) FROM circuits AS T1 INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId WHERE T1.country = ?"
    cursor.execute(query, (country,))
    result = cursor.fetchall()
    return {"count": result}

# Endpoint to get race date by circuit name
@app.get("/v1/bird/formula_1/races/date", summary="Get race date by circuit name")
async def get_race_date(circuit_name: str = Query(..., description="Name of the circuit")):
    query = "SELECT T2.date FROM circuits AS T1 INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId WHERE T1.name = ?"
    cursor.execute(query, (circuit_name,))
    result = cursor.fetchall()
    return {"date": result}

# Endpoint to get circuit URL by year and race name
@app.get("/v1/bird/formula_1/circuits/url", summary="Get circuit URL by year and race name")
async def get_circuit_url(year: int = Query(..., description="Year of the race"), race_name: str = Query(..., description="Name of the race")):
    query = "SELECT T1.url FROM circuits AS T1 INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId WHERE T2.year = ? AND T2.name = ?"
    cursor.execute(query, (year, race_name))
    result = cursor.fetchall()
    return {"url": result}

# Endpoint to get the fastest lap time for a given driver
@app.get("/v1/bird/formula_1/fastest_lap_time", summary="Get the fastest lap time for a given driver")
async def get_fastest_lap_time(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver")):
    query = """
    SELECT T2.fastestLapTime
    FROM drivers AS T1
    INNER JOIN results AS T2 ON T2.driverId = T1.driverId
    WHERE T1.forename = ? AND T1.surname = ? AND T2.fastestLapTime IS NOT NULL
    ORDER BY T2.fastestLapTime ASC
    LIMIT 1
    """
    cursor.execute(query, (forename, surname))
    result = cursor.fetchone()
    return {"fastest_lap_time": result[0] if result else None}

# Endpoint to get the driver with the fastest lap speed
@app.get("/v1/bird/formula_1/fastest_lap_speed_driver", summary="Get the driver with the fastest lap speed")
async def get_fastest_lap_speed_driver():
    query = """
    SELECT T1.forename, T1.surname
    FROM drivers AS T1
    INNER JOIN results AS T2 ON T2.driverId = T1.driverId
    WHERE T2.fastestLapTime IS NOT NULL
    ORDER BY T2.fastestLapSpeed DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"forename": result[0], "surname": result[1]} if result else {}

# Endpoint to get the winner of a specific race
@app.get("/v1/bird/formula_1/race_winner", summary="Get the winner of a specific race")
async def get_race_winner(race_name: str = Query(..., description="Name of the race"), year: int = Query(..., description="Year of the race")):
    query = """
    SELECT T3.forename, T3.surname, T3.driverRef
    FROM races AS T1
    INNER JOIN results AS T2 ON T2.raceId = T1.raceId
    INNER JOIN drivers AS T3 ON T3.driverId = T2.driverId
    WHERE T1.name = ? AND T2.rank = 1 AND T1.year = ?
    """
    cursor.execute(query, (race_name, year))
    result = cursor.fetchone()
    return {"forename": result[0], "surname": result[1], "driverRef": result[2]} if result else {}

# Endpoint to get races for a specific driver
@app.get("/v1/bird/formula_1/driver_races", summary="Get races for a specific driver")
async def get_driver_races(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver")):
    query = """
    SELECT T1.name
    FROM races AS T1
    INNER JOIN results AS T2 ON T2.raceId = T1.raceId
    INNER JOIN drivers AS T3 ON T3.driverId = T2.driverId
    WHERE T3.forename = ? AND T3.surname = ?
    """
    cursor.execute(query, (forename, surname))
    result = cursor.fetchall()
    return [{"race_name": row[0]} for row in result]

# Endpoint to get races won by a specific driver
@app.get("/v1/bird/formula_1/driver_wins", summary="Get races won by a specific driver")
async def get_driver_wins(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver")):
    query = """
    SELECT name
    FROM races
    WHERE raceId IN (
        SELECT raceId
        FROM results
        WHERE rank = 1 AND driverId = (
            SELECT driverId
            FROM drivers
            WHERE forename = ? AND surname = ?
        )
    )
    """
    cursor.execute(query, (forename, surname))
    result = cursor.fetchall()
    return [{"race_name": row[0]} for row in result]

# Endpoint to get the fastest lap speed for a specific race
@app.get("/v1/bird/formula_1/fastest_lap_speed", summary="Get the fastest lap speed for a specific race")
async def get_fastest_lap_speed(race_name: str = Query(..., description="Name of the race"), year: int = Query(..., description="Year of the race")):
    query = """
    SELECT T2.fastestLapSpeed
    FROM races AS T1
    INNER JOIN results AS T2 ON T2.raceId = T1.raceId
    WHERE T1.name = ? AND T1.year = ? AND T2.fastestLapSpeed IS NOT NULL
    ORDER BY T2.fastestLapSpeed DESC
    LIMIT 1
    """
    cursor.execute(query, (race_name, year))
    result = cursor.fetchone()
    return {"fastest_lap_speed": result[0] if result else None}

# Endpoint to get distinct years for a specific driver
@app.get("/v1/bird/formula_1/driver_years", summary="Get distinct years for a specific driver")
async def get_driver_years(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver")):
    query = """
    SELECT DISTINCT T1.year
    FROM races AS T1
    INNER JOIN results AS T2 ON T2.raceId = T1.raceId
    INNER JOIN drivers AS T3 ON T3.driverId = T2.driverId
    WHERE T3.forename = ? AND T3.surname = ?
    """
    cursor.execute(query, (forename, surname))
    result = cursor.fetchall()
    return [{"year": row[0]} for row in result]

# Endpoint to get the position order for a specific driver in a specific race
@app.get("/v1/bird/formula_1/driver_position", summary="Get the position order for a specific driver in a specific race")
async def get_driver_position(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver"), race_name: str = Query(..., description="Name of the race"), year: int = Query(..., description="Year of the race")):
    query = """
    SELECT T2.positionOrder
    FROM races AS T1
    INNER JOIN results AS T2 ON T2.raceId = T1.raceId
    INNER JOIN drivers AS T3 ON T3.driverId = T2.driverId
    WHERE T3.forename = ? AND T3.surname = ? AND T1.name = ? AND T1.year = ?
    """
    cursor.execute(query, (forename, surname, race_name, year))
    result = cursor.fetchone()
    return {"position_order": result[0] if result else None}

# Endpoint to get drivers who started at a specific grid position in a specific race
@app.get("/v1/bird/formula_1/grid_position_drivers", summary="Get drivers who started at a specific grid position in a specific race")
async def get_grid_position_drivers(grid: int = Query(..., description="Grid position"), race_name: str = Query(..., description="Name of the race"), year: int = Query(..., description="Year of the race")):
    query = """
    SELECT T3.forename, T3.surname
    FROM races AS T1
    INNER JOIN results AS T2 ON T2.raceId = T1.raceId
    INNER JOIN drivers AS T3 ON T3.driverId = T2.driverId
    WHERE T2.grid = ? AND T1.name = ? AND T1.year = ?
    """
    cursor.execute(query, (grid, race_name, year))
    result = cursor.fetchall()
    return [{"forename": row[0], "surname": row[1]} for row in result]

# Endpoint to get the count of drivers who finished a specific race
@app.get("/v1/bird/formula_1/finished_drivers_count", summary="Get the count of drivers who finished a specific race")
async def get_finished_drivers_count(race_name: str = Query(..., description="Name of the race"), year: int = Query(..., description="Year of the race")):
    query = """
    SELECT COUNT(T2.driverId)
    FROM races AS T1
    INNER JOIN results AS T2 ON T2.raceId = T1.raceId
    WHERE T1.name = ? AND T1.year = ? AND T2.time IS NOT NULL
    """
    cursor.execute(query, (race_name, year))
    result = cursor.fetchone()
    return {"finished_drivers_count": result[0] if result else 0}
# Endpoint to get fastest lap for a given race, year, and driver
@app.get("/v1/bird/formula_1/fastest_lap", summary="Get fastest lap for a given race, year, and driver")
async def get_fastest_lap(race_name: str = Query(..., description="Name of the race"),
                          year: int = Query(..., description="Year of the race"),
                          forename: str = Query(..., description="Forename of the driver"),
                          surname: str = Query(..., description="Surname of the driver")):
    query = """
    SELECT T1.fastestLap
    FROM results AS T1
    INNER JOIN races AS T2 on T1.raceId = T2.raceId
    INNER JOIN drivers AS T3 on T1.driverId = T3.driverId
    WHERE T2.name = ? AND T2.year = ? AND T3.forename = ? AND T3.surname = ?
    """
    cursor.execute(query, (race_name, year, forename, surname))
    result = cursor.fetchall()
    return result

# Endpoint to get time for a given rank, race, and year
@app.get("/v1/bird/formula_1/time_for_rank", summary="Get time for a given rank, race, and year")
async def get_time_for_rank(rank: int = Query(..., description="Rank of the driver"),
                            race_name: str = Query(..., description="Name of the race"),
                            year: int = Query(..., description="Year of the race")):
    query = """
    SELECT T1.time
    FROM results AS T1
    INNER JOIN races AS T2 on T1.raceId = T2.raceId
    WHERE T1.rank = ? AND T2.name = ? AND T2.year = ?
    """
    cursor.execute(query, (rank, race_name, year))
    result = cursor.fetchall()
    return result

# Endpoint to get driver details for a given race, time pattern, and year
@app.get("/v1/bird/formula_1/driver_details", summary="Get driver details for a given race, time pattern, and year")
async def get_driver_details(race_name: str = Query(..., description="Name of the race"),
                             time_pattern: str = Query(..., description="Time pattern"),
                             year: int = Query(..., description="Year of the race")):
    query = """
    SELECT T1.forename, T1.surname, T1.url
    FROM drivers AS T1
    INNER JOIN results AS T2 ON T1.driverId = T2.driverId
    INNER JOIN races AS T3 ON T3.raceId = T2.raceId
    WHERE T3.name = ? AND T2.time LIKE ? AND T3.year = ?
    """
    cursor.execute(query, (race_name, time_pattern, year))
    result = cursor.fetchall()
    return result

# Endpoint to get count of drivers for a given race, nationality, and year
@app.get("/v1/bird/formula_1/driver_count", summary="Get count of drivers for a given race, nationality, and year")
async def get_driver_count(race_name: str = Query(..., description="Name of the race"),
                           nationality: str = Query(..., description="Nationality of the driver"),
                           year: int = Query(..., description="Year of the race")):
    query = """
    SELECT COUNT(*)
    FROM drivers AS T1
    INNER JOIN results AS T2 ON T1.driverId = T2.driverId
    INNER JOIN races AS T3 ON T3.raceId = T2.raceId
    WHERE T3.name = ? AND T1.nationality = ? AND T3.year = ?
    """
    cursor.execute(query, (race_name, nationality, year))
    result = cursor.fetchall()
    return result

# Endpoint to get count of drivers with non-null time for a given race and year
@app.get("/v1/bird/formula_1/driver_count_non_null_time", summary="Get count of drivers with non-null time for a given race and year")
async def get_driver_count_non_null_time(race_name: str = Query(..., description="Name of the race"),
                                         year: int = Query(..., description="Year of the race")):
    query = """
    SELECT COUNT(*)
    FROM (
        SELECT T1.driverId
        FROM results AS T1
        INNER JOIN races AS T2 on T1.raceId = T2.raceId
        WHERE T2.name = ? AND T2.year = ? AND T1.time IS NOT NULL
        GROUP BY T1.driverId
        HAVING COUNT(T2.raceId) > 0
    )
    """
    cursor.execute(query, (race_name, year))
    result = cursor.fetchall()
    return result

# Endpoint to get sum of points for a given driver
@app.get("/v1/bird/formula_1/sum_points", summary="Get sum of points for a given driver")
async def get_sum_points(forename: str = Query(..., description="Forename of the driver"),
                         surname: str = Query(..., description="Surname of the driver")):
    query = """
    SELECT SUM(T2.points)
    FROM drivers AS T1
    INNER JOIN results AS T2 ON T1.driverId = T2.driverId
    WHERE T1.forename = ? AND T1.surname = ?
    """
    cursor.execute(query, (forename, surname))
    result = cursor.fetchall()
    return result

# Endpoint to get average fastest lap time for a given driver
@app.get("/v1/bird/formula_1/avg_fastest_lap_time", summary="Get average fastest lap time for a given driver")
async def get_avg_fastest_lap_time(forename: str = Query(..., description="Forename of the driver"),
                                   surname: str = Query(..., description="Surname of the driver")):
    query = """
    SELECT AVG(CAST(SUBSTR(T2.fastestLapTime, 1, INSTR(T2.fastestLapTime, ':') - 1) AS INTEGER) * 60 + CAST(SUBSTR(T2.fastestLapTime, INSTR(T2.fastestLapTime, ':') + 1) AS REAL))
    FROM drivers AS T1
    INNER JOIN results AS T2 ON T1.driverId = T2.driverId
    WHERE T1.surname = ? AND T1.forename = ?
    """
    cursor.execute(query, (surname, forename))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of non-null times for a given race and year
@app.get("/v1/bird/formula_1/percentage_non_null_times", summary="Get percentage of non-null times for a given race and year")
async def get_percentage_non_null_times(race_name: str = Query(..., description="Name of the race"),
                                        year: int = Query(..., description="Year of the race")):
    query = """
    SELECT CAST(SUM(IIF(T1.time IS NOT NULL, 1, 0)) AS REAL) * 100 / COUNT(T1.resultId)
    FROM results AS T1
    INNER JOIN races AS T2 ON T1.raceId = T2.raceId
    WHERE T2.name = ? AND T2.year = ?
    """
    cursor.execute(query, (race_name, year))
    result = cursor.fetchall()
    return result

# Endpoint to get time incremental percentage for a given race and year
@app.get("/v1/bird/formula_1/time_incremental_percentage", summary="Get time incremental percentage for a given race and year")
async def get_time_incremental_percentage(race_name: str = Query(..., description="Name of the race"),
                                          year: int = Query(..., description="Year of the race")):
    query = """
    WITH time_in_seconds AS (
        SELECT T1.positionOrder,
            CASE
                WHEN T1.positionOrder = 1 THEN (CAST(SUBSTR(T1.time, 1, 1) AS REAL) * 3600) + (CAST(SUBSTR(T1.time, 3, 2) AS REAL) * 60) + CAST(SUBSTR(T1.time, 6) AS REAL)
                ELSE CAST(SUBSTR(T1.time, 2) AS REAL)
            END AS time_seconds
        FROM results AS T1
        INNER JOIN races AS T2 ON T1.raceId = T2.raceId
        WHERE T2.name = ? AND T1.time IS NOT NULL AND T2.year = ?
    ),
    champion_time AS (
        SELECT time_seconds
        FROM time_in_seconds
        WHERE positionOrder = 1
    ),
    last_driver_incremental AS (
        SELECT time_seconds
        FROM time_in_seconds
        WHERE positionOrder = (SELECT MAX(positionOrder) FROM time_in_seconds)
    )
    SELECT (CAST((SELECT time_seconds FROM last_driver_incremental) AS REAL) * 100) / (SELECT time_seconds + (SELECT time_seconds FROM last_driver_incremental) FROM champion_time)
    """
    cursor.execute(query, (race_name, year))
    result = cursor.fetchall()
    return result

# Endpoint to get count of circuits for a given location and country
@app.get("/v1/bird/formula_1/circuit_count", summary="Get count of circuits for a given location and country")
async def get_circuit_count(location: str = Query(..., description="Location of the circuit"),
                            country: str = Query(..., description="Country of the circuit")):
    query = """
    SELECT COUNT(circuitId)
    FROM circuits
    WHERE location = ? AND country = ?
    """
    cursor.execute(query, (location, country))
    result = cursor.fetchall()
    return result

# Endpoint to get lat and lng for a given country
@app.get("/v1/bird/formula_1/circuits", summary="Get lat and lng for a given country")
async def get_circuits_by_country(country: str = Query(..., description="Country name")):
    query = "SELECT lat, lng FROM circuits WHERE country = ?"
    cursor.execute(query, (country,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of drivers for a given nationality and birth year
@app.get("/v1/bird/formula_1/drivers_count", summary="Get count of drivers for a given nationality and birth year")
async def get_drivers_count(nationality: str = Query(..., description="Nationality of the driver"), birth_year: int = Query(..., description="Birth year of the driver")):
    query = "SELECT COUNT(driverId) FROM drivers WHERE nationality = ? AND STRFTIME('%Y', dob) > ?"
    cursor.execute(query, (nationality, birth_year))
    result = cursor.fetchone()
    return result

# Endpoint to get max points for a given nationality
@app.get("/v1/bird/formula_1/max_points", summary="Get max points for a given nationality")
async def get_max_points(nationality: str = Query(..., description="Nationality of the constructor")):
    query = "SELECT MAX(T1.points) FROM constructorStandings AS T1 INNER JOIN constructors AS T2 on T1.constructorId = T2.constructorId WHERE T2.nationality = ?"
    cursor.execute(query, (nationality,))
    result = cursor.fetchone()
    return result

# Endpoint to get top constructor by points
@app.get("/v1/bird/formula_1/top_constructor", summary="Get top constructor by points")
async def get_top_constructor():
    query = "SELECT T2.name FROM constructorStandings AS T1 INNER JOIN constructors AS T2 on T1.constructorId = T2.constructorId ORDER BY T1.points DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return result

# Endpoint to get constructors with zero points for a given raceId
@app.get("/v1/bird/formula_1/zero_points_constructors", summary="Get constructors with zero points for a given raceId")
async def get_zero_points_constructors(race_id: int = Query(..., description="Race ID")):
    query = "SELECT T2.name FROM constructorStandings AS T1 INNER JOIN constructors AS T2 on T1.constructorId = T2.constructorId WHERE T1.points = 0 AND T1.raceId = ?"
    cursor.execute(query, (race_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of races with zero points for a given nationality
@app.get("/v1/bird/formula_1/zero_points_races_count", summary="Get count of races with zero points for a given nationality")
async def get_zero_points_races_count(nationality: str = Query(..., description="Nationality of the constructor")):
    query = "SELECT COUNT(T1.raceId) FROM constructorStandings AS T1 INNER JOIN constructors AS T2 on T1.constructorId = T2.constructorId WHERE T1.points = 0 AND T2.nationality = ? GROUP BY T1.constructorId HAVING COUNT(raceId) = 2"
    cursor.execute(query, (nationality,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct constructor names for rank 1
@app.get("/v1/bird/formula_1/distinct_constructors", summary="Get distinct constructor names for rank 1")
async def get_distinct_constructors():
    query = "SELECT DISTINCT T2.name FROM results AS T1 INNER JOIN constructors AS T2 on T1.constructorId = T2.constructorId WHERE T1.rank = 1"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get count of distinct constructors for laps greater than a given value and nationality
@app.get("/v1/bird/formula_1/distinct_constructors_count", summary="Get count of distinct constructors for laps greater than a given value and nationality")
async def get_distinct_constructors_count(laps: int = Query(..., description="Number of laps"), nationality: str = Query(..., description="Nationality of the constructor")):
    query = "SELECT COUNT(DISTINCT T2.constructorId) FROM results AS T1 INNER JOIN constructors AS T2 on T1.constructorId = T2.constructorId WHERE T1.laps > ? AND T2.nationality = ?"
    cursor.execute(query, (laps, nationality))
    result = cursor.fetchone()
    return result

# Endpoint to get percentage of races completed by a given nationality within a year range
@app.get("/v1/bird/formula_1/races_completed_percentage", summary="Get percentage of races completed by a given nationality within a year range")
async def get_races_completed_percentage(nationality: str = Query(..., description="Nationality of the driver"), start_year: int = Query(..., description="Start year"), end_year: int = Query(..., description="End year")):
    query = "SELECT CAST(SUM(IIF(T1.time IS NOT NULL, 1, 0)) AS REAL) * 100 / COUNT(T1.raceId) FROM results AS T1 INNER JOIN races AS T2 on T1.raceId = T2.raceId INNER JOIN drivers AS T3 on T1.driverId = T3.driverId WHERE T3.nationality = ? AND T2.year BETWEEN ? AND ?"
    cursor.execute(query, (nationality, start_year, end_year))
    result = cursor.fetchone()
    return result

# Endpoint to get average champion time for years before a given year
@app.get("/v1/bird/formula_1/average_champion_time", summary="Get average champion time for years before a given year")
async def get_average_champion_time(year: int = Query(..., description="Year")):
    query = """
    WITH time_in_seconds AS (
        SELECT T2.year, T2.raceId, T1.positionOrder,
        CASE
            WHEN T1.positionOrder = 1 THEN (CAST(SUBSTR(T1.time, 1, 1) AS REAL) * 3600) + (CAST(SUBSTR(T1.time, 3, 2) AS REAL) * 60) + CAST(SUBSTR(T1.time, 6,2) AS REAL ) + CAST(SUBSTR(T1.time, 9) AS REAL)/1000
            ELSE 0
        END AS time_seconds
        FROM results AS T1
        INNER JOIN races AS T2 ON T1.raceId = T2.raceId
        WHERE T1.time IS NOT NULL
    ),
    champion_time AS (
        SELECT year, raceId, time_seconds
        FROM time_in_seconds
        WHERE positionOrder = 1
    )
    SELECT year, AVG(time_seconds)
    FROM champion_time
    WHERE year < ?
    GROUP BY year
    HAVING AVG(time_seconds) IS NOT NULL
    """
    cursor.execute(query, (year,))
    result = cursor.fetchall()
    return result

# Endpoint to get drivers born after a certain year and with a specific rank
@app.get("/v1/bird/formula_1/drivers_by_year_and_rank", summary="Get drivers born after a certain year and with a specific rank")
async def get_drivers_by_year_and_rank(year: int = Query(..., description="Year of birth"), rank: int = Query(..., description="Rank of the driver")):
    query = """
    SELECT T2.forename, T2.surname
    FROM results AS T1
    INNER JOIN drivers AS T2 on T1.driverId = T2.driverId
    WHERE STRFTIME('%Y', T2.dob) > ? AND T1.rank = ?
    """
    cursor.execute(query, (year, rank))
    results = cursor.fetchall()
    return results

# Endpoint to get count of drivers with a specific nationality and null time
@app.get("/v1/bird/formula_1/count_drivers_by_nationality_and_null_time", summary="Get count of drivers with a specific nationality and null time")
async def get_count_drivers_by_nationality_and_null_time(nationality: str = Query(..., description="Nationality of the driver")):
    query = """
    SELECT COUNT(T1.driverId)
    FROM results AS T1
    INNER JOIN drivers AS T2 on T1.driverId = T2.driverId
    WHERE T2.nationality = ? AND T1.time IS NULL
    """
    cursor.execute(query, (nationality,))
    results = cursor.fetchall()
    return results

# Endpoint to get driver with the fastest lap time
@app.get("/v1/bird/formula_1/driver_with_fastest_lap_time", summary="Get driver with the fastest lap time")
async def get_driver_with_fastest_lap_time():
    query = """
    SELECT T2.forename, T2.surname, T1.fastestLapTime
    FROM results AS T1
    INNER JOIN drivers AS T2 on T1.driverId = T2.driverId
    WHERE T1.fastestLapTime IS NOT NULL
    ORDER BY T1.fastestLapTime ASC
    LIMIT 1
    """
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get fastest lap in a specific year with a specific time format
@app.get("/v1/bird/formula_1/fastest_lap_by_year_and_time_format", summary="Get fastest lap in a specific year with a specific time format")
async def get_fastest_lap_by_year_and_time_format(year: int = Query(..., description="Year of the race"), time_format: str = Query(..., description="Time format")):
    query = """
    SELECT T1.fastestLap
    FROM results AS T1
    INNER JOIN races AS T2 on T1.raceId = T2.raceId
    WHERE T2.year = ? AND T1.time LIKE ?
    """
    cursor.execute(query, (year, time_format))
    results = cursor.fetchall()
    return results

# Endpoint to get average fastest lap speed in a specific year and race
@app.get("/v1/bird/formula_1/avg_fastest_lap_speed_by_year_and_race", summary="Get average fastest lap speed in a specific year and race")
async def get_avg_fastest_lap_speed_by_year_and_race(year: int = Query(..., description="Year of the race"), race_name: str = Query(..., description="Name of the race")):
    query = """
    SELECT AVG(T1.fastestLapSpeed)
    FROM results AS T1
    INNER JOIN races AS T2 on T1.raceId = T2.raceId
    WHERE T2.year = ? AND T2.name = ?
    """
    cursor.execute(query, (year, race_name))
    results = cursor.fetchall()
    return results

# Endpoint to get race with the shortest milliseconds
@app.get("/v1/bird/formula_1/race_with_shortest_milliseconds", summary="Get race with the shortest milliseconds")
async def get_race_with_shortest_milliseconds():
    query = """
    SELECT T1.name, T1.year
    FROM races AS T1
    INNER JOIN results AS T2 on T1.raceId = T2.raceId
    WHERE T2.milliseconds IS NOT NULL
    ORDER BY T2.milliseconds
    LIMIT 1
    """
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get percentage of drivers born before a certain year with more than a certain number of laps
@app.get("/v1/bird/formula_1/percentage_drivers_by_year_and_laps", summary="Get percentage of drivers born before a certain year with more than a certain number of laps")
async def get_percentage_drivers_by_year_and_laps(year: int = Query(..., description="Year of birth"), laps: int = Query(..., description="Number of laps")):
    query = """
    SELECT CAST(SUM(IIF(STRFTIME('%Y', T3.dob) < ? AND T1.laps > ?, 1, 0)) AS REAL) * 100 / COUNT(*)
    FROM results AS T1
    INNER JOIN races AS T2 on T1.raceId = T2.raceId
    INNER JOIN drivers AS T3 on T1.driverId = T3.driverId
    WHERE T2.year BETWEEN 2000 AND 2005
    """
    cursor.execute(query, (year, laps))
    results = cursor.fetchall()
    return results

# Endpoint to get count of French drivers with lap time less than a certain value
@app.get("/v1/bird/formula_1/count_french_drivers_by_lap_time", summary="Get count of French drivers with lap time less than a certain value")
async def get_count_french_drivers_by_lap_time(lap_time: float = Query(..., description="Lap time in seconds")):
    query = """
    SELECT COUNT(T1.driverId)
    FROM drivers AS T1
    INNER JOIN lapTimes AS T2 on T1.driverId = T2.driverId
    WHERE T1.nationality = 'French' AND (CAST(SUBSTR(T2.time, 1, 2) AS INTEGER) * 60 + CAST(SUBSTR(T2.time, 4, 2) AS INTEGER) + CAST(SUBSTR(T2.time, 7, 2) AS REAL) / 1000) < ?
    """
    cursor.execute(query, (lap_time,))
    results = cursor.fetchall()
    return results

# Endpoint to get code of American drivers
@app.get("/v1/bird/formula_1/code_of_american_drivers", summary="Get code of American drivers")
async def get_code_of_american_drivers():
    query = """
    SELECT code
    FROM drivers
    WHERE Nationality = 'American'
    """
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get race IDs in a specific year
@app.get("/v1/bird/formula_1/race_ids_by_year", summary="Get race IDs in a specific year")
async def get_race_ids_by_year(year: int = Query(..., description="Year of the race")):
    query = """
    SELECT raceId
    FROM races
    WHERE year = ?
    """
    cursor.execute(query, (year,))
    results = cursor.fetchall()
    return results
# Endpoint to get count of driverId for a given raceId
@app.get("/v1/bird/formula_1/driver_count_by_race", summary="Get count of driverId for a given raceId")
async def get_driver_count_by_race(race_id: int = Query(..., description="ID of the race")):
    query = "SELECT COUNT(driverId) FROM driverStandings WHERE raceId = ?"
    cursor.execute(query, (race_id,))
    result = cursor.fetchone()
    return {"driver_count": result[0]}

# Endpoint to get count of drivers with a specific nationality in the top 3 youngest
@app.get("/v1/bird/formula_1/driver_count_by_nationality", summary="Get count of drivers with a specific nationality in the top 3 youngest")
async def get_driver_count_by_nationality(nationality: str = Query(..., description="Nationality of the driver")):
    query = """
    SELECT COUNT(*) FROM (
        SELECT T1.nationality FROM drivers AS T1
        ORDER BY JULIANDAY(T1.dob) DESC
        LIMIT 3
    ) AS T3 WHERE T3.nationality = ?
    """
    cursor.execute(query, (nationality,))
    result = cursor.fetchone()
    return {"driver_count": result[0]}

# Endpoint to get driverRef for a given forename and surname
@app.get("/v1/bird/formula_1/driver_by_name", summary="Get driverRef for a given forename and surname")
async def get_driver_by_name(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver")):
    query = "SELECT driverRef FROM drivers WHERE forename = ? AND surname = ?"
    cursor.execute(query, (forename, surname))
    result = cursor.fetchone()
    return {"driverRef": result[0]}

# Endpoint to get count of drivers with a specific nationality and birth year
@app.get("/v1/bird/formula_1/driver_count_by_nationality_and_year", summary="Get count of drivers with a specific nationality and birth year")
async def get_driver_count_by_nationality_and_year(nationality: str = Query(..., description="Nationality of the driver"), birth_year: int = Query(..., description="Birth year of the driver")):
    query = "SELECT COUNT(driverId) FROM drivers WHERE nationality = ? AND STRFTIME('%Y', dob) = ?"
    cursor.execute(query, (nationality, birth_year))
    result = cursor.fetchone()
    return {"driver_count": result[0]}

# Endpoint to get driverId for German drivers born between specific years
@app.get("/v1/bird/formula_1/german_drivers_by_birth_year_range", summary="Get driverId for German drivers born between specific years")
async def get_german_drivers_by_birth_year_range(start_year: int = Query(..., description="Start year of the birth year range"), end_year: int = Query(..., description="End year of the birth year range")):
    query = """
    SELECT T2.driverId FROM pitStops AS T1
    INNER JOIN drivers AS T2 on T1.driverId = T2.driverId
    WHERE T2.nationality = 'German' AND STRFTIME('%Y', T2.dob) BETWEEN ? AND ?
    ORDER BY T1.time
    LIMIT 3
    """
    cursor.execute(query, (start_year, end_year))
    result = cursor.fetchall()
    return {"driverIds": [row[0] for row in result]}

# Endpoint to get the youngest German driver
@app.get("/v1/bird/formula_1/youngest_german_driver", summary="Get the youngest German driver")
async def get_youngest_german_driver():
    query = "SELECT driverRef FROM drivers WHERE nationality = 'German' ORDER BY JULIANDAY(dob) ASC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"driverRef": result[0]}

# Endpoint to get driverId and code for drivers born in a specific year with fastest lap time
@app.get("/v1/bird/formula_1/drivers_by_birth_year_with_fastest_lap", summary="Get driverId and code for drivers born in a specific year with fastest lap time")
async def get_drivers_by_birth_year_with_fastest_lap(birth_year: int = Query(..., description="Birth year of the driver")):
    query = """
    SELECT T2.driverId, T2.code FROM results AS T1
    INNER JOIN drivers AS T2 on T1.driverId = T2.driverId
    WHERE STRFTIME('%Y', T2.dob) = ? AND T1.fastestLapTime IS NOT NULL
    """
    cursor.execute(query, (birth_year,))
    result = cursor.fetchall()
    return {"drivers": [{"driverId": row[0], "code": row[1]} for row in result]}

# Endpoint to get driverId for Spanish drivers born before a specific year
@app.get("/v1/bird/formula_1/spanish_drivers_by_birth_year", summary="Get driverId for Spanish drivers born before a specific year")
async def get_spanish_drivers_by_birth_year(birth_year: int = Query(..., description="Birth year of the driver")):
    query = """
    SELECT T2.driverId FROM pitStops AS T1
    INNER JOIN drivers AS T2 on T1.driverId = T2.driverId
    WHERE T2.nationality = 'Spanish' AND STRFTIME('%Y', T2.dob) < ?
    ORDER BY T1.time DESC
    LIMIT 10
    """
    cursor.execute(query, (birth_year,))
    result = cursor.fetchall()
    return {"driverIds": [row[0] for row in result]}

# Endpoint to get years of races with fastest lap time
@app.get("/v1/bird/formula_1/race_years_with_fastest_lap", summary="Get years of races with fastest lap time")
async def get_race_years_with_fastest_lap():
    query = "SELECT T2.year FROM results AS T1 INNER JOIN races AS T2 on T1.raceId = T2.raceId WHERE T1.fastestLapTime IS NOT NULL"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"years": [row[0] for row in result]}

# Endpoint to get the year of the race with the longest lap time
@app.get("/v1/bird/formula_1/race_year_with_longest_lap", summary="Get the year of the race with the longest lap time")
async def get_race_year_with_longest_lap():
    query = "SELECT T2.year FROM lapTimes AS T1 INNER JOIN races AS T2 on T1.raceId = T2.raceId ORDER BY T1.time DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"year": result[0]}

# Endpoint to get driverId for a given lap
@app.get("/v1/bird/formula_1/driverId", summary="Get driverId for a given lap")
async def get_driver_id_for_lap(lap: int = Query(..., description="Lap number"), limit: int = Query(5, description="Limit the number of results")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "SELECT driverId FROM lapTimes WHERE lap = ? ORDER BY time LIMIT ?"
    cursor.execute(query, (lap, limit))
    results = cursor.fetchall()
    conn.close()
    return results

# Endpoint to get the sum of valid times for a given statusId and raceId range
@app.get("/v1/bird/formula_1/sum_valid_times", summary="Get sum of valid times for a given statusId and raceId range")
async def get_sum_valid_times(statusId: int = Query(..., description="Status ID"), raceId_min: int = Query(..., description="Minimum race ID"), raceId_max: int = Query(..., description="Maximum race ID")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "SELECT SUM(IIF(time IS NOT NULL, 1, 0)) FROM results WHERE statusId = ? AND raceId BETWEEN ? AND ?"
    cursor.execute(query, (statusId, raceId_min, raceId_max))
    results = cursor.fetchall()
    conn.close()
    return results

# Endpoint to get distinct circuit details for a given country
@app.get("/v1/bird/formula_1/circuit_details", summary="Get distinct circuit details for a given country")
async def get_circuit_details(country: str = Query(..., description="Country name")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "SELECT DISTINCT location, lat, lng FROM circuits WHERE country = ?"
    cursor.execute(query, (country,))
    results = cursor.fetchall()
    conn.close()
    return results

# Endpoint to get raceId with the most valid times
@app.get("/v1/bird/formula_1/raceId_most_valid_times", summary="Get raceId with the most valid times")
async def get_race_id_most_valid_times():
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "SELECT raceId FROM results GROUP BY raceId ORDER BY COUNT(time IS NOT NULL) DESC LIMIT 1"
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results

# Endpoint to get driver details for a given raceId and q2
@app.get("/v1/bird/formula_1/driver_details", summary="Get driver details for a given raceId and q2")
async def get_driver_details(raceId: int = Query(..., description="Race ID"), q2: str = Query(..., description="q2 value")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "SELECT T2.driverRef, T2.nationality, T2.dob FROM qualifying AS T1 INNER JOIN drivers AS T2 on T1.driverId = T2.driverId WHERE T1.raceId = ? AND T1.q2 IS NOT NULL"
    cursor.execute(query, (raceId,))
    results = cursor.fetchall()
    conn.close()
    return results

# Endpoint to get race details for the youngest driver
@app.get("/v1/bird/formula_1/youngest_driver_race_details", summary="Get race details for the youngest driver")
async def get_youngest_driver_race_details():
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT T3.year, T3.name, T3.date, T3.time
    FROM qualifying AS T1
    INNER JOIN drivers AS T2 on T1.driverId = T2.driverId
    INNER JOIN races AS T3 on T1.raceId = T3.raceId
    WHERE T1.driverId = (SELECT driverId FROM drivers ORDER BY dob DESC LIMIT 1)
    ORDER BY T3.date ASC LIMIT 1
    """
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results

# Endpoint to get count of drivers with a specific status and nationality
@app.get("/v1/bird/formula_1/driver_count", summary="Get count of drivers with a specific status and nationality")
async def get_driver_count(status: str = Query(..., description="Status"), nationality: str = Query(..., description="Nationality")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT COUNT(T1.driverId)
    FROM drivers AS T1
    INNER JOIN results AS T2 on T1.driverId = T2.driverId
    INNER JOIN status AS T3 on T2.statusId = T3.statusId
    WHERE T3.status = ? AND T1.nationality = ?
    """
    cursor.execute(query, (status, nationality))
    results = cursor.fetchall()
    conn.close()
    return results

# Endpoint to get constructor URL for a given nationality
@app.get("/v1/bird/formula_1/constructor_url", summary="Get constructor URL for a given nationality")
async def get_constructor_url(nationality: str = Query(..., description="Nationality")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT T1.url
    FROM constructors AS T1
    INNER JOIN constructorStandings AS T2 on T1.constructorId = T2.constructorId
    WHERE T1.nationality = ?
    ORDER BY T2.points DESC LIMIT 1
    """
    cursor.execute(query, (nationality,))
    results = cursor.fetchall()
    conn.close()
    return results

# Endpoint to get constructor URL with the most wins
@app.get("/v1/bird/formula_1/constructor_most_wins", summary="Get constructor URL with the most wins")
async def get_constructor_most_wins():
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT T1.url
    FROM constructors AS T1
    INNER JOIN constructorStandings AS T2 on T1.constructorId = T2.constructorId
    ORDER BY T2.wins DESC LIMIT 1
    """
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results

# Endpoint to get driverId for a given race name and lap
@app.get("/v1/bird/formula_1/driverId_for_race_lap", summary="Get driverId for a given race name and lap")
async def get_driver_id_for_race_lap(race_name: str = Query(..., description="Race name"), lap: int = Query(..., description="Lap number")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT T1.driverId
    FROM lapTimes AS T1
    INNER JOIN races AS T2 on T1.raceId = T2.raceId
    WHERE T2.name = ? AND T1.lap = ?
    ORDER BY T1.time DESC LIMIT 1
    """
    cursor.execute(query, (race_name, lap))
    results = cursor.fetchall()
    conn.close()
    return results
# Endpoint to get the fastest lap time for a given lap number
@app.get("/v1/bird/formula_1/fastest_lap_time", summary="Get the fastest lap time for a given lap number")
async def get_fastest_lap_time(lap_number: int = Query(..., description="Lap number")):
    query = f"SELECT T1.milliseconds FROM lapTimes AS T1 INNER JOIN races AS T2 on T1.raceId = T2.raceId WHERE T1.lap = ? ORDER BY T1.time LIMIT 1"
    cursor.execute(query, (lap_number,))
    result = cursor.fetchone()
    return {"fastest_lap_time": result}

# Endpoint to get the average fastest lap time for a given year and race name
@app.get("/v1/bird/formula_1/average_fastest_lap_time", summary="Get the average fastest lap time for a given year and race name")
async def get_average_fastest_lap_time(year: int = Query(..., description="Year of the race"), race_name: str = Query(..., description="Name of the race")):
    query = f"SELECT AVG(T1.fastestLapTime) FROM results AS T1 INNER JOIN races AS T2 on T1.raceId = T2.raceId WHERE T1.rank < 11 AND T2.year = ? AND T2.name = ?"
    cursor.execute(query, (year, race_name))
    result = cursor.fetchone()
    return {"average_fastest_lap_time": result}

# Endpoint to get drivers with a given nationality and birth year range
@app.get("/v1/bird/formula_1/drivers_by_nationality_and_birth_year", summary="Get drivers with a given nationality and birth year range")
async def get_drivers_by_nationality_and_birth_year(nationality: str = Query(..., description="Nationality of the driver"), start_year: int = Query(..., description="Start year of birth"), end_year: int = Query(..., description="End year of birth")):
    query = f"SELECT T2.forename, T2.surname FROM pitStops AS T1 INNER JOIN drivers AS T2 on T1.driverId = T2.driverId WHERE T2.nationality = ? AND STRFTIME('%Y', T2.dob) BETWEEN ? AND ? GROUP BY T2.forename, T2.surname ORDER BY AVG(T1.duration) LIMIT 3"
    cursor.execute(query, (nationality, start_year, end_year))
    result = cursor.fetchall()
    return {"drivers": result}

# Endpoint to get race times for a given race name and year
@app.get("/v1/bird/formula_1/race_times", summary="Get race times for a given race name and year")
async def get_race_times(race_name: str = Query(..., description="Name of the race"), year: int = Query(..., description="Year of the race")):
    query = f"SELECT T1.time FROM results AS T1 INNER JOIN races AS T2 ON T1.raceId = T2.raceId WHERE T2.name = ? AND T2.year = ? AND T1.time LIKE '_:%:__.___'"
    cursor.execute(query, (race_name, year))
    result = cursor.fetchall()
    return {"race_times": result}

# Endpoint to get constructor details for a given race name and year
@app.get("/v1/bird/formula_1/constructor_details", summary="Get constructor details for a given race name and year")
async def get_constructor_details(race_name: str = Query(..., description="Name of the race"), year: int = Query(..., description="Year of the race")):
    query = f"SELECT T3.constructorRef, T3.url FROM results AS T1 INNER JOIN races AS T2 on T1.raceId = T2.raceId INNER JOIN constructors AS T3 on T1.constructorId = T3.constructorId WHERE T2.name = ? AND T2.year = ? AND T1.time LIKE '_:%:__.___'"
    cursor.execute(query, (race_name, year))
    result = cursor.fetchall()
    return {"constructor_details": result}

# Endpoint to get drivers with a given nationality and birth year range
@app.get("/v1/bird/formula_1/drivers_by_nationality_and_birth_year_range", summary="Get drivers with a given nationality and birth year range")
async def get_drivers_by_nationality_and_birth_year_range(nationality: str = Query(..., description="Nationality of the driver"), start_year: int = Query(..., description="Start year of birth"), end_year: int = Query(..., description="End year of birth")):
    query = f"SELECT forename, surname, dob FROM drivers WHERE nationality = ? AND STRFTIME('%Y', dob) BETWEEN ? AND ?"
    cursor.execute(query, (nationality, start_year, end_year))
    result = cursor.fetchall()
    return {"drivers": result}

# Endpoint to get drivers with a given nationality and birth year range ordered by date of birth
@app.get("/v1/bird/formula_1/drivers_by_nationality_and_birth_year_range_ordered", summary="Get drivers with a given nationality and birth year range ordered by date of birth")
async def get_drivers_by_nationality_and_birth_year_range_ordered(nationality: str = Query(..., description="Nationality of the driver"), start_year: int = Query(..., description="Start year of birth"), end_year: int = Query(..., description="End year of birth")):
    query = f"SELECT forename, surname, url, dob FROM drivers WHERE nationality = ? AND STRFTIME('%Y', dob) BETWEEN ? AND ? ORDER BY dob DESC"
    cursor.execute(query, (nationality, start_year, end_year))
    result = cursor.fetchall()
    return {"drivers": result}

# Endpoint to get circuit details for a given circuit name
@app.get("/v1/bird/formula_1/circuit_details", summary="Get circuit details for a given circuit name")
async def get_circuit_details(circuit_name: str = Query(..., description="Name of the circuit")):
    query = f"SELECT country, lat, lng FROM circuits WHERE name = ?"
    cursor.execute(query, (circuit_name,))
    result = cursor.fetchone()
    return {"circuit_details": result}

# Endpoint to get constructor points for a given race name and year range
@app.get("/v1/bird/formula_1/constructor_points", summary="Get constructor points for a given race name and year range")
async def get_constructor_points(race_name: str = Query(..., description="Name of the race"), start_year: int = Query(..., description="Start year of the race"), end_year: int = Query(..., description="End year of the race")):
    query = f"SELECT SUM(T1.points), T2.name, T2.nationality FROM constructorResults AS T1 INNER JOIN constructors AS T2 ON T1.constructorId = T2.constructorId INNER JOIN races AS T3 ON T3.raceid = T1.raceid WHERE T3.name = ? AND T3.year BETWEEN ? AND ? GROUP BY T2.name ORDER BY SUM(T1.points) DESC LIMIT 1"
    cursor.execute(query, (race_name, start_year, end_year))
    result = cursor.fetchone()
    return {"constructor_points": result}

# Endpoint to get average points for a given driver and race name
@app.get("/v1/bird/formula_1/average_points", summary="Get average points for a given driver and race name")
async def get_average_points(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver"), race_name: str = Query(..., description="Name of the race")):
    query = f"SELECT AVG(T2.points) FROM drivers AS T1 INNER JOIN driverStandings AS T2 ON T1.driverId = T2.driverId INNER JOIN races AS T3 ON T3.raceId = T2.raceId WHERE T1.forename = ? AND T1.surname = ? AND T3.name = ?"
    cursor.execute(query, (forename, surname, race_name))
    result = cursor.fetchone()
    return {"average_points": result}

# Endpoint to get the average number of races per year between two dates
@app.get("/v1/bird/formula_1/average_races_per_year", summary="Get the average number of races per year between two dates")
async def get_average_races_per_year(start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
                                     end_date: str = Query(..., description="End date in YYYY-MM-DD format")):
    query = f"""
    SELECT CAST(SUM(CASE WHEN year BETWEEN {start_date[:4]} AND {end_date[:4]} THEN 1 ELSE 0 END) AS REAL) / 10
    FROM races
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"average_races_per_year": result[0]}

# Endpoint to get the most common nationality among drivers
@app.get("/v1/bird/formula_1/most_common_nationality", summary="Get the most common nationality among drivers")
async def get_most_common_nationality():
    query = """
    SELECT nationality
    FROM drivers
    GROUP BY nationality
    ORDER BY COUNT(driverId) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"most_common_nationality": result[0]}

# Endpoint to get the total wins for drivers with 91 points
@app.get("/v1/bird/formula_1/total_wins_for_91_points", summary="Get the total wins for drivers with 91 points")
async def get_total_wins_for_91_points():
    query = """
    SELECT SUM(CASE WHEN points = 91 THEN wins ELSE 0 END)
    FROM driverStandings
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"total_wins_for_91_points": result[0]}

# Endpoint to get the race with the fastest lap time
@app.get("/v1/bird/formula_1/fastest_lap_race", summary="Get the race with the fastest lap time")
async def get_fastest_lap_race():
    query = """
    SELECT T1.name
    FROM races AS T1
    INNER JOIN results AS T2 ON T1.raceId = T2.raceId
    WHERE T2.fastestLapTime IS NOT NULL
    ORDER BY T2.fastestLapTime ASC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"fastest_lap_race": result[0]}

# Endpoint to get the most recent race location
@app.get("/v1/bird/formula_1/most_recent_race_location", summary="Get the most recent race location")
async def get_most_recent_race_location():
    query = """
    SELECT T1.location
    FROM circuits AS T1
    INNER JOIN races AS T2 ON T1.circuitId = T2.circuitId
    ORDER BY T2.date DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"most_recent_race_location": result[0]}

# Endpoint to get the fastest qualifying driver in a specific year and circuit
@app.get("/v1/bird/formula_1/fastest_qualifying_driver", summary="Get the fastest qualifying driver in a specific year and circuit")
async def get_fastest_qualifying_driver(year: int = Query(..., description="Year of the race"),
                                        circuit_name: str = Query(..., description="Name of the circuit")):
    query = f"""
    SELECT T2.forename, T2.surname
    FROM qualifying AS T1
    INNER JOIN drivers AS T2 ON T1.driverId = T2.driverId
    INNER JOIN races AS T3 ON T1.raceid = T3.raceid
    WHERE q3 IS NOT NULL AND T3.year = {year} AND T3.circuitId IN (
        SELECT circuitId FROM circuits WHERE name = '{circuit_name}'
    )
    ORDER BY CAST(SUBSTR(q3, 1, INSTR(q3, ':') - 1) AS INTEGER) * 60 +
             CAST(SUBSTR(q3, INSTR(q3, ':') + 1, INSTR(q3, '.') - INSTR(q3, ':') - 1) AS REAL) +
             CAST(SUBSTR(q3, INSTR(q3, '.') + 1) AS REAL) / 1000 ASC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"fastest_qualifying_driver": result}

# Endpoint to get the youngest driver in the standings
@app.get("/v1/bird/formula_1/youngest_driver_in_standings", summary="Get the youngest driver in the standings")
async def get_youngest_driver_in_standings():
    query = """
    SELECT T1.forename, T1.surname, T1.nationality, T3.name
    FROM drivers AS T1
    INNER JOIN driverStandings AS T2 ON T1.driverId = T2.driverId
    INNER JOIN races AS T3 ON T2.raceId = T3.raceId
    ORDER BY JULIANDAY(T1.dob) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"youngest_driver_in_standings": result}

# Endpoint to get the driver with the most finishes in a specific race
@app.get("/v1/bird/formula_1/most_finishes_in_race", summary="Get the driver with the most finishes in a specific race")
async def get_most_finishes_in_race(race_name: str = Query(..., description="Name of the race")):
    query = f"""
    SELECT COUNT(T1.driverId)
    FROM results AS T1
    INNER JOIN races AS T2 ON T1.raceId = T2.raceId
    INNER JOIN status AS T3 ON T1.statusId = T3.statusId
    WHERE T3.statusId = 3 AND T2.name = '{race_name}'
    GROUP BY T1.driverId
    ORDER BY COUNT(T1.driverId) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"most_finishes_in_race": result[0]}

# Endpoint to get the total wins for the youngest driver
@app.get("/v1/bird/formula_1/total_wins_youngest_driver", summary="Get the total wins for the youngest driver")
async def get_total_wins_youngest_driver():
    query = """
    SELECT SUM(T1.wins), T2.forename, T2.surname
    FROM driverStandings AS T1
    INNER JOIN drivers AS T2 ON T1.driverId = T2.driverId
    ORDER BY T2.dob ASC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"total_wins_youngest_driver": result}

# Endpoint to get the longest pit stop duration
@app.get("/v1/bird/formula_1/longest_pit_stop_duration", summary="Get the longest pit stop duration")
async def get_longest_pit_stop_duration():
    query = """
    SELECT duration
    FROM pitStops
    ORDER BY duration DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"longest_pit_stop_duration": result[0]}
# Endpoint to get the fastest lap time
@app.get("/v1/bird/formula_1/fastest_lap_time", summary="Get the fastest lap time")
async def get_fastest_lap_time():
    query = """
    SELECT time FROM lapTimes
    ORDER BY (CASE WHEN INSTR(time, ':') <> INSTR(SUBSTR(time, INSTR(time, ':') + 1), ':') + INSTR(time, ':')
    THEN CAST(SUBSTR(time, 1, INSTR(time, ':') - 1) AS REAL) * 3600
    ELSE 0 END) + (CAST(SUBSTR(time, INSTR(time, ':') - 2 * (INSTR(time, ':') = INSTR(SUBSTR(time, INSTR(time, ':') + 1), ':') + INSTR(time, ':')), INSTR(time, ':') - 1) AS REAL) * 60) +
    (CAST(SUBSTR(time, INSTR(time, ':') + 1, INSTR(time, '.') - INSTR(time, ':') - 1) AS REAL)) +
    (CAST(SUBSTR(time, INSTR(time, '.') + 1) AS REAL) / 1000) ASC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"fastest_lap_time": result[0]}

# Endpoint to get the longest pit stop duration for a driver
@app.get("/v1/bird/formula_1/longest_pit_stop_duration", summary="Get the longest pit stop duration for a driver")
async def get_longest_pit_stop_duration(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver")):
    query = """
    SELECT T1.duration FROM pitStops AS T1
    INNER JOIN drivers AS T2 on T1.driverId = T2.driverId
    WHERE T2.forename = ? AND T2.surname = ?
    ORDER BY T1.duration DESC
    LIMIT 1
    """
    cursor.execute(query, (forename, surname))
    result = cursor.fetchone()
    return {"longest_pit_stop_duration": result[0]}

# Endpoint to get the lap for a driver in a specific race
@app.get("/v1/bird/formula_1/driver_lap_in_race", summary="Get the lap for a driver in a specific race")
async def get_driver_lap_in_race(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver"), year: int = Query(..., description="Year of the race"), race_name: str = Query(..., description="Name of the race")):
    query = """
    SELECT T1.lap FROM pitStops AS T1
    INNER JOIN drivers AS T2 on T1.driverId = T2.driverId
    INNER JOIN races AS T3 on T1.raceId = T3.raceId
    WHERE T2.forename = ? AND T2.surname = ? AND T3.year = ? AND T3.name = ?
    """
    cursor.execute(query, (forename, surname, year, race_name))
    result = cursor.fetchone()
    return {"driver_lap_in_race": result[0]}

# Endpoint to get the pit stop durations for a specific race
@app.get("/v1/bird/formula_1/pit_stop_durations_in_race", summary="Get the pit stop durations for a specific race")
async def get_pit_stop_durations_in_race(year: int = Query(..., description="Year of the race"), race_name: str = Query(..., description="Name of the race")):
    query = """
    SELECT T1.duration FROM pitStops AS T1
    INNER JOIN races AS T2 on T1.raceId = T2.raceId
    WHERE T2.year = ? AND T2.name = ?
    """
    cursor.execute(query, (year, race_name))
    result = cursor.fetchall()
    return {"pit_stop_durations_in_race": result}

# Endpoint to get the lap times for a driver
@app.get("/v1/bird/formula_1/driver_lap_times", summary="Get the lap times for a driver")
async def get_driver_lap_times(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver")):
    query = """
    SELECT T1.time FROM lapTimes AS T1
    INNER JOIN drivers AS T2 on T1.driverId = T2.driverId
    WHERE T2.forename = ? AND T2.surname = ?
    """
    cursor.execute(query, (forename, surname))
    result = cursor.fetchall()
    return {"driver_lap_times": result}

# Endpoint to get the drivers with the fastest lap times
@app.get("/v1/bird/formula_1/fastest_lap_drivers", summary="Get the drivers with the fastest lap times")
async def get_fastest_lap_drivers():
    query = """
    WITH lap_times_in_seconds AS (
        SELECT driverId,
        (CASE WHEN SUBSTR(time, 1, INSTR(time, ':') - 1) <> '' THEN CAST(SUBSTR(time, 1, INSTR(time, ':') - 1) AS REAL) * 60 ELSE 0 END +
        CASE WHEN SUBSTR(time, INSTR(time, ':') + 1, INSTR(time, '.') - INSTR(time, ':') - 1) <> '' THEN CAST(SUBSTR(time, INSTR(time, ':') + 1, INSTR(time, '.') - INSTR(time, ':') - 1) AS REAL) ELSE 0 END +
        CASE WHEN SUBSTR(time, INSTR(time, '.') + 1) <> '' THEN CAST(SUBSTR(time, INSTR(time, '.') + 1) AS REAL) / 1000 ELSE 0 END) AS time_in_seconds
        FROM lapTimes
    )
    SELECT T2.forename, T2.surname, T1.driverId
    FROM (
        SELECT driverId, MIN(time_in_seconds) AS min_time_in_seconds
        FROM lap_times_in_seconds
        GROUP BY driverId
    ) AS T1
    INNER JOIN drivers AS T2 ON T1.driverId = T2.driverId
    ORDER BY T1.min_time_in_seconds ASC
    LIMIT 20
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"fastest_lap_drivers": result}

# Endpoint to get the best position for a driver
@app.get("/v1/bird/formula_1/best_position_for_driver", summary="Get the best position for a driver")
async def get_best_position_for_driver(forename: str = Query(..., description="Forename of the driver"), surname: str = Query(..., description="Surname of the driver")):
    query = """
    SELECT T1.position FROM lapTimes AS T1
    INNER JOIN drivers AS T2 on T1.driverId = T2.driverId
    WHERE T2.forename = ? AND T2.surname = ?
    ORDER BY T1.time ASC
    LIMIT 1
    """
    cursor.execute(query, (forename, surname))
    result = cursor.fetchone()
    return {"best_position_for_driver": result[0]}

# Endpoint to get the lap record for a specific race
@app.get("/v1/bird/formula_1/lap_record_for_race", summary="Get the lap record for a specific race")
async def get_lap_record_for_race(race_name: str = Query(..., description="Name of the race")):
    query = """
    WITH fastest_lap_times AS (
        SELECT T1.raceId, T1.fastestLapTime
        FROM results AS T1
        WHERE T1.FastestLapTime IS NOT NULL
    )
    SELECT MIN(fastest_lap_times.fastestLapTime) as lap_record
    FROM fastest_lap_times
    INNER JOIN races AS T2 on fastest_lap_times.raceId = T2.raceId
    INNER JOIN circuits AS T3 on T2.circuitId = T3.circuitId
    WHERE T2.name = ?
    """
    cursor.execute(query, (race_name,))
    result = cursor.fetchone()
    return {"lap_record_for_race": result[0]}

# Endpoint to get the lap record for a specific country
@app.get("/v1/bird/formula_1/lap_record_for_country", summary="Get the lap record for a specific country")
async def get_lap_record_for_country(country: str = Query(..., description="Country of the circuit")):
    query = """
    WITH fastest_lap_times AS (
        SELECT T1.raceId, T1.FastestLapTime,
        (CAST(SUBSTR(T1.FastestLapTime, 1, INSTR(T1.FastestLapTime, ':') - 1) AS REAL) * 60) +
        (CAST(SUBSTR(T1.FastestLapTime, INSTR(T1.FastestLapTime, ':') + 1, INSTR(T1.FastestLapTime, '.') - INSTR(T1.FastestLapTime, ':') - 1) AS REAL)) +
        (CAST(SUBSTR(T1.FastestLapTime, INSTR(T1.FastestLapTime, '.') + 1) AS REAL) / 1000) as time_in_seconds
        FROM results AS T1
        WHERE T1.FastestLapTime IS NOT NULL
    )
    SELECT T1.FastestLapTime as lap_record
    FROM results AS T1
    INNER JOIN races AS T2 on T1.raceId = T2.raceId
    INNER JOIN circuits AS T3 on T2.circuitId = T3.circuitId
    INNER JOIN (
        SELECT MIN(fastest_lap_times.time_in_seconds) as min_time_in_seconds
        FROM fastest_lap_times
        INNER JOIN races AS T2 on fastest_lap_times.raceId = T2.raceId
        INNER JOIN circuits AS T3 on T2.circuitId = T3.circuitId
        WHERE T3.country = ?
    ) AS T4
    ON (CAST(SUBSTR(T1.FastestLapTime, 1, INSTR(T1.FastestLapTime, ':') - 1) AS REAL) * 60) +
    (CAST(SUBSTR(T1.FastestLapTime, INSTR(T1.FastestLapTime, ':') + 1, INSTR(T1.FastestLapTime, '.') - INSTR(T1.FastestLapTime, ':') - 1) AS REAL)) +
    (CAST(SUBSTR(T1.FastestLapTime, INSTR(T1.FastestLapTime, '.') + 1) AS REAL) / 1000) = T4.min_time_in_seconds
    LIMIT 1
    """
    cursor.execute(query, (country,))
    result = cursor.fetchone()
    return {"lap_record_for_country": result[0]}

# Endpoint to get the race with the fastest lap time for a specific race
@app.get("/v1/bird/formula_1/race_with_fastest_lap_time", summary="Get the race with the fastest lap time for a specific race")
async def get_race_with_fastest_lap_time(race_name: str = Query(..., description="Name of the race")):
    query = """
    WITH fastest_lap_times AS (
        SELECT T1.raceId, T1.FastestLapTime,
        (CAST(SUBSTR(T1.FastestLapTime, 1, INSTR(T1.FastestLapTime, ':') - 1) AS REAL) * 60) +
        (CAST(SUBSTR(T1.FastestLapTime, INSTR(T1.FastestLapTime, ':') + 1, INSTR(T1.FastestLapTime, '.') - INSTR(T1.FastestLapTime, ':') - 1) AS REAL)) +
        (CAST(SUBSTR(T1.FastestLapTime, INSTR(T1.FastestLapTime, '.') + 1) AS REAL) / 1000) as time_in_seconds
        FROM results AS T1
        WHERE T1.FastestLapTime IS NOT NULL
    )
    SELECT T2.name
    FROM races AS T2
    INNER JOIN circuits AS T3 on T2.circuitId = T3.circuitId
    INNER JOIN results AS T1 on T2.raceId = T1.raceId
    INNER JOIN (
        SELECT MIN(fastest_lap_times.time_in_seconds) as min_time_in_seconds
        FROM fastest_lap_times
        INNER JOIN races AS T2 on fastest_lap_times.raceId = T2.raceId
        INNER JOIN circuits AS T3 on T2.circuitId = T3.circuitId
        WHERE T2.name = ?
    ) AS T4
    ON (CAST(SUBSTR(T1.FastestLapTime, 1, INSTR(T1.FastestLapTime, ':') - 1) AS REAL) * 60) +
    (CAST(SUBSTR(T1.FastestLapTime, INSTR(T1.FastestLapTime, ':') + 1, INSTR(T1.FastestLapTime, '.') - INSTR(T1.FastestLapTime, ':') - 1) AS REAL)) +
    (CAST(SUBSTR(T1.FastestLapTime, INSTR(T1.FastestLapTime, '.') + 1) AS REAL) / 1000) = T4.min_time_in_seconds
    WHERE T2.name = ?
    """
    cursor.execute(query, (race_name, race_name))
    result = cursor.fetchone()
    return {"race_with_fastest_lap_time": result[0]}
# Endpoint to get lap record race duration
@app.get("/v1/bird/formula_1/lap_record_race_duration", summary="Get lap record race duration for a given race name")
async def get_lap_record_race_duration(race_name: str = Query(..., description="Name of the race")):
    query = f"""
    WITH fastest_lap_times AS (
        SELECT T1.raceId, T1.driverId, T1.FastestLapTime,
        (CAST(SUBSTR(T1.FastestLapTime, 1, INSTR(T1.FastestLapTime, ':') - 1) AS REAL) * 60) +
        (CAST(SUBSTR(T1.FastestLapTime, INSTR(T1.FastestLapTime, ':') + 1, INSTR(T1.FastestLapTime, '.') - INSTR(T1.FastestLapTime, ':') - 1) AS REAL)) +
        (CAST(SUBSTR(T1.FastestLapTime, INSTR(T1.FastestLapTime, '.') + 1) AS REAL) / 1000) as time_in_seconds
        FROM results AS T1
        WHERE T1.FastestLapTime IS NOT NULL
    ),
    lap_record_race AS (
        SELECT T1.raceId, T1.driverId
        FROM results AS T1
        INNER JOIN races AS T2 on T1.raceId = T2.raceId
        INNER JOIN circuits AS T3 on T2.circuitId = T3.circuitId
        INNER JOIN (
            SELECT MIN(fastest_lap_times.time_in_seconds) as min_time_in_seconds
            FROM fastest_lap_times
            INNER JOIN races AS T2 on fastest_lap_times.raceId = T2.raceId
            INNER JOIN circuits AS T3 on T2.circuitId = T3.circuitId
            WHERE T2.name = '{race_name}'
        ) AS T4
        ON (CAST(SUBSTR(T1.FastestLapTime, 1, INSTR(T1.FastestLapTime, ':') - 1) AS REAL) * 60) +
        (CAST(SUBSTR(T1.FastestLapTime, INSTR(T1.FastestLapTime, ':') + 1, INSTR(T1.FastestLapTime, '.') - INSTR(T1.FastestLapTime, ':') - 1) AS REAL)) +
        (CAST(SUBSTR(T1.FastestLapTime, INSTR(T1.FastestLapTime, '.') + 1) AS REAL) / 1000) = T4.min_time_in_seconds
        WHERE T2.name = '{race_name}'
    )
    SELECT T4.duration
    FROM lap_record_race
    INNER JOIN pitStops AS T4 on lap_record_race.raceId = T4.raceId AND lap_record_race.driverId = T4.driverId
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"duration": result}

# Endpoint to get latitude and longitude for a given lap time
@app.get("/v1/bird/formula_1/lap_time_location", summary="Get latitude and longitude for a given lap time")
async def get_lap_time_location(lap_time: str = Query(..., description="Lap time in the format 'MM:SS.sss'")):
    query = f"""
    SELECT T3.lat, T3.lng
    FROM lapTimes AS T1
    INNER JOIN races AS T2 on T1.raceId = T2.raceId
    INNER JOIN circuits AS T3 on T2.circuitId = T3.circuitId
    WHERE T1.time = '{lap_time}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"location": result}

# Endpoint to get average pit stop time for a given driver
@app.get("/v1/bird/formula_1/average_pit_stop_time", summary="Get average pit stop time for a given driver")
async def get_average_pit_stop_time(forename: str = Query(..., description="Driver's forename"), surname: str = Query(..., description="Driver's surname")):
    query = f"""
    SELECT AVG(milliseconds)
    FROM pitStops AS T1
    INNER JOIN drivers AS T2 on T1.driverId = T2.driverId
    WHERE T2.forename = '{forename}' AND T2.surname = '{surname}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"average_pit_stop_time": result}

# Endpoint to get average lap time for a given country
@app.get("/v1/bird/formula_1/average_lap_time", summary="Get average lap time for a given country")
async def get_average_lap_time(country: str = Query(..., description="Country name")):
    query = f"""
    SELECT CAST(SUM(T1.milliseconds) AS REAL) / COUNT(T1.lap)
    FROM lapTimes AS T1
    INNER JOIN races AS T2 on T1.raceId = T2.raceId
    INNER JOIN circuits AS T3 on T2.circuitId = T3.circuitId
    WHERE T3.country = '{country}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"average_lap_time": result}
