
from fastapi import APIRouter, Query
import sqlite3

app = APIRouter()

# Database connection
conn = sqlite3.connect('invocable_api_hub/db/bird/test/superhero.sqlite', check_same_thread=False)
cursor = conn.cursor()

# Endpoint to get power name for a given superhero
@app.get("/v1/bird/superhero/power_name", summary="Get power name for a given superhero")
async def get_power_name(superhero_name: str = Query(..., description="Name of the superhero")):
    query = """
    SELECT T3.power_name
    FROM superhero AS T1
    INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id
    INNER JOIN superpower AS T3 ON T2.power_id = T3.id
    WHERE T1.superhero_name = ?
    """
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchall()
    return {"power_name": result}

# Endpoint to get count of heroes with a specific power
@app.get("/v1/bird/superhero/hero_power_count", summary="Get count of heroes with a specific power")
async def get_hero_power_count(power_name: str = Query(..., description="Name of the power")):
    query = """
    SELECT COUNT(T1.hero_id)
    FROM hero_power AS T1
    INNER JOIN superpower AS T2 ON T1.power_id = T2.id
    WHERE T2.power_name = ?
    """
    cursor.execute(query, (power_name,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of superheroes with a specific power and height
@app.get("/v1/bird/superhero/superhero_power_height_count", summary="Get count of superheroes with a specific power and height")
async def get_superhero_power_height_count(power_name: str = Query(..., description="Name of the power"), height_cm: int = Query(..., description="Height in cm")):
    query = """
    SELECT COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id
    INNER JOIN superpower AS T3 ON T2.power_id = T3.id
    WHERE T3.power_name = ? AND T1.height_cm > ?
    """
    cursor.execute(query, (power_name, height_cm))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get superheroes with more than a certain number of powers
@app.get("/v1/bird/superhero/superhero_power_count", summary="Get superheroes with more than a certain number of powers")
async def get_superhero_power_count(power_count: int = Query(..., description="Number of powers")):
    query = """
    SELECT DISTINCT T1.full_name
    FROM superhero AS T1
    INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id
    GROUP BY T1.full_name
    HAVING COUNT(T2.power_id) > ?
    """
    cursor.execute(query, (power_count,))
    result = cursor.fetchall()
    return {"superheroes": result}

# Endpoint to get count of superheroes with a specific eye color
@app.get("/v1/bird/superhero/superhero_eye_color_count", summary="Get count of superheroes with a specific eye color")
async def get_superhero_eye_color_count(eye_color: str = Query(..., description="Eye color")):
    query = """
    SELECT COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id
    WHERE T2.colour = ?
    """
    cursor.execute(query, (eye_color,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get skin color of a specific superhero
@app.get("/v1/bird/superhero/superhero_skin_color", summary="Get skin color of a specific superhero")
async def get_superhero_skin_color(superhero_name: str = Query(..., description="Name of the superhero")):
    query = """
    SELECT T2.colour
    FROM superhero AS T1
    INNER JOIN colour AS T2 ON T1.skin_colour_id = T2.id
    WHERE T1.superhero_name = ?
    """
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchone()
    return {"skin_color": result[0]}

# Endpoint to get count of superheroes with a specific power and eye color
@app.get("/v1/bird/superhero/superhero_power_eye_color_count", summary="Get count of superheroes with a specific power and eye color")
async def get_superhero_power_eye_color_count(power_name: str = Query(..., description="Name of the power"), eye_color: str = Query(..., description="Eye color")):
    query = """
    SELECT COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id
    INNER JOIN superpower AS T3 ON T2.power_id = T3.id
    INNER JOIN colour AS T4 ON T1.eye_colour_id = T4.id
    WHERE T3.power_name = ? AND T4.colour = ?
    """
    cursor.execute(query, (power_name, eye_color))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get superheroes with specific eye and hair colors
@app.get("/v1/bird/superhero/superhero_eye_hair_color", summary="Get superheroes with specific eye and hair colors")
async def get_superhero_eye_hair_color(eye_color: str = Query(..., description="Eye color"), hair_color: str = Query(..., description="Hair color")):
    query = """
    SELECT T1.superhero_name
    FROM superhero AS T1
    INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id
    INNER JOIN colour AS T3 ON T1.hair_colour_id = T3.id
    WHERE T2.colour = ? AND T3.colour = ?
    """
    cursor.execute(query, (eye_color, hair_color))
    result = cursor.fetchall()
    return {"superheroes": result}

# Endpoint to get count of superheroes from a specific publisher
@app.get("/v1/bird/superhero/superhero_publisher_count", summary="Get count of superheroes from a specific publisher")
async def get_superhero_publisher_count(publisher_name: str = Query(..., description="Name of the publisher")):
    query = """
    SELECT COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    WHERE T2.publisher_name = ?
    """
    cursor.execute(query, (publisher_name,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get superheroes ranked by height from a specific publisher
@app.get("/v1/bird/superhero/superhero_height_rank", summary="Get superheroes ranked by height from a specific publisher")
async def get_superhero_height_rank(publisher_name: str = Query(..., description="Name of the publisher")):
    query = """
    SELECT superhero_name, height_cm, RANK() OVER (ORDER BY height_cm DESC) AS HeightRank
    FROM superhero
    INNER JOIN publisher ON superhero.publisher_id = publisher.id
    WHERE publisher.publisher_name = ?
    """
    cursor.execute(query, (publisher_name,))
    result = cursor.fetchall()
    return {"superheroes": result}

# Endpoint to get publisher of a specific superhero
@app.get("/v1/bird/superhero/superhero_publisher", summary="Get publisher of a specific superhero")
async def get_superhero_publisher(superhero_name: str = Query(..., description="Name of the superhero")):
    query = """
    SELECT T2.publisher_name
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    WHERE T1.superhero_name = ?
    """
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchone()
    return {"publisher": result[0]}

# Endpoint to get eye color popularity rank from a specific publisher
@app.get("/v1/bird/superhero/eye_color_popularity", summary="Get eye color popularity rank from a specific publisher")
async def get_eye_color_popularity(publisher_name: str = Query(..., description="Name of the publisher")):
    query = """
    SELECT colour.colour AS EyeColor, COUNT(superhero.id) AS Count, RANK() OVER (ORDER BY COUNT(superhero.id) DESC) AS PopularityRank
    FROM superhero
    INNER JOIN colour ON superhero.eye_colour_id = colour.id
    INNER JOIN publisher ON superhero.publisher_id = publisher.id
    WHERE publisher.publisher_name = ?
    GROUP BY colour.colour
    """
    cursor.execute(query, (publisher_name,))
    result = cursor.fetchall()
    return {"eye_color_popularity": result}

# Endpoint to get average height of superheroes from a specific publisher
@app.get("/v1/bird/superhero/average_height", summary="Get average height of superheroes from a specific publisher")
async def get_average_height(publisher_name: str = Query(..., description="Name of the publisher")):
    query = """
    SELECT AVG(T1.height_cm)
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    WHERE T2.publisher_name = ?
    """
    cursor.execute(query, (publisher_name,))
    result = cursor.fetchone()
    return {"average_height": result[0]}

# Endpoint to get superheroes with a specific power and publisher
@app.get("/v1/bird/superhero/superhero_power_publisher", summary="Get superheroes with a specific power and publisher")
async def get_superhero_power_publisher(power_name: str = Query(..., description="Name of the power"), publisher_name: str = Query(..., description="Name of the publisher")):
    query = """
    SELECT superhero_name
    FROM superhero AS T1
    WHERE EXISTS (
        SELECT 1
        FROM hero_power AS T2
        INNER JOIN superpower AS T3 ON T2.power_id = T3.id
        WHERE T3.power_name = ? AND T1.id = T2.hero_id
    ) AND EXISTS (
        SELECT 1
        FROM publisher AS T4
        WHERE T4.publisher_name = ? AND T1.publisher_id = T4.id
    )
    """
    cursor.execute(query, (power_name, publisher_name))
    result = cursor.fetchall()
    return {"superheroes": result}

# Endpoint to get count of superheroes from a specific publisher
@app.get("/v1/bird/superhero/superhero_publisher_count_dc", summary="Get count of superheroes from a specific publisher")
async def get_superhero_publisher_count_dc(publisher_name: str = Query(..., description="Name of the publisher")):
    query = """
    SELECT COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    WHERE T2.publisher_name = ?
    """
    cursor.execute(query, (publisher_name,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get publisher of the fastest superhero
@app.get("/v1/bird/superhero/fastest_superhero_publisher", summary="Get publisher of the fastest superhero")
async def get_fastest_superhero_publisher():
    query = """
    SELECT T2.publisher_name
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    INNER JOIN hero_attribute AS T3 ON T1.id = T3.hero_id
    INNER JOIN attribute AS T4 ON T3.attribute_id = T4.id
    WHERE T4.attribute_name = 'Speed'
    ORDER BY T3.attribute_value
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"publisher": result[0]}

# Endpoint to get count of superheroes with a specific eye color and publisher
@app.get("/v1/bird/superhero/superhero_eye_color_publisher_count", summary="Get count of superheroes with a specific eye color and publisher")
async def get_superhero_eye_color_publisher_count(publisher_name: str = Query(..., description="Name of the publisher"), eye_color: str = Query(..., description="Eye color")):
    query = """
    SELECT COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    INNER JOIN colour AS T3 ON T1.eye_colour_id = T3.id
    WHERE T2.publisher_name = ? AND T3.colour = ?
    """
    cursor.execute(query, (publisher_name, eye_color))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get publisher of a specific superhero
@app.get("/v1/bird/superhero/superhero_publisher_name", summary="Get publisher of a specific superhero")
async def get_superhero_publisher_name(superhero_name: str = Query(..., description="Name of the superhero")):
    query = """
    SELECT T2.publisher_name
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    WHERE T1.superhero_name = ?
    """
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchone()
    return {"publisher": result[0]}

# Endpoint to get count of superheroes with a specific hair color
@app.get("/v1/bird/superhero/superhero_hair_color_count", summary="Get count of superheroes with a specific hair color")
async def get_superhero_hair_color_count(hair_color: str = Query(..., description="Hair color")):
    query = """
    SELECT COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN colour AS T2 ON T1.hair_colour_id = T2.id
    WHERE T2.colour = ?
    """
    cursor.execute(query, (hair_color,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get the most intelligent superhero
@app.get("/v1/bird/superhero/most_intelligent_superhero", summary="Get the most intelligent superhero")
async def get_most_intelligent_superhero():
    query = """
    SELECT T1.superhero_name
    FROM superhero AS T1
    INNER JOIN hero_attribute AS T2 ON T1.id = T2.hero_id
    INNER JOIN attribute AS T3 ON T2.attribute_id = T3.id
    WHERE T3.attribute_name = 'Intelligence'
    ORDER BY T2.attribute_value
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"superhero": result[0]}

# Endpoint to get race of a superhero
@app.get("/v1/bird/superhero/superhero/race", summary="Get race of a superhero")
async def get_superhero_race(superhero_name: str = Query(..., description="Name of the superhero")):
    query = """
    SELECT T2.race
    FROM superhero AS T1
    INNER JOIN race AS T2 ON T1.race_id = T2.id
    WHERE T1.superhero_name = ?
    """
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get superheroes with a specific attribute
@app.get("/v1/bird/superhero/superhero/attribute", summary="Get superheroes with a specific attribute")
async def get_superheroes_with_attribute(attribute_name: str = Query(..., description="Name of the attribute"), attribute_value: int = Query(..., description="Value of the attribute")):
    query = """
    SELECT superhero_name
    FROM superhero AS T1
    WHERE EXISTS (
        SELECT 1
        FROM hero_attribute AS T2
        INNER JOIN attribute AS T3 ON T2.attribute_id = T3.id
        WHERE T3.attribute_name = ? AND T2.attribute_value < ? AND T1.id = T2.hero_id
    )
    """
    cursor.execute(query, (attribute_name, attribute_value))
    result = cursor.fetchall()
    return result

# Endpoint to get superheroes with a specific power
@app.get("/v1/bird/superhero/superhero/power", summary="Get superheroes with a specific power")
async def get_superheroes_with_power(power_name: str = Query(..., description="Name of the power")):
    query = """
    SELECT T1.superhero_name
    FROM superhero AS T1
    INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id
    INNER JOIN superpower AS T3 ON T2.power_id = T3.id
    WHERE T3.power_name = ?
    """
    cursor.execute(query, (power_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of superheroes with specific attributes and gender
@app.get("/v1/bird/superhero/superhero/count", summary="Get count of superheroes with specific attributes and gender")
async def get_superhero_count(attribute_name: str = Query(..., description="Name of the attribute"), attribute_value: int = Query(..., description="Value of the attribute"), gender: str = Query(..., description="Gender of the superhero")):
    query = """
    SELECT COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN hero_attribute AS T2 ON T1.id = T2.hero_id
    INNER JOIN attribute AS T3 ON T2.attribute_id = T3.id
    INNER JOIN gender AS T4 ON T1.gender_id = T4.id
    WHERE T3.attribute_name = ? AND T2.attribute_value = ? AND T4.gender = ?
    """
    cursor.execute(query, (attribute_name, attribute_value, gender))
    result = cursor.fetchall()
    return result

# Endpoint to get the superhero with the most powers
@app.get("/v1/bird/superhero/superhero/most_powers", summary="Get the superhero with the most powers")
async def get_superhero_with_most_powers():
    query = """
    SELECT T1.superhero_name
    FROM superhero AS T1
    INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id
    GROUP BY T1.superhero_name
    ORDER BY COUNT(T2.hero_id) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get count of superheroes of a specific race
@app.get("/v1/bird/superhero/superhero/race_count", summary="Get count of superheroes of a specific race")
async def get_superhero_race_count(race: str = Query(..., description="Race of the superhero")):
    query = """
    SELECT COUNT(T1.superhero_name)
    FROM superhero AS T1
    INNER JOIN race AS T2 ON T1.race_id = T2.id
    WHERE T2.race = ?
    """
    cursor.execute(query, (race,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of bad superheroes and count of Marvel Comics superheroes
@app.get("/v1/bird/superhero/superhero/bad_percentage", summary="Get percentage of bad superheroes and count of Marvel Comics superheroes")
async def get_bad_superhero_percentage():
    query = """
    SELECT (CAST(COUNT(*) AS REAL) * 100 / (SELECT COUNT(*) FROM superhero)),
           CAST(SUM(CASE WHEN T2.publisher_name = 'Marvel Comics' THEN 1 ELSE 0 END) AS REAL)
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    INNER JOIN alignment AS T3 ON T3.id = T1.alignment_id
    WHERE T3.alignment = 'Bad'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get the difference between Marvel and DC Comics superheroes
@app.get("/v1/bird/superhero/superhero/publisher_difference", summary="Get the difference between Marvel and DC Comics superheroes")
async def get_publisher_difference():
    query = """
    SELECT SUM(CASE WHEN T2.publisher_name = 'Marvel Comics' THEN 1 ELSE 0 END) -
           SUM(CASE WHEN T2.publisher_name = 'DC Comics' THEN 1 ELSE 0 END)
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get publisher ID by name
@app.get("/v1/bird/superhero/publisher/id", summary="Get publisher ID by name")
async def get_publisher_id(publisher_name: str = Query(..., description="Name of the publisher")):
    query = """
    SELECT id
    FROM publisher
    WHERE publisher_name = ?
    """
    cursor.execute(query, (publisher_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get average attribute value
@app.get("/v1/bird/superhero/hero_attribute/average", summary="Get average attribute value")
async def get_average_attribute_value():
    query = """
    SELECT AVG(attribute_value)
    FROM hero_attribute
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get count of superheroes with null full name
@app.get("/v1/bird/superhero/superhero/null_full_name_count", summary="Get count of superheroes with null full name")
async def get_null_full_name_count():
    query = """
    SELECT COUNT(id)
    FROM superhero
    WHERE full_name IS NULL
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get eye colour of a superhero by ID
@app.get("/v1/bird/superhero/superhero/eye_colour", summary="Get eye colour of a superhero by ID")
async def get_superhero_eye_colour(superhero_id: int = Query(..., description="ID of the superhero")):
    query = """
    SELECT T2.colour
    FROM superhero AS T1
    INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id
    WHERE T1.id = ?
    """
    cursor.execute(query, (superhero_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get powers of a superhero by name
@app.get("/v1/bird/superhero/superhero/powers", summary="Get powers of a superhero by name")
async def get_superhero_powers(superhero_name: str = Query(..., description="Name of the superhero")):
    query = """
    SELECT T3.power_name
    FROM superhero AS T1
    INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id
    INNER JOIN superpower AS T3 ON T2.power_id = T3.id
    WHERE T1.superhero_name = ?
    """
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get average weight of female superheroes
@app.get("/v1/bird/superhero/superhero/average_weight", summary="Get average weight of female superheroes")
async def get_average_weight_female():
    query = """
    SELECT AVG(T1.weight_kg)
    FROM superhero AS T1
    INNER JOIN gender AS T2 ON T1.gender_id = T2.id
    WHERE T2.gender = 'Female'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get powers of male superheroes
@app.get("/v1/bird/superhero/superhero/male_powers", summary="Get powers of male superheroes")
async def get_male_superhero_powers(limit: int = Query(..., description="Limit the number of results")):
    query = """
    SELECT T3.power_name
    FROM superhero AS T1
    INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id
    INNER JOIN superpower AS T3 ON T3.id = T2.power_id
    INNER JOIN gender AS T4 ON T4.id = T1.gender_id
    WHERE T4.gender = 'Male'
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    result = cursor.fetchall()
    return result

# Endpoint to get superheroes of a specific race
@app.get("/v1/bird/superhero/superhero/race_superheroes", summary="Get superheroes of a specific race")
async def get_superheroes_by_race(race: str = Query(..., description="Race of the superhero")):
    query = """
    SELECT T1.superhero_name
    FROM superhero AS T1
    INNER JOIN race AS T2 ON T1.race_id = T2.id
    WHERE T2.race = ?
    """
    cursor.execute(query, (race,))
    result = cursor.fetchall()
    return result

# Endpoint to get superheroes with specific height and eye colour
@app.get("/v1/bird/superhero/superhero/height_eye_colour", summary="Get superheroes with specific height and eye colour")
async def get_superheroes_by_height_eye_colour(min_height: int = Query(..., description="Minimum height in cm"), max_height: int = Query(..., description="Maximum height in cm"), eye_colour: str = Query(..., description="Eye colour of the superhero")):
    query = """
    SELECT DISTINCT T1.superhero_name
    FROM superhero AS T1
    INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id
    WHERE T1.height_cm BETWEEN ? AND ? AND T2.colour = ?
    """
    cursor.execute(query, (min_height, max_height, eye_colour))
    result = cursor.fetchall()
    return result

# Endpoint to get powers of a superhero by ID
@app.get("/v1/bird/superhero/hero_power/powers", summary="Get powers of a superhero by ID")
async def get_powers_by_hero_id(hero_id: int = Query(..., description="ID of the superhero")):
    query = """
    SELECT T2.power_name
    FROM hero_power AS T1
    INNER JOIN superpower AS T2 ON T1.power_id = T2.id
    WHERE T1.hero_id = ?
    """
    cursor.execute(query, (hero_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get full name of superheroes of a specific race
@app.get("/v1/bird/superhero/superhero/race_full_name", summary="Get full name of superheroes of a specific race")
async def get_full_name_by_race(race: str = Query(..., description="Race of the superhero")):
    query = """
    SELECT T1.full_name
    FROM superhero AS T1
    INNER JOIN race AS T2 ON T1.race_id = T2.id
    WHERE T2.race = ?
    """
    cursor.execute(query, (race,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of superheroes with a specific alignment
@app.get("/v1/bird/superhero/superhero/alignment_count", summary="Get count of superheroes with a specific alignment")
async def get_superhero_alignment_count(alignment: str = Query(..., description="Alignment of the superhero")):
    query = """
    SELECT COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN alignment AS T2 ON T1.alignment_id = T2.id
    WHERE T2.alignment = ?
    """
    cursor.execute(query, (alignment,))
    result = cursor.fetchall()
    return result

# Endpoint to get race for a given weight
@app.get("/v1/bird/superhero/race_by_weight", summary="Get race for a given weight")
async def get_race_by_weight(weight_kg: int = Query(..., description="Weight in kg")):
    query = "SELECT T2.race FROM superhero AS T1 INNER JOIN race AS T2 ON T1.race_id = T2.id WHERE T1.weight_kg = ?"
    cursor.execute(query, (weight_kg,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct colour for a given height and race
@app.get("/v1/bird/superhero/colour_by_height_and_race", summary="Get distinct colour for a given height and race")
async def get_colour_by_height_and_race(height_cm: int = Query(..., description="Height in cm"), race: str = Query(..., description="Race")):
    query = "SELECT DISTINCT T3.colour FROM superhero AS T1 INNER JOIN race AS T2 ON T1.race_id = T2.id INNER JOIN colour AS T3 ON T1.hair_colour_id = T3.id WHERE T1.height_cm = ? AND T2.race = ?"
    cursor.execute(query, (height_cm, race))
    result = cursor.fetchall()
    return result

# Endpoint to get colour of the heaviest superhero
@app.get("/v1/bird/superhero/heaviest_superhero_colour", summary="Get colour of the heaviest superhero")
async def get_heaviest_superhero_colour():
    query = "SELECT T2.colour FROM superhero AS T1 INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id ORDER BY T1.weight_kg DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of superheroes from a specific publisher within a height range
@app.get("/v1/bird/superhero/publisher_percentage_by_height", summary="Get percentage of superheroes from a specific publisher within a height range")
async def get_publisher_percentage_by_height(min_height_cm: int = Query(..., description="Minimum height in cm"), max_height_cm: int = Query(..., description="Maximum height in cm")):
    query = "SELECT CAST(COUNT(CASE WHEN T2.publisher_name = 'Marvel Comics' THEN 1 ELSE NULL END) AS REAL) * 100 / COUNT(T1.id) FROM superhero AS T1 INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id WHERE T1.height_cm BETWEEN ? AND ?"
    cursor.execute(query, (min_height_cm, max_height_cm))
    result = cursor.fetchall()
    return result

# Endpoint to get male superheroes heavier than a certain weight
@app.get("/v1/bird/superhero/heavy_male_superheroes", summary="Get male superheroes heavier than a certain weight")
async def get_heavy_male_superheroes(weight_kg: int = Query(..., description="Weight in kg")):
    query = "SELECT T1.superhero_name FROM superhero AS T1 INNER JOIN gender AS T2 ON T1.gender_id = T2.id WHERE T2.gender = 'Male' AND T1.weight_kg * 100 > ( SELECT AVG(weight_kg) FROM superhero ) * ?"
    cursor.execute(query, (weight_kg,))
    result = cursor.fetchall()
    return result

# Endpoint to get the most common superpower
@app.get("/v1/bird/superhero/most_common_superpower", summary="Get the most common superpower")
async def get_most_common_superpower():
    query = "SELECT T2.power_name FROM hero_power AS T1 INNER JOIN superpower AS T2 ON T1.power_id = T2.id GROUP BY T2.power_name ORDER BY COUNT(T1.hero_id) DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get attributes of a specific superhero
@app.get("/v1/bird/superhero/superhero_attributes", summary="Get attributes of a specific superhero")
async def get_superhero_attributes(superhero_name: str = Query(..., description="Name of the superhero")):
    query = "SELECT T2.attribute_value FROM superhero AS T1 INNER JOIN hero_attribute AS T2 ON T1.id = T2.hero_id WHERE T1.superhero_name = ?"
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct powers of a specific superhero
@app.get("/v1/bird/superhero/superhero_powers", summary="Get distinct powers of a specific superhero")
async def get_superhero_powers(hero_id: int = Query(..., description="ID of the superhero")):
    query = "SELECT DISTINCT T2.power_name FROM hero_power AS T1 INNER JOIN superpower AS T2 ON T1.power_id = T2.id WHERE T1.hero_id = ?"
    cursor.execute(query, (hero_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of superheroes with a specific power
@app.get("/v1/bird/superhero/superheroes_with_power", summary="Get count of superheroes with a specific power")
async def get_superheroes_with_power(power_name: str = Query(..., description="Name of the power")):
    query = "SELECT COUNT(T1.hero_id) FROM hero_power AS T1 INNER JOIN superpower AS T2 ON T1.power_id = T2.id WHERE T2.power_name = ?"
    cursor.execute(query, (power_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get the strongest superhero
@app.get("/v1/bird/superhero/strongest_superhero", summary="Get the strongest superhero")
async def get_strongest_superhero():
    query = "SELECT T1.full_name FROM superhero AS T1 INNER JOIN hero_attribute AS T2 ON T1.id = T2.hero_id INNER JOIN attribute AS T3 ON T2.attribute_id = T3.id WHERE T3.attribute_name = 'Strength' ORDER BY T2.attribute_value DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of superheroes with a specific skin colour
@app.get("/v1/bird/superhero/skin_colour_percentage", summary="Get percentage of superheroes with a specific skin colour")
async def get_skin_colour_percentage(skin_colour: str = Query(..., description="Skin colour")):
    query = "SELECT CAST(COUNT(*) AS REAL) / SUM(CASE WHEN T2.id = 1 THEN 1 ELSE 0 END) FROM superhero AS T1 INNER JOIN colour AS T2 ON T1.skin_colour_id = T2.id WHERE T2.colour = ?"
    cursor.execute(query, (skin_colour,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of superheroes from a specific publisher
@app.get("/v1/bird/superhero/superheroes_by_publisher", summary="Get count of superheroes from a specific publisher")
async def get_superheroes_by_publisher(publisher_name: str = Query(..., description="Name of the publisher")):
    query = "SELECT COUNT(T1.id) FROM superhero AS T1 INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id WHERE T2.publisher_name = ?"
    cursor.execute(query, (publisher_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get the most durable superhero from a specific publisher
@app.get("/v1/bird/superhero/most_durable_superhero", summary="Get the most durable superhero from a specific publisher")
async def get_most_durable_superhero(publisher_name: str = Query(..., description="Name of the publisher")):
    query = "SELECT T1.superhero_name FROM superhero AS T1 INNER JOIN hero_attribute AS T2 ON T1.id = T2.hero_id INNER JOIN attribute AS T3 ON T3.id = T2.attribute_id INNER JOIN publisher AS T4 ON T4.id = T1.publisher_id WHERE T4.publisher_name = ? AND T3.attribute_name = 'Durability' ORDER BY T2.attribute_value DESC LIMIT 1"
    cursor.execute(query, (publisher_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get eye colour of a specific superhero
@app.get("/v1/bird/superhero/superhero_eye_colour", summary="Get eye colour of a specific superhero")
async def get_superhero_eye_colour(full_name: str = Query(..., description="Full name of the superhero")):
    query = "SELECT T2.colour FROM superhero AS T1 INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id WHERE T1.full_name = ?"
    cursor.execute(query, (full_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get superheroes with a specific power
@app.get("/v1/bird/superhero/superheroes_with_specific_power", summary="Get superheroes with a specific power")
async def get_superheroes_with_specific_power(power_name: str = Query(..., description="Name of the power")):
    query = "SELECT T1.superhero_name FROM superhero AS T1 INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id INNER JOIN superpower AS T3 ON T2.power_id = T3.id WHERE T3.power_name = ?"
    cursor.execute(query, (power_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get colour IDs of superheroes from a specific publisher and gender
@app.get("/v1/bird/superhero/superhero_colour_ids", summary="Get colour IDs of superheroes from a specific publisher and gender")
async def get_superhero_colour_ids(publisher_name: str = Query(..., description="Name of the publisher"), gender: str = Query(..., description="Gender")):
    query = "SELECT T1.eye_colour_id, T1.hair_colour_id, T1.skin_colour_id FROM superhero AS T1 INNER JOIN publisher AS T2 ON T2.id = T1.publisher_id INNER JOIN gender AS T3 ON T3.id = T1.gender_id WHERE T2.publisher_name = ? AND T3.gender = ?"
    cursor.execute(query, (publisher_name, gender))
    result = cursor.fetchall()
    return result

# Endpoint to get superheroes with matching colour IDs
@app.get("/v1/bird/superhero/superheroes_with_matching_colour_ids", summary="Get superheroes with matching colour IDs")
async def get_superheroes_with_matching_colour_ids():
    query = "SELECT T1.superhero_name, T2.publisher_name FROM superhero AS T1 INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id WHERE T1.eye_colour_id = T1.hair_colour_id AND T1.eye_colour_id = T1.skin_colour_id"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get race of a specific superhero
@app.get("/v1/bird/superhero/superhero_race", summary="Get race of a specific superhero")
async def get_superhero_race(superhero_name: str = Query(..., description="Name of the superhero")):
    query = "SELECT T2.race FROM superhero AS T1 INNER JOIN race AS T2 ON T1.race_id = T2.id WHERE T1.superhero_name = ?"
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of superheroes with a specific skin colour and gender
@app.get("/v1/bird/superhero/skin_colour_gender_percentage", summary="Get percentage of superheroes with a specific skin colour and gender")
async def get_skin_colour_gender_percentage(skin_colour: str = Query(..., description="Skin colour"), gender: str = Query(..., description="Gender")):
    query = "SELECT CAST(COUNT(CASE WHEN T3.colour = ? THEN T1.id ELSE NULL END) AS REAL) * 100 / COUNT(T1.id) FROM superhero AS T1 INNER JOIN gender AS T2 ON T1.gender_id = T2.id INNER JOIN colour AS T3 ON T1.skin_colour_id = T3.id WHERE T2.gender = ?"
    cursor.execute(query, (skin_colour, gender))
    result = cursor.fetchall()
    return result

# Endpoint to get race and full name of a specific superhero
@app.get("/v1/bird/superhero/superhero_race_and_full_name", summary="Get race and full name of a specific superhero")
async def get_superhero_race_and_full_name(full_name: str = Query(..., description="Full name of the superhero")):
    query = "SELECT T1.superhero_name, T2.race FROM superhero AS T1 INNER JOIN race AS T2 ON T1.race_id = T2.id WHERE T1.full_name = ?"
    cursor.execute(query, (full_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get gender of a superhero
@app.get("/v1/bird/superhero/superhero/gender", summary="Get gender of a superhero")
async def get_superhero_gender(superhero_name: str = Query(..., description="Name of the superhero")):
    query = "SELECT T2.gender FROM superhero AS T1 INNER JOIN gender AS T2 ON T1.gender_id = T2.id WHERE T1.superhero_name = ?"
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchone()
    return {"gender": result[0]}

# Endpoint to get superheroes with a specific power
@app.get("/v1/bird/superhero/superhero/power", summary="Get superheroes with a specific power")
async def get_superheroes_with_power(power_name: str = Query(..., description="Name of the power")):
    query = "SELECT T1.superhero_name FROM superhero AS T1 INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id INNER JOIN superpower AS T3 ON T2.power_id = T3.id WHERE T3.power_name = ?"
    cursor.execute(query, (power_name,))
    result = cursor.fetchall()
    return {"superheroes": [row[0] for row in result]}

# Endpoint to get count of powers for a specific superhero
@app.get("/v1/bird/superhero/superhero/power_count", summary="Get count of powers for a specific superhero")
async def get_power_count_for_superhero(superhero_name: str = Query(..., description="Name of the superhero")):
    query = "SELECT COUNT(T1.power_id) FROM hero_power AS T1 INNER JOIN superhero AS T2 ON T1.hero_id = T2.id WHERE T2.superhero_name = ?"
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchone()
    return {"power_count": result[0]}

# Endpoint to get powers of a superhero by full name
@app.get("/v1/bird/superhero/superhero/powers", summary="Get powers of a superhero by full name")
async def get_powers_of_superhero(full_name: str = Query(..., description="Full name of the superhero")):
    query = "SELECT T3.power_name FROM superhero AS T1 INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id INNER JOIN superpower AS T3 ON T2.power_id = T3.id WHERE T1.full_name = ?"
    cursor.execute(query, (full_name,))
    result = cursor.fetchall()
    return {"powers": [row[0] for row in result]}

# Endpoint to get height of superheroes by eye color
@app.get("/v1/bird/superhero/superhero/height_by_eye_color", summary="Get height of superheroes by eye color")
async def get_height_by_eye_color(eye_color: str = Query(..., description="Eye color of the superhero")):
    query = "SELECT T1.height_cm FROM superhero AS T1 INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id WHERE T2.colour = ?"
    cursor.execute(query, (eye_color,))
    result = cursor.fetchall()
    return {"heights": [row[0] for row in result]}

# Endpoint to get superheroes by eye and hair color
@app.get("/v1/bird/superhero/superhero/by_eye_and_hair_color", summary="Get superheroes by eye and hair color")
async def get_superheroes_by_eye_and_hair_color(color: str = Query(..., description="Color of the superhero's eyes and hair")):
    query = "SELECT T1.superhero_name FROM superhero AS T1 INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id AND T1.hair_colour_id = T2.id WHERE T2.colour = ?"
    cursor.execute(query, (color,))
    result = cursor.fetchall()
    return {"superheroes": [row[0] for row in result]}

# Endpoint to get eye color of superheroes by skin color
@app.get("/v1/bird/superhero/superhero/eye_color_by_skin_color", summary="Get eye color of superheroes by skin color")
async def get_eye_color_by_skin_color(skin_color: str = Query(..., description="Skin color of the superhero")):
    query = "SELECT T2.colour FROM superhero AS T1 INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id INNER JOIN colour AS T3 ON T1.skin_colour_id = T3.id WHERE T3.colour = ?"
    cursor.execute(query, (skin_color,))
    result = cursor.fetchall()
    return {"eye_colors": [row[0] for row in result]}

# Endpoint to get full name of superheroes by race
@app.get("/v1/bird/superhero/superhero/full_name_by_race", summary="Get full name of superheroes by race")
async def get_full_name_by_race(race: str = Query(..., description="Race of the superhero")):
    query = "SELECT T1.full_name FROM superhero AS T1 INNER JOIN race AS T2 ON T1.race_id = T2.id WHERE T2.race = ?"
    cursor.execute(query, (race,))
    result = cursor.fetchall()
    return {"full_names": [row[0] for row in result]}

# Endpoint to get superheroes by alignment
@app.get("/v1/bird/superhero/superhero/by_alignment", summary="Get superheroes by alignment")
async def get_superheroes_by_alignment(alignment: str = Query(..., description="Alignment of the superhero")):
    query = "SELECT T1.superhero_name FROM superhero AS T1 INNER JOIN alignment AS T2 ON T1.alignment_id = T2.id WHERE T2.alignment = ?"
    cursor.execute(query, (alignment,))
    result = cursor.fetchall()
    return {"superheroes": [row[0] for row in result]}

# Endpoint to get count of heroes with maximum strength
@app.get("/v1/bird/superhero/superhero/max_strength_count", summary="Get count of heroes with maximum strength")
async def get_max_strength_count(attribute_name: str = Query(..., description="Name of the attribute")):
    query = "SELECT COUNT(T1.hero_id) FROM hero_attribute AS T1 INNER JOIN attribute AS T2 ON T1.attribute_id = T2.id WHERE T2.attribute_name = ? AND T1.attribute_value = (SELECT MAX(attribute_value) FROM hero_attribute)"
    cursor.execute(query, (attribute_name,))
    result = cursor.fetchone()
    return {"max_strength_count": result[0]}

# Endpoint to get race and alignment of a superhero
@app.get("/v1/bird/superhero/superhero/race_and_alignment", summary="Get race and alignment of a superhero")
async def get_race_and_alignment(superhero_name: str = Query(..., description="Name of the superhero")):
    query = "SELECT T2.race, T3.alignment FROM superhero AS T1 INNER JOIN race AS T2 ON T1.race_id = T2.id INNER JOIN alignment AS T3 ON T1.alignment_id = T3.id WHERE T1.superhero_name = ?"
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchone()
    return {"race": result[0], "alignment": result[1]}

# Endpoint to get percentage of female superheroes from a specific publisher
@app.get("/v1/bird/superhero/superhero/female_percentage", summary="Get percentage of female superheroes from a specific publisher")
async def get_female_percentage(publisher_name: str = Query(..., description="Name of the publisher"), gender: str = Query(..., description="Gender of the superhero")):
    query = "SELECT CAST(COUNT(CASE WHEN T2.publisher_name = ? THEN 1 ELSE NULL END) AS REAL) * 100 / COUNT(T1.id) FROM superhero AS T1 INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id INNER JOIN gender AS T3 ON T1.gender_id = T3.id WHERE T3.gender = ?"
    cursor.execute(query, (publisher_name, gender))
    result = cursor.fetchone()
    return {"female_percentage": result[0]}

# Endpoint to get average weight of superheroes by race
@app.get("/v1/bird/superhero/superhero/average_weight_by_race", summary="Get average weight of superheroes by race")
async def get_average_weight_by_race(race: str = Query(..., description="Race of the superhero")):
    query = "SELECT CAST(SUM(T1.weight_kg) AS REAL) / COUNT(T1.id) FROM superhero AS T1 INNER JOIN race AS T2 ON T1.race_id = T2.id WHERE T2.race = ?"
    cursor.execute(query, (race,))
    result = cursor.fetchone()
    return {"average_weight": result[0]}

# Endpoint to calculate weight difference between two superheroes
@app.get("/v1/bird/superhero/superhero/weight_difference", summary="Calculate weight difference between two superheroes")
async def get_weight_difference(full_name1: str = Query(..., description="Full name of the first superhero"), full_name2: str = Query(..., description="Full name of the second superhero")):
    query = "SELECT (SELECT weight_kg FROM superhero WHERE full_name LIKE ?) - (SELECT weight_kg FROM superhero WHERE full_name LIKE ?) AS CALCULATE"
    cursor.execute(query, (full_name1, full_name2))
    result = cursor.fetchone()
    return {"weight_difference": result[0]}

# Endpoint to get average height of superheroes
@app.get("/v1/bird/superhero/superhero/average_height", summary="Get average height of superheroes")
async def get_average_height():
    query = "SELECT CAST(SUM(height_cm) AS REAL) / COUNT(id) FROM superhero"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"average_height": result[0]}

# Endpoint to get powers of a superhero by superhero name
@app.get("/v1/bird/superhero/superhero/powers_by_name", summary="Get powers of a superhero by superhero name")
async def get_powers_by_name(superhero_name: str = Query(..., description="Name of the superhero")):
    query = "SELECT T3.power_name FROM superhero AS T1 INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id INNER JOIN superpower AS T3 ON T2.power_id = T3.id WHERE T1.superhero_name = ?"
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchall()
    return {"powers": [row[0] for row in result]}

# Endpoint to get count of superheroes by race and gender
@app.get("/v1/bird/superhero/superhero/count_by_race_and_gender", summary="Get count of superheroes by race and gender")
async def get_count_by_race_and_gender(race_id: int = Query(..., description="ID of the race"), gender_id: int = Query(..., description="ID of the gender")):
    query = "SELECT COUNT(*) FROM superhero AS T1 INNER JOIN race AS T2 ON T1.race_id = T2.id INNER JOIN gender AS T3 ON T3.id = T1.gender_id WHERE T1.race_id = ? AND T1.gender_id = ?"
    cursor.execute(query, (race_id, gender_id))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get superhero with the highest speed
@app.get("/v1/bird/superhero/superhero/highest_speed", summary="Get superhero with the highest speed")
async def get_highest_speed():
    query = "SELECT T1.superhero_name FROM superhero AS T1 INNER JOIN hero_attribute AS T2 ON T1.id = T2.hero_id INNER JOIN attribute AS T3 ON T2.attribute_id = T3.id WHERE T3.attribute_name = 'Speed' ORDER BY T2.attribute_value DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"superhero": result[0]}

# Endpoint to get count of neutral superheroes
@app.get("/v1/bird/superhero/superhero/neutral_count", summary="Get count of neutral superheroes")
async def get_neutral_count():
    query = "SELECT COUNT(T1.id) FROM superhero AS T1 INNER JOIN alignment AS T2 ON T1.alignment_id = T2.id WHERE T2.alignment = 'Neutral'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"neutral_count": result[0]}

# Endpoint to get attributes of a superhero
@app.get("/v1/bird/superhero/superhero/attributes", summary="Get attributes of a superhero")
async def get_attributes(superhero_name: str = Query(..., description="Name of the superhero")):
    query = "SELECT T3.attribute_name, T2.attribute_value FROM superhero AS T1 INNER JOIN hero_attribute AS T2 ON T1.id = T2.hero_id INNER JOIN attribute AS T3 ON T2.attribute_id = T3.id WHERE T1.superhero_name = ?"
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchall()
    return {"attributes": [{"attribute_name": row[0], "attribute_value": row[1]} for row in result]}

# Endpoint to get superhero names based on eye and hair colour
@app.get("/v1/bird/superhero/superheroes/by_colours", summary="Get superhero names by eye and hair colour")
async def get_superheroes_by_colours(eye_colour: str = Query(..., description="Eye colour of the superhero"),
                                     hair_colour: str = Query(..., description="Hair colour of the superhero")):
    query = """
    SELECT T1.superhero_name
    FROM superhero AS T1
    INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id
    INNER JOIN colour AS T3 ON T1.hair_colour_id = T3.id
    WHERE T2.colour = ? AND T3.colour = ?
    """
    cursor.execute(query, (eye_colour, hair_colour))
    result = cursor.fetchall()
    return {"superheroes": result}

# Endpoint to get publisher names for given superhero names
@app.get("/v1/bird/superhero/publishers/by_superheroes", summary="Get publisher names for given superhero names")
async def get_publishers_by_superheroes(superhero_names: str = Query(..., description="Comma-separated list of superhero names")):
    names_list = superhero_names.split(',')
    placeholders = ','.join('?' for _ in names_list)
    query = f"""
    SELECT T2.publisher_name
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    WHERE T1.superhero_name IN ({placeholders})
    """
    cursor.execute(query, names_list)
    result = cursor.fetchall()
    return {"publishers": result}

# Endpoint to get count of superheroes for a given publisher ID
@app.get("/v1/bird/superhero/superheroes/count_by_publisher", summary="Get count of superheroes for a given publisher ID")
async def get_superheroes_count_by_publisher(publisher_id: int = Query(..., description="ID of the publisher")):
    query = """
    SELECT COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    WHERE T2.id = ?
    """
    cursor.execute(query, (publisher_id,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get percentage of superheroes with blue eyes
@app.get("/v1/bird/superhero/superheroes/percentage_blue_eyes", summary="Get percentage of superheroes with blue eyes")
async def get_percentage_blue_eyes():
    query = """
    SELECT CAST(COUNT(CASE WHEN T2.colour = 'Blue' THEN 1 ELSE NULL END) AS REAL) * 100 / COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get male to female ratio of superheroes
@app.get("/v1/bird/superhero/superheroes/male_to_female_ratio", summary="Get male to female ratio of superheroes")
async def get_male_to_female_ratio():
    query = """
    SELECT CAST(COUNT(CASE WHEN T2.gender = 'Male' THEN T1.id ELSE NULL END) AS REAL) / COUNT(CASE WHEN T2.gender = 'Female' THEN T1.id ELSE NULL END)
    FROM superhero AS T1
    INNER JOIN gender AS T2 ON T1.gender_id = T2.id
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"ratio": result[0]}

# Endpoint to get the tallest superhero
@app.get("/v1/bird/superhero/superheroes/tallest", summary="Get the tallest superhero")
async def get_tallest_superhero():
    query = """
    SELECT superhero_name
    FROM superhero
    ORDER BY height_cm DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"tallest_superhero": result[0]}

# Endpoint to get superpower ID by power name
@app.get("/v1/bird/superhero/superpowers/by_name", summary="Get superpower ID by power name")
async def get_superpower_by_name(power_name: str = Query(..., description="Name of the superpower")):
    query = """
    SELECT id
    FROM superpower
    WHERE power_name = ?
    """
    cursor.execute(query, (power_name,))
    result = cursor.fetchone()
    return {"superpower_id": result[0]}

# Endpoint to get superhero name by ID
@app.get("/v1/bird/superhero/superheroes/by_id", summary="Get superhero name by ID")
async def get_superhero_by_id(superhero_id: int = Query(..., description="ID of the superhero")):
    query = """
    SELECT superhero_name
    FROM superhero
    WHERE id = ?
    """
    cursor.execute(query, (superhero_id,))
    result = cursor.fetchone()
    return {"superhero_name": result[0]}

# Endpoint to get superheroes with no weight or zero weight
@app.get("/v1/bird/superhero/superheroes/no_weight", summary="Get superheroes with no weight or zero weight")
async def get_superheroes_no_weight():
    query = """
    SELECT DISTINCT full_name
    FROM superhero
    WHERE full_name IS NOT NULL AND (weight_kg IS NULL OR weight_kg = 0)
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"superheroes": result}

# Endpoint to get eye colour of a superhero by full name
@app.get("/v1/bird/superhero/superheroes/eye_colour", summary="Get eye colour of a superhero by full name")
async def get_superhero_eye_colour(full_name: str = Query(..., description="Full name of the superhero")):
    query = """
    SELECT T2.colour
    FROM superhero AS T1
    INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id
    WHERE T1.full_name = ?
    """
    cursor.execute(query, (full_name,))
    result = cursor.fetchone()
    return {"eye_colour": result[0]}

# Endpoint to get superpowers of a superhero by full name
@app.get("/v1/bird/superhero/superheroes/powers", summary="Get superpowers of a superhero by full name")
async def get_superhero_powers(full_name: str = Query(..., description="Full name of the superhero")):
    query = """
    SELECT T3.power_name
    FROM superhero AS T1
    INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id
    INNER JOIN superpower AS T3 ON T2.power_id = T3.id
    WHERE T1.full_name = ?
    """
    cursor.execute(query, (full_name,))
    result = cursor.fetchall()
    return {"powers": result}

# Endpoint to get race of superheroes by weight and height
@app.get("/v1/bird/superhero/superheroes/race_by_weight_height", summary="Get race of superheroes by weight and height")
async def get_superheroes_race_by_weight_height(weight_kg: int = Query(..., description="Weight in kg"),
                                                height_cm: int = Query(..., description="Height in cm")):
    query = """
    SELECT DISTINCT T2.race
    FROM superhero AS T1
    INNER JOIN race AS T2 ON T1.race_id = T2.id
    WHERE T1.weight_kg = ? AND T1.height_cm = ?
    """
    cursor.execute(query, (weight_kg, height_cm))
    result = cursor.fetchall()
    return {"races": result}

# Endpoint to get publisher name by superhero ID
@app.get("/v1/bird/superhero/superheroes/publisher_by_id", summary="Get publisher name by superhero ID")
async def get_publisher_by_superhero_id(superhero_id: int = Query(..., description="ID of the superhero")):
    query = """
    SELECT T2.publisher_name
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    WHERE T1.id = ?
    """
    cursor.execute(query, (superhero_id,))
    result = cursor.fetchone()
    return {"publisher_name": result[0]}

# Endpoint to get race of the superhero with the highest attribute value
@app.get("/v1/bird/superhero/superheroes/race_by_highest_attribute", summary="Get race of the superhero with the highest attribute value")
async def get_race_by_highest_attribute():
    query = """
    SELECT T3.race
    FROM superhero AS T1
    INNER JOIN hero_attribute AS T2 ON T1.id = T2.hero_id
    INNER JOIN race AS T3 ON T1.race_id = T3.id
    ORDER BY T2.attribute_value DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"race": result[0]}

# Endpoint to get alignment and powers of a superhero by name
@app.get("/v1/bird/superhero/superheroes/alignment_powers", summary="Get alignment and powers of a superhero by name")
async def get_alignment_powers_by_name(superhero_name: str = Query(..., description="Name of the superhero")):
    query = """
    SELECT T4.alignment, T3.power_name
    FROM superhero AS T1
    INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id
    INNER JOIN superpower AS T3 ON T3.id = T2.power_id
    INNER JOIN alignment AS T4 ON T1.alignment_id = T4.id
    WHERE T1.superhero_name = ?
    """
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchall()
    return {"alignment_powers": result}

# Endpoint to get superheroes with blue eyes
@app.get("/v1/bird/superhero/superheroes/blue_eyes", summary="Get superheroes with blue eyes")
async def get_superheroes_with_blue_eyes(limit: int = Query(5, description="Limit the number of results")):
    query = """
    SELECT T1.superhero_name
    FROM superhero AS T1
    INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id
    WHERE T2.colour = 'Blue'
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    result = cursor.fetchall()
    return {"superheroes": result}

# Endpoint to get average attribute value for neutral alignment superheroes
@app.get("/v1/bird/superhero/superheroes/avg_attribute_neutral", summary="Get average attribute value for neutral alignment superheroes")
async def get_avg_attribute_neutral():
    query = """
    SELECT AVG(T1.attribute_value)
    FROM hero_attribute AS T1
    INNER JOIN superhero AS T2 ON T1.hero_id = T2.id
    INNER JOIN alignment AS T3 ON T2.alignment_id = T3.id
    WHERE T3.alignment = 'Neutral'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"average_attribute": result[0]}

# Endpoint to get distinct skin colours of superheroes with max attribute value
@app.get("/v1/bird/superhero/superheroes/skin_colours_max_attribute", summary="Get distinct skin colours of superheroes with max attribute value")
async def get_skin_colours_max_attribute(attribute_value: int = Query(100, description="Attribute value")):
    query = """
    SELECT DISTINCT T2.colour
    FROM superhero AS T1
    INNER JOIN colour AS T2 ON T1.skin_colour_id = T2.id
    INNER JOIN hero_attribute AS T3 ON T1.id = T3.hero_id
    WHERE T3.attribute_value = ?
    """
    cursor.execute(query, (attribute_value,))
    result = cursor.fetchall()
    return {"skin_colours": result}

# Endpoint to get count of good female superheroes
@app.get("/v1/bird/superhero/superheroes/count_good_female", summary="Get count of good female superheroes")
async def get_count_good_female():
    query = """
    SELECT COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN alignment AS T2 ON T1.alignment_id = T2.id
    INNER JOIN gender AS T3 ON T1.gender_id = T3.id
    WHERE T2.alignment = 'Good' AND T3.gender = 'Female'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get superheroes with attribute value between a range
@app.get("/v1/bird/superhero/superheroes/by_attribute_range", summary="Get superheroes with attribute value between a range")
async def get_superheroes_by_attribute_range(min_value: int = Query(..., description="Minimum attribute value"),
                                             max_value: int = Query(..., description="Maximum attribute value")):
    query = """
    SELECT T1.superhero_name
    FROM superhero AS T1
    INNER JOIN hero_attribute AS T2 ON T1.id = T2.hero_id
    WHERE T2.attribute_value BETWEEN ? AND ?
    """
    cursor.execute(query, (min_value, max_value))
    result = cursor.fetchall()
    return {"superheroes": result}

# Endpoint to get race of superheroes with specific hair colour and gender
@app.get("/v1/bird/superhero/superhero/race", summary="Get race of superheroes with specific hair colour and gender")
async def get_superhero_race(hair_colour: str = Query(..., description="Hair colour of the superhero"),
                             gender: str = Query(..., description="Gender of the superhero")):
    query = """
    SELECT T3.race
    FROM superhero AS T1
    INNER JOIN colour AS T2 ON T1.hair_colour_id = T2.id
    INNER JOIN race AS T3 ON T1.race_id = T3.id
    INNER JOIN gender AS T4 ON T1.gender_id = T4.id
    WHERE T2.colour = ? AND T4.gender = ?
    """
    cursor.execute(query, (hair_colour, gender))
    result = cursor.fetchall()
    return {"race": result}

# Endpoint to get percentage of female bad superheroes
@app.get("/v1/bird/superhero/superhero/female_bad_percentage", summary="Get percentage of female bad superheroes")
async def get_female_bad_percentage(alignment: str = Query(..., description="Alignment of the superhero")):
    query = """
    SELECT CAST(COUNT(CASE WHEN T3.gender = 'Female' THEN T1.id ELSE NULL END) AS REAL) * 100 / COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN alignment AS T2 ON T1.alignment_id = T2.id
    INNER JOIN gender AS T3 ON T1.gender_id = T3.id
    WHERE T2.alignment = ?
    """
    cursor.execute(query, (alignment,))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get difference in count of superheroes with specific eye colours
@app.get("/v1/bird/superhero/superhero/eye_colour_difference", summary="Get difference in count of superheroes with specific eye colours")
async def get_eye_colour_difference(eye_colour_id1: int = Query(..., description="First eye colour ID"),
                                    eye_colour_id2: int = Query(..., description="Second eye colour ID")):
    query = """
    SELECT SUM(CASE WHEN T2.id = ? THEN 1 ELSE 0 END) - SUM(CASE WHEN T2.id = ? THEN 1 ELSE 0 END)
    FROM superhero AS T1
    INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id
    WHERE T1.weight_kg = 0 OR T1.weight_kg is NULL
    """
    cursor.execute(query, (eye_colour_id1, eye_colour_id2))
    result = cursor.fetchone()
    return {"difference": result[0]}

# Endpoint to get attribute value of a specific superhero
@app.get("/v1/bird/superhero/superhero/attribute_value", summary="Get attribute value of a specific superhero")
async def get_attribute_value(superhero_name: str = Query(..., description="Name of the superhero"),
                              attribute_name: str = Query(..., description="Name of the attribute")):
    query = """
    SELECT T2.attribute_value
    FROM superhero AS T1
    INNER JOIN hero_attribute AS T2 ON T1.id = T2.hero_id
    INNER JOIN attribute AS T3 ON T2.attribute_id = T3.id
    WHERE T1.superhero_name = ? AND T3.attribute_name = ?
    """
    cursor.execute(query, (superhero_name, attribute_name))
    result = cursor.fetchone()
    return {"attribute_value": result[0]}

# Endpoint to get powers of a specific superhero
@app.get("/v1/bird/superhero/superhero/powers", summary="Get powers of a specific superhero")
async def get_superhero_powers(superhero_name: str = Query(..., description="Name of the superhero")):
    query = """
    SELECT T3.power_name
    FROM superhero AS T1
    INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id
    INNER JOIN superpower AS T3 ON T2.power_id = T3.id
    WHERE T1.superhero_name = ?
    """
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchall()
    return {"powers": result}

# Endpoint to get count of bad superheroes with specific skin colour
@app.get("/v1/bird/superhero/superhero/bad_skin_colour_count", summary="Get count of bad superheroes with specific skin colour")
async def get_bad_skin_colour_count(alignment: str = Query(..., description="Alignment of the superhero"),
                                    skin_colour: str = Query(..., description="Skin colour of the superhero")):
    query = """
    SELECT COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN alignment AS T2 ON T1.alignment_id = T2.id
    INNER JOIN colour AS T3 ON T1.skin_colour_id = T3.id
    WHERE T2.alignment = ? AND T3.colour = ?
    """
    cursor.execute(query, (alignment, skin_colour))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of female superheroes from a specific publisher
@app.get("/v1/bird/superhero/superhero/female_publisher_count", summary="Get count of female superheroes from a specific publisher")
async def get_female_publisher_count(publisher_name: str = Query(..., description="Name of the publisher"),
                                     gender: str = Query(..., description="Gender of the superhero")):
    query = """
    SELECT COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    INNER JOIN gender AS T3 ON T1.gender_id = T3.id
    WHERE T2.publisher_name = ? AND T3.gender = ?
    """
    cursor.execute(query, (publisher_name, gender))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get superheroes with a specific power
@app.get("/v1/bird/superhero/superhero/power_name", summary="Get superheroes with a specific power")
async def get_superheroes_with_power(power_name: str = Query(..., description="Name of the power")):
    query = """
    SELECT T1.superhero_name
    FROM superhero AS T1
    INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id
    INNER JOIN superpower AS T3 ON T2.power_id = T3.id
    WHERE T3.power_name = ?
    ORDER BY T1.superhero_name
    """
    cursor.execute(query, (power_name,))
    result = cursor.fetchall()
    return {"superheroes": result}

# Endpoint to get gender of superheroes with a specific power
@app.get("/v1/bird/superhero/superhero/power_gender", summary="Get gender of superheroes with a specific power")
async def get_power_gender(power_name: str = Query(..., description="Name of the power")):
    query = """
    SELECT T4.gender
    FROM superhero AS T1
    INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id
    INNER JOIN superpower AS T3 ON T2.power_id = T3.id
    INNER JOIN gender AS T4 ON T1.gender_id = T4.id
    WHERE T3.power_name = ?
    """
    cursor.execute(query, (power_name,))
    result = cursor.fetchall()
    return {"genders": result}

# Endpoint to get the heaviest superhero from a specific publisher
@app.get("/v1/bird/superhero/superhero/heaviest", summary="Get the heaviest superhero from a specific publisher")
async def get_heaviest_superhero(publisher_name: str = Query(..., description="Name of the publisher")):
    query = """
    SELECT T1.superhero_name
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    WHERE T2.publisher_name = ?
    ORDER BY T1.weight_kg DESC
    LIMIT 1
    """
    cursor.execute(query, (publisher_name,))
    result = cursor.fetchone()
    return {"heaviest_superhero": result[0]}

# Endpoint to get average height of non-human superheroes from a specific publisher
@app.get("/v1/bird/superhero/superhero/average_height", summary="Get average height of non-human superheroes from a specific publisher")
async def get_average_height(publisher_name: str = Query(..., description="Name of the publisher"),
                             race: str = Query(..., description="Race of the superhero")):
    query = """
    SELECT AVG(T1.height_cm)
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    INNER JOIN race AS T3 ON T1.race_id = T3.id
    WHERE T2.publisher_name = ? AND T3.race != ?
    """
    cursor.execute(query, (publisher_name, race))
    result = cursor.fetchone()
    return {"average_height": result[0]}

# Endpoint to get count of superheroes with a specific attribute value
@app.get("/v1/bird/superhero/superhero/attribute_count", summary="Get count of superheroes with a specific attribute value")
async def get_attribute_count(attribute_name: str = Query(..., description="Name of the attribute"),
                              attribute_value: int = Query(..., description="Value of the attribute")):
    query = """
    SELECT COUNT(T3.superhero_name)
    FROM hero_attribute AS T1
    INNER JOIN attribute AS T2 ON T1.attribute_id = T2.id
    INNER JOIN superhero AS T3 ON T1.hero_id = T3.id
    WHERE T2.attribute_name = ? AND T1.attribute_value = ?
    """
    cursor.execute(query, (attribute_name, attribute_value))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get difference in count of superheroes from specific publishers
@app.get("/v1/bird/superhero/superhero/publisher_difference", summary="Get difference in count of superheroes from specific publishers")
async def get_publisher_difference(publisher_name1: str = Query(..., description="First publisher name"),
                                   publisher_name2: str = Query(..., description="Second publisher name")):
    query = """
    SELECT SUM(CASE WHEN T2.publisher_name = ? THEN 1 ELSE 0 END) - SUM(CASE WHEN T2.publisher_name = ? THEN 1 ELSE 0 END)
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    """
    cursor.execute(query, (publisher_name1, publisher_name2))
    result = cursor.fetchone()
    return {"difference": result[0]}

# Endpoint to get the attribute with the lowest value for a specific superhero
@app.get("/v1/bird/superhero/superhero/lowest_attribute", summary="Get the attribute with the lowest value for a specific superhero")
async def get_lowest_attribute(superhero_name: str = Query(..., description="Name of the superhero")):
    query = """
    SELECT T3.attribute_name
    FROM superhero AS T1
    INNER JOIN hero_attribute AS T2 ON T1.id = T2.hero_id
    INNER JOIN attribute AS T3 ON T2.attribute_id = T3.id
    WHERE T1.superhero_name = ?
    ORDER BY T2.attribute_value ASC
    LIMIT 1
    """
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchone()
    return {"lowest_attribute": result[0]}

# Endpoint to get eye colour of a specific superhero
@app.get("/v1/bird/superhero/superhero/eye_colour", summary="Get eye colour of a specific superhero")
async def get_eye_colour(superhero_name: str = Query(..., description="Name of the superhero")):
    query = """
    SELECT T2.colour
    FROM superhero AS T1
    INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id
    WHERE T1.superhero_name = ?
    """
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchone()
    return {"eye_colour": result[0]}

# Endpoint to get the tallest superhero
@app.get("/v1/bird/superhero/superhero/tallest", summary="Get the tallest superhero")
async def get_tallest_superhero():
    query = """
    SELECT superhero_name
    FROM superhero
    ORDER BY height_cm DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"tallest_superhero": result[0]}

# Endpoint to get superhero by full name
@app.get("/v1/bird/superhero/superhero/by_full_name", summary="Get superhero by full name")
async def get_superhero_by_full_name(full_name: str = Query(..., description="Full name of the superhero")):
    query = """
    SELECT superhero_name
    FROM superhero
    WHERE full_name = ?
    """
    cursor.execute(query, (full_name,))
    result = cursor.fetchone()
    return {"superhero_name": result[0]}

# Endpoint to get percentage of female superheroes from a specific publisher
@app.get("/v1/bird/superhero/superhero/female_percentage", summary="Get percentage of female superheroes from a specific publisher")
async def get_female_percentage(publisher_name: str = Query(..., description="Name of the publisher")):
    query = """
    SELECT CAST(COUNT(CASE WHEN T3.gender = 'Female' THEN 1 ELSE NULL END) AS REAL) * 100 / COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    INNER JOIN gender AS T3 ON T1.gender_id = T3.id
    WHERE T2.publisher_name = ?
    """
    cursor.execute(query, (publisher_name,))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get percentage of good superheroes from a specific publisher
@app.get("/v1/bird/superhero/superhero/good_percentage", summary="Get percentage of good superheroes from a specific publisher")
async def get_good_percentage(publisher_name: str = Query(..., description="Name of the publisher")):
    query = """
    SELECT CAST(COUNT(CASE WHEN T3.alignment = 'Good' THEN T1.id ELSE NULL END) AS REAL) * 100 / COUNT(T1.id)
    FROM superhero AS T1
    INNER JOIN publisher AS T2 ON T1.publisher_id = T2.id
    INNER JOIN alignment AS T3 ON T1.alignment_id = T3.id
    WHERE T2.publisher_name = ?
    """
    cursor.execute(query, (publisher_name,))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get count of superheroes with a specific full name prefix
@app.get("/v1/bird/superhero/superhero/full_name_prefix_count", summary="Get count of superheroes with a specific full name prefix")
async def get_full_name_prefix_count(full_name_prefix: str = Query(..., description="Prefix of the full name")):
    query = """
    SELECT COUNT(id)
    FROM superhero
    WHERE full_name LIKE ?
    """
    cursor.execute(query, (full_name_prefix + '%',))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get hero_id with minimum attribute_value
@app.get("/v1/bird/superhero/min_hero_attribute", summary="Get hero_id with minimum attribute_value")
async def get_min_hero_attribute():
    query = """
    SELECT hero_id FROM hero_attribute
    WHERE attribute_value = (
        SELECT MIN(attribute_value) FROM hero_attribute
    )
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"hero_id": result}

# Endpoint to get full_name of a superhero by superhero_name
@app.get("/v1/bird/superhero/superhero_full_name", summary="Get full_name of a superhero by superhero_name")
async def get_superhero_full_name(superhero_name: str = Query(..., description="Name of the superhero")):
    query = """
    SELECT full_name FROM superhero
    WHERE superhero_name = ?
    """
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchall()
    return {"full_name": result}

# Endpoint to get full_name of superheroes with specific weight and eye colour
@app.get("/v1/bird/superhero/superhero_by_weight_and_eye_colour", summary="Get full_name of superheroes with specific weight and eye colour")
async def get_superhero_by_weight_and_eye_colour(weight_kg: int = Query(..., description="Weight in kg"), eye_colour: str = Query(..., description="Eye colour")):
    query = """
    SELECT T1.full_name FROM superhero AS T1
    INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id
    WHERE T1.weight_kg < ? AND T2.colour = ?
    """
    cursor.execute(query, (weight_kg, eye_colour))
    result = cursor.fetchall()
    return {"full_name": result}

# Endpoint to get attribute_value of a superhero by superhero_name
@app.get("/v1/bird/superhero/superhero_attribute_value", summary="Get attribute_value of a superhero by superhero_name")
async def get_superhero_attribute_value(superhero_name: str = Query(..., description="Name of the superhero")):
    query = """
    SELECT T2.attribute_value FROM superhero AS T1
    INNER JOIN hero_attribute AS T2 ON T1.id = T2.hero_id
    WHERE T1.superhero_name = ?
    """
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchall()
    return {"attribute_value": result}

# Endpoint to get weight_kg and race of a superhero by id
@app.get("/v1/bird/superhero/superhero_weight_and_race", summary="Get weight_kg and race of a superhero by id")
async def get_superhero_weight_and_race(superhero_id: int = Query(..., description="ID of the superhero")):
    query = """
    SELECT T1.weight_kg, T2.race FROM superhero AS T1
    INNER JOIN race AS T2 ON T1.race_id = T2.id
    WHERE T1.id = ?
    """
    cursor.execute(query, (superhero_id,))
    result = cursor.fetchall()
    return {"weight_kg": result[0][0], "race": result[0][1]}

# Endpoint to get average height_cm of superheroes with specific alignment
@app.get("/v1/bird/superhero/average_height_by_alignment", summary="Get average height_cm of superheroes with specific alignment")
async def get_average_height_by_alignment(alignment: str = Query(..., description="Alignment of the superhero")):
    query = """
    SELECT AVG(T1.height_cm) FROM superhero AS T1
    INNER JOIN alignment AS T2 ON T1.alignment_id = T2.id
    WHERE T2.alignment = ?
    """
    cursor.execute(query, (alignment,))
    result = cursor.fetchone()
    return {"average_height_cm": result[0]}

# Endpoint to get hero_id of superheroes with specific power_name
@app.get("/v1/bird/superhero/hero_id_by_power_name", summary="Get hero_id of superheroes with specific power_name")
async def get_hero_id_by_power_name(power_name: str = Query(..., description="Name of the power")):
    query = """
    SELECT T1.hero_id FROM hero_power AS T1
    INNER JOIN superpower AS T2 ON T1.power_id = T2.id
    WHERE T2.power_name = ?
    """
    cursor.execute(query, (power_name,))
    result = cursor.fetchall()
    return {"hero_id": result}

# Endpoint to get eye colour of a superhero by superhero_name
@app.get("/v1/bird/superhero/superhero_eye_colour", summary="Get eye colour of a superhero by superhero_name")
async def get_superhero_eye_colour(superhero_name: str = Query(..., description="Name of the superhero")):
    query = """
    SELECT T2.colour FROM superhero AS T1
    INNER JOIN colour AS T2 ON T1.eye_colour_id = T2.id
    WHERE T1.superhero_name = ?
    """
    cursor.execute(query, (superhero_name,))
    result = cursor.fetchall()
    return {"eye_colour": result}

# Endpoint to get power_name of superheroes with height_cm greater than 80% of average height
@app.get("/v1/bird/superhero/superhero_power_by_height", summary="Get power_name of superheroes with height_cm greater than 80% of average height")
async def get_superhero_power_by_height():
    query = """
    SELECT T3.power_name FROM superhero AS T1
    INNER JOIN hero_power AS T2 ON T1.id = T2.hero_id
    INNER JOIN superpower AS T3 ON T2.power_id = T3.id
    WHERE T1.height_cm * 100 > (
        SELECT AVG(height_cm) FROM superhero
    ) * 80
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"power_name": result}