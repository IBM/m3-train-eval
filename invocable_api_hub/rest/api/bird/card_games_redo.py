from fastapi import APIRouter, Query
import sqlite3

app = APIRouter()

# Database connection
conn = sqlite3.connect('invocable_api_hub/db/bird/test/card_games.sqlite', check_same_thread=False)
cursor = conn.cursor()

# Endpoint to get card IDs with non-null cardKingdomFoilId and cardKingdomId
@app.get("/v1/bird/card_games/cards/non_null_ids", summary="Get card IDs with non-null cardKingdomFoilId and cardKingdomId")
async def get_non_null_ids():
    query = "SELECT id FROM cards WHERE cardKingdomFoilId IS NOT NULL AND cardKingdomId IS NOT NULL"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get card IDs with borderless borderColor and null cardKingdomId
@app.get("/v1/bird/card_games/cards/borderless_null_ids", summary="Get card IDs with borderless borderColor and null cardKingdomId")
async def get_borderless_null_ids():
    query = "SELECT id FROM cards WHERE borderColor = 'borderless' AND (cardKingdomId IS NULL OR cardKingdomId IS NULL)"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get card name ordered by faceConvertedManaCost
@app.get("/v1/bird/card_games/cards/name_by_mana_cost", summary="Get card name ordered by faceConvertedManaCost")
async def get_name_by_mana_cost(limit: int = Query(1, description="Limit the number of results")):
    query = "SELECT name FROM cards ORDER BY faceConvertedManaCost LIMIT ?"
    cursor.execute(query, (limit,))
    result = cursor.fetchall()
    return result

# Endpoint to get card IDs with edhrecRank less than a given value and frameVersion
@app.get("/v1/bird/card_games/cards/edhrec_rank", summary="Get card IDs with edhrecRank less than a given value and frameVersion")
async def get_edhrec_rank(edhrec_rank: int = Query(..., description="EDHREC rank"), frame_version: int = Query(..., description="Frame version")):
    query = "SELECT id FROM cards WHERE edhrecRank < ? AND frameVersion = ?"
    cursor.execute(query, (edhrec_rank, frame_version))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct card IDs banned in gladiator format with mythic rarity
@app.get("/v1/bird/card_games/cards/banned_gladiator_mythic", summary="Get distinct card IDs banned in gladiator format with mythic rarity")
async def get_banned_gladiator_mythic():
    query = "SELECT DISTINCT T1.id FROM cards AS T1 INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid WHERE T2.format = 'gladiator' AND T2.status = 'Banned' AND T1.rarity = 'mythic'"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get distinct status of artifact cards in vintage format
@app.get("/v1/bird/card_games/cards/artifact_vintage_status", summary="Get distinct status of artifact cards in vintage format")
async def get_artifact_vintage_status():
    query = "SELECT DISTINCT T2.status FROM cards AS T1 INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid WHERE T1.type = 'Artifact' AND T2.format = 'vintage' AND T1.side IS NULL"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get card IDs and artist of legal commander cards with null or '*' power
@app.get("/v1/bird/card_games/cards/legal_commander_power", summary="Get card IDs and artist of legal commander cards with null or '*' power")
async def get_legal_commander_power():
    query = "SELECT T1.id, T1.artist FROM cards AS T1 INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid WHERE T2.status = 'Legal' AND T2.format = 'commander' AND (T1.power IS NULL OR T1.power = '*')"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get card IDs, ruling text, and content warning for cards by a specific artist
@app.get("/v1/bird/card_games/cards/rulings_by_artist", summary="Get card IDs, ruling text, and content warning for cards by a specific artist")
async def get_rulings_by_artist(artist: str = Query(..., description="Artist name")):
    query = "SELECT T1.id, T2.text, T1.hasContentWarning FROM cards AS T1 INNER JOIN rulings AS T2 ON T1.uuid = T2.uuid WHERE T1.artist = ?"
    cursor.execute(query, (artist,))
    result = cursor.fetchall()
    return result

# Endpoint to get ruling text for a specific card
@app.get("/v1/bird/card_games/cards/ruling_text", summary="Get ruling text for a specific card")
async def get_ruling_text(name: str = Query(..., description="Card name"), number: str = Query(..., description="Card number")):
    query = "SELECT T2.text FROM cards AS T1 INNER JOIN rulings AS T2 ON T1.uuid = T2.uuid WHERE T1.name = ? AND T1.number = ?"
    cursor.execute(query, (name, number))
    result = cursor.fetchall()
    return result

# Endpoint to get promo card details with the most promo cards by an artist
@app.get("/v1/bird/card_games/cards/most_promo_by_artist", summary="Get promo card details with the most promo cards by an artist")
async def get_most_promo_by_artist():
    query = """
    SELECT T1.name, T1.artist, T1.isPromo
    FROM cards AS T1
    INNER JOIN rulings AS T2 ON T1.uuid = T2.uuid
    WHERE T1.isPromo = 1 AND T1.artist = (
        SELECT artist
        FROM cards
        WHERE isPromo = 1
        GROUP BY artist
        HAVING COUNT(DISTINCT uuid) = (
            SELECT MAX(count_uuid)
            FROM (
                SELECT COUNT(DISTINCT uuid) AS count_uuid
                FROM cards
                WHERE isPromo = 1
                GROUP BY artist
            )
        )
    )
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get language of a specific card
@app.get("/v1/bird/card_games/cards/language_by_card", summary="Get language of a specific card")
async def get_language_by_card(name: str = Query(..., description="Card name"), number: int = Query(..., description="Card number")):
    query = "SELECT T2.language FROM cards AS T1 INNER JOIN foreign_data AS T2 ON T1.uuid = T2.uuid WHERE T1.name = ? AND T1.number = ?"
    cursor.execute(query, (name, number))
    result = cursor.fetchall()
    return result

# Endpoint to get card names in a specific language
@app.get("/v1/bird/card_games/cards/names_by_language", summary="Get card names in a specific language")
async def get_names_by_language(language: str = Query(..., description="Language")):
    query = "SELECT T1.name FROM cards AS T1 INNER JOIN foreign_data AS T2 ON T1.uuid = T2.uuid WHERE T2.language = ?"
    cursor.execute(query, (language,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of cards in a specific language
@app.get("/v1/bird/card_games/cards/percentage_by_language", summary="Get percentage of cards in a specific language")
async def get_percentage_by_language(language: str = Query(..., description="Language")):
    query = """
    SELECT CAST(SUM(CASE WHEN T2.language = ? THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(T1.id)
    FROM cards AS T1
    INNER JOIN foreign_data AS T2 ON T1.uuid = T2.uuid
    """
    cursor.execute(query, (language,))
    result = cursor.fetchall()
    return result

# Endpoint to get set names and total set size in a specific language
@app.get("/v1/bird/card_games/sets/names_by_language", summary="Get set names and total set size in a specific language")
async def get_names_by_language(language: str = Query(..., description="Language")):
    query = "SELECT T1.name, T1.totalSetSize FROM sets AS T1 INNER JOIN set_translations AS T2 ON T1.code = T2.setCode WHERE T2.language = ?"
    cursor.execute(query, (language,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of card types by a specific artist
@app.get("/v1/bird/card_games/cards/count_by_artist", summary="Get count of card types by a specific artist")
async def get_count_by_artist(artist: str = Query(..., description="Artist name")):
    query = "SELECT COUNT(type) FROM cards WHERE artist = ?"
    cursor.execute(query, (artist,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct keywords for a specific card
@app.get("/v1/bird/card_games/cards/keywords_by_name", summary="Get distinct keywords for a specific card")
async def get_keywords_by_name(name: str = Query(..., description="Card name")):
    query = "SELECT DISTINCT keywords FROM cards WHERE name = ?"
    cursor.execute(query, (name,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of cards with a specific power
@app.get("/v1/bird/card_games/cards/count_by_power", summary="Get count of cards with a specific power")
async def get_count_by_power(power: str = Query(..., description="Power")):
    query = "SELECT COUNT(*) FROM cards WHERE power = ?"
    cursor.execute(query, (power,))
    result = cursor.fetchall()
    return result

# Endpoint to get promo types for a specific card
@app.get("/v1/bird/card_games/cards/promo_types_by_name", summary="Get promo types for a specific card")
async def get_promo_types_by_name(name: str = Query(..., description="Card name")):
    query = "SELECT promoTypes FROM cards WHERE name = ? AND promoTypes IS NOT NULL"
    cursor.execute(query, (name,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct border colors for a specific card
@app.get("/v1/bird/card_games/cards/border_color_by_name", summary="Get distinct border colors for a specific card")
async def get_border_color_by_name(name: str = Query(..., description="Card name")):
    query = "SELECT DISTINCT borderColor FROM cards WHERE name = ?"
    cursor.execute(query, (name,))
    result = cursor.fetchall()
    return result

# Endpoint to get original type for a specific card
@app.get("/v1/bird/card_games/cards/original_type_by_name", summary="Get original type for a specific card")
async def get_original_type_by_name(name: str = Query(..., description="Card name")):
    query = "SELECT originalType FROM cards WHERE name = ? AND originalType IS NOT NULL"
    cursor.execute(query, (name,))
    result = cursor.fetchall()
    return result

# Endpoint to get language for a given card name
@app.get("/v1/bird/card_games/language", summary="Get language for a given card name")
async def get_language_for_card(card_name: str = Query(..., description="Name of the card")):
    query = """
    SELECT language FROM set_translations WHERE id IN (
        SELECT id FROM cards WHERE name = ?
    )
    """
    cursor.execute(query, (card_name,))
    result = cursor.fetchall()
    return {"language": result}

# Endpoint to get count of distinct cards with restricted status and isTextless
@app.get("/v1/bird/card_games/restricted_textless_count", summary="Get count of distinct cards with restricted status and isTextless")
async def get_restricted_textless_count(status: str = Query(..., description="Status of the card"), isTextless: int = Query(..., description="Is the card textless")):
    query = """
    SELECT COUNT(DISTINCT T1.id) FROM cards AS T1
    INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid
    WHERE T2.status = ? AND T1.isTextless = ?
    """
    cursor.execute(query, (status, isTextless))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get rulings text for a given card name
@app.get("/v1/bird/card_games/rulings_text", summary="Get rulings text for a given card name")
async def get_rulings_text(card_name: str = Query(..., description="Name of the card")):
    query = """
    SELECT T2.text FROM cards AS T1
    INNER JOIN rulings AS T2 ON T1.uuid = T2.uuid
    WHERE T1.name = ?
    """
    cursor.execute(query, (card_name,))
    result = cursor.fetchall()
    return {"rulings_text": result}

# Endpoint to get count of distinct cards with restricted status and isStarter
@app.get("/v1/bird/card_games/restricted_starter_count", summary="Get count of distinct cards with restricted status and isStarter")
async def get_restricted_starter_count(status: str = Query(..., description="Status of the card"), isStarter: int = Query(..., description="Is the card a starter")):
    query = """
    SELECT COUNT(DISTINCT T1.id) FROM cards AS T1
    INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid
    WHERE T2.status = ? AND T1.isStarter = ?
    """
    cursor.execute(query, (status, isStarter))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get distinct status for a given card name
@app.get("/v1/bird/card_games/card_status", summary="Get distinct status for a given card name")
async def get_card_status(card_name: str = Query(..., description="Name of the card")):
    query = """
    SELECT DISTINCT T2.status FROM cards AS T1
    INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid
    WHERE T1.name = ?
    """
    cursor.execute(query, (card_name,))
    result = cursor.fetchall()
    return {"status": result}

# Endpoint to get distinct type for a given card name
@app.get("/v1/bird/card_games/card_type", summary="Get distinct type for a given card name")
async def get_card_type(card_name: str = Query(..., description="Name of the card")):
    query = """
    SELECT DISTINCT T1.type FROM cards AS T1
    INNER JOIN foreign_data AS T2 ON T1.uuid = T2.uuid
    WHERE T1.name = ?
    """
    cursor.execute(query, (card_name,))
    result = cursor.fetchall()
    return {"type": result}

# Endpoint to get format for a given card name
@app.get("/v1/bird/card_games/card_format", summary="Get format for a given card name")
async def get_card_format(card_name: str = Query(..., description="Name of the card")):
    query = """
    SELECT T2.format FROM cards AS T1
    INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid
    WHERE T1.name = ?
    """
    cursor.execute(query, (card_name,))
    result = cursor.fetchall()
    return {"format": result}

# Endpoint to get artist for a given language
@app.get("/v1/bird/card_games/artist_by_language", summary="Get artist for a given language")
async def get_artist_by_language(language: str = Query(..., description="Language of the card")):
    query = """
    SELECT T1.artist FROM cards AS T1
    INNER JOIN foreign_data AS T2 ON T1.uuid = T2.uuid
    WHERE T2.language = ?
    """
    cursor.execute(query, (language,))
    result = cursor.fetchall()
    return {"artist": result}

# Endpoint to get percentage of borderless cards
@app.get("/v1/bird/card_games/borderless_percentage", summary="Get percentage of borderless cards")
async def get_borderless_percentage():
    query = """
    SELECT CAST(SUM(CASE WHEN borderColor = 'borderless' THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(id) FROM cards
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get count of reprint cards in a given language
@app.get("/v1/bird/card_games/reprint_count_by_language", summary="Get count of reprint cards in a given language")
async def get_reprint_count_by_language(language: str = Query(..., description="Language of the card"), isReprint: int = Query(..., description="Is the card a reprint")):
    query = """
    SELECT COUNT(T1.id) FROM cards AS T1
    INNER JOIN foreign_data AS T2 ON T1.uuid = T2.uuid
    WHERE T2.language = ? AND T1.isReprint = ?
    """
    cursor.execute(query, (language, isReprint))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of borderless cards in a given language
@app.get("/v1/bird/card_games/borderless_count_by_language", summary="Get count of borderless cards in a given language")
async def get_borderless_count_by_language(language: str = Query(..., description="Language of the card"), borderColor: str = Query(..., description="Border color of the card")):
    query = """
    SELECT COUNT(T1.id) FROM cards AS T1
    INNER JOIN foreign_data AS T2 ON T1.uuid = T2.uuid
    WHERE T1.borderColor = ? AND T2.language = ?
    """
    cursor.execute(query, (borderColor, language))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get percentage of story spotlight cards in a given language
@app.get("/v1/bird/card_games/story_spotlight_percentage", summary="Get percentage of story spotlight cards in a given language")
async def get_story_spotlight_percentage(language: str = Query(..., description="Language of the card"), isStorySpotlight: int = Query(..., description="Is the card a story spotlight")):
    query = """
    SELECT CAST(SUM(CASE WHEN T2.language = ? THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(T1.id) FROM cards AS T1
    INNER JOIN foreign_data AS T2 ON T1.uuid = T2.uuid
    WHERE T1.isStorySpotlight = ?
    """
    cursor.execute(query, (language, isStorySpotlight))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get count of cards with a given toughness
@app.get("/v1/bird/card_games/toughness_count", summary="Get count of cards with a given toughness")
async def get_toughness_count(toughness: int = Query(..., description="Toughness of the card")):
    query = """
    SELECT COUNT(id) FROM cards WHERE toughness = ?
    """
    cursor.execute(query, (toughness,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get distinct names for a given artist
@app.get("/v1/bird/card_games/card_names_by_artist", summary="Get distinct names for a given artist")
async def get_card_names_by_artist(artist: str = Query(..., description="Artist of the card")):
    query = """
    SELECT DISTINCT name FROM cards WHERE artist = ?
    """
    cursor.execute(query, (artist,))
    result = cursor.fetchall()
    return {"names": result}

# Endpoint to get count of cards with a given availability and border color
@app.get("/v1/bird/card_games/availability_border_count", summary="Get count of cards with a given availability and border color")
async def get_availability_border_count(availability: str = Query(..., description="Availability of the card"), borderColor: str = Query(..., description="Border color of the card")):
    query = """
    SELECT COUNT(id) FROM cards WHERE availability = ? AND borderColor = ?
    """
    cursor.execute(query, (availability, borderColor))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get ids of cards with a given converted mana cost
@app.get("/v1/bird/card_games/cards_by_mana_cost", summary="Get ids of cards with a given converted mana cost")
async def get_cards_by_mana_cost(convertedManaCost: int = Query(..., description="Converted mana cost of the card")):
    query = """
    SELECT id FROM cards WHERE convertedManaCost = ?
    """
    cursor.execute(query, (convertedManaCost,))
    result = cursor.fetchall()
    return {"ids": result}

# Endpoint to get layout for a given keyword
@app.get("/v1/bird/card_games/layout_by_keyword", summary="Get layout for a given keyword")
async def get_layout_by_keyword(keyword: str = Query(..., description="Keyword of the card")):
    query = """
    SELECT layout FROM cards WHERE keywords = ?
    """
    cursor.execute(query, (keyword,))
    result = cursor.fetchall()
    return {"layout": result}

# Endpoint to get count of cards with a given original type and subtypes
@app.get("/v1/bird/card_games/original_type_subtypes_count", summary="Get count of cards with a given original type and subtypes")
async def get_original_type_subtypes_count(originalType: str = Query(..., description="Original type of the card"), subtypes: str = Query(..., description="Subtypes of the card")):
    query = """
    SELECT COUNT(id) FROM cards WHERE originalType = ? AND subtypes != ?
    """
    cursor.execute(query, (originalType, subtypes))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get ids of cards with non-null cardKingdomId and cardKingdomFoilId
@app.get("/v1/bird/card_games/cards_with_kingdom_ids", summary="Get ids of cards with non-null cardKingdomId and cardKingdomFoilId")
async def get_cards_with_kingdom_ids():
    query = """
    SELECT id FROM cards WHERE cardKingdomId IS NOT NULL AND cardKingdomFoilId IS NOT NULL
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"ids": result}

# Endpoint to get ids of cards with a given duel deck
@app.get("/v1/bird/card_games/cards_by_duel_deck", summary="Get ids of cards with a given duel deck")
async def get_cards_by_duel_deck(duelDeck: str = Query(..., description="Duel deck of the card")):
    query = """
    SELECT id FROM cards WHERE duelDeck = ?
    """
    cursor.execute(query, (duelDeck,))
    result = cursor.fetchall()
    return {"ids": result}

# Endpoint to get edhrecRank for a given frameVersion
@app.get("/v1/bird/card_games/edhrecRank", summary="Get edhrecRank for a given frameVersion")
async def get_edhrec_rank(frame_version: int = Query(..., description="Frame version of the card")):
    query = "SELECT edhrecRank FROM cards WHERE frameVersion = ?"
    cursor.execute(query, (frame_version,))
    result = cursor.fetchall()
    return result

# Endpoint to get artist for a given language
@app.get("/v1/bird/card_games/artist", summary="Get artist for a given language")
async def get_artist_for_language(language: str = Query(..., description="Language of the card")):
    query = "SELECT T1.artist FROM cards AS T1 INNER JOIN foreign_data AS T2 ON T1.uuid = T2.uuid WHERE T2.language = ?"
    cursor.execute(query, (language,))
    result = cursor.fetchall()
    return result

# Endpoint to get name for a given availability and language
@app.get("/v1/bird/card_games/name", summary="Get name for a given availability and language")
async def get_name_for_availability_and_language(availability: str = Query(..., description="Availability of the card"), language: str = Query(..., description="Language of the card")):
    query = "SELECT T1.name FROM cards AS T1 INNER JOIN foreign_data AS T2 ON T1.uuid = T2.uuid WHERE T1.availability = ? AND T2.language = ?"
    cursor.execute(query, (availability, language))
    result = cursor.fetchall()
    return result

# Endpoint to get count of banned cards with a given borderColor
@app.get("/v1/bird/card_games/banned_count", summary="Get count of banned cards with a given borderColor")
async def get_banned_count(border_color: str = Query(..., description="Border color of the card")):
    query = "SELECT COUNT(T1.id) FROM cards AS T1 INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid WHERE T2.status = 'Banned' AND T1.borderColor = ?"
    cursor.execute(query, (border_color,))
    result = cursor.fetchall()
    return result

# Endpoint to get uuid and language for a given format
@app.get("/v1/bird/card_games/uuid_language", summary="Get uuid and language for a given format")
async def get_uuid_language(format: str = Query(..., description="Format of the card")):
    query = "SELECT T1.uuid, T3.language FROM cards AS T1 INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid INNER JOIN foreign_data AS T3 ON T1.uuid = T3.uuid WHERE T2.format = ?"
    cursor.execute(query, (format,))
    result = cursor.fetchall()
    return result

# Endpoint to get text for a given card name
@app.get("/v1/bird/card_games/text", summary="Get text for a given card name")
async def get_text_for_card_name(card_name: str = Query(..., description="Name of the card")):
    query = "SELECT T2.text FROM cards AS T1 INNER JOIN rulings AS T2 ON T1.uuid = T2.uuid WHERE T1.name = ?"
    cursor.execute(query, (card_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of cards with a given frameVersion
@app.get("/v1/bird/card_games/future_count", summary="Get count of cards with a given frameVersion")
async def get_future_count(frame_version: str = Query(..., description="Frame version of the card")):
    query = "SELECT COUNT(T1.id) FROM cards AS T1 INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid WHERE T1.frameVersion = ?"
    cursor.execute(query, (frame_version,))
    result = cursor.fetchall()
    return result

# Endpoint to get id and colors for a given setCode
@app.get("/v1/bird/card_games/id_colors", summary="Get id and colors for a given setCode")
async def get_id_colors(set_code: str = Query(..., description="Set code of the card")):
    query = "SELECT id, colors FROM cards WHERE id IN ( SELECT id FROM set_translations WHERE setCode = ? )"
    cursor.execute(query, (set_code,))
    result = cursor.fetchall()
    return result

# Endpoint to get id and language for a given convertedManaCost and setCode
@app.get("/v1/bird/card_games/id_language", summary="Get id and language for a given convertedManaCost and setCode")
async def get_id_language(converted_mana_cost: int = Query(..., description="Converted mana cost of the card"), set_code: str = Query(..., description="Set code of the card")):
    query = "SELECT id, language FROM set_translations WHERE id = ( SELECT id FROM cards WHERE convertedManaCost = ? ) AND setCode = ?"
    cursor.execute(query, (converted_mana_cost, set_code))
    result = cursor.fetchall()
    return result

# Endpoint to get id and date for a given originalType
@app.get("/v1/bird/card_games/id_date", summary="Get id and date for a given originalType")
async def get_id_date(original_type: str = Query(..., description="Original type of the card")):
    query = "SELECT T1.id, T2.date FROM cards AS T1 INNER JOIN rulings AS T2 ON T1.uuid = T2.uuid WHERE T1.originalType = ?"
    cursor.execute(query, (original_type,))
    result = cursor.fetchall()
    return result

# Endpoint to get colors and format for a given id range
@app.get("/v1/bird/card_games/colors_format", summary="Get colors and format for a given id range")
async def get_colors_format(start_id: int = Query(..., description="Start id of the range"), end_id: int = Query(..., description="End id of the range")):
    query = "SELECT T1.colors, T2.format FROM cards AS T1 INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid WHERE T1.id BETWEEN ? AND ?"
    cursor.execute(query, (start_id, end_id))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct names for a given originalType and colors
@app.get("/v1/bird/card_games/distinct_names", summary="Get distinct names for a given originalType and colors")
async def get_distinct_names(original_type: str = Query(..., description="Original type of the card"), colors: str = Query(..., description="Colors of the card")):
    query = "SELECT DISTINCT T1.name FROM cards AS T1 INNER JOIN foreign_data AS T2 ON T1.uuid = T2.uuid WHERE T1.originalType = ? AND T1.colors = ?"
    cursor.execute(query, (original_type, colors))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct names for a given rarity ordered by date
@app.get("/v1/bird/card_games/distinct_names_ordered", summary="Get distinct names for a given rarity ordered by date")
async def get_distinct_names_ordered(rarity: str = Query(..., description="Rarity of the card")):
    query = "SELECT DISTINCT T1.name FROM cards AS T1 INNER JOIN rulings AS T2 ON T1.uuid = T2.uuid WHERE T1.rarity = ? ORDER BY T2.date ASC LIMIT 3"
    cursor.execute(query, (rarity,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of cards with null cardKingdomId or cardKingdomFoilId and a given artist
@app.get("/v1/bird/card_games/null_count", summary="Get count of cards with null cardKingdomId or cardKingdomFoilId and a given artist")
async def get_null_count(artist: str = Query(..., description="Artist of the card")):
    query = "SELECT COUNT(id) FROM cards WHERE (cardKingdomId IS NULL OR cardKingdomFoilId IS NULL) AND artist = ?"
    cursor.execute(query, (artist,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of cards with a given borderColor and non-null cardKingdomId and cardKingdomFoilId
@app.get("/v1/bird/card_games/non_null_count", summary="Get count of cards with a given borderColor and non-null cardKingdomId and cardKingdomFoilId")
async def get_non_null_count(border_color: str = Query(..., description="Border color of the card")):
    query = "SELECT COUNT(id) FROM cards WHERE borderColor = ? AND cardKingdomId IS NOT NULL AND cardKingdomFoilId IS NOT NULL"
    cursor.execute(query, (border_color,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of cards with a given hand, artist, and availability
@app.get("/v1/bird/card_games/hand_count", summary="Get count of cards with a given hand, artist, and availability")
async def get_hand_count(hand: str = Query(..., description="Hand of the card"), artist: str = Query(..., description="Artist of the card"), availability: str = Query(..., description="Availability of the card")):
    query = "SELECT COUNT(id) FROM cards WHERE hAND = ? AND artist = ? AND Availability = ?"
    cursor.execute(query, (hand, artist, availability))
    result = cursor.fetchall()
    return result

# Endpoint to get count of cards with a given frameVersion, availability, and hasContentWarning
@app.get("/v1/bird/card_games/content_warning_count", summary="Get count of cards with a given frameVersion, availability, and hasContentWarning")
async def get_content_warning_count(frame_version: int = Query(..., description="Frame version of the card"), availability: str = Query(..., description="Availability of the card"), has_content_warning: int = Query(..., description="Has content warning of the card")):
    query = "SELECT COUNT(id) FROM cards WHERE frameVersion = ? AND availability = ? AND hasContentWarning = ?"
    cursor.execute(query, (frame_version, availability, has_content_warning))
    result = cursor.fetchall()
    return result

# Endpoint to get manaCost for a given availability, borderColor, frameVersion, and layout
@app.get("/v1/bird/card_games/mana_cost", summary="Get manaCost for a given availability, borderColor, frameVersion, and layout")
async def get_mana_cost(availability: str = Query(..., description="Availability of the card"), border_color: str = Query(..., description="Border color of the card"), frame_version: int = Query(..., description="Frame version of the card"), layout: str = Query(..., description="Layout of the card")):
    query = "SELECT manaCost FROM cards WHERE availability = ? AND borderColor = ? AND frameVersion = ? AND layout = ?"
    cursor.execute(query, (availability, border_color, frame_version, layout))
    result = cursor.fetchall()
    return result

# Endpoint to get manaCost for a given artist
@app.get("/v1/bird/card_games/mana_cost_artist", summary="Get manaCost for a given artist")
async def get_mana_cost_artist(artist: str = Query(..., description="Artist of the card")):
    query = "SELECT manaCost FROM cards WHERE artist = ?"
    cursor.execute(query, (artist,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct subtypes and supertypes for a given availability
@app.get("/v1/bird/card_games/subtypes_supertypes", summary="Get distinct subtypes and supertypes for a given availability")
async def get_subtypes_supertypes(availability: str = Query(..., description="Availability of the card")):
    query = "SELECT DISTINCT subtypes, supertypes FROM cards WHERE availability = ? AND subtypes IS NOT NULL AND supertypes IS NOT NULL"
    cursor.execute(query, (availability,))
    result = cursor.fetchall()
    return result
# Endpoint to get setCode for a given language
@app.get("/v1/bird/card_games/set_translations", summary="Get setCode for a given language")
async def get_set_code(language: str = Query(..., description="Language of the set translation")):
    query = "SELECT setCode FROM set_translations WHERE language = ?"
    cursor.execute(query, (language,))
    result = cursor.fetchall()
    return {"setCode": result}

# Endpoint to get percentage of online-only legendary cards
@app.get("/v1/bird/card_games/cards/legendary_online_only", summary="Get percentage of online-only legendary cards")
async def get_legendary_online_only():
    query = "SELECT SUM(CASE WHEN isOnlineOnly = 1 THEN 1.0 ELSE 0 END) / COUNT(id) * 100 FROM cards WHERE frameEffects = 'legendary'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get percentage of non-textless story spotlight cards
@app.get("/v1/bird/card_games/cards/story_spotlight_non_textless", summary="Get percentage of non-textless story spotlight cards")
async def get_story_spotlight_non_textless():
    query = "SELECT CAST(SUM(CASE WHEN isTextless = 0 THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(id) FROM cards WHERE isStorySpotlight = 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get percentage of Spanish foreign data and names
@app.get("/v1/bird/card_games/foreign_data/spanish_percentage", summary="Get percentage of Spanish foreign data and names")
async def get_spanish_foreign_data_percentage():
    query = """
    SELECT
        (SELECT CAST(SUM(CASE WHEN language = 'Spanish' THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(*) FROM foreign_data),
        name
    FROM foreign_data
    WHERE language = 'Spanish'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"percentage": result}

# Endpoint to get language of sets with a given baseSetSize
@app.get("/v1/bird/card_games/sets/language_by_base_set_size", summary="Get language of sets with a given baseSetSize")
async def get_language_by_base_set_size(base_set_size: int = Query(..., description="Base set size of the set")):
    query = """
    SELECT T2.language
    FROM sets AS T1
    INNER JOIN set_translations AS T2 ON T1.code = T2.setCode
    WHERE T1.baseSetSize = ?
    """
    cursor.execute(query, (base_set_size,))
    result = cursor.fetchall()
    return {"language": result}

# Endpoint to get count of sets with a given language and block
@app.get("/v1/bird/card_games/sets/count_by_language_and_block", summary="Get count of sets with a given language and block")
async def get_count_by_language_and_block(language: str = Query(..., description="Language of the set translation"), block: str = Query(..., description="Block of the set")):
    query = """
    SELECT COUNT(T1.id)
    FROM sets AS T1
    INNER JOIN set_translations AS T2 ON T1.code = T2.setCode
    WHERE T2.language = ? AND T1.block = ?
    """
    cursor.execute(query, (language, block))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get IDs of legal cards with a given status and type
@app.get("/v1/bird/card_games/cards/legal_ids", summary="Get IDs of legal cards with a given status and type")
async def get_legal_ids(status: str = Query(..., description="Legal status of the card"), types: str = Query(..., description="Type of the card")):
    query = """
    SELECT T1.id
    FROM cards AS T1
    INNER JOIN rulings AS T2 ON T1.uuid = T2.uuid
    INNER JOIN legalities AS T3 ON T1.uuid = T3.uuid
    WHERE T3.status = ? AND T1.types = ?
    """
    cursor.execute(query, (status, types))
    result = cursor.fetchall()
    return {"ids": result}

# Endpoint to get subtypes and supertypes of cards with a given language
@app.get("/v1/bird/card_games/cards/subtypes_supertypes", summary="Get subtypes and supertypes of cards with a given language")
async def get_subtypes_supertypes(language: str = Query(..., description="Language of the foreign data")):
    query = """
    SELECT T1.subtypes, T1.supertypes
    FROM cards AS T1
    INNER JOIN foreign_data AS T2 ON T1.uuid = T2.uuid
    WHERE T2.language = ? AND T1.subtypes IS NOT NULL AND T1.supertypes IS NOT NULL
    """
    cursor.execute(query, (language,))
    result = cursor.fetchall()
    return {"subtypes_supertypes": result}

# Endpoint to get text of rulings with a given power and text pattern
@app.get("/v1/bird/card_games/cards/rulings_text", summary="Get text of rulings with a given power and text pattern")
async def get_rulings_text(power: str = Query(..., description="Power of the card"), text_pattern: str = Query(..., description="Pattern in the ruling text")):
    query = """
    SELECT T2.text
    FROM cards AS T1
    INNER JOIN rulings AS T2 ON T1.uuid = T2.uuid
    WHERE (T1.power IS NULL OR T1.power = ?) AND T2.text LIKE ?
    """
    cursor.execute(query, (power, f'%{text_pattern}%'))
    result = cursor.fetchall()
    return {"text": result}

# Endpoint to get count of cards with a given format, text, and side
@app.get("/v1/bird/card_games/cards/count_by_format_text_side", summary="Get count of cards with a given format, text, and side")
async def get_count_by_format_text_side(format: str = Query(..., description="Format of the card"), text: str = Query(..., description="Text of the ruling"), side: str = Query(..., description="Side of the card")):
    query = """
    SELECT COUNT(T1.id)
    FROM cards AS T1
    INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid
    INNER JOIN rulings AS T3 ON T1.uuid = T3.uuid
    WHERE T2.format = ? AND T3.text = ? AND T1.Side = ?
    """
    cursor.execute(query, (format, text, side))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get IDs of cards with a given artist, format, and availability
@app.get("/v1/bird/card_games/cards/ids_by_artist_format_availability", summary="Get IDs of cards with a given artist, format, and availability")
async def get_ids_by_artist_format_availability(artist: str = Query(..., description="Artist of the card"), format: str = Query(..., description="Format of the card"), availability: str = Query(..., description="Availability of the card")):
    query = """
    SELECT T1.id
    FROM cards AS T1
    INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid
    WHERE T1.artist = ? AND T2.format = ? AND T1.availability = ?
    """
    cursor.execute(query, (artist, format, availability))
    result = cursor.fetchall()
    return {"ids": result}

# Endpoint to get distinct artists of cards with a given flavor text pattern
@app.get("/v1/bird/card_games/cards/distinct_artists", summary="Get distinct artists of cards with a given flavor text pattern")
async def get_distinct_artists(flavor_text_pattern: str = Query(..., description="Pattern in the flavor text")):
    query = """
    SELECT DISTINCT T1.artist
    FROM cards AS T1
    INNER JOIN foreign_data AS T2 ON T1.uuid = T2.uuid
    WHERE T2.flavorText LIKE ?
    """
    cursor.execute(query, (f'%{flavor_text_pattern}%',))
    result = cursor.fetchall()
    return {"artists": result}

# Endpoint to get names of foreign data with a given type, layout, border color, and artist
@app.get("/v1/bird/card_games/foreign_data/names_by_type_layout_border_artist", summary="Get names of foreign data with a given type, layout, border color, and artist")
async def get_names_by_type_layout_border_artist(types: str = Query(..., description="Type of the card"), layout: str = Query(..., description="Layout of the card"), border_color: str = Query(..., description="Border color of the card"), artist: str = Query(..., description="Artist of the card"), language: str = Query(..., description="Language of the foreign data")):
    query = """
    SELECT name
    FROM foreign_data
    WHERE uuid IN (
        SELECT uuid
        FROM cards
        WHERE types = ? AND layout = ? AND borderColor = ? AND artist = ?
    ) AND language = ?
    """
    cursor.execute(query, (types, layout, border_color, artist, language))
    result = cursor.fetchall()
    return {"names": result}

# Endpoint to get count of distinct cards with a given rarity and ruling date
@app.get("/v1/bird/card_games/cards/count_distinct_by_rarity_date", summary="Get count of distinct cards with a given rarity and ruling date")
async def get_count_distinct_by_rarity_date(rarity: str = Query(..., description="Rarity of the card"), date: str = Query(..., description="Date of the ruling")):
    query = """
    SELECT COUNT(DISTINCT T1.id)
    FROM cards AS T1
    INNER JOIN rulings AS T2 ON T1.uuid = T2.uuid
    WHERE T1.rarity = ? AND T2.date = ?
    """
    cursor.execute(query, (rarity, date))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get languages of sets with a given block and base set size
@app.get("/v1/bird/card_games/sets/languages_by_block_base_set_size", summary="Get languages of sets with a given block and base set size")
async def get_languages_by_block_base_set_size(block: str = Query(..., description="Block of the set"), base_set_size: int = Query(..., description="Base set size of the set")):
    query = """
    SELECT T2.language
    FROM sets AS T1
    INNER JOIN set_translations AS T2 ON T1.code = T2.setCode
    WHERE T1.block = ? AND T1.baseSetSize = ?
    """
    cursor.execute(query, (block, base_set_size))
    result = cursor.fetchall()
    return {"languages": result}

# Endpoint to get percentage of cards without content warning in a given format and status
@app.get("/v1/bird/card_games/cards/percentage_no_content_warning", summary="Get percentage of cards without content warning in a given format and status")
async def get_percentage_no_content_warning(format: str = Query(..., description="Format of the card"), status: str = Query(..., description="Status of the card")):
    query = """
    SELECT CAST(SUM(CASE WHEN T1.hasContentWarning = 0 THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(T1.id)
    FROM cards AS T1
    INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid
    WHERE T2.format = ? AND T2.status = ?
    """
    cursor.execute(query, (format, status))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get percentage of cards with a given language and power
@app.get("/v1/bird/card_games/cards/percentage_by_language_power", summary="Get percentage of cards with a given language and power")
async def get_percentage_by_language_power(language: str = Query(..., description="Language of the foreign data"), power: str = Query(..., description="Power of the card")):
    query = """
    SELECT CAST(SUM(CASE WHEN T2.language = ? THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(T1.id)
    FROM cards AS T1
    INNER JOIN foreign_data AS T2 ON T1.uuid = T2.uuid
    WHERE T1.power IS NULL OR T1.power = ?
    """
    cursor.execute(query, (language, power))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get percentage of sets with a given language and type
@app.get("/v1/bird/card_games/sets/percentage_by_language_type", summary="Get percentage of sets with a given language and type")
async def get_percentage_by_language_type(language: str = Query(..., description="Language of the set translation"), type: str = Query(..., description="Type of the set")):
    query = """
    SELECT CAST(SUM(CASE WHEN T2.language = ? THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(T1.id)
    FROM sets AS T1
    INNER JOIN set_translations AS T2 ON T1.code = T2.setCode
    WHERE T1.type = ?
    """
    cursor.execute(query, (language, type))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get distinct availabilities of cards with a given artist
@app.get("/v1/bird/card_games/cards/distinct_availabilities", summary="Get distinct availabilities of cards with a given artist")
async def get_distinct_availabilities(artist: str = Query(..., description="Artist of the card")):
    query = "SELECT DISTINCT availability FROM cards WHERE artist = ?"
    cursor.execute(query, (artist,))
    result = cursor.fetchall()
    return {"availabilities": result}

# Endpoint to get count of cards with a given edhrecRank and border color
@app.get("/v1/bird/card_games/cards/count_by_edhrec_rank_border_color", summary="Get count of cards with a given edhrecRank and border color")
async def get_count_by_edhrec_rank_border_color(edhrec_rank: int = Query(..., description="EDHREC rank of the card"), border_color: str = Query(..., description="Border color of the card")):
    query = "SELECT COUNT(id) FROM cards WHERE edhrecRank > ? AND borderColor = ?"
    cursor.execute(query, (edhrec_rank, border_color))
    result = cursor.fetchone()
    return {"count": result[0]}
# Endpoint to get count of oversized, reprint, and promo cards
@app.get("/v1/bird/card_games/count_oversized_reprint_promo", summary="Get count of oversized, reprint, and promo cards")
async def get_count_oversized_reprint_promo(isOversized: int = Query(..., description="Is the card oversized"),
                                            isReprint: int = Query(..., description="Is the card a reprint"),
                                            isPromo: int = Query(..., description="Is the card a promo")):
    query = f"SELECT COUNT(id) FROM cards WHERE isOversized = {isOversized} AND isReprint = {isReprint} AND isPromo = {isPromo}"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get card names with specific power and promo type
@app.get("/v1/bird/card_games/card_names_by_power_promo", summary="Get card names with specific power and promo type")
async def get_card_names_by_power_promo(power: str = Query(..., description="Power of the card"),
                                        promoTypes: str = Query(..., description="Promo type of the card"),
                                        limit: int = Query(3, description="Limit the number of results")):
    query = f"SELECT name FROM cards WHERE (power IS NULL OR power LIKE '%{power}%') AND promoTypes = '{promoTypes}' ORDER BY name LIMIT {limit}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"card_names": [row[0] for row in result]}

# Endpoint to get language of a card by multiverseid
@app.get("/v1/bird/card_games/language_by_multiverseid", summary="Get language of a card by multiverseid")
async def get_language_by_multiverseid(multiverseid: int = Query(..., description="Multiverse ID of the card")):
    query = f"SELECT language FROM foreign_data WHERE multiverseid = {multiverseid}"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"language": result[0]}

# Endpoint to get cardKingdomFoilId and cardKingdomId of cards
@app.get("/v1/bird/card_games/card_kingdom_ids", summary="Get cardKingdomFoilId and cardKingdomId of cards")
async def get_card_kingdom_ids(limit: int = Query(3, description="Limit the number of results")):
    query = f"SELECT cardKingdomFoilId, cardKingdomId FROM cards WHERE cardKingdomFoilId IS NOT NULL AND cardKingdomId IS NOT NULL ORDER BY cardKingdomFoilId LIMIT {limit}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"card_kingdom_ids": result}

# Endpoint to get percentage of textless normal layout cards
@app.get("/v1/bird/card_games/percentage_textless_normal_layout", summary="Get percentage of textless normal layout cards")
async def get_percentage_textless_normal_layout():
    query = "SELECT CAST(SUM(CASE WHEN isTextless = 1 AND layout = 'normal' THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(*) FROM cards"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get card IDs by subtypes and side
@app.get("/v1/bird/card_games/card_ids_by_subtypes_side", summary="Get card IDs by subtypes and side")
async def get_card_ids_by_subtypes_side(subtypes: str = Query(..., description="Subtypes of the card"),
                                        side: str = Query(None, description="Side of the card")):
    query = f"SELECT id FROM cards WHERE subtypes = '{subtypes}' AND side IS {side}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"card_ids": [row[0] for row in result]}

# Endpoint to get set names with null mtgoCode
@app.get("/v1/bird/card_games/set_names_with_null_mtgoCode", summary="Get set names with null mtgoCode")
async def get_set_names_with_null_mtgoCode(limit: int = Query(3, description="Limit the number of results")):
    query = f"SELECT name FROM sets WHERE mtgoCode IS NULL ORDER BY name LIMIT {limit}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"set_names": [row[0] for row in result]}

# Endpoint to get language of a set by mcmName and setCode
@app.get("/v1/bird/card_games/language_by_mcmName_setCode", summary="Get language of a set by mcmName and setCode")
async def get_language_by_mcmName_setCode(mcmName: str = Query(..., description="MCM name of the set"),
                                          setCode: str = Query(..., description="Set code of the set")):
    query = f"SELECT T2.language FROM sets AS T1 INNER JOIN set_translations AS T2 ON T1.code = T2.setCode WHERE T1.mcmName = '{mcmName}' AND T2.setCode = '{setCode}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"language": result[0]}

# Endpoint to get set name and translation by translation ID
@app.get("/v1/bird/card_games/set_name_translation_by_id", summary="Get set name and translation by translation ID")
async def get_set_name_translation_by_id(translation_id: int = Query(..., description="Translation ID")):
    query = f"SELECT T1.name, T2.translation FROM sets AS T1 INNER JOIN set_translations AS T2 ON T1.code = T2.setCode WHERE T2.id = {translation_id} GROUP BY T1.name, T2.translation"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"set_name_translation": result}

# Endpoint to get language and type of a set by translation ID
@app.get("/v1/bird/card_games/language_type_by_translation_id", summary="Get language and type of a set by translation ID")
async def get_language_type_by_translation_id(translation_id: int = Query(..., description="Translation ID")):
    query = f"SELECT T2.language, T1.type FROM sets AS T1 INNER JOIN set_translations AS T2 ON T1.code = T2.setCode WHERE T2.id = {translation_id}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"language_type": result}

# Endpoint to get set name and ID by block and language
@app.get("/v1/bird/card_games/set_name_id_by_block_language", summary="Get set name and ID by block and language")
async def get_set_name_id_by_block_language(block: str = Query(..., description="Block of the set"),
                                            language: str = Query(..., description="Language of the set"),
                                            limit: int = Query(2, description="Limit the number of results")):
    query = f"SELECT T1.name, T1.id FROM sets AS T1 INNER JOIN set_translations AS T2 ON T1.code = T2.setCode WHERE T1.block = '{block}' AND T2.language = '{language}' ORDER BY T1.id LIMIT {limit}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"set_name_id": result}

# Endpoint to get set name and ID by language, isFoilOnly, and isForeignOnly
@app.get("/v1/bird/card_games/set_name_id_by_language_foil_foreign", summary="Get set name and ID by language, isFoilOnly, and isForeignOnly")
async def get_set_name_id_by_language_foil_foreign(language: str = Query(..., description="Language of the set"),
                                                   isFoilOnly: int = Query(..., description="Is the set foil only"),
                                                   isForeignOnly: int = Query(..., description="Is the set foreign only")):
    query = f"SELECT T1.name, T1.id FROM sets AS T1 INNER JOIN set_translations AS T2 ON T1.code = T2.setCode WHERE T2.language = '{language}' AND T1.isFoilOnly = {isFoilOnly} AND T1.isForeignOnly = {isForeignOnly}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"set_name_id": result}

# Endpoint to get set ID by language grouped by baseSetSize
@app.get("/v1/bird/card_games/set_id_by_language_grouped_baseSetSize", summary="Get set ID by language grouped by baseSetSize")
async def get_set_id_by_language_grouped_baseSetSize(language: str = Query(..., description="Language of the set"),
                                                     limit: int = Query(1, description="Limit the number of results")):
    query = f"SELECT T1.id FROM sets AS T1 INNER JOIN set_translations AS T2 ON T1.code = T2.setCode WHERE T2.language = '{language}' GROUP BY T1.baseSetSize ORDER BY T1.baseSetSize DESC LIMIT {limit}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"set_id": [row[0] for row in result]}

# Endpoint to get percentage of sets with specific language and isOnlineOnly
@app.get("/v1/bird/card_games/percentage_sets_by_language_onlineOnly", summary="Get percentage of sets with specific language and isOnlineOnly")
async def get_percentage_sets_by_language_onlineOnly(language: str = Query(..., description="Language of the set"),
                                                     isOnlineOnly: int = Query(..., description="Is the set online only")):
    query = f"SELECT CAST(SUM(CASE WHEN T2.language = '{language}' AND T1.isOnlineOnly = {isOnlineOnly} THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(*) FROM sets AS T1 INNER JOIN set_translations AS T2 ON T1.code = T2.setCode"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get count of sets by language and mtgoCode
@app.get("/v1/bird/card_games/count_sets_by_language_mtgoCode", summary="Get count of sets by language and mtgoCode")
async def get_count_sets_by_language_mtgoCode(language: str = Query(..., description="Language of the set"),
                                              mtgoCode: str = Query(None, description="MTGO code of the set")):
    query = f"SELECT COUNT(T1.id) FROM sets AS T1 INNER JOIN set_translations AS T2 ON T2.setCode = T1.code WHERE T2.language = '{language}' AND (T1.mtgoCode IS NULL OR T1.mtgoCode = '{mtgoCode}')"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get card IDs by borderColor
@app.get("/v1/bird/card_games/card_ids_by_borderColor", summary="Get card IDs by borderColor")
async def get_card_ids_by_borderColor(borderColor: str = Query(..., description="Border color of the card")):
    query = f"SELECT id FROM cards WHERE borderColor = '{borderColor}' GROUP BY id"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"card_ids": [row[0] for row in result]}

# Endpoint to get card IDs by frameEffects
@app.get("/v1/bird/card_games/card_ids_by_frameEffects", summary="Get card IDs by frameEffects")
async def get_card_ids_by_frameEffects(frameEffects: str = Query(..., description="Frame effects of the card")):
    query = f"SELECT id FROM cards WHERE frameEffects = '{frameEffects}' GROUP BY id"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"card_ids": [row[0] for row in result]}

# Endpoint to get card IDs by borderColor and isFullArt
@app.get("/v1/bird/card_games/card_ids_by_borderColor_isFullArt", summary="Get card IDs by borderColor and isFullArt")
async def get_card_ids_by_borderColor_isFullArt(borderColor: str = Query(..., description="Border color of the card"),
                                                isFullArt: int = Query(..., description="Is the card full art")):
    query = f"SELECT id FROM cards WHERE borderColor = '{borderColor}' AND isFullArt = {isFullArt}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"card_ids": [row[0] for row in result]}

# Endpoint to get language of a set translation by ID
@app.get("/v1/bird/card_games/language_by_set_translation_id", summary="Get language of a set translation by ID")
async def get_language_by_set_translation_id(translation_id: int = Query(..., description="Translation ID")):
    query = f"SELECT language FROM set_translations WHERE id = {translation_id}"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"language": result[0]}

# Endpoint to get set name by code
@app.get("/v1/bird/card_games/set_name_by_code", summary="Get set name by code")
async def get_set_name_by_code(code: str = Query(..., description="Code of the set")):
    query = f"SELECT name FROM sets WHERE code = '{code}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"set_name": result[0]}
# Endpoint to get distinct language for a given name
@app.get("/v1/bird/card_games/language", summary="Get distinct language for a given name")
async def get_language_for_name(name: str = Query(..., description="Name of the item")):
    query = "SELECT DISTINCT language FROM foreign_data WHERE name = ?"
    cursor.execute(query, (name,))
    result = cursor.fetchall()
    return result

# Endpoint to get setCode for a given release date
@app.get("/v1/bird/card_games/set_code_by_release_date", summary="Get setCode for a given release date")
async def get_set_code_by_release_date(release_date: str = Query(..., description="Release date of the set")):
    query = "SELECT T2.setCode FROM sets AS T1 INNER JOIN set_translations AS T2 ON T2.setCode = T1.code WHERE T1.releaseDate = ?"
    cursor.execute(query, (release_date,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct baseSetSize and setCode for given blocks
@app.get("/v1/bird/card_games/base_set_size_and_set_code", summary="Get distinct baseSetSize and setCode for given blocks")
async def get_base_set_size_and_set_code(blocks: str = Query(..., description="Blocks to filter by, comma-separated")):
    blocks_list = blocks.split(',')
    query = "SELECT DISTINCT T1.baseSetSize, T2.setCode FROM sets AS T1 INNER JOIN set_translations AS T2 ON T2.setCode = T1.code WHERE T1.block IN ({})".format(','.join('?' for _ in blocks_list))
    cursor.execute(query, blocks_list)
    result = cursor.fetchall()
    return result

# Endpoint to get setCode for a given type
@app.get("/v1/bird/card_games/set_code_by_type", summary="Get setCode for a given type")
async def get_set_code_by_type(set_type: str = Query(..., description="Type of the set")):
    query = "SELECT T2.setCode FROM sets AS T1 INNER JOIN set_translations AS T2 ON T2.setCode = T1.code WHERE T1.type = ? GROUP BY T2.setCode"
    cursor.execute(query, (set_type,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct name and type for a given watermark
@app.get("/v1/bird/card_games/name_and_type_by_watermark", summary="Get distinct name and type for a given watermark")
async def get_name_and_type_by_watermark(watermark: str = Query(..., description="Watermark to filter by")):
    query = "SELECT DISTINCT T1.name, T1.type FROM cards AS T1 INNER JOIN foreign_data AS T2 ON T2.uuid = T1.uuid WHERE T1.watermark = ?"
    cursor.execute(query, (watermark,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct language and flavorText for a given watermark
@app.get("/v1/bird/card_games/language_and_flavor_text_by_watermark", summary="Get distinct language and flavorText for a given watermark")
async def get_language_and_flavor_text_by_watermark(watermark: str = Query(..., description="Watermark to filter by")):
    query = "SELECT DISTINCT T2.language, T2.flavorText FROM cards AS T1 INNER JOIN foreign_data AS T2 ON T2.uuid = T1.uuid WHERE T1.watermark = ?"
    cursor.execute(query, (watermark,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of cards with convertedManaCost of 10 for a given name
@app.get("/v1/bird/card_games/percentage_of_cards_with_mana_cost", summary="Get percentage of cards with convertedManaCost of 10 for a given name")
async def get_percentage_of_cards_with_mana_cost(name: str = Query(..., description="Name of the card")):
    query = "SELECT CAST(SUM(CASE WHEN T1.convertedManaCost = 10 THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(T1.id), T1.name FROM cards AS T1 INNER JOIN sets AS T2 ON T2.code = T1.setCode WHERE T1.name = ?"
    cursor.execute(query, (name,))
    result = cursor.fetchall()
    return result

# Endpoint to get setCode for a given type
@app.get("/v1/bird/card_games/set_code_by_type_commander", summary="Get setCode for a given type (commander)")
async def get_set_code_by_type_commander(set_type: str = Query(..., description="Type of the set")):
    query = "SELECT T2.setCode FROM sets AS T1 INNER JOIN set_translations AS T2 ON T2.setCode = T1.code WHERE T1.type = ?"
    cursor.execute(query, (set_type,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct name and type for a given watermark
@app.get("/v1/bird/card_games/name_and_type_by_watermark_abzan", summary="Get distinct name and type for a given watermark (abzan)")
async def get_name_and_type_by_watermark_abzan(watermark: str = Query(..., description="Watermark to filter by")):
    query = "SELECT DISTINCT T1.name, T1.type FROM cards AS T1 INNER JOIN foreign_data AS T2 ON T2.uuid = T1.uuid WHERE T1.watermark = ?"
    cursor.execute(query, (watermark,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct language and type for a given watermark
@app.get("/v1/bird/card_games/language_and_type_by_watermark_azorius", summary="Get distinct language and type for a given watermark (azorius)")
async def get_language_and_type_by_watermark_azorius(watermark: str = Query(..., description="Watermark to filter by")):
    query = "SELECT DISTINCT T2.language, T1.type FROM cards AS T1 INNER JOIN foreign_data AS T2 ON T2.uuid = T1.uuid WHERE T1.watermark = ?"
    cursor.execute(query, (watermark,))
    result = cursor.fetchall()
    return result

# Endpoint to get sum of cards with specific artist and non-null cardKingdomFoilId and cardKingdomId
@app.get("/v1/bird/card_games/sum_of_cards_by_artist", summary="Get sum of cards with specific artist and non-null cardKingdomFoilId and cardKingdomId")
async def get_sum_of_cards_by_artist(artist: str = Query(..., description="Artist to filter by")):
    query = "SELECT SUM(CASE WHEN artist = ? AND cardKingdomFoilId IS NOT NULL AND cardKingdomId IS NOT NULL THEN 1 ELSE 0 END) FROM cards"
    cursor.execute(query, (artist,))
    result = cursor.fetchall()
    return result

# Endpoint to get sum of cards with specific availability and hand
@app.get("/v1/bird/card_games/sum_of_cards_by_availability_and_hand", summary="Get sum of cards with specific availability and hand")
async def get_sum_of_cards_by_availability_and_hand(availability: str = Query(..., description="Availability to filter by"), hand: str = Query(..., description="Hand to filter by")):
    query = "SELECT SUM(CASE WHEN availability = ? AND hand = ? THEN 1 ELSE 0 END) FROM cards"
    cursor.execute(query, (availability, hand))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct name for a given isTextless
@app.get("/v1/bird/card_games/distinct_name_by_is_textless", summary="Get distinct name for a given isTextless")
async def get_distinct_name_by_is_textless(is_textless: int = Query(..., description="isTextless to filter by")):
    query = "SELECT DISTINCT name FROM cards WHERE isTextless = ?"
    cursor.execute(query, (is_textless,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct manaCost for a given name
@app.get("/v1/bird/card_games/distinct_mana_cost_by_name", summary="Get distinct manaCost for a given name")
async def get_distinct_mana_cost_by_name(name: str = Query(..., description="Name to filter by")):
    query = "SELECT DISTINCT manaCost FROM cards WHERE name = ?"
    cursor.execute(query, (name,))
    result = cursor.fetchall()
    return result

# Endpoint to get sum of cards with specific power and borderColor
@app.get("/v1/bird/card_games/sum_of_cards_by_power_and_border_color", summary="Get sum of cards with specific power and borderColor")
async def get_sum_of_cards_by_power_and_border_color(border_color: str = Query(..., description="Border color to filter by")):
    query = "SELECT SUM(CASE WHEN power LIKE '%*%' OR power IS NULL THEN 1 ELSE 0 END) FROM cards WHERE borderColor = ?"
    cursor.execute(query, (border_color,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct name for a given isPromo and side
@app.get("/v1/bird/card_games/distinct_name_by_is_promo_and_side", summary="Get distinct name for a given isPromo and side")
async def get_distinct_name_by_is_promo_and_side(is_promo: int = Query(..., description="isPromo to filter by"), side: str = Query(..., description="Side to filter by")):
    query = "SELECT DISTINCT name FROM cards WHERE isPromo = ? AND side IS NOT NULL"
    cursor.execute(query, (is_promo,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct subtypes and supertypes for a given name
@app.get("/v1/bird/card_games/distinct_subtypes_and_supertypes_by_name", summary="Get distinct subtypes and supertypes for a given name")
async def get_distinct_subtypes_and_supertypes_by_name(name: str = Query(..., description="Name to filter by")):
    query = "SELECT DISTINCT subtypes, supertypes FROM cards WHERE name = ?"
    cursor.execute(query, (name,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct purchaseUrls for a given promoTypes
@app.get("/v1/bird/card_games/distinct_purchase_urls_by_promo_types", summary="Get distinct purchaseUrls for a given promoTypes")
async def get_distinct_purchase_urls_by_promo_types(promo_types: str = Query(..., description="Promo types to filter by")):
    query = "SELECT DISTINCT purchaseUrls FROM cards WHERE promoTypes = ?"
    cursor.execute(query, (promo_types,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of cards with specific availability and borderColor
@app.get("/v1/bird/card_games/count_of_cards_by_availability_and_border_color", summary="Get count of cards with specific availability and borderColor")
async def get_count_of_cards_by_availability_and_border_color(availability: str = Query(..., description="Availability to filter by"), border_color: str = Query(..., description="Border color to filter by")):
    query = "SELECT COUNT(CASE WHEN availability LIKE ? AND borderColor = ? THEN 1 ELSE NULL END) FROM cards"
    cursor.execute(query, (availability, border_color))
    result = cursor.fetchall()
    return result

# Endpoint to get name for a given list of names ordered by convertedManaCost
@app.get("/v1/bird/card_games/name_by_names_ordered_by_converted_mana_cost", summary="Get name for a given list of names ordered by convertedManaCost")
async def get_name_by_names_ordered_by_converted_mana_cost(names: str = Query(..., description="Names to filter by, comma-separated")):
    names_list = names.split(',')
    query = "SELECT name FROM cards WHERE name IN ({}) ORDER BY convertedManaCost DESC LIMIT 1".format(','.join('?' for _ in names_list))
    cursor.execute(query, names_list)
    result = cursor.fetchall()
    return result

# Endpoint to get artist for a given flavor name
@app.get("/v1/bird/card_games/artist", summary="Get artist for a given flavor name")
async def get_artist_for_flavor_name(flavor_name: str = Query(..., description="Flavor name of the card")):
    query = "SELECT artist FROM cards WHERE flavorName = ?"
    cursor.execute(query, (flavor_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get names of cards with a specific frame version
@app.get("/v1/bird/card_games/card_names", summary="Get names of cards with a specific frame version")
async def get_card_names(frame_version: int = Query(..., description="Frame version of the card"), limit: int = Query(3, description="Limit the number of results")):
    query = "SELECT name FROM cards WHERE frameVersion = ? ORDER BY convertedManaCost DESC LIMIT ?"
    cursor.execute(query, (frame_version, limit))
    result = cursor.fetchall()
    return result

# Endpoint to get translation for a given set code and language
@app.get("/v1/bird/card_games/set_translation", summary="Get translation for a given set code and language")
async def get_set_translation(set_code: str = Query(..., description="Set code of the card"), language: str = Query(..., description="Language of the translation")):
    query = "SELECT translation FROM set_translations WHERE setCode IN (SELECT setCode FROM cards WHERE name = ?) AND language = ?"
    cursor.execute(query, (set_code, language))
    result = cursor.fetchall()
    return result

# Endpoint to count distinct translations for a given set code
@app.get("/v1/bird/card_games/count_translations", summary="Count distinct translations for a given set code")
async def count_translations(set_code: str = Query(..., description="Set code of the card")):
    query = "SELECT COUNT(DISTINCT translation) FROM set_translations WHERE setCode IN (SELECT setCode FROM cards WHERE name = ?) AND translation IS NOT NULL"
    cursor.execute(query, (set_code,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct card names for a given translation
@app.get("/v1/bird/card_games/distinct_card_names", summary="Get distinct card names for a given translation")
async def get_distinct_card_names(translation: str = Query(..., description="Translation of the set")):
    query = "SELECT DISTINCT T1.name FROM cards AS T1 INNER JOIN set_translations AS T2 ON T2.setCode = T1.setCode WHERE T2.translation = ?"
    cursor.execute(query, (translation,))
    result = cursor.fetchall()
    return result

# Endpoint to check if a card has a Korean translation
@app.get("/v1/bird/card_games/has_korean_translation", summary="Check if a card has a Korean translation")
async def has_korean_translation(card_name: str = Query(..., description="Name of the card")):
    query = "SELECT IIF(SUM(CASE WHEN T2.language = 'Korean' AND T2.translation IS NOT NULL THEN 1 ELSE 0 END) > 0, 'YES', 'NO') FROM cards AS T1 INNER JOIN set_translations AS T2 ON T2.setCode = T1.setCode WHERE T1.name = ?"
    cursor.execute(query, (card_name,))
    result = cursor.fetchall()
    return result

# Endpoint to count cards with a specific translation and artist
@app.get("/v1/bird/card_games/count_cards_with_translation_and_artist", summary="Count cards with a specific translation and artist")
async def count_cards_with_translation_and_artist(translation: str = Query(..., description="Translation of the set"), artist: str = Query(..., description="Artist of the card")):
    query = "SELECT COUNT(T1.id) FROM cards AS T1 INNER JOIN set_translations AS T2 ON T2.setCode = T1.setCode WHERE T2.translation = ? AND T1.artist = ?"
    cursor.execute(query, (translation, artist))
    result = cursor.fetchall()
    return result

# Endpoint to get base set size for a given translation
@app.get("/v1/bird/card_games/base_set_size", summary="Get base set size for a given translation")
async def get_base_set_size(translation: str = Query(..., description="Translation of the set")):
    query = "SELECT T1.baseSetSize FROM sets AS T1 INNER JOIN set_translations AS T2 ON T2.setCode = T1.code WHERE T2.translation = ?"
    cursor.execute(query, (translation,))
    result = cursor.fetchall()
    return result

# Endpoint to get translation for a given set name and language
@app.get("/v1/bird/card_games/set_translation_by_name", summary="Get translation for a given set name and language")
async def get_set_translation_by_name(set_name: str = Query(..., description="Name of the set"), language: str = Query(..., description="Language of the translation")):
    query = "SELECT T2.translation FROM sets AS T1 INNER JOIN set_translations AS T2 ON T2.setCode = T1.code WHERE T1.name = ? AND T2.language = ?"
    cursor.execute(query, (set_name, language))
    result = cursor.fetchall()
    return result

# Endpoint to check if a card has an MTGO code
@app.get("/v1/bird/card_games/has_mtgo_code", summary="Check if a card has an MTGO code")
async def has_mtgo_code(card_name: str = Query(..., description="Name of the card")):
    query = "SELECT IIF(T2.mtgoCode IS NOT NULL, 'YES', 'NO') FROM cards AS T1 INNER JOIN sets AS T2 ON T2.code = T1.setCode WHERE T1.name = ?"
    cursor.execute(query, (card_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct release dates for a given card name
@app.get("/v1/bird/card_games/distinct_release_dates", summary="Get distinct release dates for a given card name")
async def get_distinct_release_dates(card_name: str = Query(..., description="Name of the card")):
    query = "SELECT DISTINCT T2.releaseDate FROM cards AS T1 INNER JOIN sets AS T2 ON T2.code = T1.setCode WHERE T1.name = ?"
    cursor.execute(query, (card_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get set type for a given translation
@app.get("/v1/bird/card_games/set_type", summary="Get set type for a given translation")
async def get_set_type(translation: str = Query(..., description="Translation of the set")):
    query = "SELECT T1.type FROM sets AS T1 INNER JOIN set_translations AS T2 ON T2.setCode = T1.code WHERE T2.translation = ?"
    cursor.execute(query, (translation,))
    result = cursor.fetchall()
    return result

# Endpoint to count distinct sets with a specific block and language
@app.get("/v1/bird/card_games/count_distinct_sets", summary="Count distinct sets with a specific block and language")
async def count_distinct_sets(block: str = Query(..., description="Block of the set"), language: str = Query(..., description="Language of the translation")):
    query = "SELECT COUNT(DISTINCT T1.id) FROM sets AS T1 INNER JOIN set_translations AS T2 ON T2.setCode = T1.code WHERE T1.block = ? AND T2.language = ? AND T2.translation IS NOT NULL"
    cursor.execute(query, (block, language))
    result = cursor.fetchall()
    return result

# Endpoint to check if a card is foreign only
@app.get("/v1/bird/card_games/is_foreign_only", summary="Check if a card is foreign only")
async def is_foreign_only(card_name: str = Query(..., description="Name of the card")):
    query = "SELECT IIF(isForeignOnly = 1, 'YES', 'NO') FROM cards AS T1 INNER JOIN sets AS T2 ON T2.code = T1.setCode WHERE T1.name = ?"
    cursor.execute(query, (card_name,))
    result = cursor.fetchall()
    return result

# Endpoint to count sets with a specific translation, base set size, and language
@app.get("/v1/bird/card_games/count_sets_with_translation", summary="Count sets with a specific translation, base set size, and language")
async def count_sets_with_translation(translation: str = Query(..., description="Translation of the set"), base_set_size: int = Query(..., description="Base set size of the set"), language: str = Query(..., description="Language of the translation")):
    query = "SELECT COUNT(T1.id) FROM sets AS T1 INNER JOIN set_translations AS T2 ON T2.setCode = T1.code WHERE T2.translation IS NOT NULL AND T1.baseSetSize < ? AND T2.language = ?"
    cursor.execute(query, (base_set_size, language))
    result = cursor.fetchall()
    return result

# Endpoint to count cards with a specific border color and set name
@app.get("/v1/bird/card_games/count_cards_with_border_color", summary="Count cards with a specific border color and set name")
async def count_cards_with_border_color(border_color: str = Query(..., description="Border color of the card"), set_name: str = Query(..., description="Name of the set")):
    query = "SELECT SUM(CASE WHEN T1.borderColor = ? THEN 1 ELSE 0 END) FROM cards AS T1 INNER JOIN sets AS T2 ON T2.code = T1.setCode WHERE T2.name = ?"
    cursor.execute(query, (border_color, set_name))
    result = cursor.fetchall()
    return result

# Endpoint to get card name with a specific set name and order by converted mana cost
@app.get("/v1/bird/card_games/card_name_by_set_name", summary="Get card name with a specific set name and order by converted mana cost")
async def get_card_name_by_set_name(set_name: str = Query(..., description="Name of the set"), limit: int = Query(1, description="Limit the number of results")):
    query = "SELECT T1.name FROM cards AS T1 INNER JOIN sets AS T2 ON T2.code = T1.setCode WHERE T2.name = ? ORDER BY T1.convertedManaCost DESC LIMIT ?"
    cursor.execute(query, (set_name, limit))
    result = cursor.fetchall()
    return result

# Endpoint to get artists for a specific set name and artist names
@app.get("/v1/bird/card_games/artists_by_set_name", summary="Get artists for a specific set name and artist names")
async def get_artists_by_set_name(set_name: str = Query(..., description="Name of the set"), artist_names: str = Query(..., description="Comma-separated list of artist names")):
    artist_list = artist_names.split(',')
    placeholders = ','.join(['?'] * len(artist_list))
    query = f"SELECT T1.artist FROM cards AS T1 INNER JOIN sets AS T2 ON T2.code = T1.setCode WHERE T2.name = ? AND T1.artist IN ({placeholders}) GROUP BY T1.artist"
    cursor.execute(query, (set_name, *artist_list))
    result = cursor.fetchall()
    return result

# Endpoint to get card name with a specific set name and number
@app.get("/v1/bird/card_games/card_name_by_set_name_and_number", summary="Get card name with a specific set name and number")
async def get_card_name_by_set_name_and_number(set_name: str = Query(..., description="Name of the set"), number: int = Query(..., description="Number of the card")):
    query = "SELECT T1.name FROM cards AS T1 INNER JOIN sets AS T2 ON T2.code = T1.setCode WHERE T2.name = ? AND T1.number = ?"
    cursor.execute(query, (set_name, number))
    result = cursor.fetchall()
    return result

# Endpoint to count cards with a specific power and set name
@app.get("/v1/bird/card_games/count_cards_with_power", summary="Count cards with a specific power and set name")
async def count_cards_with_power(set_name: str = Query(..., description="Name of the set"), converted_mana_cost: int = Query(..., description="Converted mana cost of the card")):
    query = "SELECT SUM(CASE WHEN T1.power LIKE '*' OR T1.power IS NULL THEN 1 ELSE 0 END) FROM cards AS T1 INNER JOIN sets AS T2 ON T2.code = T1.setCode WHERE T2.name = ? AND T1.convertedManaCost > ?"
    cursor.execute(query, (set_name, converted_mana_cost))
    result = cursor.fetchall()
    return result
# Endpoint to get flavor text for a given card name and language
@app.get("/v1/bird/card_games/flavor_text", summary="Get flavor text for a given card name and language")
async def get_flavor_text(card_name: str = Query(..., description="Name of the card"), language: str = Query(..., description="Language of the flavor text")):
    query = """
    SELECT T2.flavorText
    FROM cards AS T1
    INNER JOIN foreign_data AS T2 ON T2.uuid = T1.uuid
    WHERE T1.name = ? AND T2.language = ?
    """
    cursor.execute(query, (card_name, language))
    result = cursor.fetchall()
    return result

# Endpoint to get languages for a given card name with non-null flavor text
@app.get("/v1/bird/card_games/languages", summary="Get languages for a given card name with non-null flavor text")
async def get_languages(card_name: str = Query(..., description="Name of the card")):
    query = """
    SELECT T2.language
    FROM cards AS T1
    INNER JOIN foreign_data AS T2 ON T2.uuid = T1.uuid
    WHERE T1.name = ? AND T2.flavorText IS NOT NULL
    """
    cursor.execute(query, (card_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct types for a given card name and language
@app.get("/v1/bird/card_games/distinct_types", summary="Get distinct types for a given card name and language")
async def get_distinct_types(card_name: str = Query(..., description="Name of the card"), language: str = Query(..., description="Language of the card")):
    query = """
    SELECT DISTINCT T1.type
    FROM cards AS T1
    INNER JOIN foreign_data AS T2 ON T2.uuid = T1.uuid
    WHERE T1.name = ? AND T2.language = ?
    """
    cursor.execute(query, (card_name, language))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct text for a given set name and language
@app.get("/v1/bird/card_games/distinct_text", summary="Get distinct text for a given set name and language")
async def get_distinct_text(set_name: str = Query(..., description="Name of the set"), language: str = Query(..., description="Language of the text")):
    query = """
    SELECT DISTINCT T1.text
    FROM foreign_data AS T1
    INNER JOIN cards AS T2 ON T2.uuid = T1.uuid
    INNER JOIN sets AS T3 ON T3.code = T2.setCode
    WHERE T3.name = ? AND T1.language = ?
    """
    cursor.execute(query, (set_name, language))
    result = cursor.fetchall()
    return result

# Endpoint to get card names for a given set name and language, ordered by converted mana cost
@app.get("/v1/bird/card_games/card_names", summary="Get card names for a given set name and language, ordered by converted mana cost")
async def get_card_names(set_name: str = Query(..., description="Name of the set"), language: str = Query(..., description="Language of the card")):
    query = """
    SELECT T2.name
    FROM foreign_data AS T1
    INNER JOIN cards AS T2 ON T2.uuid = T1.uuid
    INNER JOIN sets AS T3 ON T3.code = T2.setCode
    WHERE T3.name = ? AND T1.language = ?
    ORDER BY T2.convertedManaCost DESC
    """
    cursor.execute(query, (set_name, language))
    result = cursor.fetchall()
    return result

# Endpoint to get ruling dates for a given card name
@app.get("/v1/bird/card_games/ruling_dates", summary="Get ruling dates for a given card name")
async def get_ruling_dates(card_name: str = Query(..., description="Name of the card")):
    query = """
    SELECT T2.date
    FROM cards AS T1
    INNER JOIN rulings AS T2 ON T2.uuid = T1.uuid
    WHERE T1.name = ?
    """
    cursor.execute(query, (card_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of cards with converted mana cost of 7 in a given set
@app.get("/v1/bird/card_games/percentage_mana_cost_7", summary="Get percentage of cards with converted mana cost of 7 in a given set")
async def get_percentage_mana_cost_7(set_name: str = Query(..., description="Name of the set")):
    query = """
    SELECT CAST(SUM(CASE WHEN T1.convertedManaCost = 7 THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(T1.id)
    FROM cards AS T1
    INNER JOIN sets AS T2 ON T2.code = T1.setCode
    WHERE T2.name = ?
    """
    cursor.execute(query, (set_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of cards with both cardKingdomFoilId and cardKingdomId in a given set
@app.get("/v1/bird/card_games/percentage_card_kingdom_ids", summary="Get percentage of cards with both cardKingdomFoilId and cardKingdomId in a given set")
async def get_percentage_card_kingdom_ids(set_name: str = Query(..., description="Name of the set")):
    query = """
    SELECT CAST(SUM(CASE WHEN T1.cardKingdomFoilId IS NOT NULL AND T1.cardKingdomId IS NOT NULL THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(T1.id)
    FROM cards AS T1
    INNER JOIN sets AS T2 ON T2.code = T1.setCode
    WHERE T2.name = ?
    """
    cursor.execute(query, (set_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get set codes for a given release date
@app.get("/v1/bird/card_games/set_codes_by_release_date", summary="Get set codes for a given release date")
async def get_set_codes_by_release_date(release_date: str = Query(..., description="Release date of the set")):
    query = """
    SELECT code
    FROM sets
    WHERE releaseDate = ?
    GROUP BY releaseDate, code
    """
    cursor.execute(query, (release_date,))
    result = cursor.fetchall()
    return result

# Endpoint to get keyrune code for a given set code
@app.get("/v1/bird/card_games/keyrune_code", summary="Get keyrune code for a given set code")
async def get_keyrune_code(set_code: str = Query(..., description="Code of the set")):
    query = """
    SELECT keyruneCode
    FROM sets
    WHERE code = ?
    """
    cursor.execute(query, (set_code,))
    result = cursor.fetchall()
    return result

# Endpoint to get mcmId for a given set code
@app.get("/v1/bird/card_games/mcm_id", summary="Get mcmId for a given set code")
async def get_mcm_id(set_code: str = Query(..., description="Code of the set")):
    query = """
    SELECT mcmId
    FROM sets
    WHERE code = ?
    """
    cursor.execute(query, (set_code,))
    result = cursor.fetchall()
    return result

# Endpoint to get mcmName for a given release date
@app.get("/v1/bird/card_games/mcm_name", summary="Get mcmName for a given release date")
async def get_mcm_name(release_date: str = Query(..., description="Release date of the set")):
    query = """
    SELECT mcmName
    FROM sets
    WHERE releaseDate = ?
    """
    cursor.execute(query, (release_date,))
    result = cursor.fetchall()
    return result

# Endpoint to get type for a given set name
@app.get("/v1/bird/card_games/set_type", summary="Get type for a given set name")
async def get_set_type(set_name: str = Query(..., description="Name of the set")):
    query = """
    SELECT type
    FROM sets
    WHERE name LIKE ?
    """
    cursor.execute(query, ('%' + set_name + '%',))
    result = cursor.fetchall()
    return result

# Endpoint to get parent code for a given set name
@app.get("/v1/bird/card_games/parent_code", summary="Get parent code for a given set name")
async def get_parent_code(set_name: str = Query(..., description="Name of the set")):
    query = """
    SELECT parentCode
    FROM sets
    WHERE name = ?
    """
    cursor.execute(query, (set_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get ruling text and content warning for a given artist
@app.get("/v1/bird/card_games/ruling_text_content_warning", summary="Get ruling text and content warning for a given artist")
async def get_ruling_text_content_warning(artist: str = Query(..., description="Name of the artist")):
    query = """
    SELECT T2.text, CASE WHEN T1.hasContentWarning = 1 THEN 'YES' ELSE 'NO' END
    FROM cards AS T1
    INNER JOIN rulings AS T2 ON T2.uuid = T1.uuid
    WHERE T1.artist = ?
    """
    cursor.execute(query, (artist,))
    result = cursor.fetchall()
    return result

# Endpoint to get release date for a given card name
@app.get("/v1/bird/card_games/release_date", summary="Get release date for a given card name")
async def get_release_date(card_name: str = Query(..., description="Name of the card")):
    query = """
    SELECT T2.releaseDate
    FROM cards AS T1
    INNER JOIN sets AS T2 ON T2.code = T1.setCode
    WHERE T1.name = ?
    """
    cursor.execute(query, (card_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get base set size for a given translation
@app.get("/v1/bird/card_games/base_set_size", summary="Get base set size for a given translation")
async def get_base_set_size(translation: str = Query(..., description="Translation of the set")):
    query = """
    SELECT T1.baseSetSize
    FROM sets AS T1
    INNER JOIN set_translations AS T2 ON T2.setCode = T1.code
    WHERE T2.translation = ?
    """
    cursor.execute(query, (translation,))
    result = cursor.fetchall()
    return result

# Endpoint to get type for a given translation
@app.get("/v1/bird/card_games/type_by_translation", summary="Get type for a given translation")
async def get_type_by_translation(translation: str = Query(..., description="Translation of the set")):
    query = """
    SELECT type
    FROM sets
    WHERE code IN (SELECT setCode FROM set_translations WHERE translation = ?)
    """
    cursor.execute(query, (translation,))
    result = cursor.fetchall()
    return result

# Endpoint to get translations for a given card name, language, and non-null translation
@app.get("/v1/bird/card_games/translations", summary="Get translations for a given card name, language, and non-null translation")
async def get_translations(card_name: str = Query(..., description="Name of the card"), language: str = Query(..., description="Language of the translation")):
    query = """
    SELECT T2.translation
    FROM cards AS T1
    INNER JOIN set_translations AS T2 ON T2.setCode = T1.setCode
    WHERE T1.name = ? AND T2.language = ? AND T2.translation IS NOT NULL
    """
    cursor.execute(query, (card_name, language))
    result = cursor.fetchall()
    return result

# Endpoint to get count of distinct translations for a given set name and non-null translation
@app.get("/v1/bird/card_games/count_distinct_translations", summary="Get count of distinct translations for a given set name and non-null translation")
async def get_count_distinct_translations(set_name: str = Query(..., description="Name of the set")):
    query = """
    SELECT COUNT(DISTINCT T2.translation)
    FROM sets AS T1
    INNER JOIN set_translations AS T2 ON T2.setCode = T1.code
    WHERE T1.name = ? AND T2.translation IS NOT NULL
    """
    cursor.execute(query, (set_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get translation for a given card name and language
@app.get("/v1/bird/card_games/translation", summary="Get translation for a given card name and language")
async def get_translation(card_name: str = Query(..., description="Name of the card"), language: str = Query(..., description="Language of the translation")):
    query = """
    SELECT T2.translation
    FROM cards AS T1
    INNER JOIN set_translations AS T2 ON T2.setCode = T1.setCode
    WHERE T1.name = ? AND T2.language = ? AND T2.translation IS NOT NULL
    """
    cursor.execute(query, (card_name, language))
    result = cursor.fetchall()
    return result

# Endpoint to get card name for a given set name
@app.get("/v1/bird/card_games/card_name", summary="Get card name for a given set name")
async def get_card_name(set_name: str = Query(..., description="Name of the set")):
    query = """
    SELECT T1.name
    FROM cards AS T1
    INNER JOIN sets AS T2 ON T2.code = T1.setCode
    WHERE T2.name = ?
    ORDER BY T1.convertedManaCost DESC
    LIMIT 1
    """
    cursor.execute(query, (set_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get release date for a given translation
@app.get("/v1/bird/card_games/release_date", summary="Get release date for a given translation")
async def get_release_date(translation: str = Query(..., description="Translation of the set")):
    query = """
    SELECT T1.releaseDate
    FROM sets AS T1
    INNER JOIN set_translations AS T2 ON T2.setCode = T1.code
    WHERE T2.translation = ?
    """
    cursor.execute(query, (translation,))
    result = cursor.fetchall()
    return result

# Endpoint to get set types for a given card name
@app.get("/v1/bird/card_games/set_types", summary="Get set types for a given card name")
async def get_set_types(card_name: str = Query(..., description="Name of the card")):
    query = """
    SELECT type
    FROM sets
    WHERE code IN (
        SELECT setCode
        FROM cards
        WHERE name = ?
    )
    """
    cursor.execute(query, (card_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of cards for a given set name and mana cost
@app.get("/v1/bird/card_games/card_count", summary="Get count of cards for a given set name and mana cost")
async def get_card_count(set_name: str = Query(..., description="Name of the set"), mana_cost: int = Query(..., description="Mana cost of the card")):
    query = """
    SELECT COUNT(id)
    FROM cards
    WHERE setCode IN (
        SELECT code
        FROM sets
        WHERE name = ?
    ) AND convertedManaCost = ?
    """
    cursor.execute(query, (set_name, mana_cost))
    result = cursor.fetchall()
    return result

# Endpoint to get translations for a given set name and language
@app.get("/v1/bird/card_games/set_translations", summary="Get translations for a given set name and language")
async def get_set_translations(set_name: str = Query(..., description="Name of the set"), language: str = Query(..., description="Language of the translation")):
    query = """
    SELECT translation
    FROM set_translations
    WHERE setCode IN (
        SELECT code
        FROM sets
        WHERE name = ?
    ) AND language = ?
    """
    cursor.execute(query, (set_name, language))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of non-foil only sets for a given language
@app.get("/v1/bird/card_games/non_foil_percentage", summary="Get percentage of non-foil only sets for a given language")
async def get_non_foil_percentage(language: str = Query(..., description="Language of the translation")):
    query = """
    SELECT CAST(SUM(CASE WHEN isNonFoilOnly = 1 THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(id)
    FROM sets
    WHERE code IN (
        SELECT setCode
        FROM set_translations
        WHERE language = ?
    )
    """
    cursor.execute(query, (language,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of online only sets for a given language
@app.get("/v1/bird/card_games/online_only_percentage", summary="Get percentage of online only sets for a given language")
async def get_online_only_percentage(language: str = Query(..., description="Language of the translation")):
    query = """
    SELECT CAST(SUM(CASE WHEN isOnlineOnly = 1 THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(id)
    FROM sets
    WHERE code IN (
        SELECT setCode
        FROM set_translations
        WHERE language = ?
    )
    """
    cursor.execute(query, (language,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct availability for a given artist and textless status
@app.get("/v1/bird/card_games/distinct_availability", summary="Get distinct availability for a given artist and textless status")
async def get_distinct_availability(artist: str = Query(..., description="Name of the artist"), is_textless: int = Query(..., description="Textless status")):
    query = """
    SELECT DISTINCT availability
    FROM cards
    WHERE artist = ? AND isTextless = ?
    """
    cursor.execute(query, (artist, is_textless))
    result = cursor.fetchall()
    return result

# Endpoint to get set with the largest base set size
@app.get("/v1/bird/card_games/largest_base_set", summary="Get set with the largest base set size")
async def get_largest_base_set():
    query = """
    SELECT id
    FROM sets
    ORDER BY baseSetSize DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get artist for a given side status
@app.get("/v1/bird/card_games/artist", summary="Get artist for a given side status")
async def get_artist(side: str = Query(..., description="Side status")):
    query = """
    SELECT artist
    FROM cards
    WHERE side IS ?
    ORDER BY convertedManaCost DESC
    LIMIT 1
    """
    cursor.execute(query, (side,))
    result = cursor.fetchall()
    return result

# Endpoint to get frame effects for a given cardKingdomFoilId and cardKingdomId
@app.get("/v1/bird/card_games/frame_effects", summary="Get frame effects for a given cardKingdomFoilId and cardKingdomId")
async def get_frame_effects(cardKingdomFoilId: int = Query(..., description="Card Kingdom Foil ID"), cardKingdomId: int = Query(..., description="Card Kingdom ID")):
    query = """
    SELECT frameEffects
    FROM cards
    WHERE cardKingdomFoilId IS NOT NULL AND cardKingdomId IS NOT NULL
    GROUP BY frameEffects
    ORDER BY COUNT(frameEffects) DESC
    LIMIT 1
    """
    cursor.execute(query, (cardKingdomFoilId, cardKingdomId))
    result = cursor.fetchall()
    return result

# Endpoint to get sum of power for a given hasFoil and duelDeck status
@app.get("/v1/bird/card_games/sum_power", summary="Get sum of power for a given hasFoil and duelDeck status")
async def get_sum_power(hasFoil: int = Query(..., description="Has Foil status"), duelDeck: str = Query(..., description="Duel Deck status")):
    query = """
    SELECT SUM(CASE WHEN power = '*' OR power IS NULL THEN 1 ELSE 0 END)
    FROM cards
    WHERE hasFoil = ? AND duelDeck = ?
    """
    cursor.execute(query, (hasFoil, duelDeck))
    result = cursor.fetchall()
    return result

# Endpoint to get set with the largest total set size for a given type
@app.get("/v1/bird/card_games/largest_total_set", summary="Get set with the largest total set size for a given type")
async def get_largest_total_set(set_type: str = Query(..., description="Type of the set")):
    query = """
    SELECT id
    FROM sets
    WHERE type = ?
    ORDER BY totalSetSize DESC
    LIMIT 1
    """
    cursor.execute(query, (set_type,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct card names for a given format
@app.get("/v1/bird/card_games/distinct_card_names", summary="Get distinct card names for a given format")
async def get_distinct_card_names(format: str = Query(..., description="Format of the card")):
    query = """
    SELECT DISTINCT name
    FROM cards
    WHERE uuid IN (
        SELECT uuid
        FROM legalities
        WHERE format = ?
    )
    ORDER BY manaCost DESC
    LIMIT 0, 10
    """
    cursor.execute(query, (format,))
    result = cursor.fetchall()
    return result

# Endpoint to get original release date and format for a given rarity and status
@app.get("/v1/bird/card_games/original_release_date", summary="Get original release date and format for a given rarity and status")
async def get_original_release_date(rarity: str = Query(..., description="Rarity of the card"), status: str = Query(..., description="Status of the card")):
    query = """
    SELECT T1.originalReleaseDate, T2.format
    FROM cards AS T1
    INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid
    WHERE T1.rarity = ? AND T1.originalReleaseDate IS NOT NULL AND T2.status = ?
    ORDER BY T1.originalReleaseDate
    LIMIT 1
    """
    cursor.execute(query, (rarity, status))
    result = cursor.fetchall()
    return result

# Endpoint to get count of cards for a given artist and language
@app.get("/v1/bird/card_games/card_count_by_artist_language", summary="Get count of cards for a given artist and language")
async def get_card_count_by_artist_language(artist: str = Query(..., description="Name of the artist"), language: str = Query(..., description="Language of the card")):
    query = """
    SELECT COUNT(T3.id)
    FROM (
        SELECT T1.id
        FROM cards AS T1
        INNER JOIN foreign_data AS T2 ON T2.uuid = T1.uuid
        WHERE T1.artist = ? AND T2.language = ?
        GROUP BY T1.id
    ) AS T3
    """
    cursor.execute(query, (artist, language))
    result = cursor.fetchall()
    return result

# Endpoint to get count of cards for a given rarity, type, name, and status
@app.get("/v1/bird/card_games/card_count_by_rarity_type_name_status", summary="Get count of cards for a given rarity, type, name, and status")
async def get_card_count_by_rarity_type_name_status(rarity: str = Query(..., description="Rarity of the card"), card_type: str = Query(..., description="Type of the card"), name: str = Query(..., description="Name of the card"), status: str = Query(..., description="Status of the card")):
    query = """
    SELECT COUNT(T1.id)
    FROM cards AS T1
    INNER JOIN legalities AS T2 ON T2.uuid = T1.uuid
    WHERE T1.rarity = ? AND T1.types = ? AND T1.name = ? AND T2.status = ?
    """
    cursor.execute(query, (rarity, card_type, name, status))
    result = cursor.fetchall()
    return result

# Endpoint to get banned cards for the format with the most banned cards
@app.get("/v1/bird/card_games/banned_cards", summary="Get banned cards for the format with the most banned cards")
async def get_banned_cards():
    query = """
    WITH MaxBanned AS (
        SELECT format, COUNT(*) AS count_banned
        FROM legalities
        WHERE status = 'Banned'
        GROUP BY format
        ORDER BY COUNT(*) DESC
        LIMIT 1
    )
    SELECT T2.format, T1.name
    FROM cards AS T1
    INNER JOIN legalities AS T2 ON T2.uuid = T1.uuid
    INNER JOIN MaxBanned MB ON MB.format = T2.format
    WHERE T2.status = 'Banned'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get languages for a given set name
@app.get("/v1/bird/card_games/set_languages", summary="Get languages for a given set name")
async def get_set_languages(set_name: str = Query(..., description="Name of the set")):
    query = """
    SELECT language
    FROM set_translations
    WHERE id IN (
        SELECT id
        FROM sets
        WHERE name = ?
    )
    """
    cursor.execute(query, (set_name,))
    result = cursor.fetchall()
    return result
# Endpoint to get artist and format
@app.get("/v1/bird/card_games/artist_format", summary="Get artist and format")
async def get_artist_format(limit: int = Query(1, description="Limit the number of results")):
    query = """
    SELECT T1.artist, T2.format
    FROM cards AS T1
    INNER JOIN legalities AS T2 ON T2.uuid = T1.uuid
    GROUP BY T1.artist
    ORDER BY COUNT(T1.id) ASC
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    results = cursor.fetchall()
    return results

# Endpoint to get distinct status
@app.get("/v1/bird/card_games/distinct_status", summary="Get distinct status")
async def get_distinct_status(frame_version: int = Query(..., description="Frame version"),
                              has_content_warning: int = Query(..., description="Has content warning"),
                              artist: str = Query(..., description="Artist name"),
                              format: str = Query(..., description="Format")):
    query = """
    SELECT DISTINCT T2.status
    FROM cards AS T1
    INNER JOIN legalities AS T2 ON T2.uuid = T1.uuid
    WHERE T1.frameVersion = ? AND T1.hasContentWarning = ? AND T1.artist = ? AND T2.format = ?
    """
    cursor.execute(query, (frame_version, has_content_warning, artist, format))
    results = cursor.fetchall()
    return results

# Endpoint to get name and format
@app.get("/v1/bird/card_games/name_format", summary="Get name and format")
async def get_name_format(edhrec_rank: int = Query(..., description="EDHREC rank"),
                          status: str = Query(..., description="Status")):
    query = """
    SELECT T1.name, T2.format
    FROM cards AS T1
    INNER JOIN legalities AS T2 ON T2.uuid = T1.uuid
    WHERE T1.edhrecRank = ? AND T2.status = ?
    GROUP BY T1.name, T2.format
    """
    cursor.execute(query, (edhrec_rank, status))
    results = cursor.fetchall()
    return results

# Endpoint to get average id and language
@app.get("/v1/bird/card_games/average_id_language", summary="Get average id and language")
async def get_average_id_language(start_date: str = Query(..., description="Start date"),
                                  end_date: str = Query(..., description="End date")):
    query = """
    SELECT (CAST(SUM(T1.id) AS REAL) / COUNT(T1.id)) / 4, T2.language
    FROM sets AS T1
    INNER JOIN set_translations AS T2 ON T1.id = T2.id
    WHERE T1.releaseDate BETWEEN ? AND ?
    GROUP BY T1.releaseDate
    ORDER BY COUNT(T2.language) DESC
    LIMIT 1
    """
    cursor.execute(query, (start_date, end_date))
    results = cursor.fetchall()
    return results

# Endpoint to get distinct artist
@app.get("/v1/bird/card_games/distinct_artist", summary="Get distinct artist")
async def get_distinct_artist(availability: str = Query(..., description="Availability"),
                              border_color: str = Query(..., description="Border color")):
    query = """
    SELECT DISTINCT artist
    FROM cards
    WHERE availability = ? AND BorderColor = ?
    """
    cursor.execute(query, (availability, border_color))
    results = cursor.fetchall()
    return results

# Endpoint to get uuid
@app.get("/v1/bird/card_games/uuid", summary="Get uuid")
async def get_uuid(format: str = Query(..., description="Format"),
                   status: str = Query(..., description="Status")):
    query = """
    SELECT uuid
    FROM legalities
    WHERE format = ? AND (status = ? OR status = ?)
    """
    cursor.execute(query, (format, status, status))
    results = cursor.fetchall()
    return results

# Endpoint to get count of id
@app.get("/v1/bird/card_games/count_id", summary="Get count of id")
async def get_count_id(artist: str = Query(..., description="Artist name"),
                       availability: str = Query(..., description="Availability")):
    query = """
    SELECT COUNT(id)
    FROM cards
    WHERE artist = ? AND availability = ?
    """
    cursor.execute(query, (artist, availability))
    results = cursor.fetchall()
    return results

# Endpoint to get text
@app.get("/v1/bird/card_games/text", summary="Get text")
async def get_text(artist: str = Query(..., description="Artist name")):
    query = """
    SELECT T2.text
    FROM cards AS T1
    INNER JOIN rulings AS T2 ON T2.uuid = T1.uuid
    WHERE T1.artist = ?
    ORDER BY T2.date DESC
    """
    cursor.execute(query, (artist,))
    results = cursor.fetchall()
    return results

# Endpoint to get distinct name and format
@app.get("/v1/bird/card_games/distinct_name_format", summary="Get distinct name and format")
async def get_distinct_name_format(set_code: str = Query(..., description="Set code")):
    query = """
    SELECT DISTINCT T2.name,
           CASE WHEN T1.status = 'Legal' THEN T1.format ELSE NULL END
    FROM legalities AS T1
    INNER JOIN cards AS T2 ON T2.uuid = T1.uuid
    WHERE T2.setCode IN (SELECT code FROM sets WHERE name = ?)
    """
    cursor.execute(query, (set_code,))
    results = cursor.fetchall()
    return results

# Endpoint to get set names
@app.get("/v1/bird/card_games/set_names", summary="Get set names")
async def get_set_names(language: str = Query(..., description="Language")):
    query = """
    SELECT name
    FROM sets
    WHERE code IN (SELECT setCode FROM set_translations WHERE language = ? AND language NOT LIKE '%Japanese%')
    """
    cursor.execute(query, (language,))
    results = cursor.fetchall()
    return results

# Endpoint to get distinct frame version and name
@app.get("/v1/bird/card_games/distinct_frame_version_name", summary="Get distinct frame version and name")
async def get_distinct_frame_version_name(artist: str = Query(..., description="Artist name")):
    query = """
    SELECT DISTINCT T1.frameVersion, T1.name, IIF(T2.status = 'Banned', T1.name, 'NO')
    FROM cards AS T1
    INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid
    WHERE T1.artist = ?
    """
    cursor.execute(query, (artist,))
    results = cursor.fetchall()
    return results
