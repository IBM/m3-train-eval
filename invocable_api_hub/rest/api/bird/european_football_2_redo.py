

from fastapi import APIRouter, Query
import sqlite3

app = APIRouter()

# Database connection
conn = sqlite3.connect('invocable_api_hub/db/bird/test/european_football_2.sqlite', check_same_thread=False)
cursor = conn.cursor()

# Endpoint to get player with the highest overall rating
@app.get("/v1/bird/european_football_2/player_highest_overall_rating", summary="Get player with the highest overall rating")
async def get_player_highest_overall_rating():
    query = "SELECT player_api_id FROM Player_Attributes ORDER BY overall_rating DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"player_api_id": result[0]}

# Endpoint to get tallest player
@app.get("/v1/bird/european_football_2/tallest_player", summary="Get the tallest player")
async def get_tallest_player():
    query = "SELECT player_name FROM Player ORDER BY height DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"player_name": result[0]}

# Endpoint to get player with the lowest potential
@app.get("/v1/bird/european_football_2/player_lowest_potential", summary="Get player with the lowest potential")
async def get_player_lowest_potential():
    query = "SELECT preferred_foot FROM Player_Attributes WHERE potential IS NOT NULL ORDER BY potential ASC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"preferred_foot": result[0]}

# Endpoint to get count of players with specific overall rating and defensive work rate
@app.get("/v1/bird/european_football_2/player_count_by_rating_and_work_rate", summary="Get count of players with specific overall rating and defensive work rate")
async def get_player_count_by_rating_and_work_rate(min_rating: int = Query(..., description="Minimum overall rating"),
                                                   max_rating: int = Query(..., description="Maximum overall rating"),
                                                   work_rate: str = Query(..., description="Defensive work rate")):
    query = f"SELECT COUNT(id) FROM Player_Attributes WHERE overall_rating BETWEEN {min_rating} AND {max_rating} AND defensive_work_rate = '{work_rate}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get top players by crossing
@app.get("/v1/bird/european_football_2/top_players_by_crossing", summary="Get top players by crossing")
async def get_top_players_by_crossing(limit: int = Query(..., description="Number of top players to retrieve")):
    query = f"SELECT id FROM Player_Attributes ORDER BY crossing DESC LIMIT {limit}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"player_ids": [row[0] for row in result]}

# Endpoint to get league with the most goals in a season
@app.get("/v1/bird/european_football_2/league_most_goals_in_season", summary="Get league with the most goals in a season")
async def get_league_most_goals_in_season(season: str = Query(..., description="Season")):
    query = f"SELECT t2.name FROM Match AS t1 INNER JOIN League AS t2 ON t1.league_id = t2.id WHERE t1.season = '{season}' GROUP BY t2.name ORDER BY SUM(t1.home_team_goal + t1.away_team_goal) DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"league_name": result[0]}

# Endpoint to get team with the most losses in a season
@app.get("/v1/bird/european_football_2/team_most_losses_in_season", summary="Get team with the most losses in a season")
async def get_team_most_losses_in_season(season: str = Query(..., description="Season")):
    query = f"SELECT teamDetails.team_long_name FROM Match AS matchData INNER JOIN Team AS teamDetails ON matchData.home_team_api_id = teamDetails.team_api_id WHERE matchData.season = '{season}' AND matchData.home_team_goal - matchData.away_team_goal < 0 GROUP BY matchData.home_team_api_id ORDER BY COUNT(*) ASC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"team_name": result[0]}

# Endpoint to get top players by penalties
@app.get("/v1/bird/european_football_2/top_players_by_penalties", summary="Get top players by penalties")
async def get_top_players_by_penalties(limit: int = Query(..., description="Number of top players to retrieve")):
    query = f"SELECT t2.player_name FROM Player_Attributes AS t1 INNER JOIN Player AS t2 ON t1.id = t2.id ORDER BY t1.penalties DESC LIMIT {limit}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"player_names": [row[0] for row in result]}

# Endpoint to get top team by away wins in a specific league and season
@app.get("/v1/bird/european_football_2/top_team_by_away_wins", summary="Get top team by away wins in a specific league and season")
async def get_top_team_by_away_wins(league_name: str = Query(..., description="League name"),
                                    season: str = Query(..., description="Season")):
    query = f"SELECT teamInfo.team_long_name FROM League AS leagueData INNER JOIN Match AS matchData ON leagueData.id = matchData.league_id INNER JOIN Team AS teamInfo ON matchData.away_team_api_id = teamInfo.team_api_id WHERE leagueData.name = '{league_name}' AND matchData.season = '{season}' AND matchData.away_team_goal - matchData.home_team_goal > 0 GROUP BY matchData.away_team_api_id ORDER BY COUNT(*) DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"team_name": result[0]}

# Endpoint to get teams with the lowest build-up play speed
@app.get("/v1/bird/european_football_2/teams_lowest_build_up_play_speed", summary="Get teams with the lowest build-up play speed")
async def get_teams_lowest_build_up_play_speed(limit: int = Query(..., description="Number of teams to retrieve")):
    query = f"SELECT t1.buildUpPlaySpeed FROM Team_Attributes AS t1 INNER JOIN Team AS t2 ON t1.team_api_id = t2.team_api_id ORDER BY t1.buildUpPlaySpeed ASC LIMIT {limit}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"build_up_play_speeds": [row[0] for row in result]}

# Endpoint to get league with the most draws in a season
@app.get("/v1/bird/european_football_2/league_most_draws_in_season", summary="Get league with the most draws in a season")
async def get_league_most_draws_in_season(season: str = Query(..., description="Season")):
    query = f"SELECT t2.name FROM Match AS t1 INNER JOIN League AS t2 ON t1.league_id = t2.id WHERE t1.season = '{season}' AND t1.home_team_goal = t1.away_team_goal GROUP BY t2.name ORDER BY COUNT(t1.id) DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"league_name": result[0]}

# Endpoint to get age of players with specific sprint speed in a specific year range
@app.get("/v1/bird/european_football_2/player_age_by_sprint_speed", summary="Get age of players with specific sprint speed in a specific year range")
async def get_player_age_by_sprint_speed(min_year: int = Query(..., description="Minimum year"),
                                          max_year: int = Query(..., description="Maximum year"),
                                          sprint_speed: int = Query(..., description="Sprint speed")):
    query = f"SELECT DISTINCT DATETIME() - T2.birthday age FROM Player_Attributes AS t1 INNER JOIN Player AS t2 ON t1.player_api_id = t2.player_api_id WHERE STRFTIME('%Y',t1.`date`) >= '{min_year}' AND STRFTIME('%Y',t1.`date`) <= '{max_year}' AND t1.sprint_speed >= {sprint_speed}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"ages": [row[0] for row in result]}

# Endpoint to get league with the most matches
@app.get("/v1/bird/european_football_2/league_most_matches", summary="Get league with the most matches")
async def get_league_most_matches():
    query = "SELECT t2.name, t1.max_count FROM League AS t2 JOIN (SELECT league_id, MAX(cnt) AS max_count FROM (SELECT league_id, COUNT(id) AS cnt FROM Match GROUP BY league_id) AS subquery) AS t1 ON t1.league_id = t2.id"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"leagues": [{"name": row[0], "max_count": row[1]} for row in result]}

# Endpoint to get average height of players born in a specific year range
@app.get("/v1/bird/european_football_2/average_height_by_birth_year", summary="Get average height of players born in a specific year range")
async def get_average_height_by_birth_year(min_year: int = Query(..., description="Minimum year"),
                                            max_year: int = Query(..., description="Maximum year")):
    query = f"SELECT SUM(height) / COUNT(id) FROM Player WHERE SUBSTR(birthday, 1, 4) BETWEEN '{min_year}' AND '{max_year}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"average_height": result[0]}

# Endpoint to get player with the highest overall rating in a specific year
@app.get("/v1/bird/european_football_2/player_highest_overall_rating_in_year", summary="Get player with the highest overall rating in a specific year")
async def get_player_highest_overall_rating_in_year(year: int = Query(..., description="Year")):
    query = f"SELECT player_api_id FROM Player_Attributes WHERE SUBSTR(`date`, 1, 4) = '{year}' ORDER BY overall_rating DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"player_api_id": result[0]}

# Endpoint to get teams with specific build-up play speed range
@app.get("/v1/bird/european_football_2/teams_by_build_up_play_speed_range", summary="Get teams with specific build-up play speed range")
async def get_teams_by_build_up_play_speed_range(min_speed: int = Query(..., description="Minimum build-up play speed"),
                                                  max_speed: int = Query(..., description="Maximum build-up play speed")):
    query = f"SELECT DISTINCT team_fifa_api_id FROM Team_Attributes WHERE buildUpPlaySpeed > {min_speed} AND buildUpPlaySpeed < {max_speed}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"team_fifa_api_ids": [row[0] for row in result]}

# Endpoint to get teams with above-average build-up play passing in a specific year
@app.get("/v1/bird/european_football_2/teams_above_average_build_up_play_passing", summary="Get teams with above-average build-up play passing in a specific year")
async def get_teams_above_average_build_up_play_passing(year: int = Query(..., description="Year")):
    query = f"SELECT DISTINCT t4.team_long_name FROM Team_Attributes AS t3 INNER JOIN Team AS t4 ON t3.team_api_id = t4.team_api_id WHERE SUBSTR(t3.`date`, 1, 4) = '{year}' AND t3.buildUpPlayPassing > ( SELECT CAST(SUM(t2.buildUpPlayPassing) AS REAL) / COUNT(t1.id) FROM Team AS t1 INNER JOIN Team_Attributes AS t2 ON t1.team_api_id = t2.team_api_id WHERE STRFTIME('%Y',t2.`date`) = '{year}')"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"team_names": [row[0] for row in result]}

# Endpoint to get percentage of left-footed players born in a specific year range
@app.get("/v1/bird/european_football_2/percentage_left_footed_players", summary="Get percentage of left-footed players born in a specific year range")
async def get_percentage_left_footed_players(min_year: int = Query(..., description="Minimum year"),
                                              max_year: int = Query(..., description="Maximum year")):
    query = f"SELECT CAST(COUNT(CASE WHEN t2.preferred_foot = 'left' THEN t1.id ELSE NULL END) AS REAL) * 100 / COUNT(t1.id) percent FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE SUBSTR(t1.birthday, 1, 4) BETWEEN '{min_year}' AND '{max_year}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get leagues with the fewest goals
@app.get("/v1/bird/european_football_2/leagues_fewest_goals", summary="Get leagues with the fewest goals")
async def get_leagues_fewest_goals(limit: int = Query(..., description="Number of leagues to retrieve")):
    query = f"SELECT t1.name, SUM(t2.home_team_goal) + SUM(t2.away_team_goal) FROM League AS t1 INNER JOIN Match AS t2 ON t1.id = t2.league_id GROUP BY t1.name ORDER BY SUM(t2.home_team_goal) + SUM(t2.away_team_goal) ASC LIMIT {limit}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"leagues": [{"name": row[0], "total_goals": row[1]} for row in result]}

# Endpoint to get average long shots per game for a specific player
@app.get("/v1/bird/european_football_2/average_long_shots_per_game", summary="Get average long shots per game for a specific player")
async def get_average_long_shots_per_game(player_name: str = Query(..., description="Player name")):
    query = f"SELECT CAST(SUM(t2.long_shots) AS REAL) / COUNT(t2.`date`) FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE t1.player_name = '{player_name}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"average_long_shots": result[0]}
# Endpoint to get players with height greater than a given value
@app.get("/v1/bird/european_football_2/players_by_height", summary="Get players with height greater than a given value")
async def get_players_by_height(height: int = Query(..., description="Height of the player")):
    query = """
    SELECT t1.player_name
    FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE t1.height > ?
    GROUP BY t1.id
    ORDER BY CAST(SUM(t2.heading_accuracy) AS REAL) / COUNT(t2.player_fifa_api_id) DESC
    LIMIT 10
    """
    cursor.execute(query, (height,))
    result = cursor.fetchall()
    return {"players": result}

# Endpoint to get teams with specific buildUpPlayDribblingClass and chanceCreationPassing
@app.get("/v1/bird/european_football_2/teams_by_attributes", summary="Get teams with specific buildUpPlayDribblingClass and chanceCreationPassing")
async def get_teams_by_attributes(buildUpPlayDribblingClass: str = Query(..., description="Build up play dribbling class"), year: int = Query(..., description="Year")):
    query = """
    SELECT t3.team_long_name
    FROM Team AS t3
    INNER JOIN Team_Attributes AS t4 ON t3.team_api_id = t4.team_api_id
    WHERE t4.buildUpPlayDribblingClass = ?
    AND t4.chanceCreationPassing < (
        SELECT CAST(SUM(t2.chanceCreationPassing) AS REAL) / COUNT(t1.id)
        FROM Team AS t1
        INNER JOIN Team_Attributes AS t2 ON t1.team_api_id = t2.team_api_id
        WHERE t2.buildUpPlayDribblingClass = ?
        AND SUBSTR(t2.date, 1, 4) = ?
    )
    ORDER BY t4.chanceCreationPassing DESC
    """
    cursor.execute(query, (buildUpPlayDribblingClass, buildUpPlayDribblingClass, str(year)))
    result = cursor.fetchall()
    return {"teams": result}

# Endpoint to get leagues with a positive goal difference in a specific season
@app.get("/v1/bird/european_football_2/leagues_by_goal_difference", summary="Get leagues with a positive goal difference in a specific season")
async def get_leagues_by_goal_difference(season: str = Query(..., description="Season")):
    query = """
    SELECT t1.name
    FROM League AS t1
    INNER JOIN Match AS t2 ON t1.id = t2.league_id
    WHERE t2.season = ?
    GROUP BY t1.name
    HAVING (CAST(SUM(t2.home_team_goal) AS REAL) / COUNT(DISTINCT t2.id)) - (CAST(SUM(t2.away_team_goal) AS REAL) / COUNT(DISTINCT t2.id)) > 0
    """
    cursor.execute(query, (season,))
    result = cursor.fetchall()
    return {"leagues": result}

# Endpoint to get team short name by team long name
@app.get("/v1/bird/european_football_2/team_short_name", summary="Get team short name by team long name")
async def get_team_short_name(team_long_name: str = Query(..., description="Team long name")):
    query = """
    SELECT team_short_name
    FROM Team
    WHERE team_long_name = ?
    """
    cursor.execute(query, (team_long_name,))
    result = cursor.fetchall()
    return {"team_short_name": result}

# Endpoint to get players by birth month and year
@app.get("/v1/bird/european_football_2/players_by_birth_month_year", summary="Get players by birth month and year")
async def get_players_by_birth_month_year(birth_month_year: str = Query(..., description="Birth month and year in YYYY-MM format")):
    query = """
    SELECT player_name
    FROM Player
    WHERE SUBSTR(birthday, 1, 7) = ?
    """
    cursor.execute(query, (birth_month_year,))
    result = cursor.fetchall()
    return {"players": result}

# Endpoint to get distinct attacking work rate for a specific player
@app.get("/v1/bird/european_football_2/attacking_work_rate", summary="Get distinct attacking work rate for a specific player")
async def get_attacking_work_rate(player_name: str = Query(..., description="Player name")):
    query = """
    SELECT DISTINCT t2.attacking_work_rate
    FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE t1.player_name = ?
    """
    cursor.execute(query, (player_name,))
    result = cursor.fetchall()
    return {"attacking_work_rate": result}

# Endpoint to get distinct buildUpPlayPositioningClass for a specific team
@app.get("/v1/bird/european_football_2/buildUpPlayPositioningClass", summary="Get distinct buildUpPlayPositioningClass for a specific team")
async def get_buildUpPlayPositioningClass(team_long_name: str = Query(..., description="Team long name")):
    query = """
    SELECT DISTINCT t2.buildUpPlayPositioningClass
    FROM Team AS t1
    INNER JOIN Team_attributes AS t2 ON t1.team_fifa_api_id = t2.team_fifa_api_id
    WHERE t1.team_long_name = ?
    """
    cursor.execute(query, (team_long_name,))
    result = cursor.fetchall()
    return {"buildUpPlayPositioningClass": result}

# Endpoint to get heading accuracy for a specific player on a specific date
@app.get("/v1/bird/european_football_2/heading_accuracy", summary="Get heading accuracy for a specific player on a specific date")
async def get_heading_accuracy(player_name: str = Query(..., description="Player name"), date: str = Query(..., description="Date in YYYY-MM-DD format")):
    query = """
    SELECT t2.heading_accuracy
    FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE t1.player_name = ?
    AND SUBSTR(t2.date, 1, 10) = ?
    """
    cursor.execute(query, (player_name, date))
    result = cursor.fetchall()
    return {"heading_accuracy": result}

# Endpoint to get overall rating for a specific player in a specific year
@app.get("/v1/bird/european_football_2/overall_rating", summary="Get overall rating for a specific player in a specific year")
async def get_overall_rating(player_name: str = Query(..., description="Player name"), year: int = Query(..., description="Year")):
    query = """
    SELECT t2.overall_rating
    FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE t1.player_name = ?
    AND strftime('%Y', t2.date) = ?
    """
    cursor.execute(query, (player_name, str(year)))
    result = cursor.fetchall()
    return {"overall_rating": result}

# Endpoint to get count of matches in a specific season for a specific league
@app.get("/v1/bird/european_football_2/match_count", summary="Get count of matches in a specific season for a specific league")
async def get_match_count(season: str = Query(..., description="Season"), league_name: str = Query(..., description="League name")):
    query = """
    SELECT COUNT(t2.id)
    FROM League AS t1
    INNER JOIN Match AS t2 ON t1.id = t2.league_id
    WHERE t2.season = ?
    AND t1.name = ?
    """
    cursor.execute(query, (season, league_name))
    result = cursor.fetchall()
    return {"match_count": result}

# Endpoint to get preferred foot of the youngest player
@app.get("/v1/bird/european_football_2/preferred_foot_youngest_player", summary="Get preferred foot of the youngest player")
async def get_preferred_foot_youngest_player():
    query = """
    SELECT t2.preferred_foot
    FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    ORDER BY t1.birthday DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"preferred_foot": result}

# Endpoint to get players with maximum potential
@app.get("/v1/bird/european_football_2/players_with_max_potential", summary="Get players with maximum potential")
async def get_players_with_max_potential():
    query = """
    SELECT DISTINCT(t1.player_name)
    FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE t2.potential = (SELECT MAX(potential) FROM Player_Attributes)
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"players": result}

# Endpoint to get count of players with weight less than a given value and preferred foot
@app.get("/v1/bird/european_football_2/player_count_by_weight_and_foot", summary="Get count of players with weight less than a given value and preferred foot")
async def get_player_count_by_weight_and_foot(weight: int = Query(..., description="Weight of the player"), preferred_foot: str = Query(..., description="Preferred foot")):
    query = """
    SELECT COUNT(DISTINCT t1.id)
    FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE t1.weight < ?
    AND t2.preferred_foot = ?
    """
    cursor.execute(query, (weight, preferred_foot))
    result = cursor.fetchall()
    return {"player_count": result}

# Endpoint to get distinct team short names with a specific chanceCreationPassingClass
@app.get("/v1/bird/european_football_2/team_short_names_by_passing_class", summary="Get distinct team short names with a specific chanceCreationPassingClass")
async def get_team_short_names_by_passing_class(chanceCreationPassingClass: str = Query(..., description="Chance creation passing class")):
    query = """
    SELECT DISTINCT t1.team_short_name
    FROM Team AS t1
    INNER JOIN Team_attributes AS t2 ON t1.team_api_id = t2.team_api_id
    WHERE t2.chanceCreationPassingClass = ?
    """
    cursor.execute(query, (chanceCreationPassingClass,))
    result = cursor.fetchall()
    return {"team_short_names": result}

# Endpoint to get distinct defensive work rate for a specific player
@app.get("/v1/bird/european_football_2/defensive_work_rate", summary="Get distinct defensive work rate for a specific player")
async def get_defensive_work_rate(player_name: str = Query(..., description="Player name")):
    query = """
    SELECT DISTINCT t2.defensive_work_rate
    FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE t1.player_name = ?
    """
    cursor.execute(query, (player_name,))
    result = cursor.fetchall()
    return {"defensive_work_rate": result}

# Endpoint to get birthday of the player with the highest overall rating
@app.get("/v1/bird/european_football_2/birthday_highest_overall_rating", summary="Get birthday of the player with the highest overall rating")
async def get_birthday_highest_overall_rating():
    query = """
    SELECT t1.birthday
    FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    ORDER BY t2.overall_rating DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"birthday": result}

# Endpoint to get leagues in a specific country
@app.get("/v1/bird/european_football_2/leagues_by_country", summary="Get leagues in a specific country")
async def get_leagues_by_country(country_name: str = Query(..., description="Country name")):
    query = """
    SELECT t2.name
    FROM Country AS t1
    INNER JOIN League AS t2 ON t1.id = t2.country_id
    WHERE t1.name = ?
    """
    cursor.execute(query, (country_name,))
    result = cursor.fetchall()
    return {"leagues": result}

# Endpoint to get average home team goals for a specific country in a specific season
@app.get("/v1/bird/european_football_2/average_home_team_goals", summary="Get average home team goals for a specific country in a specific season")
async def get_average_home_team_goals(country_name: str = Query(..., description="Country name"), season: str = Query(..., description="Season")):
    query = """
    SELECT CAST(SUM(t2.home_team_goal) AS REAL) / COUNT(t2.id)
    FROM Country AS t1
    INNER JOIN Match AS t2 ON t1.id = t2.country_id
    WHERE t1.name = ?
    AND t2.season = ?
    """
    cursor.execute(query, (country_name, season))
    result = cursor.fetchall()
    return {"average_home_team_goals": result}

# Endpoint to get finishing average for players with max and min height
@app.get("/v1/bird/european_football_2/finishing_average_by_height", summary="Get finishing average for players with max and min height")
async def get_finishing_average_by_height():
    query = """
    SELECT A
    FROM (
        SELECT AVG(finishing) result, 'Max' A
        FROM Player AS T1
        INNER JOIN Player_Attributes AS T2 ON T1.player_api_id = T2.player_api_id
        WHERE T1.height = (SELECT MAX(height) FROM Player)
        UNION
        SELECT AVG(finishing) result, 'Min' A
        FROM Player AS T1
        INNER JOIN Player_Attributes AS T2 ON T1.player_api_id = T2.player_api_id
        WHERE T1.height = (SELECT MIN(height) FROM Player)
    )
    ORDER BY result DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"finishing_average": result}

# Endpoint to get players with height greater than a given value
@app.get("/v1/bird/european_football_2/players_by_height_simple", summary="Get players with height greater than a given value")
async def get_players_by_height_simple(height: int = Query(..., description="Height of the player")):
    query = """
    SELECT player_name
    FROM Player
    WHERE height > ?
    """
    cursor.execute(query, (height,))
    result = cursor.fetchall()
    return {"players": result}

# Endpoint to get count of players born after a certain year
@app.get("/v1/bird/european_football_2/player_count_by_birth_year", summary="Get count of players born after a certain year")
async def get_player_count_by_birth_year(year: int = Query(..., description="Year of birth")):
    query = f"SELECT COUNT(id) FROM Player WHERE STRFTIME('%Y', birthday) > '{year}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of players with specific weight and name pattern
@app.get("/v1/bird/european_football_2/player_count_by_weight_and_name", summary="Get count of players with specific weight and name pattern")
async def get_player_count_by_weight_and_name(weight: int = Query(..., description="Weight of the player"), name_pattern: str = Query(..., description="Name pattern")):
    query = f"SELECT COUNT(id) FROM Player WHERE weight > {weight} AND player_name LIKE '{name_pattern}%'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get distinct player names with overall rating and date range
@app.get("/v1/bird/european_football_2/distinct_player_names_by_rating_and_date", summary="Get distinct player names with overall rating and date range")
async def get_distinct_player_names_by_rating_and_date(rating: int = Query(..., description="Overall rating"), start_year: int = Query(..., description="Start year"), end_year: int = Query(..., description="End year")):
    query = f"SELECT DISTINCT t1.player_name FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE t2.overall_rating > {rating} AND SUBSTR(t2.`date`, 1, 4) BETWEEN '{start_year}' AND '{end_year}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"player_names": [row[0] for row in result]}

# Endpoint to get potential of a player
@app.get("/v1/bird/european_football_2/player_potential", summary="Get potential of a player")
async def get_player_potential(player_name: str = Query(..., description="Name of the player")):
    query = f"SELECT t2.potential FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE t1.player_name = '{player_name}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"potential": result[0]}

# Endpoint to get distinct player IDs and names with preferred foot
@app.get("/v1/bird/european_football_2/distinct_players_by_preferred_foot", summary="Get distinct player IDs and names with preferred foot")
async def get_distinct_players_by_preferred_foot(preferred_foot: str = Query(..., description="Preferred foot")):
    query = f"SELECT DISTINCT t1.id, t1.player_name FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE t2.preferred_foot = '{preferred_foot}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"players": [{"id": row[0], "name": row[1]} for row in result]}

# Endpoint to get distinct team long names with build up play speed class
@app.get("/v1/bird/european_football_2/distinct_teams_by_build_up_play_speed_class", summary="Get distinct team long names with build up play speed class")
async def get_distinct_teams_by_build_up_play_speed_class(build_up_play_speed_class: str = Query(..., description="Build up play speed class")):
    query = f"SELECT DISTINCT t1.team_long_name FROM Team AS t1 INNER JOIN Team_Attributes AS t2 ON t1.team_api_id = t2.team_api_id WHERE t2.buildUpPlaySpeedClass = '{build_up_play_speed_class}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"team_long_names": [row[0] for row in result]}

# Endpoint to get distinct build up play passing class for a team
@app.get("/v1/bird/european_football_2/distinct_build_up_play_passing_class", summary="Get distinct build up play passing class for a team")
async def get_distinct_build_up_play_passing_class(team_short_name: str = Query(..., description="Team short name")):
    query = f"SELECT DISTINCT t2.buildUpPlayPassingClass FROM Team AS t1 INNER JOIN Team_Attributes AS t2 ON t1.team_api_id = t2.team_api_id WHERE t1.team_short_name = '{team_short_name}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"build_up_play_passing_class": [row[0] for row in result]}

# Endpoint to get distinct team short names with build up play passing
@app.get("/v1/bird/european_football_2/distinct_teams_by_build_up_play_passing", summary="Get distinct team short names with build up play passing")
async def get_distinct_teams_by_build_up_play_passing(build_up_play_passing: int = Query(..., description="Build up play passing")):
    query = f"SELECT DISTINCT t1.team_short_name FROM Team AS t1 INNER JOIN Team_Attributes AS t2 ON t1.team_api_id = t2.team_api_id WHERE t2.buildUpPlayPassing > {build_up_play_passing}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"team_short_names": [row[0] for row in result]}

# Endpoint to get average overall rating for players with specific height and date range
@app.get("/v1/bird/european_football_2/average_overall_rating_by_height_and_date", summary="Get average overall rating for players with specific height and date range")
async def get_average_overall_rating_by_height_and_date(height: int = Query(..., description="Height of the player"), start_year: int = Query(..., description="Start year"), end_year: int = Query(..., description="End year")):
    query = f"SELECT CAST(SUM(t2.overall_rating) AS REAL) / COUNT(t2.id) FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE t1.height > {height} AND STRFTIME('%Y',t2.`date`) >= '{start_year}' AND STRFTIME('%Y',t2.`date`) <= '{end_year}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"average_overall_rating": result[0]}

# Endpoint to get shortest player
@app.get("/v1/bird/european_football_2/shortest_player", summary="Get shortest player")
async def get_shortest_player():
    query = "SELECT player_name FROM player ORDER BY height ASC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"shortest_player": result[0]}

# Endpoint to get country name for a specific league
@app.get("/v1/bird/european_football_2/country_name_by_league", summary="Get country name for a specific league")
async def get_country_name_by_league(league_name: str = Query(..., description="League name")):
    query = f"SELECT t1.name FROM Country AS t1 INNER JOIN League AS t2 ON t1.id = t2.country_id WHERE t2.name = '{league_name}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"country_name": result[0]}

# Endpoint to get distinct team short names with specific build up play attributes
@app.get("/v1/bird/european_football_2/distinct_teams_by_build_up_play_attributes", summary="Get distinct team short names with specific build up play attributes")
async def get_distinct_teams_by_build_up_play_attributes(build_up_play_speed: int = Query(..., description="Build up play speed"), build_up_play_dribbling: int = Query(..., description="Build up play dribbling"), build_up_play_passing: int = Query(..., description="Build up play passing")):
    query = f"SELECT DISTINCT t1.team_short_name FROM Team AS t1 INNER JOIN Team_Attributes AS t2 ON t1.team_api_id = t2.team_api_id WHERE t2.buildUpPlaySpeed = {build_up_play_speed} AND t2.buildUpPlayDribbling = {build_up_play_dribbling} AND t2.buildUpPlayPassing = {build_up_play_passing}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"team_short_names": [row[0] for row in result]}

# Endpoint to get average overall rating for a specific player
@app.get("/v1/bird/european_football_2/average_overall_rating_by_player", summary="Get average overall rating for a specific player")
async def get_average_overall_rating_by_player(player_name: str = Query(..., description="Player name")):
    query = f"SELECT CAST(SUM(t2.overall_rating) AS REAL) / COUNT(t2.id) FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE t1.player_name = '{player_name}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"average_overall_rating": result[0]}

# Endpoint to get count of matches for a specific league and date range
@app.get("/v1/bird/european_football_2/match_count_by_league_and_date", summary="Get count of matches for a specific league and date range")
async def get_match_count_by_league_and_date(league_name: str = Query(..., description="League name"), start_date: str = Query(..., description="Start date (YYYY-MM)"), end_date: str = Query(..., description="End date (YYYY-MM)")):
    query = f"SELECT COUNT(t2.id) FROM League AS t1 INNER JOIN Match AS t2 ON t1.id = t2.league_id WHERE t1.name = '{league_name}' AND SUBSTR(t2.`date`, 1, 7) BETWEEN '{start_date}' AND '{end_date}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"match_count": result[0]}

# Endpoint to get team short names with specific home team goal
@app.get("/v1/bird/european_football_2/teams_by_home_team_goal", summary="Get team short names with specific home team goal")
async def get_teams_by_home_team_goal(home_team_goal: int = Query(..., description="Home team goal")):
    query = f"SELECT t1.team_short_name FROM Team AS t1 INNER JOIN Match AS t2 ON t1.team_api_id = t2.home_team_api_id WHERE t2.home_team_goal = {home_team_goal}"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"team_short_names": [row[0] for row in result]}

# Endpoint to get player name with specific potential and ordered by balance
@app.get("/v1/bird/european_football_2/player_by_potential_and_balance", summary="Get player name with specific potential and ordered by balance")
async def get_player_by_potential_and_balance(potential: int = Query(..., description="Potential")):
    query = f"SELECT t1.player_name FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE t2.potential = '{potential}' ORDER BY t2.balance DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"player_name": result[0]}

# Endpoint to get ball control difference between two players
@app.get("/v1/bird/european_football_2/ball_control_difference", summary="Get ball control difference between two players")
async def get_ball_control_difference(player1_name: str = Query(..., description="First player name"), player2_name: str = Query(..., description="Second player name")):
    query = f"""
    SELECT
        CAST(SUM(CASE WHEN t1.player_name = '{player1_name}' THEN t2.ball_control ELSE 0 END) AS REAL) / COUNT(CASE WHEN t1.player_name = '{player1_name}' THEN t2.id ELSE NULL END) -
        CAST(SUM(CASE WHEN t1.player_name = '{player2_name}' THEN t2.ball_control ELSE 0 END) AS REAL) / COUNT(CASE WHEN t1.player_name = '{player2_name}' THEN t2.id ELSE NULL END)
    FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"ball_control_difference": result[0]}

# Endpoint to get team long name by team short name
@app.get("/v1/bird/european_football_2/team_long_name_by_short_name", summary="Get team long name by team short name")
async def get_team_long_name_by_short_name(team_short_name: str = Query(..., description="Team short name")):
    query = f"SELECT team_long_name FROM Team WHERE team_short_name = '{team_short_name}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"team_long_name": result[0]}

# Endpoint to get player name by player names and ordered by birthday
@app.get("/v1/bird/european_football_2/player_by_names_and_birthday", summary="Get player name by player names and ordered by birthday")
async def get_player_by_names_and_birthday(player_names: str = Query(..., description="Player names separated by comma")):
    names_list = player_names.split(',')
    names_str = ', '.join([f"'{name.strip()}'" for name in names_list])
    query = f"SELECT player_name FROM Player WHERE player_name IN ({names_str}) ORDER BY birthday ASC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"player_name": result[0]}

# Endpoint to get tallest player
@app.get("/v1/bird/european_football_2/tallest_player", summary="Get tallest player")
async def get_tallest_player():
    query = "SELECT player_name FROM Player ORDER BY height DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"tallest_player": result[0]}
# Endpoint to get count of players with specific attributes
@app.get("/v1/bird/european_football_2/player_attributes_count", summary="Get count of players with specific attributes")
async def get_player_attributes_count(preferred_foot: str = Query(..., description="Preferred foot of the player"),
                                      attacking_work_rate: str = Query(..., description="Attacking work rate of the player")):
    query = f"SELECT COUNT(player_api_id) FROM Player_Attributes WHERE preferred_foot = ? AND attacking_work_rate = ?"
    cursor.execute(query, (preferred_foot, attacking_work_rate))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get country name based on league name
@app.get("/v1/bird/european_football_2/country_by_league", summary="Get country name based on league name")
async def get_country_by_league(league_name: str = Query(..., description="Name of the league")):
    query = f"SELECT t1.name FROM Country AS t1 INNER JOIN League AS t2 ON t1.id = t2.country_id WHERE t2.name = ?"
    cursor.execute(query, (league_name,))
    result = cursor.fetchone()
    return {"country_name": result[0]}

# Endpoint to get league name based on country name
@app.get("/v1/bird/european_football_2/league_by_country", summary="Get league name based on country name")
async def get_league_by_country(country_name: str = Query(..., description="Name of the country")):
    query = f"SELECT t2.name FROM Country AS t1 INNER JOIN League AS t2 ON t1.id = t2.country_id WHERE t1.name = ?"
    cursor.execute(query, (country_name,))
    result = cursor.fetchone()
    return {"league_name": result[0]}

# Endpoint to get top player by overall rating
@app.get("/v1/bird/european_football_2/top_player_by_rating", summary="Get top player by overall rating")
async def get_top_player_by_rating():
    query = f"SELECT t1.player_name FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id ORDER BY t2.overall_rating DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"player_name": result[0]}

# Endpoint to get count of players with specific attributes and birth year
@app.get("/v1/bird/european_football_2/player_count_by_attributes", summary="Get count of players with specific attributes and birth year")
async def get_player_count_by_attributes(birth_year: int = Query(..., description="Birth year of the player"),
                                         defensive_work_rate: str = Query(..., description="Defensive work rate of the player")):
    query = f"SELECT COUNT(DISTINCT t1.player_name) FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE STRFTIME('%Y',t1.birthday) < ? AND t2.defensive_work_rate = ?"
    cursor.execute(query, (birth_year, defensive_work_rate))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get top player by crossing attribute
@app.get("/v1/bird/european_football_2/top_player_by_crossing", summary="Get top player by crossing attribute")
async def get_top_player_by_crossing(player_names: str = Query(..., description="Comma-separated list of player names")):
    player_names_list = player_names.split(',')
    query = f"SELECT t1.player_name, t2.crossing FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE t1.player_name IN ({','.join(['?']*len(player_names_list))}) ORDER BY t2.crossing DESC LIMIT 1"
    cursor.execute(query, player_names_list)
    result = cursor.fetchone()
    return {"player_name": result[0], "crossing": result[1]}

# Endpoint to get heading accuracy of a player
@app.get("/v1/bird/european_football_2/player_heading_accuracy", summary="Get heading accuracy of a player")
async def get_player_heading_accuracy(player_name: str = Query(..., description="Name of the player")):
    query = f"SELECT t2.heading_accuracy FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE t1.player_name = ?"
    cursor.execute(query, (player_name,))
    result = cursor.fetchone()
    return {"heading_accuracy": result[0]}

# Endpoint to get count of players with specific attributes
@app.get("/v1/bird/european_football_2/player_count_by_height_volleys", summary="Get count of players with specific attributes")
async def get_player_count_by_height_volleys(height: int = Query(..., description="Height of the player"),
                                             volleys: int = Query(..., description="Volleys attribute of the player")):
    query = f"SELECT COUNT(DISTINCT t1.id) FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE t1.height > ? AND t2.volleys > ?"
    cursor.execute(query, (height, volleys))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get players with specific attributes
@app.get("/v1/bird/european_football_2/players_by_attributes", summary="Get players with specific attributes")
async def get_players_by_attributes(volleys: int = Query(..., description="Volleys attribute of the player"),
                                    dribbling: int = Query(..., description="Dribbling attribute of the player")):
    query = f"SELECT DISTINCT t1.player_name FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE t2.volleys > ? AND t2.dribbling > ?"
    cursor.execute(query, (volleys, dribbling))
    result = cursor.fetchall()
    return {"players": [row[0] for row in result]}

# Endpoint to get count of matches for a country in a specific season
@app.get("/v1/bird/european_football_2/match_count_by_country_season", summary="Get count of matches for a country in a specific season")
async def get_match_count_by_country_season(country_name: str = Query(..., description="Name of the country"),
                                            season: str = Query(..., description="Season of the matches")):
    query = f"SELECT COUNT(t2.id) FROM Country AS t1 INNER JOIN Match AS t2 ON t1.id = t2.country_id WHERE t1.name = ? AND t2.season = ?"
    cursor.execute(query, (country_name, season))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get long passing attribute of the youngest player
@app.get("/v1/bird/european_football_2/youngest_player_long_passing", summary="Get long passing attribute of the youngest player")
async def get_youngest_player_long_passing():
    query = f"SELECT t2.long_passing FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id ORDER BY t1.birthday ASC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"long_passing": result[0]}

# Endpoint to get count of matches for a league in a specific month
@app.get("/v1/bird/european_football_2/match_count_by_league_month", summary="Get count of matches for a league in a specific month")
async def get_match_count_by_league_month(league_name: str = Query(..., description="Name of the league"),
                                          month: str = Query(..., description="Month of the matches (YYYY-MM)")):
    query = f"SELECT COUNT(t2.id) FROM League AS t1 INNER JOIN Match AS t2 ON t1.id = t2.league_id WHERE t1.name = ? AND SUBSTR(t2.`date`, 1, 7) = ?"
    cursor.execute(query, (league_name, month))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get league with the most matches in a specific season
@app.get("/v1/bird/european_football_2/league_with_most_matches", summary="Get league with the most matches in a specific season")
async def get_league_with_most_matches(season: str = Query(..., description="Season of the matches")):
    query = f"""
    SELECT t1.name FROM League AS t1
    JOIN Match AS t2 ON t1.id = t2.league_id
    WHERE t2.season = ?
    GROUP BY t1.name
    HAVING COUNT(t2.id) = (
        SELECT MAX(match_count) FROM (
            SELECT COUNT(t2.id) AS match_count
            FROM Match AS t2
            WHERE t2.season = ?
            GROUP BY t2.league_id
        )
    )
    """
    cursor.execute(query, (season, season))
    result = cursor.fetchone()
    return {"league_name": result[0]}

# Endpoint to get average overall rating of players born before a specific year
@app.get("/v1/bird/european_football_2/average_overall_rating_by_birth_year", summary="Get average overall rating of players born before a specific year")
async def get_average_overall_rating_by_birth_year(birth_year: int = Query(..., description="Birth year of the players")):
    query = f"SELECT SUM(t2.overall_rating) / COUNT(t1.id) FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE SUBSTR(t1.birthday, 1, 4) < ?"
    cursor.execute(query, (birth_year,))
    result = cursor.fetchone()
    return {"average_overall_rating": result[0]}

# Endpoint to get overall rating difference between two players
@app.get("/v1/bird/european_football_2/overall_rating_difference", summary="Get overall rating difference between two players")
async def get_overall_rating_difference(player1_name: str = Query(..., description="Name of the first player"),
                                        player2_name: str = Query(..., description="Name of the second player")):
    query = f"""
    SELECT (SUM(CASE WHEN t1.player_name = ? THEN t2.overall_rating ELSE 0 END) * 1.0 -
            SUM(CASE WHEN t1.player_name = ? THEN t2.overall_rating ELSE 0 END)) * 100 /
            SUM(CASE WHEN t1.player_name = ? THEN t2.overall_rating ELSE 0 END)
    FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    """
    cursor.execute(query, (player1_name, player2_name, player2_name))
    result = cursor.fetchone()
    return {"overall_rating_difference": result[0]}

# Endpoint to get average build-up play speed of a team
@app.get("/v1/bird/european_football_2/average_build_up_play_speed", summary="Get average build-up play speed of a team")
async def get_average_build_up_play_speed(team_name: str = Query(..., description="Name of the team")):
    query = f"SELECT CAST(SUM(t2.buildUpPlaySpeed) AS REAL) / COUNT(t2.id) FROM Team AS t1 INNER JOIN Team_Attributes AS t2 ON t1.team_api_id = t2.team_api_id WHERE t1.team_long_name = ?"
    cursor.execute(query, (team_name,))
    result = cursor.fetchone()
    return {"average_build_up_play_speed": result[0]}

# Endpoint to get average overall rating of a player
@app.get("/v1/bird/european_football_2/average_overall_rating_by_player", summary="Get average overall rating of a player")
async def get_average_overall_rating_by_player(player_name: str = Query(..., description="Name of the player")):
    query = f"SELECT CAST(SUM(t2.overall_rating) AS REAL) / COUNT(t2.id) FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE t1.player_name = ?"
    cursor.execute(query, (player_name,))
    result = cursor.fetchone()
    return {"average_overall_rating": result[0]}

# Endpoint to get sum of crossing attribute of a player
@app.get("/v1/bird/european_football_2/sum_crossing_by_player", summary="Get sum of crossing attribute of a player")
async def get_sum_crossing_by_player(player_name: str = Query(..., description="Name of the player")):
    query = f"SELECT SUM(t2.crossing) FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE t1.player_name = ?"
    cursor.execute(query, (player_name,))
    result = cursor.fetchone()
    return {"sum_crossing": result[0]}

# Endpoint to get chance creation passing attributes of a team
@app.get("/v1/bird/european_football_2/chance_creation_passing_by_team", summary="Get chance creation passing attributes of a team")
async def get_chance_creation_passing_by_team(team_name: str = Query(..., description="Name of the team")):
    query = f"SELECT t2.chanceCreationPassing, t2.chanceCreationPassingClass FROM Team AS t1 INNER JOIN Team_Attributes AS t2 ON t1.team_api_id = t2.team_api_id WHERE t1.team_long_name = ? ORDER BY t2.chanceCreationPassing DESC LIMIT 1"
    cursor.execute(query, (team_name,))
    result = cursor.fetchone()
    return {"chance_creation_passing": result[0], "chance_creation_passing_class": result[1]}

# Endpoint to get preferred foot of a player
@app.get("/v1/bird/european_football_2/preferred_foot_by_player", summary="Get preferred foot of a player")
async def get_preferred_foot_by_player(player_name: str = Query(..., description="Name of the player")):
    query = f"SELECT DISTINCT t2.preferred_foot FROM Player AS t1 INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id WHERE t1.player_name = ?"
    cursor.execute(query, (player_name,))
    result = cursor.fetchone()
    return {"preferred_foot": result[0]}

# Endpoint to get the maximum overall rating for a given player
@app.get("/v1/bird/european_football_2/max_overall_rating", summary="Get the maximum overall rating for a given player")
async def get_max_overall_rating(player_name: str = Query(..., description="Name of the player")):
    query = """
    SELECT MAX(t2.overall_rating) FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE t1.player_name = ?
    """
    cursor.execute(query, (player_name,))
    result = cursor.fetchone()
    return {"max_overall_rating": result[0]}

# Endpoint to get the average away team goals for a given team and country
@app.get("/v1/bird/european_football_2/average_away_team_goals", summary="Get the average away team goals for a given team and country")
async def get_average_away_team_goals(team_name: str = Query(..., description="Name of the team"), country_name: str = Query(..., description="Name of the country")):
    query = """
    SELECT CAST(SUM(T1.away_team_goal) AS REAL) / COUNT(T1.id) FROM "Match" AS T1
    INNER JOIN TEAM AS T2 ON T1.away_team_api_id = T2.team_api_id
    INNER JOIN Country AS T3 ON T1.country_id = T3.id
    WHERE T2.team_long_name = ? AND T3.name = ?
    """
    cursor.execute(query, (team_name, country_name))
    result = cursor.fetchone()
    return {"average_away_team_goals": result[0]}

# Endpoint to get the player name with specific attributes
@app.get("/v1/bird/european_football_2/player_name_by_attributes", summary="Get the player name with specific attributes")
async def get_player_name_by_attributes(date: str = Query(..., description="Date in YYYY-MM-DD format"), overall_rating: int = Query(..., description="Overall rating")):
    query = """
    SELECT t1.player_name FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE SUBSTR(t2.`date`, 1, 10) = ? AND t2.overall_rating = ?
    ORDER BY t1.birthday ASC LIMIT 1
    """
    cursor.execute(query, (date, overall_rating))
    result = cursor.fetchone()
    return {"player_name": result[0]}

# Endpoint to get the overall rating for a given player on a specific date
@app.get("/v1/bird/european_football_2/overall_rating_by_date", summary="Get the overall rating for a given player on a specific date")
async def get_overall_rating_by_date(date: str = Query(..., description="Date in YYYY-MM-DD format"), player_name: str = Query(..., description="Name of the player")):
    query = """
    SELECT t2.overall_rating FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE SUBSTR(t2.`date`, 1, 10) = ? AND t1.player_name = ?
    """
    cursor.execute(query, (date, player_name))
    result = cursor.fetchone()
    return {"overall_rating": result[0]}

# Endpoint to get the potential for a given player on a specific date
@app.get("/v1/bird/european_football_2/player_potential_by_date", summary="Get the potential for a given player on a specific date")
async def get_player_potential_by_date(date: str = Query(..., description="Date in YYYY-MM-DD format"), player_name: str = Query(..., description="Name of the player")):
    query = """
    SELECT t2.potential FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE SUBSTR(t2.`date`, 1, 10) = ? AND t1.player_name = ?
    """
    cursor.execute(query, (date, player_name))
    result = cursor.fetchone()
    return {"potential": result[0]}

# Endpoint to get the attacking work rate for a given player on a specific date
@app.get("/v1/bird/european_football_2/attacking_work_rate_by_date", summary="Get the attacking work rate for a given player on a specific date")
async def get_attacking_work_rate_by_date(date: str = Query(..., description="Date in YYYY-MM-DD format"), player_name: str = Query(..., description="Name of the player")):
    query = """
    SELECT t2.attacking_work_rate FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE t2.`date` LIKE ? AND t1.player_name = ?
    """
    cursor.execute(query, (date + '%', player_name))
    result = cursor.fetchone()
    return {"attacking_work_rate": result[0]}

# Endpoint to get the defensive work rate for a given player on a specific date
@app.get("/v1/bird/european_football_2/defensive_work_rate_by_date", summary="Get the defensive work rate for a given player on a specific date")
async def get_defensive_work_rate_by_date(date: str = Query(..., description="Date in YYYY-MM-DD format"), player_name: str = Query(..., description="Name of the player")):
    query = """
    SELECT t2.defensive_work_rate FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_fifa_api_id = t2.player_fifa_api_id
    WHERE SUBSTR(t2.`date`, 1, 10) = ? AND t1.player_name = ?
    """
    cursor.execute(query, (date, player_name))
    result = cursor.fetchone()
    return {"defensive_work_rate": result[0]}

# Endpoint to get the date with the highest crossing for a given player
@app.get("/v1/bird/european_football_2/highest_crossing_date", summary="Get the date with the highest crossing for a given player")
async def get_highest_crossing_date(player_name: str = Query(..., description="Name of the player")):
    query = """
    SELECT `date` FROM (
        SELECT t2.crossing, t2.`date` FROM Player AS t1
        INNER JOIN Player_Attributes AS t2 ON t1.player_fifa_api_id = t2.player_fifa_api_id
        WHERE t1.player_name = ?
        ORDER BY t2.crossing DESC
    ) ORDER BY date DESC LIMIT 1
    """
    cursor.execute(query, (player_name,))
    result = cursor.fetchone()
    return {"highest_crossing_date": result[0]}

# Endpoint to get the build-up play speed class for a given team on a specific date
@app.get("/v1/bird/european_football_2/build_up_play_speed_class", summary="Get the build-up play speed class for a given team on a specific date")
async def get_build_up_play_speed_class(team_name: str = Query(..., description="Name of the team"), date: str = Query(..., description="Date in YYYY-MM-DD format")):
    query = """
    SELECT t2.buildUpPlaySpeedClass FROM Team AS t1
    INNER JOIN Team_Attributes AS t2 ON t1.team_api_id = t2.team_api_id
    WHERE t1.team_long_name = ? AND SUBSTR(t2.`date`, 1, 10) = ?
    """
    cursor.execute(query, (team_name, date))
    result = cursor.fetchone()
    return {"build_up_play_speed_class": result[0]}

# Endpoint to get the build-up play dribbling class for a given team on a specific date
@app.get("/v1/bird/european_football_2/build_up_play_dribbling_class", summary="Get the build-up play dribbling class for a given team on a specific date")
async def get_build_up_play_dribbling_class(team_name: str = Query(..., description="Name of the team"), date: str = Query(..., description="Date in YYYY-MM-DD format")):
    query = """
    SELECT t2.buildUpPlayDribblingClass FROM Team AS t1
    INNER JOIN Team_Attributes AS t2 ON t1.team_api_id = t2.team_api_id
    WHERE t1.team_short_name = ? AND SUBSTR(t2.`date`, 1, 10) = ?
    """
    cursor.execute(query, (team_name, date))
    result = cursor.fetchone()
    return {"build_up_play_dribbling_class": result[0]}

# Endpoint to get the build-up play passing class for a given team on a specific date
@app.get("/v1/bird/european_football_2/build_up_play_passing_class", summary="Get the build-up play passing class for a given team on a specific date")
async def get_build_up_play_passing_class(team_name: str = Query(..., description="Name of the team"), date: str = Query(..., description="Date in YYYY-MM-DD format")):
    query = """
    SELECT t2.buildUpPlayPassingClass FROM Team AS t1
    INNER JOIN Team_Attributes AS t2 ON t1.team_api_id = t2.team_api_id
    WHERE t1.team_long_name = ? AND t2.`date` LIKE ?
    """
    cursor.execute(query, (team_name, date + '%'))
    result = cursor.fetchone()
    return {"build_up_play_passing_class": result[0]}

# Endpoint to get the chance creation passing class for a given team on a specific date
@app.get("/v1/bird/european_football_2/chance_creation_passing_class", summary="Get the chance creation passing class for a given team on a specific date")
async def get_chance_creation_passing_class(team_name: str = Query(..., description="Name of the team"), date: str = Query(..., description="Date in YYYY-MM-DD format")):
    query = """
    SELECT t2.chanceCreationPassingClass FROM Team AS t1
    INNER JOIN Team_Attributes AS t2 ON t1.team_api_id = t2.team_api_id
    WHERE t1.team_long_name = ? AND SUBSTR(t2.`date`, 1, 10) = ?
    """
    cursor.execute(query, (team_name, date))
    result = cursor.fetchone()
    return {"chance_creation_passing_class": result[0]}

# Endpoint to get the chance creation crossing class for a given team on a specific date
@app.get("/v1/bird/european_football_2/chance_creation_crossing_class", summary="Get the chance creation crossing class for a given team on a specific date")
async def get_chance_creation_crossing_class(team_name: str = Query(..., description="Name of the team"), date: str = Query(..., description="Date in YYYY-MM-DD format")):
    query = """
    SELECT t2.chanceCreationCrossingClass FROM Team AS t1
    INNER JOIN Team_Attributes AS t2 ON t1.team_api_id = t2.team_api_id
    WHERE t1.team_long_name = ? AND SUBSTR(t2.`date`, 1, 10) = ?
    """
    cursor.execute(query, (team_name, date))
    result = cursor.fetchone()
    return {"chance_creation_crossing_class": result[0]}

# Endpoint to get the chance creation shooting class for a given team on a specific date
@app.get("/v1/bird/european_football_2/chance_creation_shooting_class", summary="Get the chance creation shooting class for a given team on a specific date")
async def get_chance_creation_shooting_class(team_name: str = Query(..., description="Name of the team"), date: str = Query(..., description="Date in YYYY-MM-DD format")):
    query = """
    SELECT t2.chanceCreationShootingClass FROM Team AS t1
    INNER JOIN Team_Attributes AS t2 ON t1.team_api_id = t2.team_api_id
    WHERE t1.team_long_name = ? AND t2.`date` LIKE ?
    """
    cursor.execute(query, (team_name, date + '%'))
    result = cursor.fetchone()
    return {"chance_creation_shooting_class": result[0]}

# Endpoint to get the average overall rating for a given player within a date range
@app.get("/v1/bird/european_football_2/average_overall_rating", summary="Get the average overall rating for a given player within a date range")
async def get_average_overall_rating(player_name: str = Query(..., description="Name of the player"), start_date: str = Query(..., description="Start date in YYYY-MM-DD format"), end_date: str = Query(..., description="End date in YYYY-MM-DD format")):
    query = """
    SELECT CAST(SUM(t2.overall_rating) AS REAL) / COUNT(t2.id) FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_fifa_api_id = t2.player_fifa_api_id
    WHERE t1.player_name = ? AND SUBSTR(t2.`date`, 1, 10) BETWEEN ? AND ?
    """
    cursor.execute(query, (player_name, start_date, end_date))
    result = cursor.fetchone()
    return {"average_overall_rating": result[0]}

# Endpoint to get the percentage difference in overall rating between two players on a specific date
@app.get("/v1/bird/european_football_2/overall_rating_percentage_difference", summary="Get the percentage difference in overall rating between two players on a specific date")
async def get_overall_rating_percentage_difference(date: str = Query(..., description="Date in YYYY-MM-DD format"), player1_name: str = Query(..., description="Name of the first player"), player2_name: str = Query(..., description="Name of the second player")):
    query = """
    SELECT (SUM(CASE WHEN t1.player_name = ? THEN t2.overall_rating ELSE 0 END) * 1.0 - SUM(CASE WHEN t1.player_name = ? THEN t2.overall_rating ELSE 0 END)) * 100 / SUM(CASE WHEN t1.player_name = ? THEN t2.overall_rating ELSE 0 END) LvsJ_percent FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_fifa_api_id = t2.player_fifa_api_id
    WHERE SUBSTR(t2.`date`, 1, 10) = ?
    """
    cursor.execute(query, (player1_name, player2_name, player1_name, date))
    result = cursor.fetchone()
    return {"overall_rating_percentage_difference": result[0]}

# Endpoint to get the tallest player
@app.get("/v1/bird/european_football_2/tallest_player", summary="Get the tallest player")
async def get_tallest_player():
    query = """
    SELECT player_name FROM (
        SELECT player_name, height, DENSE_RANK() OVER (ORDER BY height DESC) as rank
        FROM Player
    ) WHERE rank = 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"tallest_player": result[0]}

# Endpoint to get the top 10 heaviest players
@app.get("/v1/bird/european_football_2/top_heaviest_players", summary="Get the top 10 heaviest players")
async def get_top_heaviest_players():
    query = """
    SELECT player_api_id FROM Player ORDER BY weight DESC LIMIT 10
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"top_heaviest_players": [row[0] for row in result]}

# Endpoint to get players older than a certain age
@app.get("/v1/bird/european_football_2/players_older_than", summary="Get players older than a certain age")
async def get_players_older_than(age: int = Query(..., description="Age threshold")):
    query = """
    SELECT player_name FROM Player
    WHERE CAST((JULIANDAY('now') - JULIANDAY(birthday)) AS REAL) / 365 >= ?
    """
    cursor.execute(query, (age,))
    result = cursor.fetchall()
    return {"players_older_than": [row[0] for row in result]}

# Endpoint to get the sum of home team goals for a given player
@app.get("/v1/bird/european_football_2/sum_home_team_goals", summary="Get the sum of home team goals for a given player")
async def get_sum_home_team_goals(player_name: str = Query(..., description="Name of the player")):
    query = """
    SELECT SUM(t2.home_team_goal) FROM Player AS t1
    INNER JOIN match AS t2 ON t1.player_api_id = t2.away_player_9
    WHERE t1.player_name = ?
    """
    cursor.execute(query, (player_name,))
    result = cursor.fetchone()
    return {"sum_home_team_goals": result[0]}

# Endpoint to get the sum of away team goals for given player names
@app.get("/v1/bird/european_football_2/sum_away_team_goals", summary="Get the sum of away team goals for given player names")
async def get_sum_away_team_goals(player_names: str = Query(..., description="Comma-separated list of player names")):
    player_names_list = player_names.split(',')
    query = f"""
    SELECT SUM(t2.away_team_goal)
    FROM Player AS t1
    INNER JOIN match AS t2 ON t1.player_api_id = t2.away_player_5
    WHERE t1.player_name IN ({','.join(['?']*len(player_names_list))})
    """
    cursor.execute(query, player_names_list)
    result = cursor.fetchone()
    return {"sum_away_team_goals": result[0]}

# Endpoint to get the sum of home team goals for players under a certain age
@app.get("/v1/bird/european_football_2/sum_home_team_goals", summary="Get the sum of home team goals for players under a certain age")
async def get_sum_home_team_goals(age: int = Query(..., description="Age of the players")):
    query = """
    SELECT SUM(t2.home_team_goal)
    FROM Player AS t1
    INNER JOIN match AS t2 ON t1.player_api_id = t2.away_player_1
    WHERE datetime(CURRENT_TIMESTAMP, 'localtime') - datetime(t1.birthday) < ?
    """
    cursor.execute(query, (age,))
    result = cursor.fetchone()
    return {"sum_home_team_goals": result[0]}

# Endpoint to get distinct player names with the highest overall rating
@app.get("/v1/bird/european_football_2/highest_overall_rating", summary="Get distinct player names with the highest overall rating")
async def get_highest_overall_rating():
    query = """
    SELECT DISTINCT t1.player_name
    FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE t2.overall_rating = (SELECT MAX(overall_rating) FROM Player_Attributes)
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"highest_overall_rating": [row[0] for row in result]}

# Endpoint to get the player name with the highest potential
@app.get("/v1/bird/european_football_2/highest_potential", summary="Get the player name with the highest potential")
async def get_highest_potential():
    query = """
    SELECT DISTINCT t1.player_name
    FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    ORDER BY t2.potential DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"highest_potential": result[0]}

# Endpoint to get distinct player names with a high attacking work rate
@app.get("/v1/bird/european_football_2/high_attacking_work_rate", summary="Get distinct player names with a high attacking work rate")
async def get_high_attacking_work_rate():
    query = """
    SELECT DISTINCT t1.player_name
    FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE t2.attacking_work_rate = 'high'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"high_attacking_work_rate": [row[0] for row in result]}

# Endpoint to get the youngest player with finishing skill of 1
@app.get("/v1/bird/european_football_2/youngest_finishing_player", summary="Get the youngest player with finishing skill of 1")
async def get_youngest_finishing_player():
    query = """
    SELECT DISTINCT t1.player_name
    FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE t2.finishing = 1
    ORDER BY t1.birthday ASC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"youngest_finishing_player": result[0]}

# Endpoint to get player names from a specific country
@app.get("/v1/bird/european_football_2/players_by_country", summary="Get player names from a specific country")
async def get_players_by_country(country_name: str = Query(..., description="Name of the country")):
    query = """
    SELECT t3.player_name
    FROM Country AS t1
    INNER JOIN Match AS t2 ON t1.id = t2.country_id
    INNER JOIN Player AS t3 ON t2.home_player_1 = t3.player_api_id
    WHERE t1.name = ?
    """
    cursor.execute(query, (country_name,))
    result = cursor.fetchall()
    return {"players_by_country": [row[0] for row in result]}

# Endpoint to get distinct country names with players having vision greater than a certain value
@app.get("/v1/bird/european_football_2/countries_with_high_vision", summary="Get distinct country names with players having vision greater than a certain value")
async def get_countries_with_high_vision(vision: int = Query(..., description="Vision value")):
    query = """
    SELECT DISTINCT t4.name
    FROM Player_Attributes AS t1
    INNER JOIN Player AS t2 ON t1.player_api_id = t2.player_api_id
    INNER JOIN Match AS t3 ON t2.player_api_id = t3.home_player_8
    INNER JOIN Country AS t4 ON t3.country_id = t4.id
    WHERE t1.vision > ?
    """
    cursor.execute(query, (vision,))
    result = cursor.fetchall()
    return {"countries_with_high_vision": [row[0] for row in result]}

# Endpoint to get the country with the highest average player weight
@app.get("/v1/bird/european_football_2/highest_avg_weight_country", summary="Get the country with the highest average player weight")
async def get_highest_avg_weight_country():
    query = """
    SELECT t1.name
    FROM Country AS t1
    INNER JOIN Match AS t2 ON t1.id = t2.country_id
    INNER JOIN Player AS t3 ON t2.home_player_1 = t3.player_api_id
    GROUP BY t1.name
    ORDER BY AVG(t3.weight) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"highest_avg_weight_country": result[0]}

# Endpoint to get distinct team long names with a specific build-up play speed class
@app.get("/v1/bird/european_football_2/teams_by_build_up_play_speed", summary="Get distinct team long names with a specific build-up play speed class")
async def get_teams_by_build_up_play_speed(build_up_play_speed: str = Query(..., description="Build-up play speed class")):
    query = """
    SELECT DISTINCT t1.team_long_name
    FROM Team AS t1
    INNER JOIN Team_Attributes AS t2 ON t1.team_api_id = t2.team_api_id
    WHERE t2.buildUpPlaySpeedClass = ?
    """
    cursor.execute(query, (build_up_play_speed,))
    result = cursor.fetchall()
    return {"teams_by_build_up_play_speed": [row[0] for row in result]}

# Endpoint to get distinct team short names with a specific chance creation passing class
@app.get("/v1/bird/european_football_2/teams_by_chance_creation_passing", summary="Get distinct team short names with a specific chance creation passing class")
async def get_teams_by_chance_creation_passing(chance_creation_passing: str = Query(..., description="Chance creation passing class")):
    query = """
    SELECT DISTINCT t1.team_short_name
    FROM Team AS t1
    INNER JOIN Team_Attributes AS t2 ON t1.team_api_id = t2.team_api_id
    WHERE t2.chanceCreationPassingClass = ?
    """
    cursor.execute(query, (chance_creation_passing,))
    result = cursor.fetchall()
    return {"teams_by_chance_creation_passing": [row[0] for row in result]}

# Endpoint to get the average height of players from a specific country
@app.get("/v1/bird/european_football_2/avg_height_by_country", summary="Get the average height of players from a specific country")
async def get_avg_height_by_country(country_name: str = Query(..., description="Name of the country")):
    query = """
    SELECT CAST(SUM(T1.height) AS REAL) / COUNT(T1.id)
    FROM Player AS T1
    INNER JOIN Match AS T2 ON T1.id = T2.id
    INNER JOIN Country AS T3 ON T2.country_id = T3.ID
    WHERE T3.NAME = ?
    """
    cursor.execute(query, (country_name,))
    result = cursor.fetchone()
    return {"avg_height_by_country": result[0]}

# Endpoint to get player names with height greater than a certain value
@app.get("/v1/bird/european_football_2/players_by_height", summary="Get player names with height greater than a certain value")
async def get_players_by_height(height: int = Query(..., description="Height value")):
    query = """
    SELECT player_name
    FROM Player
    WHERE height > ?
    ORDER BY player_name
    LIMIT 3
    """
    cursor.execute(query, (height,))
    result = cursor.fetchall()
    return {"players_by_height": [row[0] for row in result]}

# Endpoint to get the count of players born after a certain year and with a specific name prefix
@app.get("/v1/bird/european_football_2/count_players_by_birth_year_and_name", summary="Get the count of players born after a certain year and with a specific name prefix")
async def get_count_players_by_birth_year_and_name(birth_year: int = Query(..., description="Birth year"), name_prefix: str = Query(..., description="Name prefix")):
    query = """
    SELECT COUNT(id)
    FROM Player
    WHERE birthday > ? AND player_name LIKE ?
    """
    cursor.execute(query, (birth_year, f"{name_prefix}%"))
    result = cursor.fetchone()
    return {"count_players_by_birth_year_and_name": result[0]}

# Endpoint to get the difference in jumping ability between two player IDs
@app.get("/v1/bird/european_football_2/jumping_ability_difference", summary="Get the difference in jumping ability between two player IDs")
async def get_jumping_ability_difference(player_id1: int = Query(..., description="First player ID"), player_id2: int = Query(..., description="Second player ID")):
    query = """
    SELECT SUM(CASE WHEN t1.id = ? THEN t1.jumping ELSE 0 END) - SUM(CASE WHEN t1.id = ? THEN t1.jumping ELSE 0 END)
    FROM Player_Attributes AS t1
    """
    cursor.execute(query, (player_id1, player_id2))
    result = cursor.fetchone()
    return {"jumping_ability_difference": result[0]}

# Endpoint to get player attribute IDs with a specific preferred foot and ordered by potential
@app.get("/v1/bird/european_football_2/player_attributes_by_preferred_foot", summary="Get player attribute IDs with a specific preferred foot and ordered by potential")
async def get_player_attributes_by_preferred_foot(preferred_foot: str = Query(..., description="Preferred foot")):
    query = """
    SELECT id
    FROM Player_Attributes
    WHERE preferred_foot = ?
    ORDER BY potential DESC
    LIMIT 5
    """
    cursor.execute(query, (preferred_foot,))
    result = cursor.fetchall()
    return {"player_attributes_by_preferred_foot": [row[0] for row in result]}

# Endpoint to get the count of players with a specific preferred foot and maximum crossing skill
@app.get("/v1/bird/european_football_2/count_players_by_preferred_foot_and_max_crossing", summary="Get the count of players with a specific preferred foot and maximum crossing skill")
async def get_count_players_by_preferred_foot_and_max_crossing(preferred_foot: str = Query(..., description="Preferred foot")):
    query = """
    SELECT COUNT(t1.id)
    FROM Player_Attributes AS t1
    WHERE t1.preferred_foot = ? AND t1.crossing = (SELECT MAX(crossing) FROM Player_Attributes)
    """
    cursor.execute(query, (preferred_foot,))
    result = cursor.fetchone()
    return {"count_players_by_preferred_foot_and_max_crossing": result[0]}

# Endpoint to get the percentage of players with strength and stamina greater than a certain value
@app.get("/v1/bird/european_football_2/percentage_players_by_strength_and_stamina", summary="Get the percentage of players with strength and stamina greater than a certain value")
async def get_percentage_players_by_strength_and_stamina(strength: int = Query(..., description="Strength value"), stamina: int = Query(..., description="Stamina value")):
    query = """
    SELECT CAST(COUNT(CASE WHEN strength > ? AND stamina > ? THEN id ELSE NULL END) AS REAL) * 100 / COUNT(id)
    FROM Player_Attributes t
    """
    cursor.execute(query, (strength, stamina))
    result = cursor.fetchone()
    return {"percentage_players_by_strength_and_stamina": result[0]}

# Endpoint to get country names from a specific league
@app.get("/v1/bird/european_football_2/countries_by_league", summary="Get country names from a specific league")
async def get_countries_by_league(league_name: str = Query(..., description="League name")):
    query = """
    SELECT name
    FROM Country
    WHERE id IN (SELECT country_id FROM League WHERE name = ?)
    """
    cursor.execute(query, (league_name,))
    result = cursor.fetchall()
    return {"countries_by_league": [row[0] for row in result]}

# Endpoint to get home and away team goals for a specific league and date
@app.get("/v1/bird/european_football_2/goals_by_league_and_date", summary="Get home and away team goals for a specific league and date")
async def get_goals_by_league_and_date(league_name: str = Query(..., description="League name"), match_date: str = Query(..., description="Match date")):
    query = """
    SELECT t2.home_team_goal, t2.away_team_goal
    FROM League AS t1
    INNER JOIN Match AS t2 ON t1.id = t2.league_id
    WHERE t1.name = ? AND t2.`date` LIKE ?
    """
    cursor.execute(query, (league_name, f"{match_date}%"))
    result = cursor.fetchall()
    return {"goals_by_league_and_date": [{"home_team_goal": row[0], "away_team_goal": row[1]} for row in result]}

# Endpoint to get player attributes for a given player name
@app.get("/v1/bird/european_football_2/player_attributes", summary="Get player attributes for a given player name")
async def get_player_attributes(player_name: str = Query(..., description="Name of the player")):
    query = """
    SELECT sprint_speed, agility, acceleration
    FROM Player_Attributes
    WHERE player_api_id IN (
        SELECT player_api_id
        FROM Player
        WHERE player_name = ?
    )
    """
    cursor.execute(query, (player_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get build up play speed class for a given team name
@app.get("/v1/bird/european_football_2/team_build_up_play_speed", summary="Get build up play speed class for a given team name")
async def get_team_build_up_play_speed(team_name: str = Query(..., description="Name of the team")):
    query = """
    SELECT DISTINCT t1.buildUpPlaySpeedClass
    FROM Team_Attributes AS t1
    INNER JOIN Team AS t2 ON t1.team_api_id = t2.team_api_id
    WHERE t2.team_long_name = ?
    """
    cursor.execute(query, (team_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of matches for a given league and season
@app.get("/v1/bird/european_football_2/match_count", summary="Get count of matches for a given league and season")
async def get_match_count(league_name: str = Query(..., description="Name of the league"), season: str = Query(..., description="Season")):
    query = """
    SELECT COUNT(t2.id)
    FROM League AS t1
    INNER JOIN Match AS t2 ON t1.id = t2.league_id
    WHERE t1.name = ? AND t2.season = ?
    """
    cursor.execute(query, (league_name, season))
    result = cursor.fetchall()
    return result

# Endpoint to get max home team goal for a given league
@app.get("/v1/bird/european_football_2/max_home_team_goal", summary="Get max home team goal for a given league")
async def get_max_home_team_goal(league_name: str = Query(..., description="Name of the league")):
    query = """
    SELECT MAX(t2.home_team_goal)
    FROM League AS t1
    INNER JOIN Match AS t2 ON t1.id = t2.league_id
    WHERE t1.name = ?
    """
    cursor.execute(query, (league_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get player attributes for the heaviest player
@app.get("/v1/bird/european_football_2/heaviest_player_attributes", summary="Get player attributes for the heaviest player")
async def get_heaviest_player_attributes():
    query = """
    SELECT id, finishing, curve
    FROM Player_Attributes
    WHERE player_api_id = (
        SELECT player_api_id
        FROM Player
        ORDER BY weight DESC
        LIMIT 1
    )
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get top leagues by match count for a given season
@app.get("/v1/bird/european_football_2/top_leagues_by_match_count", summary="Get top leagues by match count for a given season")
async def get_top_leagues_by_match_count(season: str = Query(..., description="Season")):
    query = """
    SELECT t1.name
    FROM League AS t1
    INNER JOIN Match AS t2 ON t1.id = t2.league_id
    WHERE t2.season = ?
    GROUP BY t1.name
    ORDER BY COUNT(t2.id) DESC
    LIMIT 4
    """
    cursor.execute(query, (season,))
    result = cursor.fetchall()
    return result

# Endpoint to get team with the most away goals
@app.get("/v1/bird/european_football_2/team_with_most_away_goals", summary="Get team with the most away goals")
async def get_team_with_most_away_goals():
    query = """
    SELECT t2.team_long_name
    FROM Match AS t1
    INNER JOIN Team AS t2 ON t1.away_team_api_id = t2.team_api_id
    ORDER BY t1.away_team_goal DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get players with the highest overall rating
@app.get("/v1/bird/european_football_2/players_with_highest_overall_rating", summary="Get players with the highest overall rating")
async def get_players_with_highest_overall_rating():
    query = """
    SELECT DISTINCT t1.player_name
    FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE t2.overall_rating = (
        SELECT MAX(overall_rating)
        FROM Player_Attributes
    )
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of players with overall rating above 70 and height below 180
@app.get("/v1/bird/european_football_2/percentage_players_above_rating", summary="Get percentage of players with overall rating above 70 and height below 180")
async def get_percentage_players_above_rating():
    query = """
    SELECT CAST(COUNT(CASE WHEN t2.overall_rating > 70 THEN t1.id ELSE NULL END) AS REAL) * 100 / COUNT(t1.id) percent
    FROM Player AS t1
    INNER JOIN Player_Attributes AS t2 ON t1.player_api_id = t2.player_api_id
    WHERE t1.height < 180
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result
