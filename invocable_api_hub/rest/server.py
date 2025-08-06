from fastapi import FastAPI

from invocable_api_hub.rest.api.bird import formula_1_redo

from invocable_api_hub.rest.api.bird import california_schools_redo as california_schools_bird_redo
from invocable_api_hub.rest.api.bird import card_games_redo as card_games_redo
from invocable_api_hub.rest.api.bird import codebase_community_redo 
from invocable_api_hub.rest.api.bird import debit_card_specializing_redo, european_football_2_redo, financial_redo, student_club_redo, superhero_redo, thrombosis_prediction_redo, toxicology_redo

app = FastAPI()  # mount multiple routers within the FastAPI application

app.include_router(california_schools_bird_redo.app)  # include_router() takes an instance of the router as an argument
app.include_router(card_games_redo.app)
app.include_router(codebase_community_redo.app)

app.include_router(debit_card_specializing_redo.app)

app.include_router(european_football_2_redo.app)
app.include_router(financial_redo.app)
app.include_router(formula_1_redo.app)
app.include_router(student_club_redo.app)
app.include_router(superhero_redo.app)
app.include_router(thrombosis_prediction_redo.app)
app.include_router(toxicology_redo.app)