import os

from envs.retrievers.elser import ElserRetriever

"""
{
    "name": "retriever_clapnq_airline",
    "arguments": {
        "query": "What airport serves as a hub for American Airlines Inc.: AA in New York City?"
    }
}
"""

ES_CONFIG = {
	"top_k": 10
}

DOMAINS = ["california_schools", "card_games", "codebase_community", "debit_card_specializing", "european_football_2",
		   "financial", "formula_1", "student_club", "superhero", "thrombosis_prediction", "toxicology"
	, "music_platform_2", "shooting", "car_retails", "airline", "human_resources", "student_loan", "codebase_comments",
		   "language_corpus", "bike_share_1", "cookbook", "software_company", "donor", "authors"
	, "shipping", "video_games", "sales", "olympics", "university", "talkingdata", "simpson_episodes", "movielens",
		   "mondial_geo", "legislator", "regional_sales", "world_development_indicators", "food_inspection_2"
	, "retail_world", "citeseer", "computer_student", "college_completion", "synthea", "book_publishing_company",
		   "trains", "retails", "soccer_2016", "law_episode", "food_inspection", "european_football_1",
		   "mental_health_survey"
	, "hockey", "public_review_platform", "retail_complains", "ice_hockey_draft", "menu", "cs_semester", "beer_factory",
		   "cars", "genes", "shakespeare", "image_and_language", "disney", "music_tracker", "works_cycles"
	, "movie_platform", "books", "social_media", "restaurant", "superstore", "address", "chicago_crime",
		   "professional_basketball", "coinmarketcap", "movies_4", "sales_in_weather", "app_store", "craftbeer",
		   "movie", "world", "movie_3"]


def set_retriever_index():
	try:
		HOST_NAME = os.getenv('ES_HOSTNAME')
	except BaseException:
		raise ValueError(
			"You need to set the env var ES_HOSTNAME to use the retrievers."
		)
	try:
		USERNAME = os.getenv('ES_USERNAME')
	except BaseException:
		raise ValueError(
			"You need to set the env var ES_USERNAME to use the retrievers."
		)
	try:
		PASSWORD = os.getenv('ES_PASSWORD')
	except BaseException:
		raise ValueError(
			"You need to set the env var ES_PASSWORD to use the retrievers."
		)
	try:
		CERT = os.getenv('ES_CERT_PATH')
	except BaseException:
		raise ValueError(
			"You need to set the env var ES_CERT_PATH which points to the certificate to use the retrievers."
		)
	retriever = ElserRetriever(host_name=HOST_NAME, username=USERNAME, password=PASSWORD, cert=CERT)
	return retriever


def make_retriever(index_name: str = "api-before-rag"):
	def retriever_clapnq_domain(query: str) -> dict:
		retriever = set_retriever_index()
		docs_lst = retriever.retrieve_passages(query, ES_CONFIG["top_k"], index_name=index_name)[
			1]  # Currently kept constant to api-before-rag will be updated to clapnq-{domain_name}
		observation = []
		for item in docs_lst:
			observation.append({
				"id": item["document_id"],
				"text": item["text"],  # TODO: Add relevancy score as a field
			})
		return {"documents": observation}  # TODO: Add the 'error' key in case of any error/failure
	
	return retriever_clapnq_domain

# for i in DOMAINS:
#     func_name = f"retriever_clapnq_{i}"
#     globals()[func_name] = make_retriever("api-before-rag")  # Functions added to global scope REPLACE domain name by clapnq-{domain_name}
#
# if __name__ == '__main__':
#     rag_docs=retriever_clapnq_airline(query="I am able to retrieve documents related to snow white the cartoon")
