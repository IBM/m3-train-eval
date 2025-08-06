from envs.retrievers.elastic import Elastic
import random

random.seed(123)


class RandomRetriever(Elastic):
    def __init__(
            self,
            host_name: str,
            username: str,
            password: str,
            cert: str
    ):
        super(RandomRetriever, self).__init__(
            host_name=host_name, username=username,
            password=password, cert=cert,
        )

    def get_query(self, text):
        random_value = random.randint(1, 1000000)
        query = {
            "size": 1,
            "query": {
                "function_score": {
                    "functions": [
                        {
                            "random_score": {
                                "seed": random_value
                            }
                        }
                    ],
                    "score_mode": "sum",
                }
            }
        }

        return query

    def search(self, query, top_k, index_name):
        query["size"] = top_k
        res = self.client.search(index=index_name, body=query)
        return res


if __name__ == "__main__":
    import json

    with open('config.json') as json_data:
        d = json.load(json_data)
    es_config = d["es_config"]
    index_name = "askhr-v2-appr"
    retriever = RandomRetriever(**es_config)
    top_k = 10
    q = retriever.get_query("")
    res = retriever.search(q, 1, index_name)
    q = retriever.get_query("")
    res1 = retriever.search(q, 1, index_name)
    q = retriever.get_query("")
    res2 = retriever.search(q, 1, index_name)
    print(res)
    print(res1)
    print(res2)
    for rank, r in enumerate(res.body["hits"]["hits"]):
        item = {
            "document_id": r["_id"],
            "source": r["_source"]["productId"],
            "score": r["_score"],
            "text": r["_source"]["text"],
            "title": r["_source"]["title"],
        }

        print(item)
