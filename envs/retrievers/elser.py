import json

from envs.retrievers.elastic import 
import os


class ElserRetriever(Elastic):
    def __init__(self,
                 host_name: str,
                 username: str,
                 password: str,
                 cert: str,
                 **kwargs):
        super(ElserRetriever, self).__init__(host_name=host_name, username=username, password=password, cert=cert)

    def get_query(self, text, domains: list[str] = None, exclude_domains: list[str] = None):
        query = {
            "bool": {
                "must": {
                    "text_expansion": {
                        "ml.tokens": {
                            "model_id": ".elser_model_2",
                            "model_text": text
                        }
                    }
                }
            }
        }
        if domains:
            query["bool"]["filter"] = {
                "terms": {
                    "productId": [domain for domain in domains]
                }
            }
        if exclude_domains:
            query["bool"]["must_not"] = {
                "terms": {
                    "productId": [domain for domain in exclude_domains]
                }
            }
        return query

    def search(self, query, top_k, index_name):
        res = self.client.search(
            index=index_name,
            query=query,
            size=top_k,
        )
        return res


if __name__ == '__main__':
    es_cert_path = "../../es_cert"
    es_config = {
        "username": os.getenv("ES_USERNAME", None),
        "password": os.getenv("ES_PASSWORD", None),
        "cert": es_cert_path,
        "host_name": os.getenv("ES_HOSTNAME", None),
        "index_name": "api-before-rag",
        "top_k": 1
    }
    doc_db = ElserRetriever(**es_config)
    retrieval_query = "Which element, represented by 'c', is known for its ability to form four covalent bonds?"
    document_text, contexts = doc_db.retrieve_passages(retrieval_query, es_config["top_k"], es_config["index_name"])
    print(document_text)
    print(json.dumps(contexts, indent=2))