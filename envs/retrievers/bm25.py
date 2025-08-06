# codebase attribution
# Sara Rosenthal's commit to https://github.ibm.com/conversational-ai/model-runner/blob/main/src/retrievers/bm25.py

from envs.retrievers.elastic import Elastic


class BM25Retriever(Elastic):
    def __init__(self,
                 host_name: str,
                 username: str,
                 password: str,
                 cert: str,
                 ):
        super(BM25Retriever, self).__init__(
            host_name=host_name,
            username=username,
            password=password,
            cert=cert,
        )

    def get_query(self, text):
        query = {
            "bool": {
                "must": {"multi_match": {"query": text, "fields": ["text"]}},
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
