# codebase attribution
# Sara Rosenthal's codebase commit in https://github.ibm.com/conversational-ai/model-runner/blob/main/src/retrievers/elastic.py

from abc import ABC, abstractmethod
import re
from elasticsearch import Elasticsearch


class Elastic(ABC):
    @abstractmethod
    def __init__(
            self,
            host_name: str,
            username: str,
            password: str,
            cert: str,
    ):
        client = Elasticsearch(
            host_name,
            ca_certs=cert,
            basic_auth=(username, password)
        )
        # client.cluster.put_settings(body={
        #     "persistent": {
        #         "indices.id_field_data.enabled": True
        #     }
        # })

        self.client = client
        try:
            res = self.client.info()
        except Exception as e:
            print(f"Error: {e}")
            raise e

    @abstractmethod
    def search(self, query, top_k, index_name):
        pass

    @abstractmethod
    def get_query(self, text):
        pass

    def check_for_duplicate(self, docu1, docu2) -> str:
        doc1_docs = [
            d.strip() for d in re.split("document_\d+", docu1.strip()) if len(d) > 0
        ]
        doc2_docs = [
            d.strip() for d in re.split("document_\d+", docu2.strip()) if len(d) > 0
        ]
        new_docs = docu1
        count = len(doc1_docs)

        for doc2 in doc2_docs:
            if doc2 in docu1:
                continue
            else:
                count += 1
                new_docs += "\n"
                new_docs += "document_"
                new_docs += str(count)
                new_docs += "\n"
                new_docs += doc2
        return new_docs.strip()

    def retrieve_passages(self, query, topk, index_name):
        processed_query = self.get_query(query)

        res = self.search(processed_query, topk, index_name)
        contexts = []
        outputs = []

        hits = res.body['hits']['hits']
        for rank, item in enumerate(hits):
            text = item["_source"]["text"]
            same = 0
            for docu in outputs:
                if docu == text:
                    same = 1
            if same == 0:
                outputs.append(text)
                contexts.append(
                    {
                        "document_id": item["_id"],
                        "source": item["_source"]["productId"],
                        "score": item["_score"],
                        "text": item["_source"]["text"],
                        "title": item["_source"]["title"],
                    }
                )
            document_text = ""

            for i, doc in enumerate(contexts):
                text = doc["text"]
                text = re.sub("\n+", "\n", text)
                docid = i + 1
                document_text += "document_"
                document_text += str(docid)
                document_text += "\n"
                document_text += text
                document_text += "\n"

            document_text = document_text.strip()

        return document_text, contexts
