from langchain.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.vectorstores import Chroma
"""
set OPENAI_API_KEY
TODO: enhance the pipeline to use other providers and other models
"""
class ChromaDBBasedRetrieverTool:
    def __init__(self, file_paths, chunk_size=1500, chunk_overlap=150, persist_directory="chroma"):
        self.file_paths = file_paths
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.persist_directory = persist_directory
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap
        )
        self.embedding = OpenAIEmbeddings()

    def load_documents(self):
        """Load documents from the file paths provided."""
        loaders = [PyPDFLoader(file) for file in self.file_paths]
        docs = []
        for loader in loaders:
            docs.extend(loader.load())
        return docs

    def split_documents(self, docs):
        """Split the documents into smaller chunks."""
        return self.text_splitter.split_documents(docs)

    def embed_and_store(self, splits):
        """Embed the document chunks and store them in a Chroma vector database."""
        vectordb = Chroma.from_documents(
            documents=splits,
            embedding=self.embedding,
            persist_directory=self.persist_directory
        )
        return vectordb

    def retrieve_similar_docs(self, vectordb, query, k=3):
        """Retrieve similar documents based on a query."""
        return vectordb.similarity_search(query, k=k)

    def run(self, query, k=3):
        print("hello")
        docs = self.load_documents()

        splits = self.split_documents(docs)

        vectordb = self.embed_and_store(splits)

        results = self.retrieve_similar_docs(vectordb, query, k=k)
        
        return results

def run():
    # Example usage
    file_paths = ["data/Employee-Handbook.pdf"]
    rag_pipeline = ChromaDBBasedRetrieverTool(file_paths)

    # Query the pipeline
    question = "is there an email I can ask for help?"
    results = rag_pipeline.run(question, k=3)

    # Print the results
    for idx, doc in enumerate(results):
        print(f"Result {idx + 1}: {doc.page_content}")

# uncomment if needed
# run()