from dotenv import load_dotenv
from envs.retrievers import m3_retrievers
from pathlib import Path
import json
import copy
import argparse
from extras import chunking
from tqdm import tqdm
load_dotenv()

RETRIEVER_TOP_K = 10

def read_json_files(data_dir):
    data_path = Path(data_dir)
    
    if not data_path.is_dir():
        raise ValueError(f"Invalid directory: {data_path}")
    
    json_data = []
    for json_file in data_path.glob("*.json"):
        with open(json_file, 'r') as f:
            json_data.append(json.load(f))
    
    return json_data

def process_json_files(input_dir: str, output_dir: str, topk: int, domain: str):
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    if not input_path.exists() or not input_path.is_dir():
        raise ValueError(f"Invalid input directory: {input_dir}")
    
    output_path.mkdir(parents=True, exist_ok=True)
    
    if domain:
        json_files = list(input_path.glob(f"{domain}_*.json"))
        if not json_files:
            print(f"No JSON files found matching pattern '{domain}_*.json' in {input_dir}")
            return
    else:
        json_files = list(input_path.glob("*.json"))
        if not json_files:
            print(f"No JSON files found in {input_dir}")
            return
    
    for json_file in tqdm(json_files):
        print(f"Processing: {json_file.name}")
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        if isinstance(data, dict):
            data = [data]
        
        try:
            processed_data = chunk_raw_input(data, topk)
        except Exception as e:
            print(f"ERROR: {json_file} has an error {str(e)}")
            continue
        
        output_filename = json_file.stem + "_chunked.json"
        output_file_path = output_path / output_filename
        with open(output_file_path, 'w') as f:
            json.dump(processed_data, f, indent=4)
            
def query_retriever(retriever, query: str, answer: str, index_name: str, topk: int, raw_chunks: list):
    """Queries the retriever to get 'topk' samples including the chunks already in the raw data
    Discards duplicate samples/chunks
    Note: If the raw chunks are already >topk, we retain all of them.
    Returns dictionary with 2 keys -- first containing list of chunks and gold_chunk boolean after scoring
    second containing an error boolean whether or not a gold chunk was found in the topk chunks
    """
    retrieved_lst = retriever.retrieve_passages(query, RETRIEVER_TOP_K, index_name=index_name)[1]
    raw_chunks = [chunk.strip() for chunk in raw_chunks]
    consolidated_chunks = copy.deepcopy(raw_chunks)
    current_topk = len(raw_chunks)
    print(f"Query: {query}")
    print("CURRENT TOPK")
    print(current_topk)
    print(f"Existing chunk length: {len(consolidated_chunks)}")
    for doc in retrieved_lst:
        if current_topk >= topk:
            break
        if doc['text'].strip() not in raw_chunks:
            consolidated_chunks.append(doc['text'].strip())
            current_topk += 1
    print(f"Final chunk length {len(consolidated_chunks)}")
    
    chunk_indices = chunking.score_chunks(
        chunks=consolidated_chunks,
        query=query,
        answer=answer
    )
    chunk_indices_set = set(chunk_indices)
    chunk_info = {}
    chunk_info["chunks"] = [
        {"text": chunk, "gold_chunk": i in chunk_indices_set}
        for i, chunk in enumerate(consolidated_chunks)
    ]
    chunk_info["chunks_error"] = not chunk_indices
    return chunk_info


def chunk_raw_input(data: list, topk: int):
    retriever = m3_retrievers.set_retriever_index()
    tokenizer = chunking.get_tokenizer() 
    for sample in tqdm(data, desc="Samples in file"):
        for turn_idx, turn in enumerate(sample["turns"]):
            if "RAG" in turn["type"]:
                turn_gold_seq = turn['gold_sequence']
                for hop_idx, hop in enumerate(turn_gold_seq):
                    if hop["question_type"] == "RAG":
                        collection = 'clapnq-' + hop['db_id'].replace('_', '-')
                        text = "\n".join(hop["OUTPUT_AFTER_EXECUTING_API"])
                        chunks, positions = chunking.split_text(
                                                tokenizer=tokenizer,
                                                text=text
                                                )
                        chunk_info = query_retriever(
                            retriever=retriever,
                            query=hop["question"],
                            answer=hop["answer"],
                            index_name=collection,
                            topk=topk,
                            raw_chunks=chunks
                        )
                        hop['chunk_info'] = chunk_info
    return data
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_dir', '-i', required=True, 
                       help='Path to raw data folder from root directory')
    parser.add_argument('--output_dir', '-o', required=True, 
                       help='Path to output folder directory to write to')
    parser.add_argument('--topk', type=int, default=5, 
                       help='Number of chunks to store in raw data')
    parser.add_argument('--domain', '-d', help="Specific domain to run chunking for")
    
    args = parser.parse_args()
    process_json_files(args.input_dir, args.output_dir, args.topk, args.domain)

if __name__ == "__main__":
    main()


