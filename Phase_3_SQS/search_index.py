from whoosh.index import open_dir
from whoosh.qparser import QueryParser
from whoosh import scoring
import os

# Path to the index directory
INDEX_DIR = "Phase_3/indexdir"

def search_index():
    # Check if the index directory exists and is not empty
    if not os.path.exists(INDEX_DIR) or not os.listdir(INDEX_DIR):
        print("[ERROR] Whoosh index directory not found or is empty.")
        return

    # Open the existing Whoosh index
    ix = open_dir(INDEX_DIR)

    # Create a query parser for the 'content' field
    parser = QueryParser("content", schema=ix.schema)

    # Use BM25F scoring for better relevance (optional)
    with ix.searcher(weighting=scoring.BM25F()) as searcher:
        while True:
            query_text = input("\n🔍 Enter search query (or type 'exit'): ").strip()
            if query_text.lower() in ["exit", "quit"]:
                print("Exiting search.")
                break

            try:
                query = parser.parse(query_text)
                results = searcher.search(query, limit=10)

                print(f"\n📄 Found {len(results)} result(s):")
                for i, hit in enumerate(results):
                    print(f"\nResult #{i+1}")
                    print(f"URL: {hit['url']}")
                    print(f"Snippet: {hit.highlights('content', top=2)}")

            except Exception as e:
                print(f"[ERROR] Failed to search: {e}")

if __name__ == "__main__":
    search_index()