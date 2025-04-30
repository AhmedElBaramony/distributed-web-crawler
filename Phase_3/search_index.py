from whoosh import index
from whoosh.qparser import QueryParser
import os

def main():
    index_dir = "indexdir"

    if not os.path.exists(index_dir):
        print("❌ Whoosh index not found. Make sure 'indexdir/' exists.")
        return

    print("🔎 Loading Whoosh index...")

    try:
        ix = index.open_dir(index_dir)
    except Exception as e:
        print(f"❌ Error opening index: {e}")
        return

    with ix.searcher() as searcher:
        parser = QueryParser("content", schema=ix.schema)

        print("✅ Index loaded successfully!")
        print(f"📚 Available fields: {list(ix.schema.names())}")
        print("Type a search query (use AND/OR, wildcards, quotes). Type 'exit' to quit.\n")

        while True:
            query_str = input("🔍 Enter search: ").strip()
            if query_str.lower() == "exit":
                print("👋 Exiting.")
                break
            if not query_str:
                print("⚠️ Please enter a search term.")
                continue

            try:
                query = parser.parse(query_str)
                results = searcher.search(query, limit=20)
                if results:
                    print(f"✅ Found {len(results)} result(s):")
                    for hit in results:
                        print(f" - {hit['url']}")
                else:
                    print("❌ No results found.")
            except Exception as e:
                print(f"❌ Error parsing query: {e}")

if __name__ == "__main__":
    main()