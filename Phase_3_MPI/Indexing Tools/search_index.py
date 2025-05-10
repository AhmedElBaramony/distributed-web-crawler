from whoosh import index
from whoosh.qparser import QueryParser, MultifieldParser
from whoosh import scoring
from whoosh.highlight import HtmlFormatter, ContextFragmenter
import os
from datetime import datetime

def format_timestamp(timestamp):
    if isinstance(timestamp, datetime):
        return timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return "Unknown"

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
        # Create a multifield parser that searches both title and content
        parser = MultifieldParser(["title", "content"], schema=ix.schema)
        
        # Configure highlighting
        formatter = HtmlFormatter(between="...")
        fragmenter = ContextFragmenter(maxchars=200, surround=50)

        print("✅ Index loaded successfully!")
        print(f"📚 Available fields: {list(ix.schema.names())}")
        print("\nSearch Tips:")
        print("- Use AND/OR/NOT for boolean search (e.g., 'python AND web')")
        print("- Use quotes for phrase search (e.g., '\"web crawler\"')")
        print("- Use wildcards (e.g., 'web*' for web, website, etc.)")
        print("- Use field:value syntax (e.g., 'title:python')")
        print("- Type 'exit' to quit\n")

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
                results = searcher.search(query, limit=20, scored=True)
                
                if results:
                    print(f"\n✅ Found {len(results)} result(s):")
                    for hit in results:
                        print(f"\n📌 URL: {hit['url']}")
                        print(f"📝 Title: {hit['title']}")
                        print(f"⏰ Indexed: {format_timestamp(hit['timestamp'])}")
                        
                        # Highlight matching text
                        if 'content' in hit:
                            highlighted = hit.highlights("content", 
                                                       text=hit['content'],
                                                       fragmenter=fragmenter,
                                                       formatter=formatter)
                            if highlighted:
                                print(f"📄 Snippet: {highlighted}")
                        
                        print("-" * 80)
                else:
                    print("❌ No results found.")
                    
            except Exception as e:
                print(f"❌ Error parsing query: {e}")

if __name__ == "__main__":
    main()