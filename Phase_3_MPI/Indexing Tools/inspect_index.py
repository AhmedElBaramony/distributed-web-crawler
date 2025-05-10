from whoosh import index
import os

index_dir = "Phase_3_MPI/indexdir"

if not os.path.exists(index_dir):
    print("❌ Index directory does not exist.")
    exit()

if not index.exists_in(index_dir):
    print("❌ Whoosh index not found in directory.")
    exit()

ix = index.open_dir(index_dir)

with ix.searcher() as searcher:
    # Number of documents
    num_docs = searcher.doc_count()
    num_docs_all = searcher.doc_count_all()

    # Unique indexed terms in the 'content' field
    unique_terms = list(searcher.lexicon("content"))
    num_unique_words = len(unique_terms)

    print(f"✅ Whoosh index found in '{index_dir}'")
    print(f"📄 Live documents indexed: {num_docs}")
    print(f"📑 All documents (including deleted): {num_docs_all}")
    print(f"🔤 Total unique words indexed: {num_unique_words}")