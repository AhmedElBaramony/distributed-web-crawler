# search_index_csv.py

import csv

def load_index_from_csv(filename="index.csv"):
    """Load the index from a CSV file into a dictionary."""
    index = {}
    try:
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                word = row['Word'].lower()
                url = row['URL']
                if word not in index:
                    index[word] = set()
                index[word].add(url)
    except FileNotFoundError:
        print("❌ index.csv not found! Make sure the index file exists.")
        return None
    return index

def search_word(index, word):
    """Search for a word in the index and return URLs."""
    word = word.lower()
    return index.get(word, set())

def main():
    print("🔎 Loading the index from CSV...")
    index = load_index_from_csv()
    
    if index is None:
        return

    print("✅ Index loaded successfully!")
    print(f"Total keywords indexed: {len(index)}")
    print("\nType a word to search for URLs. Type 'exit' to quit.\n")

    while True:
        query = input("🔍 Enter search word: ").strip()
        if query.lower() == 'exit':
            print("👋 Exiting search.")
            break
        if not query:
            print("⚠️ Please enter a word.")
            continue

        results = search_word(index, query)
        if results:
            print(f"✅ Word '{query}' found in {len(results)} page(s):")
            for url in results:
                print(f" - {url}")
        else:
            print(f"❌ Word '{query}' not found in any page.")

if __name__ == "__main__":
    main()
