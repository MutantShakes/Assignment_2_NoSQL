import os
import sys
import re

def merge_output_files(output_dir, merged_file):
    """Merges multiple Hadoop output files into a single file."""
    with open(merged_file, 'w', encoding="utf-8") as outfile:
        for filename in sorted(os.listdir(output_dir)):
            filepath = os.path.join(output_dir, filename)
            if filename.startswith("part-") and os.path.isfile(filepath):
                with open(filepath, 'r', encoding="utf-8") as infile:
                    for line in infile:
                        outfile.write(line)
    print(f"✅ Merged output saved to: {merged_file}")

def find_latest_wikipedia_article(wiki_dir):
    """Finds the latest Wikipedia article based on the highest document ID."""
    latest_doc = None
    latest_doc_id = -1

    for filename in os.listdir(wiki_dir):
        match = re.match(r"(\d+)\.txt$", filename)  # Extract docID from "12345.txt"
        if match:
            doc_id = int(match.group(1))
            if doc_id > latest_doc_id:
                latest_doc_id = doc_id
                latest_doc = filename

    if latest_doc:
        latest_doc_path = os.path.join(wiki_dir, latest_doc)
        print(f"✅ Latest Wikipedia article: {latest_doc} (ID: {latest_doc_id})")
        return latest_doc_path
    else:
        print("❌ No valid Wikipedia article found.")
        return None

def tokenize_article(filepath):
    """Reads and tokenizes an article into a dictionary {index: word}."""
    word_index = {}
    with open(filepath, 'r', encoding="utf-8") as file:
        words = file.read().split()  # Splitting on whitespace to get words
        for i, word in enumerate(words):
            word_index[i] = word
    return word_index

def parse_merged_output(merged_file):
    """Parses the merged output and stores words by index."""
    merged_index = {}
    with open(merged_file, 'r', encoding="utf-8") as file:
        for line in file:
            parts = line.strip().split("\t")  # Assuming tab-separated values
            if len(parts) == 2:
                try:
                    index = int(parts[0])  # Extract index
                    word = parts[1].split(",")[-1].strip()  # Extract word
                    merged_index[index] = word
                except ValueError:
                    continue  # Skip malformed lines
    return merged_index

def compare_indexed_words(wiki_index, merged_index):
    """Compares words at the same index between the Wikipedia article and the merged file."""
    max_index = max(len(wiki_index), len(merged_index))
    
    print("\n🔍 **Comparison Results:**")
    differences_found = False
    tot_differences = 0
    for i in range(max_index):
        wiki_word = wiki_index.get(i, "<No Data>")
        merged_word = merged_index.get(i, "<No Data>")

        if wiki_word != merged_word:
            differences_found = True
            tot_differences+=1
            # print(f"\n❌ **Mismatch at Index {i}:**")
            # print(f"   📜 Wikipedia: {wiki_word}")
            # print(f"   📝 Merged Output: {merged_word}")

    if not differences_found:
        print("\n✅ No differences found! The output matches the Wikipedia article.")
    else:
        print("\n❌ Differences found = ",tot_differences)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 merge_and_compare.py <output_dir> <wiki_articles_dir>")
        sys.exit(1)

    output_dir = sys.argv[1]
    wiki_articles_dir = sys.argv[2]

    merged_file = "merged_output.txt"
    
    # Step 1: Merge all reducer output files
    merge_output_files(output_dir, merged_file)

    # Step 2: Find the most recent Wikipedia article
    latest_article = find_latest_wikipedia_article(wiki_articles_dir)
    if not latest_article:
        sys.exit("❌ No Wikipedia article found. Exiting.")

    # Step 3: Tokenize Wikipedia article and parse merged output
    wiki_index = tokenize_article(latest_article)
    merged_index = parse_merged_output(merged_file)

    # Step 4: Compare words at the same index
    compare_indexed_words(wiki_index, merged_index)

