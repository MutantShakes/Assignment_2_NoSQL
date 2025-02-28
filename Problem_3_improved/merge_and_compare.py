import os
import re

def find_latest_wikipedia_article(wiki_dir):
    """Find the Wikipedia article with the highest docID."""
    latest_file = None
    max_doc_id = -1

    for file in os.listdir(wiki_dir):
        match = re.match(r"(\d+)\.txt", file)  # Match docID.txt pattern
        if match:
            doc_id = int(match.group(1))
            if doc_id > max_doc_id:
                max_doc_id = doc_id
                latest_file = os.path.join(wiki_dir, file)

    return latest_file, max_doc_id

def merge_hadoop_output(output_dir, merged_file):
    """Merge all Hadoop output files into a single output file."""
    with open(merged_file, 'w', encoding='utf-8') as out_f:
        for file in sorted(os.listdir(output_dir)):
            if file.startswith("part-"):
                with open(os.path.join(output_dir, file), 'r', encoding='utf-8') as in_f:
                    out_f.write(in_f.read())

def index_wikipedia_article(wiki_file):
    """Index words in the Wikipedia article using 0-based indexing."""
    indexed_words = {}
    with open(wiki_file, 'r', encoding='utf-8') as f:
        words = f.read().split()
        for i, word in enumerate(words):
            indexed_words[i] = word
    return indexed_words

def compare_with_latest(merged_file, latest_wiki_file):
    """Compare the merged Hadoop output with the latest Wikipedia article."""
    wiki_words = index_wikipedia_article(latest_wiki_file)

    with open(merged_file, 'r', encoding='utf-8') as f:
        merged_lines = f.readlines()

    discrepancies = []
    for line in merged_lines:
        parts = line.strip().split("\t")
        if len(parts) != 2:
            continue

        try:
            index = int(parts[0])
            extracted_word = parts[1].strip()

            if index in wiki_words:
                wiki_word = wiki_words[index]
                if extracted_word != wiki_word:
                    discrepancies.append((index, extracted_word, wiki_word))
        except ValueError:
            continue

    print("\n🔹 Comparison Results 🔹")
    if discrepancies:
        print(f"\n⚠️ {len(discrepancies)} discrepancies found!")
        for idx, extracted, expected in discrepancies[:10]:  # Show first 10 mismatches
            print(f"Index {idx}: Expected '{expected}', Found '{extracted}'")
    else:
        print("✅ No discrepancies found! Output matches the latest Wikipedia article.")

# Paths (Modify as needed)
wiki_dir = "Wikipedia-EN-20120601_ARTICLES"  # Directory containing Wikipedia .txt files
hadoop_output_dir = "prob_3_output"  # Directory containing Hadoop part-* files
merged_output_file = "merged_output.txt"

# Step 1: Find latest Wikipedia article
latest_wiki_file, max_doc_id = find_latest_wikipedia_article(wiki_dir)
print(f"✅ Latest Wikipedia article: {latest_wiki_file} (docID: {max_doc_id})")

# Step 2: Merge Hadoop output files
merge_hadoop_output(hadoop_output_dir, merged_output_file)
print(f"✅ Merged Hadoop output saved to: {merged_output_file}")

# Step 3: Compare the merged output with the latest Wikipedia article
compare_with_latest(merged_output_file, latest_wiki_file)
