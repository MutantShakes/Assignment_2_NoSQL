def compare_articles(wiki_file, reconstructed_file):
    """Compares the latest Wikipedia article with the reconstructed article from MapReduce output."""
    
    # Read Wikipedia article (tokenized with 0-based indexing)
    with open(wiki_file, 'r', encoding='utf-8') as f:
        wiki_words = f.read().split()

    # Read reconstructed article
    with open(reconstructed_file, 'r', encoding='utf-8') as f:
        reconstructed_words = f.read().split()

    min_length = min(len(wiki_words), len(reconstructed_words))

    # Compare words index-by-index
    for i in range(min_length):
        if wiki_words[i] != reconstructed_words[i]:
            print(f"❌ Mismatch at index {i}: Expected '{wiki_words[i]}', Found '{reconstructed_words[i]}'")

    # If the reconstructed article is longer than the Wikipedia article
    if len(reconstructed_words) > len(wiki_words):
        print(f"⚠️ Extra words in reconstructed article starting from index {min_length}: {' '.join(reconstructed_words[min_length:])}")

    # If the Wikipedia article is longer
    elif len(wiki_words) > len(reconstructed_words):
        print(f"⚠️ Missing words in reconstructed article starting from index {min_length}: {' '.join(wiki_words[min_length:])}")

    else:
        print("✅ Articles match perfectly!")

# Paths (Modify as needed)
latest_wiki_article = "./Wikipedia-EN-20120601_ARTICLES/567579.txt"  # Latest Wikipedia article
reconstructed_article = "reconstructed_article.txt"  # Reconstructed from MapReduce output

# Run comparison
compare_articles(latest_wiki_article, reconstructed_article)

