import os

def reconstruct_article(output_file, reconstructed_file):
    """Reconstruct the article from MapReduce output."""
    index_word_map = {}

    # Read MapReduce output
    with open(output_file, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) == 2:
                try:
                    index = int(parts[0])
                    word = parts[1]
                    index_word_map[index] = word
                except ValueError:
                    continue

    # Sort by index and reconstruct text
    sorted_words = [index_word_map[i] for i in sorted(index_word_map.keys())]

    # Write the reconstructed article
    with open(reconstructed_file, 'w', encoding='utf-8') as f:
        f.write(" ".join(sorted_words) + "\n")

    print(f"✅ Reconstructed article saved to: {reconstructed_file}")

# Paths (Modify as needed)
mapreduce_output_file = "merged_output.txt"  # Output from Reduce task
reconstructed_article_file = "reconstructed_article.txt"  # Final article

# Run reconstruction
reconstruct_article(mapreduce_output_file, reconstructed_article_file)

