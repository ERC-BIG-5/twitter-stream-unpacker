import matplotlib.pyplot as plt
import numpy as np

from src.consts import BASE_STAT_PATH
from src.util import load_data

# Sample data (replace this with your actual data)
hashtag_freq = load_data(BASE_STAT_PATH / "hashtags_en.json")

# Extract frequencies from the dictionary
frequencies = list(hashtag_freq.values())

CUSTOM_BINS = True

if CUSTOM_BINS:
    # Define custom bins
    bins = [0, 5, 50, 500, 2000, 5000, 8000, 10000,15000]
    # Count frequencies in each bin
    counts, _ = np.histogram(frequencies, bins=bins)

    # Create the bar plot
    plt.figure(figsize=(12, 6))
    bar_positions = range(len(counts))
    bars = plt.bar(bar_positions, counts, align='center', edgecolor='black')
    # Create custom x-tick labels
    tick_labels = [f'{bins[i]}-{bins[i+1]}' for i in range(len(bins)-1)]
    plt.xticks(bar_positions, tick_labels, rotation=45, ha='right')
else:
    # Create the histogram
    plt.figure(figsize=(10, 6))
    counts, bins, _ = plt.hist(frequencies, bins=10, edgecolor='black')


plt.yscale('log')

# Customize the plot
plt.title('Distribution of Hashtag Frequencies')
plt.xlabel('Frequency')
plt.ylabel('Number of Hashtags')

# Add a grid for better readability
# plt.grid(True, linestyle='--', alpha=0.7)


# If you want to save the plot instead of showing it:
# plt.savefig('hashtag_frequency_histogram.png')

# Print the number of hashtags in each bin
print("\nNumber of hashtags in each frequency range:")
# Annotate each bar with its count and range
for i, (bar, count) in enumerate(zip(bars, counts)):
    if count > 0:  # Only annotate non-zero bars
        print(f"Frequency range {bins[i]}-{bins[i + 1]}: {int(count)} hashtags")

        if CUSTOM_BINS:
            plt.text(i, count, f'{int(count)}', ha='center', va='bottom')


# Calculate and print total number of hashtags
total_hashtags = sum(counts)
print(f"\nTotal number of hashtags: {int(total_hashtags)}")


# Show the plot
plt.tight_layout()
plt.show()
