import random

def generate_large_dataset(file_path, num_rows):
    keys = [f"key_{i}" for i in range(1000)]  # 1000 unique keys
    with open(file_path, 'w') as f:
        for _ in range(num_rows):
            key = random.choice(keys)
            value = random.uniform(0, 1000)
            f.write(f"{key},{value}\n")

# Generate a dataset with 1 billion rows
generate_large_dataset('large_dataset.txt', 1_000_000_000)

# Generate a dataset with 1 billion rows
# generate_large_dataset('small_dataset.txt', 1_000_000)