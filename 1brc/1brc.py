import multiprocessing as mp
import os
import mmap
import time
from collections import defaultdict

# Constants
# FILE_PATH = 'large_dataset.txt'
# CHUNK_SIZE = 10_000_000  # Number of rows per chunk

FILE_PATH = 'small_dataset.txt'
CHUNK_SIZE = 100_000  # Number of rows per chunk

NUM_PROCESSES = os.cpu_count()  # Use all available CPU cores

def process_chunk(chunk):
    """
    Process a chunk of rows and return a dictionary of aggregated results.
    """
    result = defaultdict(float)
    for line in chunk:
        key, value = line.strip().split(',')
        result[key] += float(value)
    return result

def chunk_generator(file_path, chunk_size):
    """
    Generator function to yield chunks of rows from the file.
    """
    with open(file_path, 'r+') as f:
        mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        while True:
            lines = []
            for _ in range(chunk_size):
                line = mmapped_file.readline()
                if not line:
                    break
                lines.append(line.decode('utf-8'))
            if not lines:
                break
            yield lines

def merge_results(results):
    """
    Merge partial results from multiple processes into a single dictionary.
    """
    final_result = defaultdict(float)
    for result in results:
        for key, value in result.items():
            final_result[key] += value
    return final_result

def main():
    start_time = time.time()

    # Create a pool of worker processes
    with mp.Pool(processes=NUM_PROCESSES) as pool:
        # Process chunks in parallel
        partial_results = pool.map(process_chunk, chunk_generator(FILE_PATH, CHUNK_SIZE))

    # Merge results from all processes
    final_result = merge_results(partial_results)

    # Print the final result
    for key, value in final_result.items():
        print(f"{key}: {value}")

    print(f"Processing completed in {time.time() - start_time:.2f} seconds.")

if __name__ == '__main__':
    main()