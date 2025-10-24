"""
Optimized dataset loading with significant speed improvements.
Key optimizations:
- Batch processing with progress tracking
- Memory-efficient streaming
- Parallel processing where possible
- Reduced I/O operations
"""
import os
from dotenv import load_dotenv
from huggingface_hub import login
from datasets import load_dataset
import csv
import time
from concurrent.futures import ThreadPoolExecutor
import threading

load_dotenv()
HF_TOKEN = os.getenv("HF_TOKEN")

def process_dataset_streaming(dataset_name, output_file, repo_key, hash_key, dataset_version=None):
    """Process dataset with streaming and batch writing for speed."""
    print(f"Loading {dataset_name}...")
    
    # Load dataset
    if dataset_version:
        dataset = load_dataset(dataset_name, dataset_version, streaming=True)
    else:
        dataset = load_dataset(dataset_name, streaming=True)
    
    batch_size = 1000  # Process in batches for memory efficiency
    batch_data = []
    total_processed = 0
    
    start_time = time.monotonic()
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["repo_name", "commit_hash"])
        
        for row in dataset['train']:
            batch_data.append([row[repo_key], row[hash_key]])
            total_processed += 1
            
            # Write in batches for speed
            if len(batch_data) >= batch_size:
                writer.writerows(batch_data)
                batch_data = []
                
                # Progress update
                if total_processed % 10000 == 0:
                    elapsed = time.monotonic() - start_time
                    rate = total_processed / elapsed
                    print(f"  Processed {total_processed:,} items at {rate:.0f} items/sec")
        
        # Write remaining data
        if batch_data:
            writer.writerows(batch_data)
    
    elapsed = time.monotonic() - start_time
    rate = total_processed / elapsed
    print(f"Completed {dataset_name}: {total_processed:,} items in {elapsed:.2f}s ({rate:.0f} items/sec)")
    return total_processed

def main():
    """Optimized main function with parallel processing."""
    start_time = time.monotonic()
    
    login(token=HF_TOKEN)
    
    print("Starting optimized dataset processing...")
    
    # Process both datasets in parallel for maximum speed
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Submit both tasks
        v1_future = executor.submit(
            process_dataset_streaming,
            "bigcode/the-stack-dedup",
            'starcoder_v1_repos.csv',
            "max_stars_repo_name",
            "max_stars_repo_head_hexsha"
        )
        
        v2_future = executor.submit(
            process_dataset_streaming,
            "bigcode/the-stack-v2-dedup",
            'starcoder_v2_repos.csv',
            "repo_name",
            "revision_id",
            "default"
        )
        
        # Wait for both to complete
        v1_count = v1_future.result()
        v2_count = v2_future.result()
    
    total_time = time.monotonic() - start_time
    print(f"\n=== OPTIMIZATION RESULTS ===")
    print(f"V1 repos processed: {v1_count:,}")
    print(f"V2 repos processed: {v2_count:,}")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Combined processing rate: {(v1_count + v2_count) / total_time:.0f} items/sec")

if __name__ == "__main__":
    main()
