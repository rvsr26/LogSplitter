import multiprocessing
import random
import time
import re
from collections import defaultdict
from functools import reduce, lru_cache
from typing import List, Dict, Any, Tuple, DefaultDict, Callable

# --- CONFIGURATION ---
LOG_SIZE: int = 5_000_000
NUM_WORKERS: int = multiprocessing.cpu_count()
# Each worker processes an equal chunk of the total log size
CHUNK_SIZE: int = LOG_SIZE // NUM_WORKERS 

IPS: List[str] = [f"192.168.1.{i}" for i in range(1, 50)]
# Increased weighting for 200 to better simulate real-world logs
STATUS_CODES: List[int] = [200] * 10 + [404, 500, 301]

# --- HELPER FUNCTIONS ---

# Use lru_cache to ensure the expensive regex compilation happens only once per worker
@lru_cache(maxsize=1)
def get_log_pattern() -> re.Pattern:
    """Pre-compile the regular expression pattern."""
    # Group 1: IP address (e.g., 192.168.1.1)
    # Group 2: Status code (e.g., 200)
    # The pattern looks for the IP first, then skips to find STATUS: followed by digits.
    return re.compile(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*STATUS:(\d+)")

def generate_log_line(weighted_ips: List[str]) -> str:
    """Generates a single mock log line."""
    ip: str = random.choice(weighted_ips)
    code: int = random.choice(STATUS_CODES)
    timestamp: float = time.time()
    # Using f-string is good, no change needed here.
    return f"{timestamp:.6f} - INFO - {ip} - REQUEST:GET /api/v1/data - STATUS:{code}"

# --- MAPREDUCE STAGES ---

def mapper_function(chunk_size: int) -> Dict[str, DefaultDict[Any, int]]:
    """
    MAP Stage: Generates a log chunk and processes it to count IPs and Status Codes.
    This function is executed by each worker process.
    """
    # Generate data locally within the worker process (more memory efficient for large logs)
    log_pattern: re.Pattern = get_log_pattern()
    weighted_ips: List[str] = IPS + ['10.0.0.1'] * 10
    
    local_ip_counts: DefaultDict[str, int] = defaultdict(int)
    local_code_counts: DefaultDict[str, int] = defaultdict(int)
    
    for _ in range(chunk_size):
        line: str = generate_log_line(weighted_ips)
        match = log_pattern.search(line)
        
        if match:
            # Note: The group(2) (status code) is captured as a string '200', '404', etc.
            ip, code = match.groups()
            
            local_ip_counts[ip] += 1
            local_code_counts[code] += 1
            
    return {'ips': local_ip_counts, 'codes': local_code_counts}

def reducer_function(
    accumulated_result: Dict[str, DefaultDict[Any, int]], 
    new_result: Dict[str, DefaultDict[Any, int]]
) -> Dict[str, DefaultDict[Any, int]]:
    """
    REDUCE Stage: Aggregates the partial results from the mappers.
    Executed sequentially by functools.reduce.
    """
    # Directly update the accumulated dictionaries
    for ip, count in new_result['ips'].items():
        accumulated_result['ips'][ip] += count
        
    for code, count in new_result['codes'].items():
        accumulated_result['codes'][code] += count
        
    return accumulated_result

# --- MAIN EXECUTION ---

def run_map_reduce_job():
    """Main function to orchestrate the parallel MapReduce job."""
    print(f"--- 📊 STARTING LOG ANALYSIS ENGINE ---")
    print(f"Dataset Size: {LOG_SIZE:,} records")
    print(f"Active Workers: {NUM_WORKERS}")
    
    start_global = time.time()

    # The input for the map function is the size of the chunk each worker needs to generate/process.
    chunk_sizes: List[int] = [CHUNK_SIZE] * NUM_WORKERS 
    
    print(f"[Main] Starting Mapping Phase (Generating data and counting)...")
    map_start = time.time()
    
    # Use Pool context manager for guaranteed resource cleanup
    with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
        # pool.map distributes the elements of chunk_sizes to the mapper_function
        mapped_results = pool.map(mapper_function, chunk_sizes)
        
    print(f"[Main] Mapping finished in {time.time() - map_start:.4f}s")
    
    print(f"[Main] Starting Reduction Phase (Aggregating results)...")
    reduce_start = time.time()
    
    # Define the initial state (must be the same structure as the map output)
    initial_state: Dict[str, DefaultDict[Any, int]] = {
        'ips': defaultdict(int), 
        'codes': defaultdict(int)
    }
    
    # Use functools.reduce to sequentially merge the mapped results
    final_result = reduce(reducer_function, mapped_results, initial_state)
    
    print(f"[Main] Reduction finished in {time.time() - reduce_start:.4f}s")
    duration = time.time() - start_global

    # --- RESULTS OUTPUT ---
    
    print("\n" + "=" * 60)
    print(f"✅ ANALYSIS COMPLETE in {duration:.4f} seconds (Total Time)")
    print("=" * 60)
    
    # 1. Top IPs
    sorted_ips: List[Tuple[str, int]] = sorted(
        final_result['ips'].items(), 
        key=lambda x: x[1], 
        reverse=True
    )[:3]
    
    print(f"🥇 TOP 3 SUSPICIOUS IPs (Highest Request Count):")
    for rank, (ip, count) in enumerate(sorted_ips, 1):
        # Using f-strings with commas for large numbers
        print(f"  {rank}. **{ip}** : {count:,} requests")
        
    # 2. System Health
    total_requests: int = sum(final_result['codes'].values())
    error_500: int = final_result['codes'].get('500', 0)
    
    print(f"\n🩺 SYSTEM HEALTH REPORT:")
    print(f"  Total Requests Processed: {total_requests:,}")
    print(f"  Successful (200) Requests: {final_result['codes'].get('200', 0):,}")
    print(f"  Server Errors (500): {error_500:,}")
    
    error_rate: float = (error_500 / total_requests) * 100 if total_requests else 0
    print(f"  **Server Error Rate (500/Total): {error_rate:.2f}%**")
    print("=" * 60)

if __name__ == "__main__":
    # Add a check to ensure MapReduce works properly even if LOG_SIZE is not perfectly divisible
    if LOG_SIZE % NUM_WORKERS != 0:
        print(f"Warning: LOG_SIZE ({LOG_SIZE}) is not perfectly divisible by NUM_WORKERS ({NUM_WORKERS}). Some workers will process fewer logs.")
    run_map_reduce_job()