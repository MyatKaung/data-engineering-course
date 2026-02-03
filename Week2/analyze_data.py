import urllib.request
import gzip
import os

def analyze_taxi_data(color, year, month):
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month:02d}.csv.gz"
    print(f"Downloading {url}...")
    
    try:
        # Open URL
        with urllib.request.urlopen(url) as response:
            # Check status? urllib throws URLError on error typically.
            
            # Write to temp file
            temp_filename = f"temp_{color}_{year}_{month}.csv.gz"
            with open(temp_filename, 'wb') as f:
                while True:
                    chunk = response.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
                
        # Now read and measure
        uncompressed_size = 0
        line_count = 0
        
        with gzip.open(temp_filename, 'rb') as f_in:
            for chunk in f_in:
                uncompressed_size += len(chunk)
                line_count += chunk.count(b'\n')
        
        # Cleanup
        os.remove(temp_filename)
        
        return uncompressed_size, line_count - 1 # Subtract header
        
    except Exception as e:
        print(f"Error processing {color} {year}-{month}: {e}")
        return 0, 0

import concurrent.futures

def process_file(args):
    color, year, month = args
    size, rows = analyze_taxi_data(color, year, month)
    return color, year, month, size, rows

def main():
    # Q1: Yellow Taxi 2020-12 uncompressed file size
    print("--- Q1 ---")
    size_q1, rows_q1 = analyze_taxi_data('yellow', 2020, 12)
    print(f"Yellow 2020-12: {size_q1 / 1024 / 1024:.2f} MiB, {rows_q1} rows")

    # Prepare tasks for Q3, Q4, Q5
    tasks = []
    
    # Q3: Yellow 2020 (all months)
    for m in range(1, 13):
        tasks.append(('yellow', 2020, m))
        
    # Q4: Green 2020 (all months)
    for m in range(1, 13):
        tasks.append(('green', 2020, m))
        
    # Q5: Yellow 2021-03
    tasks.append(('yellow', 2021, 3))
    
    results = {}
    
    print("\nStarting parallel processing for Q3, Q4, Q5...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_args = {executor.submit(process_file, args): args for args in tasks}
        for future in concurrent.futures.as_completed(future_to_args):
            args = future_to_args[future]
            try:
                c, y, m, size, rows = future.result()
                results[(c, y, m)] = rows
                print(f"Finished {c} {y}-{m}: {rows} rows")
            except Exception as e:
                print(f"Exception for {args}: {e}")
                
    # Aggregate Q3
    total_yellow_2020 = sum(results.get(('yellow', 2020, m), 0) for m in range(1, 13))
    print(f"\n--- Q3 --- Total Yellow 2020 Rows: {total_yellow_2020}")
    
    # Aggregate Q4
    total_green_2020 = sum(results.get(('green', 2020, m), 0) for m in range(1, 13))
    print(f"\n--- Q4 --- Total Green 2020 Rows: {total_green_2020}")
    
    # Q5
    q5_rows = results.get(('yellow', 2021, 3), 0)
    print(f"\n--- Q5 --- Yellow 2021-03 Rows: {q5_rows}")

if __name__ == "__main__":
    main()
