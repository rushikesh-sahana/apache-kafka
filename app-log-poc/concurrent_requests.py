import urllib.request
import urllib.error
import concurrent.futures
import time

URL = "http://localhost:8000/produce-logs"
NUM_REQUESTS = 50

def make_request(request_id):
    try:
        # We use a POST or GET depending on how the endpoint is defined in main.py. 
        # Usually, simple endpoints are GET. If it requires POST, we'll need to pass data.
        # Assuming GET based on previous curl commands.
        req = urllib.request.Request(URL, method="GET")
        start_time = time.time()
        
        with urllib.request.urlopen(req) as response:
            status = response.status
            body = response.read().decode('utf-8')
            elapsed = time.time() - start_time
            return f"Request {request_id:02d} | Status: {status} | Time: {elapsed:.3f}s | Response: {body}"
    except urllib.error.URLError as e:
        return f"Request {request_id:02d} | Error: {e}"
    except Exception as e:
        return f"Request {request_id:02d} | Unexpected Error: {e}"

def main():
    print(f"Starting {NUM_REQUESTS} concurrent requests to {URL}...")
    start_time = time.time()
    
    # Use ThreadPoolExecutor to run requests concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        # Submit all tasks to the executor
        futures = {executor.submit(make_request, i): i for i in range(1, NUM_REQUESTS + 1)}
        
        # As they complete, print the result
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                print(result)
            except Exception as e:
                print(f"Task generated an exception: {e}")

    total_time = time.time() - start_time
    print(f"\nAll {NUM_REQUESTS} requests completed in {total_time:.3f} seconds.")

if __name__ == "__main__":
    main()
