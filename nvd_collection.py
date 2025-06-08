import requests
import json
import time
from collections import deque

BASE_URL = "https://services.nvd.nist.gov/rest/json/cves/2.0"
PAGE_MAX = 2000
# BASE_URL = "https://services.nvd.nist.gov/rest/json/cvehistory/2.0"
# PAGE_MAX = 5000
OUT_FILE = "cve_data.jsonl"
more_data = True
offset = 0
total_results = 0
MAX_RETRIES = 5  # Number of retry attempts
INITIAL_BACKOFF = 1  # Seconds

# Rate limiting variables (made from NVD's API requirements)
MAX_REQUESTS = 5  # Maximum requests allowed (API key gets up to 50)
TIME_WINDOW = 30  # Time window in seconds
request_times = deque()  # Store timestamps of recent requests

def wait_for_rate_limit():
    """Wait if necessary to comply with rate limit"""
    now = time.time()

    # Remove requests older than the time window
    while request_times and now - request_times[0] >= TIME_WINDOW:
        request_times.popleft()

    # If we've hit the rate limit, wait until we can make another request
    if len(request_times) >= MAX_REQUESTS:
        sleep_time = TIME_WINDOW - (now - request_times[0]) + 0.1  # Add small buffer
        if sleep_time > 0:
            print(f"Rate limit reached. Waiting {sleep_time:.1f} seconds...")
            time.sleep(sleep_time)
            # Clean up old requests after waiting
            now = time.time()
            while request_times and now - request_times[0] >= TIME_WINDOW:
                request_times.popleft()

    request_times.append(now)

with open(OUT_FILE, 'w') as file:
    while more_data:
        retries = 0
        while retries <= MAX_RETRIES:
            try:
                # Wait for rate limit compliance before making request
                wait_for_rate_limit()

                current_url = f"{BASE_URL}?resultsPerPage={PAGE_MAX}&startIndex={offset}"
                page_response = requests.get(current_url)
                page_response.raise_for_status()
                page_data = page_response.json()

                if not total_results:
                    total_results = page_data["totalResults"]

                #total_items = len(page_data["cveChanges"])
                #for change in page_data["cveChanges"]:
                    #file.write(json.dumps(change['change']) + '\n')

                total_items = len(page_data["vulnerabilities"])
                for change in page_data["vulnerabilities"]:
                    file.write(json.dumps(change['cve']) + '\n')

                offset += PAGE_MAX
                print(f"Collected: {offset}/{total_results}")

                if total_items < PAGE_MAX:
                    print("Collected all the data from NVD's CVE History endpoint.")
                    more_data = False

                break  # Exit retry loop if done collecting data

            except requests.exceptions.HTTPError as e:
                if page_response.status_code == 429:  # Too Many Requests
                    print("Rate limited by server. Waiting longer than usually required...")
                    time.sleep(45)  # Wait 45 seconds for 429 errors
                    retries += 1
                else:
                    wait_time = INITIAL_BACKOFF * (2 ** retries)
                    print(f"HTTP Error: {e} — retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    retries += 1
            except Exception as e:
                wait_time = INITIAL_BACKOFF * (2 ** retries)
                print(f"Error: {e} — retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                retries += 1
        else:
            print(f"Failed after {MAX_RETRIES} retries. Exiting.")
            break
