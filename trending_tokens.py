import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# Function to save JSON data to a file
def save_to_file(data, filename):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)

# Function to handle a single request
def fetch_page_data(page, headers):
    base_url = "https://app.geckoterminal.com/api/p1/pools"
    query_params = {
        "include": "dex,dex.network,tokens",
        "page": str(page),
        "include_network_metrics": "true",
        "sort": "-24h_transactions"
    }

    # Make the request
    response = requests.get(base_url, headers=headers, params=query_params)

    # Check the response status
    if response.status_code != 200:
        return (page, None, response.status_code)

    # Convert response content to JSON
    data = response.json()

    directory = 'trending_tokens'
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Save the data to a file
    filename = os.path.join(directory, f'data_page_{page}.json')
    save_to_file(data, filename)
    return (page, filename, response.status_code)

# Function to parallelize the fetching and saving process
def fetch_and_save_data_parallel(headers, start_page=1, end_page=500, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_page_data, page, headers) for page in range(start_page, end_page + 1)]
        for future in as_completed(futures):
            page, filename, status_code = future.result()
            if status_code == 200:
                print(f"Saved data for page {page} to {filename}")
            else:
                print(f"Request failed on page {page} with status code: {status_code}")
                break

# Headers to simulate a browser
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Fetch and save data for pages 1 to 500 in parallel
fetch_and_save_data_parallel(headers)
