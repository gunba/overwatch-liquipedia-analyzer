import json
import logging
import os
import re
import sys
import time
import traceback
from glob import glob
from urllib.parse import unquote

import requests
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    filename="error.log",
    level=logging.ERROR,
    format="%(asctime)s:%(levelname)s:%(message)s",
)

# Define headers and user agent
headers = {
    "User-Agent": "MatchCorrelationAnalysis/1.0 (sensis@gmail.com)",
    "Accept-Encoding": "gzip",
}


# Function to make a request with rate limiting
def make_request(url, params, delay=4):
    time.sleep(delay)
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()  # Raise an error for bad status codes
    return response.json()


# Function to fetch and save wikitext data based on title
def fetch_and_save_wikitext_data(title, index):
    url = "https://liquipedia.net/overwatch/api.php"
    params = {
        "action": "query",
        "format": "json",
        "titles": title,
        "prop": "revisions",
        "rvprop": "content",
        "rvslots": "*",
    }

    # Print the exact URL and parameters used in the request for debugging
    print(f"Requesting URL: {url} with params: {params}")

    data = make_request(url, params)
    page = next(iter(data["query"]["pages"].values()))
    wikitext = page["revisions"][0]["slots"]["main"]["*"]

    # Save the original wikitext dump
    os.makedirs("tournaments_text", exist_ok=True)
    text_filename = (
        f"{index:04d}_" + re.sub(r"[^a-zA-Z0-9]", "_", title) + ".txt"
    )
    text_filepath = os.path.join("tournaments_text", text_filename)
    with open(text_filepath, "w", encoding="utf-8") as text_file:
        text_file.write(wikitext)
    print(f"Original wikitext saved to {text_filepath}")


def count_total_links():
    json_files = glob("headers_raw/*.json")
    total_links = 0

    for file in json_files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
            total_links += len(data["links"])

    return total_links


def process_links(start_index=0):
    json_files = glob("headers_raw/*.json")
    current_index = 0

    # Initialize tqdm progress bar
    total_links = count_total_links()
    with tqdm(total=total_links, initial=start_index) as pbar:
        for file in json_files:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)
                for link in data["links"]:
                    if current_index >= start_index:
                        title = unquote(link.replace("/overwatch/", ""))
                        try:
                            fetch_and_save_wikitext_data(title, current_index)
                        except Exception as e:
                            error_message = f"Failed to process {title} at index {current_index}: {e}"
                            logging.error(error_message)
                            logging.error(traceback.format_exc())
                            print(error_message)
                            print(traceback.format_exc())
                            raise e
                        pbar.update(1)
                    current_index += 1

    return current_index


if __name__ == "__main__":
    # Check if a starting index is provided as a command-line argument
    starting_index = int(sys.argv[1]) if len(sys.argv) > 1 else 0

    # Count total links
    total_links = count_total_links()
    print(f"Total links to process: {total_links}")

    # Start processing links from the given starting index
    final_index = process_links(starting_index)
    print(f"Processed up to index: {final_index}")
