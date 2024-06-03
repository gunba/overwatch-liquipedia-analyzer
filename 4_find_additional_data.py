import logging
import os
import re
import time
import traceback
from glob import glob

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


# Function to scan tournaments_text folder and fetch additional links
def scan_and_fetch_additional_links():
    text_files = glob("tournaments_text/*.txt")
    pattern = re.compile(
        r"\[\[([^|]+)\|[^]]*click here[^]]*\]\]", re.IGNORECASE
    )
    links_to_fetch = []
    current_index = len(text_files)

    # Collect all valid links
    for text_file in text_files:
        with open(text_file, "r", encoding="utf-8") as f:
            content = f.read()

        matches = pattern.findall(content)
        for match in matches:
            title = match.strip()
            links_to_fetch.append(title)

    # Use tqdm to show progress
    with tqdm(total=len(links_to_fetch)) as pbar:
        for title in links_to_fetch:
            try:
                fetch_and_save_wikitext_data(title, current_index)
            except Exception as e:
                error_message = (
                    f"Failed to process {title} at index {current_index}: {e}"
                )
                logging.error(error_message)
                logging.error(traceback.format_exc())
                print(error_message)
                print(traceback.format_exc())
                continue
            current_index += 1
            pbar.update(1)


if __name__ == "__main__":
    scan_and_fetch_additional_links()
