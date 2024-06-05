import json
import logging
import os
import re
import sys
import time
import traceback
from datetime import datetime
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

# Compile the regex pattern for hidden tournament links
pattern = re.compile(r"\[\[([^|\]]+/[^|\]]+)(\|[^|\]]*)?\]\]", re.IGNORECASE)


# Function to make a request with rate limiting
def make_request(url, params, delay=4):
    time.sleep(delay)
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()  # Raise an error for bad status codes
    return response.json()


# Function to fetch and save wikitext data based on title
def fetch_and_save_wikitext_data(title, processed_links, skip_existing=False):
    os.makedirs("tournaments_text", exist_ok=True)
    text_filename = re.sub(r"[^a-zA-Z0-9]", "_", title) + ".txt"
    text_filepath = os.path.join("tournaments_text", text_filename)

    # Check if file already exists and handle accordingly
    if skip_existing and os.path.exists(text_filepath):
        with open(text_filepath, "r", encoding="utf-8") as text_file:
            wikitext = text_file.read()
        print(f"Skipping download, using existing file: {text_filepath}")
    else:
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
        with open(text_filepath, "w", encoding="utf-8") as text_file:
            text_file.write(wikitext)
        print(f"Original wikitext saved to {text_filepath}")

    # Add the processed link to the set
    processed_links.add(title)

    # Process hidden tournament links
    hidden_links = pattern.findall(wikitext)
    for hidden_title, _ in hidden_links:
        hidden_title = hidden_title.strip()
        if hidden_title not in processed_links:
            print(f"Found hidden tournament link: {hidden_title}")
            try:
                fetch_and_save_wikitext_data(
                    hidden_title, processed_links, skip_existing
                )
            except Exception as e:
                error_message = (
                    f"Failed to process hidden link {hidden_title}: {e}"
                )
                logging.error(error_message)
                logging.error(traceback.format_exc())
                print(error_message)
                print(traceback.format_exc())
                continue


def count_total_links(from_date):
    json_files = glob("headers_raw/*.json")
    total_links = 0

    for file in json_files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
            total_links += len([
                t
                for t in data["tournaments"]
                if datetime.strptime(t["end_date"], "%Y-%m-%d")
                >= datetime.strptime(from_date, "%Y-%m-%d")
            ])

    return total_links


def process_links(start_index=0, from_date="2024-06-01", skip_existing=False):
    json_files = glob("headers_raw/*.json")
    current_index = 0
    processed_links = set()

    # Initialize tqdm progress bar
    total_links = count_total_links(from_date)
    with tqdm(total=total_links, initial=start_index) as pbar:
        for file in json_files:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)
                for tournament in data["tournaments"]:
                    end_date = datetime.strptime(
                        tournament["end_date"], "%Y-%m-%d"
                    )
                    if end_date >= datetime.strptime(from_date, "%Y-%m-%d"):
                        title = unquote(
                            tournament["link"].replace("/overwatch/", "")
                        )
                        if title not in processed_links:
                            if current_index >= start_index:
                                try:
                                    fetch_and_save_wikitext_data(
                                        title, processed_links, skip_existing
                                    )
                                except Exception as e:
                                    error_message = f"Failed to process {title} at index {current_index}: {e}"
                                    logging.error(error_message)
                                    logging.error(traceback.format_exc())
                                    print(error_message)
                                    print(traceback.format_exc())
                                    continue
                                pbar.update(1)
                            processed_links.add(title)
                        current_index += 1

    return current_index


if __name__ == "__main__":
    # Check if a starting index is provided as a command-line argument
    starting_index = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    from_date = sys.argv[2] if len(sys.argv) > 2 else "2024-06-01"
    skip_existing = bool(int(sys.argv[3])) if len(sys.argv) > 3 else False

    # Count total links
    total_links = count_total_links(from_date)
    print(f"Total links to process: {total_links}")

    # Start processing links from the given starting index
    final_index = process_links(starting_index, from_date, skip_existing)
    print(f"Processed up to index: {final_index}")
