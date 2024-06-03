import json
import os
import time
from urllib.parse import quote, urlencode

import requests
from tqdm import tqdm

# Set up headers and API endpoint
headers = {
    "User-Agent": "MatchCorrelationAnalysis/1.0 (sensis@gmail.com)",
    "Accept-Encoding": "gzip",
}

api_endpoint = "https://liquipedia.net/overwatch/api.php"

# Create the output directory if it doesn't exist
output_dir = "headers_text"
os.makedirs(output_dir, exist_ok=True)

# Define the tournament categories and their year ranges
categories_with_years = [
    ("B-Tier_Tournaments", 2015),
    ("C-Tier_Tournaments", 2015),
    ("D-Tier_Tournaments", 2015),
    ("Qualifier_Tournaments", 2015),
    ("Weekly_Tournaments", 2015),
]

categories_without_years = [
    "S-Tier_Tournaments",
    "A-Tier_Tournaments",
    "Monthly_Tournaments",
]

# Get the current year
current_year = 2024

# Generate the list of pages to fetch
pages_to_fetch = []

for category, start_year in categories_with_years:
    for year in range(start_year, current_year + 1):
        pages_to_fetch.append(f"{category}/{year}")

for category in categories_without_years:
    pages_to_fetch.append(category)


def fetch_wikitext(page):
    encoded_page = quote(page)
    params = {"action": "parse", "format": "json", "page": encoded_page}
    full_url = f"{api_endpoint}?{urlencode(params)}"

    response = requests.get(full_url, headers=headers)
    response.raise_for_status()  # Ensure we notice bad responses
    response_json = response.json()
    return response_json  # Return the JSON response as a dictionary


# Fetch and save the data
for page in tqdm(pages_to_fetch, desc="Fetching tournament data"):
    try:
        wikitext = fetch_wikitext(page)

        # Sanitize the filename
        filename = page.replace("/", "_") + ".json"

        filepath = os.path.join(output_dir, filename)
        with open(filepath, "w", encoding="utf-8") as file:
            json.dump(
                wikitext, file, ensure_ascii=False, indent=4
            )  # Write JSON data

        # Respect rate limiting
        time.sleep(30)  # Add a 30-second delay
    except Exception as e:
        print(f"Error fetching data for {page}: {e}")

print("Data fetching complete.")
