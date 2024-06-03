import json
import os

from bs4 import BeautifulSoup

# Define the folders
input_folder = "headers_text"
output_folder = "headers_raw"

# Create output folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Loop through all .json files in the input folder
for filename in os.listdir(input_folder):
    if filename.endswith(".json"):
        # Construct the full path to the file
        input_path = os.path.join(input_folder, filename)

        # Read the JSON content from the file
        with open(input_path, "r", encoding="utf-8") as file:
            content = file.read()
            data = json.loads(content)

        # Parse the HTML content
        html_content = data.get("parse", {}).get("text", {}).get("*", "")
        soup = BeautifulSoup(html_content, "html.parser")

        # Find all <div class="gridCell Tournament Header">
        tournament_divs = soup.find_all(
            "div", class_="gridCell Tournament Header"
        )

        # Extract the last <a href> link within those divs
        links = []
        for div in tournament_divs:
            a_tags = div.find_all("a", href=True)
            if a_tags:  # Check if there are any <a> tags
                links.append(a_tags[-1]["href"])

        # Prepare the output JSON
        output_data = {"links": links}

        # Write the output JSON to a file in the output folder
        output_path = os.path.join(output_folder, filename)
        with open(output_path, "w", encoding="utf-8") as output_file:
            json.dump(output_data, output_file, indent=4)

print("Processing complete. Check the headers_raw folder for output files.")
