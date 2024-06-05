import json
import os
from datetime import datetime

from bs4 import BeautifulSoup

# Define the folders
input_folder = "headers_text"
output_folder = "headers_raw"

# Create output folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)


# Function to parse the date and extract the end date
def extract_end_date(date_text):
    # Remove commas
    date_text = date_text.replace(",", "")

    # Handle date ranges
    if "-" in date_text:
        parts = date_text.split(" ")
        # Check if the date is in the format "Jun 6 - 7, 2024"
        if len(parts) == 5 and parts[1].isdigit() and parts[3].isdigit():
            month = parts[0]
            end_date_str = f"{month} {parts[3]} {parts[4]}"
        # Check if the date is in the format "Apr 15 - May 17, 2024"
        elif len(parts) == 6:
            end_date_str = f"{parts[3]} {parts[4]} {parts[5]}"
        # Standard range case "May 25, 2024"
        else:
            start_date_str, end_date_str = date_text.split(" - ")
    else:
        end_date_str = date_text

    end_date = datetime.strptime(end_date_str, "%b %d %Y")
    return end_date.strftime("%Y-%m-%d")


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

        # Extract the last <a href> link and the date within those divs
        tournaments = []
        for div in tournament_divs:
            a_tags = div.find_all("a", href=True)
            if a_tags:  # Check if there are any <a> tags
                link = a_tags[-1]["href"]

                # Find the next sibling div with class "gridCell EventDetails Date Header"
                date_div = div.find_next_sibling(
                    "div", class_="gridCell EventDetails Date Header"
                )
                if date_div:
                    date_text = date_div.text.strip()
                    end_date = extract_end_date(date_text)

                    # Append the link and end date to the list
                    tournaments.append({"link": link, "end_date": end_date})

        # Prepare the output JSON
        output_data = {"tournaments": tournaments}

        # Write the output JSON to a file in the output folder
        output_path = os.path.join(output_folder, filename)
        with open(output_path, "w", encoding="utf-8") as output_file:
            json.dump(output_data, output_file, indent=4)

print("Processing complete. Check the headers_raw folder for output files.")
