import json
import os
import re
import shutil

from tqdm import tqdm

from clean_data import clean_json


# Function to extract elements from wikitext
def extract_elements(wikitext, element_name):
    pattern = re.compile(
        r"(\|\S+)?\{\{" + re.escape(element_name) + r"\n(.*?)\n\}\}", re.DOTALL
    )
    return pattern.findall(wikitext)


# Function to parse key-value pairs from wikitext elements
def parse_wikitext_element(element):
    lines = element.split("\n")
    element_dict = {}
    current_key = None
    current_value = None

    for line in lines:
        if line.strip().startswith("|"):
            if current_key is not None:
                # Save the previous key-value pair
                element_dict[current_key.strip().lstrip("|")] = (
                    current_value.strip()
                )
            # Split the new line by the first '=' to get the key and value
            key_value = line[1:].split("=", 1)
            if len(key_value) == 2:
                current_key, current_value = key_value
            else:
                current_key = key_value[0]
                current_value = ""
        else:
            if current_key is not None:
                # Continue the value on the next line
                current_value += "\n" + line

    # Add the last key-value pair
    if current_key is not None:
        element_dict[current_key.strip().lstrip("|")] = current_value.strip()

    return element_dict


# Function to clear a directory
def clear_directory(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)


# Function to process and save wikitext data from files
def process_and_save_wikitext_data():
    # Create raw data directory if it doesn't exist
    os.makedirs("tournaments_raw", exist_ok=True)
    os.makedirs("tournaments_data", exist_ok=True)

    # Clear raw and data directories
    clear_directory("tournaments_raw")
    clear_directory("tournaments_data")

    text_files = [
        f for f in os.listdir("tournaments_text") if f.endswith(".txt")
    ]

    for text_filename in tqdm(text_files, desc="Processing files"):
        text_filepath = os.path.join("tournaments_text", text_filename)
        with open(text_filepath, "r", encoding="utf-8") as text_file:
            wikitext = text_file.read()

        # Skip files containing "{{LegacyBracket", "{{LegacyMatchList", or "{{LegacySingleMatch"
        if (
            "{{LegacyBracket" in wikitext
            or "{{LegacyMatchList" in wikitext
            or "{{LegacySingleMatch" in wikitext
        ):
            continue

        # Extract and parse elementsa
        tournament_info_raw = extract_elements(wikitext, "Infobox league")
        team_cards_raw = extract_elements(wikitext, "TeamCard")
        match_cards_raw = extract_elements(wikitext, "Match")

        # Parse tournament info and team cards
        tournament_info = []
        for optional, content in tournament_info_raw:
            element_data = parse_wikitext_element(content)
            if optional:
                element_data["optional"] = optional.strip().lstrip("|")
            tournament_info.append(element_data)

        team_cards = []
        for optional, content in team_cards_raw:
            element_data = parse_wikitext_element(content)
            if optional:
                element_data["optional"] = optional.strip().lstrip("|")
            team_cards.append(element_data)

        # Parse match cards and add round info
        match_cards = []
        for optional, content in match_cards_raw:
            match_data = parse_wikitext_element(content)
            if optional:
                match_data["optional"] = optional.strip().lstrip("|")
            match_cards.append(match_data)

        # Organize data into JSON
        data = {
            "tournament_info": tournament_info,
            "team_cards": team_cards,
            "match_cards": match_cards,
        }

        # Generate filename from title
        filename = text_filename.replace(".txt", ".json")
        raw_filepath = os.path.join("tournaments_raw", filename)

        # Save the raw extracted data to a JSON file
        with open(raw_filepath, "w", encoding="utf-8") as raw_file:
            json.dump(data, raw_file, indent=4)
        print(f"Raw extracted data saved to {raw_filepath}")

        # Apply our cleaning function.
        cleaned_data = clean_json(data)

        # Save the cleaned data to a JSON file
        cleaned_filepath = os.path.join("tournaments_data", filename)

        with open(cleaned_filepath, "w", encoding="utf-8") as cleaned_file:
            json.dump(cleaned_data, cleaned_file, indent=4)
        print(f"Cleaned data saved to {cleaned_filepath}")


if __name__ == "__main__":
    process_and_save_wikitext_data()
