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


# Function to process legacy match elements
def process_legacy_match_elements(wikitext):
    lines = wikitext.split("\n")
    match_records = []
    current_record = {}
    current_prefix = None
    is_match_started = False

    for line in lines:
        if re.match(r"\|R\d+[A-Z]\d+team=", line) and not is_match_started:
            key_value_pairs = line.split("|")[
                1:
            ]  # Split by pipe and remove the first empty element
            for kv in key_value_pairs:
                if "=" in kv:
                    key, value = kv.split("=", 1)
                    match = re.match(r"(R\d+[A-Z]\d+)(.*)", key)
                    if match:
                        prefix, key = match.groups()
                        if not current_prefix:
                            current_prefix = prefix
                        if prefix not in current_record:
                            current_record[prefix] = {}
                        current_record[prefix][key.strip()] = value.strip()
            is_match_started = True

        elif not line.startswith("|") and is_match_started:
            # Process the nested R/[A-Z] keys
            for prefix, sub_dict in current_record.items():
                for sub_key in list(sub_dict.keys()):
                    nested_match = re.match(r"(R\d+[A-Z]\d+)(.*)", sub_key)
                    if nested_match:
                        nested_prefix, nested_key = nested_match.groups()
                        if nested_prefix != prefix:
                            # Remove the nested R/[A-Z] prefix and add "2" suffix
                            sub_dict[nested_key.strip() + "2"] = sub_dict.pop(
                                sub_key
                            )
                        else:
                            sub_dict[nested_key.strip() + "1"] = sub_dict.pop(
                                sub_key
                            )

            for prefix, sub_dict in current_record.items():
                sub_dict["key"] = prefix
                match_records.append(sub_dict)

            current_record = {}
            current_prefix = None
            is_match_started = False

        else:
            key_value_pairs = line.split("|")[
                1:
            ]  # Split by pipe and remove the first empty element
            for kv in key_value_pairs:
                if "=" in kv:
                    key, value = kv.split("=", 1)
                    if current_prefix:
                        if current_prefix not in current_record:
                            current_record[current_prefix] = {}
                        current_record[current_prefix][key.strip()] = (
                            value.strip()
                        )

    if current_record:
        for prefix, sub_dict in current_record.items():
            sub_dict["key"] = prefix
            match_records.append(sub_dict)

    return match_records


# Function to process match maps elements
def process_match_maps_elements(wikitext):
    lines = wikitext.split("\n")
    match_records = []
    current_record = {}
    current_key = None
    is_match_started = False

    for line in lines:
        if "{{MatchMaps" in line:
            match = re.search(r"\|(\S+?)\s*=\s*\{\{MatchMaps", line)
            if match:
                current_key = match.group(1).strip()
                is_match_started = True
                current_record = {"key": current_key}
        elif is_match_started:
            if not line.startswith("|"):
                match_records.append(current_record)
                is_match_started = False
                current_key = None
            else:
                key_value_pairs = line.split("|")[1:]
                for kv in key_value_pairs:
                    if "=" in kv:
                        key, value = kv.split("=", 1)
                        current_record[key.strip()] = value.strip()

    if current_record and current_key:
        match_records.append(current_record)

    return match_records


# Function to rename keys in legacy match cards
def rename_legacy_match_keys(legacy_match_cards):
    renamed_keys = {"team": "team1", "score": "score1", "win": "win1"}
    for record in legacy_match_cards:
        for old_key, new_key in renamed_keys.items():
            if old_key in record:
                record[new_key] = record.pop(old_key)
    return legacy_match_cards


# Function to process and save wikitext data from files
def process_and_save_wikitext_data():
    # Create raw data directory if it doesn't exist
    os.makedirs("legacy_tournaments_raw", exist_ok=True)
    os.makedirs("legacy_tournaments_data", exist_ok=True)

    # Clear raw and data directories
    clear_directory("legacy_tournaments_raw")
    clear_directory("legacy_tournaments_data")

    text_files = [
        f for f in os.listdir("tournaments_text") if f.endswith(".txt")
    ]

    for text_filename in tqdm(text_files, desc="Processing files"):
        text_filepath = os.path.join("tournaments_text", text_filename)
        with open(text_filepath, "r", encoding="utf-8") as text_file:
            wikitext = text_file.read()

        # Only process files containing "{{LegacyBracket", "{{LegacyMatchList", "{{LegacySingleMatch", or "{{MatchMaps"
        if (
            "{{LegacyBracket" not in wikitext
            and "{{LegacyMatchList" not in wikitext
            and "{{LegacySingleMatch" not in wikitext
            and "{{MatchMaps" not in wikitext
        ):
            continue

        # Extract and parse elements
        tournament_info_raw = extract_elements(wikitext, "Infobox league")
        team_cards_raw = extract_elements(wikitext, "TeamCard")

        # Process legacy match elements
        legacy_match_cards = process_legacy_match_elements(wikitext)

        # Rename keys in legacy match cards
        legacy_match_cards = rename_legacy_match_keys(legacy_match_cards)

        # Process match maps elements
        match_maps = process_match_maps_elements(wikitext)

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

        # Organize data into JSON
        data = {
            "tournament_info": tournament_info,
            "team_cards": team_cards,
            "legacy_match_cards": legacy_match_cards,
            "match_maps": match_maps,
        }

        # Generate filename from title
        filename = text_filename.replace(".txt", ".json")
        raw_filepath = os.path.join("legacy_tournaments_raw", filename)

        # Save the raw extracted data to a JSON file
        with open(raw_filepath, "w", encoding="utf-8") as raw_file:
            json.dump(data, raw_file, indent=4)
        print(f"Raw extracted data saved to {raw_filepath}")

        # Apply our cleaning function.
        cleaned_data = clean_json(data)

        # Save the cleaned data to a JSON file
        cleaned_filepath = os.path.join("legacy_tournaments_data", filename)

        with open(cleaned_filepath, "w", encoding="utf-8") as cleaned_file:
            json.dump(cleaned_data, cleaned_file, indent=4)
        print(f"Cleaned data saved to {cleaned_filepath}")


if __name__ == "__main__":
    process_and_save_wikitext_data()
