import json
import os
import re
import shutil

from tqdm import tqdm

from clean_data import clean_json


def extract_elements(wikitext, element_name):
    pattern = re.compile(
        r"(\|\S+)?\{\{" + re.escape(element_name) + r"\n(.*?)\n\}\}", re.DOTALL
    )
    return pattern.findall(wikitext)


def parse_wikitext_element(element):
    lines = element.split("\n")
    element_dict = {}
    current_key = None
    current_value = None

    for line in lines:
        if line.strip().startswith("|"):
            if current_key is not None:
                element_dict[current_key.strip().lstrip("|")] = (
                    current_value.strip()
                )
            key_value = line[1:].split("=", 1)
            if len(key_value) == 2:
                current_key, current_value = key_value
            else:
                current_key = key_value[0]
                current_value = ""
        else:
            if current_key is not None:
                current_value += "\n" + line

    if current_key is not None:
        element_dict[current_key.strip().lstrip("|")] = current_value.strip()

    return element_dict


def clear_directory(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)


def process_legacy_match_elements(wikitext):
    lines = wikitext.split("\n")
    match_records = []
    current_record = {}
    current_prefix = None
    is_match_started = False

    for line in lines:
        if re.match(r"\|R\d+[A-Z]\d+team=", line) and not is_match_started:
            key_value_pairs = line.split("|")[1:]
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
            for prefix, sub_dict in current_record.items():
                for sub_key in list(sub_dict.keys()):
                    nested_match = re.match(r"(R\d+[A-Z]\d+)(.*)", sub_key)
                    if nested_match:
                        nested_prefix, nested_key = nested_match.groups()
                        if nested_prefix != prefix:
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
            key_value_pairs = line.split("|")[1:]
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


def rename_legacy_match_keys(legacy_match_cards):
    renamed_keys = {"team": "team1", "score": "score1", "win": "win1"}
    for record in legacy_match_cards:
        for old_key, new_key in renamed_keys.items():
            if old_key in record:
                record[new_key] = record.pop(old_key)
    return legacy_match_cards


def convert_score(value):
    if value in ["W", "L", "DQ", "FF"]:
        return 1 if value == "W" else 0
    try:
        return int(value)
    except ValueError:
        return 0


def unify_match_format(data):
    unified_matches = []

    def process_match_card(match):
        match["format"] = "match_cards"
        opponent1_score = (
            int(match["opponent1_score"]) if "opponent1_score" in match else 0
        )
        opponent2_score = (
            int(match["opponent2_score"]) if "opponent2_score" in match else 0
        )

        for key, value in match.items():
            if re.match(r"map\d+", key) and isinstance(value, dict):
                if value.get("winner") == "1":
                    opponent1_score += 1
                elif value.get("winner") == "2":
                    opponent2_score += 1

        match["opponent1_score"] = opponent1_score
        match["opponent2_score"] = opponent2_score

        match["winner"] = (
            1
            if opponent1_score > opponent2_score
            else (2 if opponent1_score < opponent2_score else 0)
        )

        return match

    def process_match_map(match):
        # Check if 'winner' exists and handle 'draw'
        if "winner" in match and match["winner"] == "draw":
            match["winner"] = 0
        elif "winner" not in match or match["winner"] == "":
            match["winner"] = 0

        unified_match = {
            "date": match.get("date"),
            "date_timezone": "UTC",
            "youtube": match.get("stream"),
            "opponent1": match.get("team1"),
            "opponent2": match.get("team2"),
            "opponent1_score": convert_score(match.get("games1", 0)),
            "opponent2_score": convert_score(match.get("games2", 0)),
            "winner": int(match.get("winner", 0)),
            "format": "match_maps",
        }

        map_index = 1
        while f"map{map_index}" in match:
            map_score = match.get(f"map{map_index}score", "0-0").split("-")
            score1 = convert_score(map_score[0])
            score2 = convert_score(map_score[1]) if len(map_score) > 1 else 0
            winner = convert_score(match.get(f"map{map_index}win", 0))

            if winner == 0 and (score1 != 0 or score2 != 0):
                winner = 1 if score1 > score2 else 2 if score2 > score1 else 0

            unified_match[f"map{map_index}"] = {
                "map": match.get(f"map{map_index}"),
                "mode": "",  # Mode information is not available
                "score1": score1,
                "score2": score2,
                "winner": winner,
            }

            map_index += 1

        return unified_match

    def process_legacy_match_card(match):
        unified_match = {
            "date": match.get("date"),
            "date_timezone": match.get("date_timezone", "UTC"),
            "opponent1": match.get("team1"),
            "opponent2": match.get("team2"),
            "opponent1_score": convert_score(match.get("score1", 0)),
            "opponent2_score": convert_score(match.get("score2", 0)),
            "winner": 1
            if match.get("win1", "0") == "1"
            else 2
            if match.get("win2", "0") == "1"
            else 0,
            "format": "legacy_match_cards",
        }

        # If we have valid scores, overwrite winner.
        if unified_match["opponent1_score"] > unified_match["opponent2_score"]:
            unified_match["winner"] = 1
        elif (
            unified_match["opponent1_score"] < unified_match["opponent2_score"]
        ):
            unified_match["winner"] = 2

        return unified_match

    if "match_cards" in data:
        unified_matches.extend(
            process_match_card(match) for match in data["match_cards"]
        )

    if "match_maps" in data:
        unified_matches.extend(
            process_match_map(match)
            for match in data["match_maps"]
            if "winner" in match
        )

    if "legacy_match_cards" in data:
        unified_matches.extend(
            process_legacy_match_card(match)
            for match in data["legacy_match_cards"]
        )

    return unified_matches


def process_and_save_wikitext_data():
    os.makedirs("tournaments_raw", exist_ok=True)
    os.makedirs("tournaments_data", exist_ok=True)

    clear_directory("tournaments_raw")
    clear_directory("tournaments_data")

    text_files = [
        f for f in os.listdir("tournaments_text") if f.endswith(".txt")
    ]

    for text_filename in tqdm(text_files, desc="Processing files"):
        text_filepath = os.path.join("tournaments_text", text_filename)
        with open(text_filepath, "r", encoding="utf-8") as text_file:
            wikitext = text_file.read()

        is_legacy = any(
            legacy_tag in wikitext
            for legacy_tag in [
                "{{LegacyBracket",
                "{{LegacyMatchList",
                "{{LegacySingleMatch",
                "{{MatchMaps",
            ]
        )

        tournament_info_raw = extract_elements(wikitext, "Infobox league")
        team_cards_raw = extract_elements(wikitext, "TeamCard")

        tournament_info = []
        team_cards = []

        for optional, content in tournament_info_raw:
            element_data = parse_wikitext_element(content)
            if optional:
                element_data["optional"] = optional.strip().lstrip("|")
            tournament_info.append(element_data)

        for optional, content in team_cards_raw:
            element_data = parse_wikitext_element(content)
            if optional:
                element_data["optional"] = optional.strip().lstrip("|")
            team_cards.append(element_data)

        if is_legacy:
            legacy_match_cards = process_legacy_match_elements(wikitext)
            legacy_match_cards = rename_legacy_match_keys(legacy_match_cards)
            match_maps = process_match_maps_elements(wikitext)

            data = {
                "tournament_info": tournament_info,
                "team_cards": team_cards,
                "legacy_match_cards": legacy_match_cards,
                "match_maps": match_maps,
            }

        else:
            match_cards_raw = extract_elements(wikitext, "Match")
            match_cards = []
            for optional, content in match_cards_raw:
                match_data = parse_wikitext_element(content)
                if optional:
                    match_data["optional"] = optional.strip().lstrip("|")
                match_cards.append(match_data)

            data = {
                "tournament_info": tournament_info,
                "team_cards": team_cards,
                "match_cards": match_cards,
            }

        filename = text_filename.replace(".txt", ".json")
        raw_filepath = os.path.join("tournaments_raw", filename)

        with open(raw_filepath, "w", encoding="utf-8") as raw_file:
            json.dump(data, raw_file, indent=4)
        print(f"Raw extracted data saved to {raw_filepath}")

        # Apply our cleaning function
        data = clean_json(data)

        unified_match_cards = unify_match_format(data)
        data["match_cards"] = unified_match_cards

        data.pop("legacy_match_cards", None)
        data.pop("match_maps", None)

        cleaned_filepath = os.path.join("tournaments_data", filename)

        with open(cleaned_filepath, "w", encoding="utf-8") as cleaned_file:
            json.dump(data, cleaned_file, indent=4)
        print(f"Cleaned data saved to {cleaned_filepath}")


if __name__ == "__main__":
    process_and_save_wikitext_data()
