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


def parse_wikitext_element(element, optional=None):
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

    if optional:
        element_dict["optional"] = optional.strip().lstrip("|")

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
    is_match_started = False

    for line in lines:
        if "{{MatchMaps" in line:
            match = re.search(r"(\S+?)?\s*=\s*\{\{MatchMaps", line)
            current_record = {}
            if match and match.group(1):
                current_record["optional"] = match.group(1).strip()
            is_match_started = True
        elif is_match_started:
            if not line.startswith("|"):
                match_records.append(current_record)
                is_match_started = False
            else:
                key_value_pairs = line.split("|")[1:]
                for kv in key_value_pairs:
                    if "=" in kv:
                        key, value = kv.split("=", 1)
                        current_record[key.strip()] = value.strip()

    return match_records


def rename_legacy_match_keys(legacy_match_cards):
    renamed_keys = {"team": "team1", "score": "score1", "win": "win1"}
    for record in legacy_match_cards:
        for old_key, new_key in renamed_keys.items():
            if old_key in record:
                record[new_key] = record.pop(old_key)
    return legacy_match_cards


def convert_score(value):
    if isinstance(value, int):
        return value

    cleaned_value = re.sub(r"\W+", "", value).upper()

    if cleaned_value == "W":
        return 1

    try:
        return int(cleaned_value)
    except ValueError:
        return 0


def unify_match_format(data):
    unified_matches = []

    def process_match_card(match):
        match["format"] = "match_cards"
        opponent1_score = convert_score(match.get("opponent1_score", 0))
        opponent2_score = convert_score(match.get("opponent2_score", 0))

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

    def process_legacy_match(match, format_type):
        unified_match = {
            "date": match.get("date"),
            "date_timezone": match.get("date_timezone", "UTC"),
            "opponent1": match.get("team1"),
            "opponent2": match.get("team2"),
            "opponent1_score": convert_score(match.get("score1", 0)),
            "opponent2_score": convert_score(match.get("score2", 0)),
            "winner": 1
            if match.get("win1", "0") == "1"
            else (2 if match.get("win2", "0") == "1" else 0),
            "format": format_type,
        }

        if unified_match["opponent1_score"] > unified_match["opponent2_score"]:
            unified_match["winner"] = 1
        elif (
            unified_match["opponent1_score"] < unified_match["opponent2_score"]
        ):
            unified_match["winner"] = 2

        # Retain all other original values
        for key, value in match.items():
            if key not in unified_match:
                unified_match[key] = value

        return unified_match

    def process_match_map(match):
        unified_match = {
            "date": match.get("date"),
            "date_timezone": "UTC",
            "youtube": match.get("stream"),
            "opponent1": match.get("team1"),
            "opponent2": match.get("team2"),
            "opponent1_score": convert_score(match.get("games1", 0)),
            "opponent2_score": convert_score(match.get("games2", 0)),
            "winner": convert_score(match.get("winner", 0)),
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
                "mode": "",
                "score1": score1,
                "score2": score2,
                "winner": winner,
            }

            map_index += 1

        # Retain all other original values
        for key, value in match.items():
            if key not in unified_match:
                unified_match[key] = value

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
            process_legacy_match(match, "legacy_match_cards")
            for match in data["legacy_match_cards"]
        )

    # Clean up map data
    for match in unified_matches:
        for key in list(match.keys()):
            if re.match(r"map\d+", key):
                map_data = match[key]
                if isinstance(map_data, str) or not map_data.get("winner"):
                    del match[key]
                else:
                    if "score1" in map_data:
                        match[key]["score1"] = convert_score(map_data["score1"])
                    if "score2" in map_data:
                        match[key]["score2"] = convert_score(map_data["score2"])
                    match[key]["winner"] = convert_score(map_data["winner"])

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

        if (
            text_filename
            == "Overwatch_League_Season_2_Regular_Season_Stage_1.txt"
        ):
            print("debug catch!")

        tournament_info_raw = extract_elements(
            wikitext, "Infobox league"
        ) or extract_elements(wikitext, "HiddenDataBox")
        team_cards_raw = extract_elements(wikitext, "TeamCard")

        tournament_info = [
            parse_wikitext_element(content, optional)
            for optional, content in tournament_info_raw
        ]
        team_cards = [
            parse_wikitext_element(content, optional)
            for optional, content in team_cards_raw
        ]

        if is_legacy:
            legacy_match_cards = rename_legacy_match_keys(
                process_legacy_match_elements(wikitext)
            )
            match_maps = process_match_maps_elements(wikitext)

            data = {
                "tournament_info": tournament_info,
                "team_cards": team_cards,
                "legacy_match_cards": legacy_match_cards,
                "match_maps": match_maps,
            }

        else:
            match_cards_raw = extract_elements(wikitext, "Match")
            match_cards = [
                parse_wikitext_element(content, optional)
                for optional, content in match_cards_raw
            ]

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
