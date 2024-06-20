import json
import re
from dateutil import parser

def split_key_value(key, value):
    """Splits a key-value pair if the value contains '|' and '=' and does not have nested elements."""
    if "|" in value and "=" in value and "{{" not in value and "}}" not in value:
        sub_values = value.split("|")
        new_entries = {}
        new_entries[key] = sub_values[0]
        for sub_value in sub_values[1:]:
            if "=" in sub_value:
                sub_key, sub_val = sub_value.split("=", 1)
                new_entries[sub_key] = sub_val
        return new_entries
    else:
        return {key: value}

def process_special_elements(key, value):
    """Processes special elements in double curly brackets."""
    if "{{abbr/" in value.lower():
        abbr_match = re.match(r"(.+?)\s*\{\{abbr\/(.*?)\}\}\s*", value, re.IGNORECASE)
        if abbr_match:
            date, time, _ = normalize_date(abbr_match.group(1).strip())
            return {
                key: date,
                f"{key}_time": time,
                f"{key}_timezone": abbr_match.group(2).strip(),
            }
    elif re.match(r"\{\{(?:TeamOpponent|LiteralOpponent)\|", value):
        opponent_match = re.match(
            r"\{\{(?:TeamOpponent|LiteralOpponent)\|(.*?)(?:\|score=([^|]*))?\}\}",
            value,
        )
        if opponent_match:
            result = {key: opponent_match.group(1).strip()}
            if opponent_match.group(2) is not None:
                score = opponent_match.group(2).split("|")[0].strip()
                result[f"{key}_score"] = score
            return result
    elif re.match(r".*\{\{map\|", value, re.IGNORECASE):
        # Extract the map part only
        map_part = re.search(r"\{\{map\|.*", value, re.IGNORECASE).group()
        map_details = re.findall(r"(\w+)=(.*?)(?:\||\}\})", map_part)
        map_dict = {normalize_score_key(k): v for k, v in map_details}
        return {key: map_dict}
    elif re.match(r"\{\{TIERTEXT/\d+\}\}", value):
        # Extract the number from TIERTEXT
        tiertext_match = re.match(r"\{\{TIERTEXT/(\d+)\}\}", value)
        if tiertext_match:
            return {key: convert_liquipediatier(tiertext_match.group(1).strip())}
    return {key: value}

def normalize_score_key(key):
    """Normalizes score keys to a standard format."""
    if key == "score":
        return "score1"
    elif key == "score2":
        return "score2"
    return key

def normalize_date(date_str):
    """Normalizes date strings to a standard format."""
    try:
        # Handle ISO 8601 format with 'T' and milliseconds
        if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$", date_str):
            date_part = date_str.split('T')[0]
            return date_part, "", None
        else:
            parsed_date = parser.parse(date_str)
            date = parsed_date.strftime("%Y-%m-%d")
            time = parsed_date.strftime("%H:%M:%S") if parsed_date.strftime("%H:%M:%S") != "00:00:00" else ""
            return date, time, None
    except (ValueError, OverflowError) as e:
        return date_str, "", str(e)

def convert_liquipediatier(value):
    """Converts liquipediatier value to an integer, defaulting to 5 if invalid."""
    if '|' in value:
        value = value.split('|')[0]  # Take only the part before the pipe
    
    try:
        value = int(value)
        if value > 0:
            return value
    except (ValueError, TypeError):
        pass
    return 5


def process_dict(d):
    """Processes a dictionary, handling nested dictionaries and lists."""
    new_dict = {}
    for key, value in d.items():
        if isinstance(value, dict):
            new_dict[key] = process_dict(value)
        elif isinstance(value, list):
            new_dict[key] = [
                process_dict(item) if isinstance(item, dict) else item
                for item in value
            ]
        elif isinstance(value, str):
            if key.lower() == 'liquipediatier':
                new_dict[key] = convert_liquipediatier(value)
            elif value.startswith("[") and value.endswith("]"):
                # Ignore values that are objects enclosed in square brackets
                new_dict[key] = value
            elif "{{" in value and "}}" in value:
                # Handle special elements
                special_elements = process_special_elements(key, value)
                new_dict.update(special_elements)
            else:
                new_dict.update(split_key_value(key, value))
        else:
            new_dict[key] = value
    return new_dict

def sanitize_keys(data):
    """Sanitizes keys by replacing hyphens with underscores."""
    if isinstance(data, dict):
        return {key.replace("-", "_"): sanitize_keys(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [sanitize_keys(item) if isinstance(item, dict) else item for item in data]
    else:
        return data

def normalize_dates_in_dict(d):
    """Normalizes date strings in the dictionary to a standard format."""
    keys_to_update = []
    for key, value in d.items():
        if isinstance(value, dict):
            normalize_dates_in_dict(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    normalize_dates_in_dict(item)
        elif isinstance(value, str) and 'date' in key.lower():
            date, time, _ = normalize_date(value)
            if date != value:  # Only add if the value was changed
                keys_to_update.append((key, date, time))
    for key, date, time in keys_to_update:
        d[key] = date
        if time:
            d[f"{key}_time"] = time
    return d

def strip_and_uppercase_values(d):
    """Strips and converts all string values to uppercase."""
    if isinstance(d, dict):
        for key, value in d.items():
            if isinstance(value, dict):
                strip_and_uppercase_values(value)
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        strip_and_uppercase_values(item)
                    elif isinstance(item, str):
                        value[i] = item.strip().upper()
            elif isinstance(value, str):
                d[key] = value.strip().upper()
    elif isinstance(d, list):
        for i, item in enumerate(d):
            if isinstance(item, dict):
                strip_and_uppercase_values(item)
            elif isinstance(item, str):
                d[i] = item.strip().upper()
    return d

def extract_members(d):
    """Extracts members from the dictionary and stores them in a members list."""
    if isinstance(d, dict):
        members = []
        keys_to_remove = []

        for key, value in d.items():
            if re.match(r"p\d+$", key):
                role_type = "player"
                index = key[1:]
            elif re.match(r"t\d+[a-zA-Z]\d+$", key):
                if key[2].isalpha() and key[2] == "p":
                    role_type = "sub"
                    index = f"{key[:2]}pos{key[3:]}"
                else:
                    role_type = "staff"
                    index = f"{key}pos"
            else:
                role_type = None

            if role_type:
                pos_key = f"pos{index}" if role_type == "player" else index
                flag_key = f"{key}flag"
                member_name = value
                member_role = d.get(pos_key, "")
                member_flag = d.get(flag_key, "")
                members.append({
                    "name": member_name,
                    "position": member_role,
                    "flag": member_flag,
                    "role_type": role_type
                })
                keys_to_remove.extend([key, pos_key, flag_key])

            if isinstance(value, dict):
                extract_members(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        extract_members(item)

        for key in keys_to_remove:
            if key in d:
                del d[key]

        if members:
            d["members"] = members

    elif isinstance(d, list):
        for item in d:
            if isinstance(item, dict):
                extract_members(item)

    return d

def clean_json(data):
    """Processes a JSON object."""
    sanitized_data = sanitize_keys(data)
    processed_data = process_dict(sanitized_data)
    stripped_data = strip_and_uppercase_values(processed_data)
    normalized_data = normalize_dates_in_dict(stripped_data)
    final_data = extract_members(normalized_data)
    return final_data

# Example usage in another script:
if __name__ == "__main__":
    # Example JSON object
    data = {
        "team": "Toronto Defiant",
        "p1": "MER1T|pos1=dps",
        "p2": "Sugarfree|pos2=dps",
        "p3": "SOMEONE|pos3=tank",
        "p4": "Vega|pos4=sup",
        "p5": "Rupal|pos5=sup",
        "t2title": "Staff",
        "t2c1": "Casores|t2c1pos=head coach",
        "t2c2": "Danny|t2c2pos=analyst|t2c2link=Danny (Canadian Coach)",
        "qualifier": "[[Overwatch_Champions_Series/2024/North_America/Stage_2/Main_Event|NA Stage 2]]",
        "placement": "1",
        "bestof": "5",
        "date": "May 31st, 2024 - 13:45  {{Abbr/PDT}}",
        "youtube": "|twitch=ow_esports2",
        "mvp": "",
        "opponent1": "{{TeamOpponent|Spacestation Gaming}}",
        "opponent2": "{{TeamOpponent|NRG Shock|score=3}}",
        "map1": "{{Map|map=Antarctic Peninsula|mode=Control|score1=2|score2=0|winner=1}}",
        "map2": "{{Map|map=Midtown|mode=Hybrid|score1=3|score2=0|winner=1}}",
        "map3": "{{Map|map=Watchpoint: Gibraltar|mode=Escort|score=0|score2=3|winner=2}}",
        "comment": "Casters: {{player|Jaws|flag=uk}} & {{player|Nekkra|flag=us}}",
        "optional": "R1M1=",
        "map4": "{{map|map=Dorado|mode=Escort|score1=2|score2=1|winner=1}}",
        "opponent3": "{{TeamOpponent|Strawberry Stranglers|score=FF}}",
        "opponent4": "Project Flying|score=W|walkover=1",
        "opponent5": "{{LiteralOpponent|Test Opponent|score=}}",
        "opponent6": "{{TeamOpponent|ngred|score=-}}",
        "extra": "MerryGo{{Map|map=|mode=|score1=|score2=|winner=}}",
        "team2": "seoul dynasty |games2=0",
        "tier": "{{TIERTEXT/5}}",
        "liquipediatier": "3|liquitestvalue=xx",
    }

    processed_data = clean_json(data)
    print(json.dumps(processed_data, indent=4))
