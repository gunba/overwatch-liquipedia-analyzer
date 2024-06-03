import json
import re


def split_key_value(key, value):
    """Splits a key-value pair if the value contains '|' and '=' and does not have nested elements."""
    if (
        "|" in value
        and "=" in value
        and "{{" not in value
        and "}}" not in value
    ):
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
    if "{{Abbr/" in value:
        abbr_match = re.match(r"(.+?)\s\{\{Abbr\/(.*?)\}\}", value)
        if abbr_match:
            return {
                key: abbr_match.group(1).strip(),
                f"{key}_timezone": abbr_match.group(2).strip(),
            }
    elif re.match(r"\{\{TeamOpponent\|", value):
        opponent_match = re.match(
            r"\{\{TeamOpponent\|(.*?)(?:\|score=(\d+))?\}\}", value
        )
        if opponent_match:
            result = {key: opponent_match.group(1).strip()}
            if opponent_match.group(2):
                result[f"{key}_score"] = int(opponent_match.group(2))
            return result
    elif re.match(r"\{\{map\|", value, re.IGNORECASE):
        map_details = re.findall(r"(\w+)=(.*?)(?:\||\}\})", value)
        map_dict = {k: v for k, v in map_details}
        return {key: map_dict}
    return {key: value}


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
            if value.startswith("[") and value.endswith("]"):
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


def clean_json(data):
    """Processes a JSON object."""
    return process_dict(data)


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
        "date": "May 31, 2024 - 13:45 {{Abbr/PDT}}",
        "youtube": "|twitch=ow_esports2",
        "mvp": "",
        "opponent1": "{{TeamOpponent|Spacestation Gaming}}",
        "opponent2": "{{TeamOpponent|NRG Shock|score=3}}",
        "map1": "{{Map|map=Antarctic Peninsula|mode=Control|score1=2|score2=0|winner=1}}",
        "map2": "{{Map|map=Midtown|mode=Hybrid|score1=3|score2=0|winner=1}}",
        "map3": "{{Map|map=Watchpoint: Gibraltar|mode=Escort|score1=3|score2=1|winner=1}}",
        "comment": "Casters: {{player|Jaws|flag=uk}} & {{player|Nekkra|flag=us}}",
        "optional": "R1M1=",
        "map4": "{{map|map=Dorado|mode=Escort|score1=2|score2=1|winner=1}}",
    }

    processed_data = clean_json(data)
    print(json.dumps(processed_data, indent=4))
