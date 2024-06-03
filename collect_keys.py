import json
import os
from collections import Counter


def process_json_files(folder_path):
    tournament_info_keys = Counter()
    team_info_keys = Counter()

    # Traverse the folder
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)

                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                    # Process tournament_info
                    if "tournament_info" in data:
                        for tournament in data["tournament_info"]:
                            tournament_info_keys.update(tournament.keys())

                    # Process team_cards
                    if "team_cards" in data:
                        for team in data["team_cards"]:
                            team_info_keys.update(team.keys())

    return tournament_info_keys, team_info_keys


def save_to_file(file_path, data):
    with open(file_path, "w", encoding="utf-8") as f:
        for key, count in sorted(
            data.items(), key=lambda item: item[1], reverse=True
        ):
            f.write(f"{key}: {count}\n")


# Process legacy_tournaments_data folder
legacy_tournament_info_keys, legacy_team_info_keys = process_json_files(
    "legacy_tournaments_data"
)

# Process tournaments_data folder
tournament_info_keys, team_info_keys = process_json_files("tournaments_data")

# Save results to files
save_to_file("legacy_tournament_info_keys.txt", legacy_tournament_info_keys)
save_to_file("legacy_team_info_keys.txt", legacy_team_info_keys)
save_to_file("tournament_info_keys.txt", tournament_info_keys)
save_to_file("team_info_keys.txt", team_info_keys)

print("Keys and frequencies have been written to files.")
