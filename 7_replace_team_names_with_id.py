import sqlite3

import pandas as pd
from fuzzywuzzy import fuzz, process


# Step 1: Download the Teams, Matches, and Tournaments tables
def download_tables():
    conn = sqlite3.connect("tournaments.db")
    teams_df = pd.read_sql_query("SELECT * FROM Teams", conn)
    matches_df = pd.read_sql_query("SELECT * FROM Matches", conn)
    conn.close()
    return teams_df, matches_df


# Step 2: Define the Overwatch League acronym mappings
acronym_mapping = {
    "SFS": "SAN FRANCISCO SHOCK",
    "VAL": "LOS ANGELES VALIANT",
    "SHD": "SHANGHAI DRAGONS",
    "SEO": "SEOUL DYNASTY",
    "DAL": "DALLAS FUEL",
    "FLA": "FLORIDA MAYHEM",
    "LDN": "LONDON SPITFIRE",
    "PHI": "PHILADELPHIA FUSION",
    "HOU": "HOUSTON OUTLAWS",
    "NYE": "NEW YORK EXCELSIOR",
    "BOS": "BOSTON UPRISING",
    "GLA": "LOS ANGELES GLADIATORS",
    "TOR": "TORONTO DEFIANT",
    "GZC": "GUANGZHOU CHARGE",
    "CDH": "CHENGDU HUNTERS",
    "HZS": "HANGZHOU SPARK",
    "PAR": "PARIS ETERNAL",
    "WAS": "WASHINGTON JUSTICE",
    "VAN": "VANCOUVER TITANS",
    "ATL": "ATLANTA REIGN",
    "INF": "SEOUL INFERNAL",
    "LVE": "LAS VEGAS ETERNAL",
    "LG": "LUMINOSITY GAMING",
    "NV": "ENVYUS",
    "C9": "CLOUD9",
    "COL": "COMPLEXITY",
    "BLK": "BLANK ESPORTS",
    "FAZE": "FAZE CLAN",
    "NGRED": "NORTHERN GAMING RED",
    "METHOD": "METHOD ESPORTS",
    "GFE": "GALE FORCE ESPORTS",
    "NIP": "NINJAS IN PYJAMAS",
    "BLG": "BILIBILI GAMING",
    "T1W": "THE ONE WINNER",
    "LG EVIL": "LUMINOSITY GAMING EVIL",
    "LDLC": "TEAM LDLC",
    "VG": "VICI GAMING",
    "MY": "MIRACULOUS YOUNGSTER",
    "LF": "LUCKY FUTURE",
    "WE.WHITE": "WORLD ELITE WHITE",
    "WE": "WORLD ELITE",
    "MELTY": "MELTY ESPORTS",
    "CLG": "COUNTER LOGIC GAMING",
    "MT1": "MIRACLE TEAM ONE",
    "LGD": "LGD GAMING",
    "CC": "TEAM CC",
    "FTD": "FTD CLUB",
    "LGE": "LINGAN E-SPORTS",
    "SKG": "TEAM SKADIS GIFT",
    "OMG": "OH MY GOD",
    "CL": "TEAM CELESTIAL",
    "AHQ": "AHQ E-SPORTS CLUB",
    "FW": "FLASH WOLVES",
    "M17": "MACHI ESPORTS",
    "DTN.GOLD": "DETONATOR.GOLD",
    "HKA": "HONG KONG ATTITUDE",
    "SST": "SUNSISTER",
    "FB": "FIREBALL",
    "LS": "LIBALENT SUPREME",
    "MT": "MEGA THUNDER",
    "MYTH": "MYTH CLUB",
    "FIN": "FINLAND",
    "HKG": "HONG KONG",
    "ITA": "ITALY",
    "AUS": "AUSTRALIA",
    "DNK": "DENMARK",
    "SAU": "SAUDI ARABIA",
    "NLD": "NETHERLANDS",
    "SGP": "SINGAPORE",
    "ZAF": "SOUTH AFRICA",
    "SWE": "SWEDEN",
    "JPN": "JAPAN",
    "KOR": "KOREA",
    "PRT": "PORTUGAL",
    "NOR": "NORWAY",
    "NZL": "NEW ZEALAND",
    "AUT": "AUSTRIA",
    "TWN": "TAIWAN",
    "CHN": "CHINA",
    "PRY": "PARAGUAY",
    "LVA": "LATVIA",
    "MEX": "MEXICO",
    "DEU": "GERMANY",
    "ESP": "SPAIN",
    "FTG": "FOR THE GAMER",
    "LFZ": "LUCKY FUTURE ZERO",
    "HTP": "HERO TACITURN PANTHER",
    "MSC": "MOSS SEVEN CLUB",
    "FL": "FIAT LUX",
    "SN1": "SUPER NUMBER 1",
    "TFV": "TEAM FOR VICTORY",
    "ZWD": "ZENITH WITHOUT DOWN",
    "FG": "FLAG GAMING",
    "EM": "ELEMENT MYSTIC",
    "NC W": "NC WOLVES",
    "LH": "LUNATIC HAI",
    "LH2": "LUNATIC HAI 2",
    "BSG": "BONS SPIRIT GAMING",
    "DTN.KR": "DETONATOR.KOREA",
    "EG": "EVIL GENIUSES",
    "MOUZ": "MOUSESPORTS",
    "SYF": "STAY FROSTY",
    "ATH": "ATHLETICO",
    "DS": "DARK SIDE",
    "MM GC": "MASTERMINDS GC",
    "MM BLUE": "MASTERMINDS BLUE",
    "FFF": "FIRST FABULOUS FIGHTER",
    "YL": "TEAM YL",
    "WKG": "WUKONG GAMING",
    "USG": "UNSOLD STUFF GAMING",
    "MC": "MYTH CLUB",
    "NGA.T": "NGA.Titan",
    "NWB.Y": "NEWBEE.Y",
    "NWB": "NEWBEE",
    "NGA.S": "NGA.SCYTHE",
    "ahq": "AHQ E-SPORTS CLUB",
    "BA": "BLACK ANANAS",
    "Royal": "STAR HORN ROYAL CLUB",
    "SHRC": "STAR HORN ROYAL CLUB",
    "DR": "DAWN RAID",
    "JHG.R": "JHG.RED",
    "SV": "SUPER VALIANT GAMING",
    "Snake": "SNAKE ESPORTS",
    "WE.W": "WE.WHITE",
    "GK": "GANK",
    "B.o.o.T": "B.o.o.T Gaming",
    "FTW": "FOR THE WIN",
    "HU STORM": "HARRISBURG STORM",
    "BGH": "BRAZIL GAMING HOUSE",
    "LTP": "LIKE THIS PLAYER",
}

# Initialize the log file
log_file = "matches_log.txt"
open(log_file, "w").close()  # Clear the log file at the start


def log_change(match_id, original_team, new_team_id, method, teams_df):
    new_team_name = (
        teams_df.loc[teams_df["id"] == new_team_id, "team"].values[0]
        if new_team_id
        else "None"
    )
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(
            f"Match ID: {match_id}, Original Team: {original_team}, New Team: {new_team_name}, Method: {method}\n"
        )


# Step 3: Normalize team names in the Matches table
def normalize_team_names(teams_df, matches_df):
    def map_acronym(team):
        return acronym_mapping.get(team, team)

    def find_exact_match(team, tournament_id):
        teams_in_tournament = teams_df[
            teams_df["tournament_id"] == tournament_id
        ]
        exact_match = teams_in_tournament[teams_in_tournament["team"] == team]
        if not exact_match.empty:
            return exact_match.iloc[0]["id"]
        return None

    def find_nearby_exact_match(team, initial_team_index):
        start_index = max(0, initial_team_index - 5000)
        end_index = min(len(teams_df), initial_team_index + 5000)
        nearby_teams = teams_df.iloc[start_index:end_index].copy()

        nearby_teams["index_diff"] = (
            nearby_teams.index.to_series() - initial_team_index
        ).abs()
        exact_match = nearby_teams[nearby_teams["team"] == team]

        if not exact_match.empty:
            return exact_match.loc[exact_match["index_diff"].idxmin()]["id"]
        return None

    def find_fuzzy_match(team, tournament_id):
        teams_in_tournament = teams_df[
            teams_df["tournament_id"] == tournament_id
        ]
        if teams_in_tournament.empty:
            return None
        closest_team, score = process.extractOne(
            team, teams_in_tournament["team"], scorer=fuzz.ratio
        )[:2]
        if score >= 45:
            return teams_in_tournament[
                teams_in_tournament["team"] == closest_team
            ].iloc[0]["id"]
        return None

    def normalize_team_id(row):
        try:
            team1 = map_acronym(row["team1"])
            team2 = map_acronym(row["team2"])
            tournament_id = row["tournament_id"]
            match_id = row["id"]

            # Process team1
            team1_id = find_exact_match(team1, tournament_id)
            if not team1_id:
                initial_team_index = teams_df[
                    teams_df["tournament_id"] == tournament_id
                ].index[0]
                team1_id = find_nearby_exact_match(team1, initial_team_index)
                if team1_id:
                    log_change(
                        match_id,
                        team1,
                        team1_id,
                        "Nearby Exact Match",
                        teams_df,
                    )
            if not team1_id:
                team1_id = find_fuzzy_match(team1, tournament_id)
                if team1_id:
                    log_change(
                        match_id, team1, team1_id, "Fuzzy Match", teams_df
                    )
            if not team1_id:
                log_change(match_id, team1, None, "Gave Up", teams_df)

            # Process team2
            team2_id = find_exact_match(team2, tournament_id)
            if not team2_id:
                initial_team_index = teams_df[
                    teams_df["tournament_id"] == tournament_id
                ].index[0]
                team2_id = find_nearby_exact_match(team2, initial_team_index)
                if team2_id:
                    log_change(
                        match_id,
                        team2,
                        team2_id,
                        "Nearby Exact Match",
                        teams_df,
                    )
            if not team2_id:
                team2_id = find_fuzzy_match(team2, tournament_id)
                if team2_id:
                    log_change(
                        match_id, team2, team2_id, "Fuzzy Match", teams_df
                    )
            if not team2_id:
                log_change(match_id, team2, None, "Gave Up", teams_df)

            return team1_id, team2_id

        except Exception as e:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"Error processing match ID {row['id']}: {e}\n")
            return None, None

    # Apply normalization to the matches DataFrame
    results = matches_df.apply(normalize_team_id, axis=1, result_type="expand")
    matches_df[["team1_id", "team2_id"]] = results

    # Filter out rows with None values for team1_id or team2_id
    matches_df = matches_df.dropna(subset=["team1_id", "team2_id"])

    return matches_df


# Step 4: Add new columns and update the existing Matches table
def update_matches_table(normalized_matches_df):
    conn = sqlite3.connect("tournaments.db")
    cursor = conn.cursor()

    # Check if columns team1_id and team2_id exist in the Matches table
    cursor.execute("PRAGMA table_info(Matches)")
    columns = [info[1] for info in cursor.fetchall()]

    # Drop columns team1_id and team2_id if they exist
    if "team1_id" in columns:
        cursor.execute("ALTER TABLE Matches DROP COLUMN team1_id")
    if "team2_id" in columns:
        cursor.execute("ALTER TABLE Matches DROP COLUMN team2_id")

    # Add new columns team1_id and team2_id to the Matches table if they don't exist
    cursor.execute("ALTER TABLE Matches ADD COLUMN team1_id INTEGER")
    cursor.execute("ALTER TABLE Matches ADD COLUMN team2_id INTEGER")

    # Update the new columns with the normalized team IDs
    update_query = """
    UPDATE Matches
    SET team1_id = ?,
        team2_id = ?
    WHERE id = ?
    """
    for index, row in normalized_matches_df.iterrows():
        cursor.execute(
            update_query, (row["team1_id"], row["team2_id"], row["id"])
        )

    # Commit transaction
    conn.commit()
    conn.close()


# Main script
teams_df, matches_df = download_tables()
normalized_matches_df = normalize_team_names(teams_df, matches_df)
update_matches_table(normalized_matches_df)

print("Team name normalization completed. Check matches_log.txt for details.")
