import sqlite3
from datetime import datetime, timedelta

import pandas as pd
from fuzzywuzzy import fuzz, process


# Step 1: Download the Teams, Matches, and Tournaments tables
def download_tables():
    conn = sqlite3.connect("tournaments.db")
    teams_df = pd.read_sql_query("SELECT * FROM Teams", conn)
    matches_df = pd.read_sql_query("SELECT * FROM Matches", conn)
    tournaments_df = pd.read_sql_query("SELECT * FROM Tournaments", conn)
    conn.close()
    return teams_df, matches_df, tournaments_df


# Step 2: Define the Overwatch League acronym mappings
acronym_mapping = {
    "SFS": "San Francisco Shock",
    "VAL": "Los Angeles Valiant",
    "SHD": "Shanghai Dragons",
    "SEO": "Seoul Dynasty",
    "DAL": "Dallas Fuel",
    "FLA": "Florida Mayhem",
    "LDN": "London Spitfire",
    "PHI": "Philadelphia Fusion",
    "HOU": "Houston Outlaws",
    "NYE": "New York Excelsior",
    "BOS": "Boston Uprising",
    "GLA": "Los Angeles Gladiators",
    "TOR": "Toronto Defiant",
    "GZC": "Guangzhou Charge",
    "CDH": "Chengdu Hunters",
    "HZS": "Hangzhou Spark",
    "PAR": "Paris Eternal",
    "WAS": "Washington Justice",
    "VAN": "Vancouver Titans",
    "ATL": "Atlanta Reign",
    "INF": "Seoul Infernal",
    "LVE": "Las Vegas Eternal",
}


# Step 3: Normalize team names in the Matches table
def normalize_team_names(teams_df, matches_df, tournaments_df, log_file):
    unmatched_teams = []
    matched_teams = []

    def log_message(message):
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(message + "\n")

    def map_acronym(team):
        return acronym_mapping.get(team, team)

    def get_closest_tournament_by_date(team_name, match_date):
        match_date = datetime.strptime(match_date, "%Y-%m-%d")
        team_records = teams_df[
            teams_df["team"].str.lower() == team_name.lower()
        ]
        if team_records.empty:
            return None
        tournament_ids = team_records["tournament_id"].unique()
        tournament_records = tournaments_df[
            tournaments_df["id"].isin(tournament_ids)
        ]
        if tournament_records.empty:
            return None
        min_diff = timedelta(days=30)
        closest_tournament = None
        for _, tournament in tournament_records.iterrows():
            tournament_dates = []
            for col in ["date", "sdate", "edate"]:
                date_str = tournament[col]
                if pd.notna(date_str):
                    try:
                        t_date = datetime.strptime(date_str, "%Y-%m-%d")
                        tournament_dates.append(t_date)
                    except ValueError:
                        continue  # Skip dates that do not match the format
            for t_date in tournament_dates:
                date_diff = abs(t_date - match_date)
                if date_diff <= min_diff:
                    min_diff = date_diff
                    closest_tournament = tournament
        return closest_tournament

    def acronym_match(acronym, full_name):
        acronym_chars = "".join([c for c in full_name if c.isupper()])
        return acronym == acronym_chars

    def get_closest_team_id(team, match_date, tournament_teams, tournament_id):
        if team.lower() in [t.lower() for t in tournament_teams["team"].values]:
            matched_team = tournament_teams[
                tournament_teams["team"].str.lower() == team.lower()
            ].iloc[0]
            matched_team_id = matched_team["id"]
            matched_team_name = matched_team["team"]
            log_message(
                f"Exact match found: {team} -> {matched_team_name} (ID: {matched_team_id})"
            )
            return matched_team_id

        for t in tournament_teams["team"].values:
            if acronym_match(team, t):
                matched_team = tournament_teams[
                    tournament_teams["team"] == t
                ].iloc[0]
                matched_team_id = matched_team["id"]
                matched_team_name = matched_team["team"]
                log_message(
                    f"Acronym match found: {team} -> {matched_team_name} (ID: {matched_team_id})"
                )
                return matched_team_id

        closest_match, score = process.extractOne(
            team, tournament_teams["team"].values, scorer=fuzz.ratio
        )
        if score >= 30:  # Adjust the cutoff as necessary
            matched_team = tournament_teams[
                tournament_teams["team"] == closest_match
            ].iloc[0]
            matched_team_id = matched_team["id"]
            matched_team_name = matched_team["team"]
            log_message(
                f"Fuzzy match found: {team} -> {matched_team_name} (ID: {matched_team_id}, Score: {score})"
            )
            return matched_team_id

        unmatched_teams.append(
            f"Unmatched team: {team} in tournament {tournament_id}"
        )
        log_message(f"Unmatched team: {team} in tournament {tournament_id}")
        return None

    def normalize_team_id(team, match_date, tournament_id):
        if team == "BYE":
            return None
        team = map_acronym(team)
        tournament_teams = teams_df[teams_df["tournament_id"] == tournament_id]
        if not tournament_teams.empty:
            return get_closest_team_id(
                team, match_date, tournament_teams, tournament_id
            )

        closest_tournament = get_closest_tournament_by_date(team, match_date)
        if closest_tournament is not None:
            tournament_teams = teams_df[
                teams_df["tournament_id"] == closest_tournament["id"]
            ]
            if not tournament_teams.empty:
                return get_closest_team_id(
                    team, match_date, tournament_teams, closest_tournament["id"]
                )

        return None

    matches_df["team1_id"] = matches_df.apply(
        lambda row: normalize_team_id(
            row["team1"], row["date"], row["tournament_id"]
        ),
        axis=1,
    )
    matches_df["team2_id"] = matches_df.apply(
        lambda row: normalize_team_id(
            row["team2"], row["date"], row["tournament_id"]
        ),
        axis=1,
    )

    return matches_df, matched_teams, unmatched_teams


# Step 4: Upload the normalized Matches table back to the database
def upload_matches_table(matches_df, log_file):
    conn = sqlite3.connect("tournaments.db")
    cursor = conn.cursor()

    # Begin transaction
    cursor.execute("BEGIN;")

    # Delete all rows in the Matches table
    cursor.execute("DELETE FROM Matches;")

    # Insert updated rows, filtering out rows with None values for team1 or team2
    data_to_insert = matches_df.drop(columns=["team1", "team2"]).rename(
        columns={"team1_id": "team1", "team2_id": "team2"}
    )
    data_to_insert = data_to_insert.dropna(subset=["team1", "team2"])

    insert_query = """
    INSERT INTO Matches (id, tournament_id, date, date_time, date_timezone, team1, team2, score1, score2, winner, mvp, comment, owl, vod, format) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cursor.executemany(insert_query, data_to_insert.to_records(index=False))
    except sqlite3.IntegrityError:
        with open(log_file, "a", encoding="utf-8") as f:
            for record in data_to_insert.to_records(index=False):
                try:
                    cursor.execute(insert_query, record)
                except sqlite3.IntegrityError as record_error:
                    f.write(
                        f"Error inserting record {record}: {record_error}\n"
                    )

    # Commit transaction
    conn.commit()
    conn.close()


# Main script
log_file = "matches_log.txt"
open(log_file, "w").close()  # Clear the log file at the start

teams_df, matches_df, tournaments_df = download_tables()
normalized_matches_df, matched_teams, unmatched_teams = normalize_team_names(
    teams_df, matches_df, tournaments_df, log_file
)
upload_matches_table(normalized_matches_df, log_file)

print("Team name normalization completed. Check matches_log.txt for details.")
