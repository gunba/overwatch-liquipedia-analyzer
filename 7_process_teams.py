import sqlite3

import pandas as pd


# Step 1: Download the Tournaments, Teams, and Members tables
def download_tables():
    conn = sqlite3.connect("tournaments.db")
    tournaments_df = pd.read_sql_query("SELECT * FROM Tournaments", conn)
    teams_df = pd.read_sql_query("SELECT * FROM Teams", conn)
    members_df = pd.read_sql_query("SELECT * FROM Members", conn)
    conn.close()
    return tournaments_df, teams_df, members_df


# Step 2: Find the closest tournament with team data based on different criteria
def find_closest_tournament_with_teams(
    tournaments_df, teams_df, current_tournament
):
    current_index = current_tournament["id"]
    filtered_df = tournaments_df.dropna(subset=["team_number"]).copy()

    def has_teams(tournament_id):
        return not teams_df[teams_df["tournament_id"] == tournament_id].empty

    def get_longest_contained_tournament(filtered_df, current_tournament):
        contained_df = filtered_df.loc[
            filtered_df["name"].apply(lambda x: x in current_tournament["name"])
        ]
        if not contained_df.empty:
            contained_df = contained_df.assign(
                name_length=contained_df["name"].str.len()
            )
            contained_df = contained_df.sort_values(
                by="name_length", ascending=False
            )
            for _, row in contained_df.iterrows():
                if has_teams(row["id"]):
                    return row
        return None

    def calculate_similarity_score(name1, name2, index_diff):
        identical_chars = sum(1 for a, b in zip(name1, name2) if a == b)
        return identical_chars - index_diff

    def get_best_match(filtered_df, current_tournament):
        scores = filtered_df.apply(
            lambda row: calculate_similarity_score(
                row["name"],
                current_tournament["name"],
                abs(row["id"] - current_index),
            ),
            axis=1,
        )
        best_match = filtered_df.loc[scores.idxmax()]
        if has_teams(best_match["id"]):
            return best_match
        return None

    # 1. Prioritize the name containment
    closest_by_name = get_longest_contained_tournament(
        filtered_df, current_tournament
    )
    if closest_by_name is not None:
        return closest_by_name

    # 2. Match by best similarity score and index distance
    same_series_organizer = filtered_df[
        (
            (filtered_df["series"] == current_tournament["series"])
            | (filtered_df["organizer"] == current_tournament["organizer"])
        )
        & (filtered_df["id"] != current_index)  # Exclude current tournament
    ]
    if not same_series_organizer.empty:
        closest_by_similarity = get_best_match(
            same_series_organizer, current_tournament
        )
        if closest_by_similarity is not None:
            return closest_by_similarity

    # 3. Fallback to directly preceding index if no series/organizer match found
    closest_preceding_fallback = get_best_match(filtered_df, current_tournament)
    if closest_preceding_fallback is not None:
        return closest_preceding_fallback

    return None


# Step 3: Impute missing team data and associated member data
def impute_team_and_member_data(tournaments_df, teams_df, members_df):
    updated_teams = []
    updated_members = []
    max_team_id = teams_df["id"].max() if not teams_df.empty else 0
    max_member_id = members_df["id"].max() if not members_df.empty else 0

    for _, tournament in tournaments_df.iterrows():
        if tournament["id"] not in teams_df["tournament_id"].values:
            closest_tournament = find_closest_tournament_with_teams(
                tournaments_df, teams_df, tournament
            )

            if closest_tournament is not None:
                closest_teams = teams_df[
                    teams_df["tournament_id"] == closest_tournament["id"]
                ]
                for _, team in closest_teams.iterrows():
                    max_team_id += 1
                    new_team_id = max_team_id
                    updated_teams.append({
                        "id": new_team_id,
                        "tournament_id": tournament["id"],
                        "team": team["team"],
                    })

                    # Copy associated members
                    closest_members = members_df[
                        members_df["team_id"] == team["id"]
                    ]
                    for _, member in closest_members.iterrows():
                        max_member_id += 1
                        updated_members.append({
                            "id": max_member_id,
                            "team_id": new_team_id,
                            "name": member["name"],
                            "role": member["role"],
                            "flag": member["flag"],
                            "section": member["section"],
                        })

    return updated_teams, updated_members


# Step 4: Upload the imputed team and member data back to the database
def upload_data(updated_teams, updated_members):
    conn = sqlite3.connect("tournaments.db")
    cursor = conn.cursor()

    # Begin transaction
    cursor.execute("BEGIN;")

    # Insert updated teams
    insert_team_query = (
        "INSERT INTO Teams (id, tournament_id, team) VALUES (?, ?, ?)"
    )
    cursor.executemany(
        insert_team_query,
        [
            (int(team["id"]), int(team["tournament_id"]), team["team"])
            for team in updated_teams
        ],
    )

    # Insert updated members
    insert_member_query = "INSERT INTO Members (id, team_id, name, role, flag, section) VALUES (?, ?, ?, ?, ?, ?)"
    cursor.executemany(
        insert_member_query,
        [
            (
                int(member["id"]),
                int(member["team_id"]),
                member["name"],
                member["role"],
                member["flag"],
                member["section"],
            )
            for member in updated_members
        ],
    )

    # Commit transaction
    conn.commit()
    conn.close()


# Main script
tournaments_df, teams_df, members_df = download_tables()
updated_teams, updated_members = impute_team_and_member_data(
    tournaments_df, teams_df, members_df
)
upload_data(updated_teams, updated_members)

print("Team and member data imputation completed.")
