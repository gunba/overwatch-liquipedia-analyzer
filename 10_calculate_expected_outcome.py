import sqlite3
import pandas as pd
from tqdm import tqdm

def calculate_win_probability(team1_elo, team2_elo):
    if team1_elo == team2_elo:
        return 0.5  # If ELOs are equal, the win chance is 50%
    expected_score_team1 = 1 / (1 + 10 ** ((team2_elo - team1_elo) / 400))
    return expected_score_team1

def update_team_win_chances(db_path):
    conn = sqlite3.connect(db_path)

    # Load data into pandas DataFrames
    matches_df = pd.read_sql_query(
        """
        SELECT id, team1_id, team2_id, winner FROM Matches
        WHERE winner IN (1, 2) AND team1_id IS NOT NULL AND team2_id IS NOT NULL
        """, conn
    )
    ratings_df = pd.read_sql_query(
        "SELECT match_id, unique_id, player_elo_before, player_elo_after FROM Ratings", conn
    )

    # Initialize a dictionary to store average ELO for each match and team
    match_elos = {}

    # Process each match
    for match_id in tqdm(matches_df["id"].unique(), desc="Processing matches"):
        match_info = matches_df[matches_df["id"] == match_id].iloc[0]
        winner = match_info["winner"]

        # Determine players in team1 and team2 based on their ELO change
        if winner == 1:
            team1_players = ratings_df[(ratings_df["match_id"] == match_id) & 
                                       (ratings_df["player_elo_before"] < ratings_df["player_elo_after"])]
            team2_players = ratings_df[(ratings_df["match_id"] == match_id) & 
                                       (ratings_df["player_elo_before"] > ratings_df["player_elo_after"])]
        else:  # winner == 2
            team1_players = ratings_df[(ratings_df["match_id"] == match_id) & 
                                       (ratings_df["player_elo_before"] > ratings_df["player_elo_after"])]
            team2_players = ratings_df[(ratings_df["match_id"] == match_id) & 
                                       (ratings_df["player_elo_before"] < ratings_df["player_elo_after"])]

        if team1_players.empty or team2_players.empty:
            team1_avg_elo = 1500
            team2_avg_elo = 1500
        else:
            team1_avg_elo = team1_players["player_elo_before"].mean()
            team2_avg_elo = team2_players["player_elo_before"].mean()

        match_elos[match_id] = {
            "team1_avg_elo": team1_avg_elo,
            "team2_avg_elo": team2_avg_elo,
        }

    # Drop the column if it exists and then add it as FLOAT
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(Matches)")
    columns = [column[1] for column in cursor.fetchall()]
    if "team1_winchance" in columns:
        cursor.execute("ALTER TABLE Matches DROP COLUMN team1_winchance")
    cursor.execute("ALTER TABLE Matches ADD COLUMN team1_winchance FLOAT")

    # Calculate the win probability for team1 and update the Matches table
    for match_id in tqdm(matches_df["id"].unique(), desc="Updating matches"):
        if match_id in match_elos:
            win_chance = calculate_win_probability(
                match_elos[match_id]["team1_avg_elo"], match_elos[match_id]["team2_avg_elo"]
            )
            try:
                cursor.execute(
                    f"UPDATE Matches SET team1_winchance = '{win_chance}' WHERE id = '{match_id}'"
                )
            except sqlite3.Error as e:
                print(f"SQLite error: {e}")
    
    conn.commit()
    conn.close()

# Usage
update_team_win_chances("tournaments.db")
