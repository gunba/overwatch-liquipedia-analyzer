import sqlite3

import pandas as pd
from tqdm import tqdm

# Configuration parameters
config = {
    "base_k": 150,  # Base K-factor for ELO calculations
    "initial_elo": 1500.0,  # Initial ELO rating for all players and coaches
    "tier_multiplier_factor_pre_2024": 12,  # Pre-2024 tier multiplier factor
    "tier_exponent_pre_2024": 2,  # Pre-2024 tier exponent
    "tier_multiplier_factor_post_2024": 6,  # Post-2024 tier multiplier factor
    "tier_exponent_post_2024": 1,  # Post-2024 tier exponent
    "max_elo": 3000,  # Maximum ELO value
    "batch_size": 1000,  # Batch size for database inserts
    "elo_midpoint": 1500,  # Midpoint for curbing factor
    "elo_divisor": 2000,  # Divisor for curbing factor
    "elo_exponent": 1.5,  # Exponent for curbing factor
}


def calculate_elo_change(
    player_elo,
    opponent_elo,
    outcome,
    base_k,
    tier,
    tier_multiplier_factor,
    tier_exponent,
    max_elo,
    elo_midpoint,
    elo_divisor,
    elo_exponent,
):
    expected_score = 1 / (1 + 10 ** ((opponent_elo - player_elo) / 400))
    tier_multiplier = tier_multiplier_factor / ((tier + 1) ** tier_exponent)
    k = (base_k * tier_multiplier) / (
        1 + (abs(player_elo - opponent_elo) / 400) ** 2
    )
    change = k * (outcome - expected_score)

    # Apply gradual curbing factor based on distance from elo_midpoint and proximity to max_elo
    distance_from_mid = abs(player_elo - elo_midpoint)
    proximity_to_max = (max_elo - player_elo) / max_elo

    # Scale change based on proximity to max_elo using a sigmoid-like function
    scale_factor = 1 / (1 + (distance_from_mid / elo_divisor) ** elo_exponent)
    curbed_change = change * proximity_to_max * scale_factor

    return curbed_change


def update_elo_ratings(db_path, config):
    base_k = config["base_k"]
    initial_elo = config["initial_elo"]
    tier_multiplier_factor_pre_2024 = config["tier_multiplier_factor_pre_2024"]
    tier_exponent_pre_2024 = config["tier_exponent_pre_2024"]
    tier_multiplier_factor_post_2024 = config[
        "tier_multiplier_factor_post_2024"
    ]
    tier_exponent_post_2024 = config["tier_exponent_post_2024"]
    max_elo = config["max_elo"]
    batch_size = config["batch_size"]
    elo_midpoint = config["elo_midpoint"]
    elo_divisor = config["elo_divisor"]
    elo_exponent = config["elo_exponent"]

    conn = sqlite3.connect(db_path)

    # Drop the existing Ratings table if it exists
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS Ratings")
    cursor.execute("""
        CREATE TABLE Ratings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            unique_id INTEGER,
            match_id INTEGER,
            date TEXT,
            player_elo_before REAL,
            player_elo_after REAL,
            coach_elo_before REAL,
            coach_elo_after REAL,
            is_coach INTEGER
        )
    """)
    conn.commit()

    # Load data into pandas DataFrames
    matches_df = pd.read_sql_query(
        """
        SELECT m.id, m.date, m.team1_id, m.team2_id, m.winner, t.liquipediatier 
        FROM Matches m 
        JOIN Tournaments t ON m.tournament_id = t.id 
        ORDER BY m.date
    """,
        conn,
    )
    members_df = pd.read_sql_query(
        "SELECT unique_id, team_id, job FROM Members WHERE job IN (0, 1)", conn
    )

    # Initialize ELO ratings for each unique player and coach
    unique_ids = members_df["unique_id"].unique()
    elo_ratings = pd.DataFrame({
        "unique_id": unique_ids,
        "player_elo": initial_elo,
        "coach_elo": initial_elo,
    })

    # Initialize a list to store ELO ratings
    ratings_list = []

    # Process matches with tqdm progress bar
    for _, match in tqdm(
        matches_df.iterrows(), total=len(matches_df), desc="Processing matches"
    ):
        match_id, date, team1_id, team2_id, winner, tier = (
            match["id"],
            match["date"],
            match["team1_id"],
            match["team2_id"],
            match["winner"],
            match["liquipediatier"],
        )

        # Determine which scaling factors to use
        if date < "2024-01-01":
            current_tier_multiplier_factor = tier_multiplier_factor_pre_2024
            current_tier_exponent = tier_exponent_pre_2024
        else:
            current_tier_multiplier_factor = tier_multiplier_factor_post_2024
            current_tier_exponent = tier_exponent_post_2024

        # Skip draws
        if winner not in [1, 2]:
            continue

        # Get players and coaches for each team
        team1_players = members_df[
            (members_df["team_id"] == team1_id) & (members_df["job"] == 0)
        ]
        team2_players = members_df[
            (members_df["team_id"] == team2_id) & (members_df["job"] == 0)
        ]
        team1_all = members_df[members_df["team_id"] == team1_id]
        team2_all = members_df[members_df["team_id"] == team2_id]

        if team1_players.empty or team2_players.empty:
            continue  # Skip matches with incomplete data

        # Calculate team average ELO excluding and including coaches
        team1_avg_elo_player = elo_ratings.loc[
            elo_ratings["unique_id"].isin(team1_players["unique_id"]),
            "player_elo",
        ].mean()
        team2_avg_elo_player = elo_ratings.loc[
            elo_ratings["unique_id"].isin(team2_players["unique_id"]),
            "player_elo",
        ].mean()
        team1_avg_elo_coach = elo_ratings.loc[
            elo_ratings["unique_id"].isin(team1_all["unique_id"]), "coach_elo"
        ].mean()
        team2_avg_elo_coach = elo_ratings.loc[
            elo_ratings["unique_id"].isin(team2_all["unique_id"]), "coach_elo"
        ].mean()

        # Determine match outcome
        if winner == 1:
            outcome1, outcome2 = 1, 0
        else:  # winner == 2
            outcome1, outcome2 = 0, 1

        # Calculate ELO change for all members of team1
        for unique_id in team1_all["unique_id"].unique():
            is_coach = (
                members_df.loc[
                    members_df["unique_id"] == unique_id, "job"
                ].values[0]
                == 1
            )
            player_elo_before = float(
                elo_ratings.loc[
                    elo_ratings["unique_id"] == unique_id, "player_elo"
                ].iloc[0]
            )
            coach_elo_before = float(
                elo_ratings.loc[
                    elo_ratings["unique_id"] == unique_id, "coach_elo"
                ].iloc[0]
            )

            player_elo_change = calculate_elo_change(
                player_elo_before,
                team2_avg_elo_player,
                outcome1,
                base_k,
                tier,
                current_tier_multiplier_factor,
                current_tier_exponent,
                max_elo,
                elo_midpoint,
                elo_divisor,
                elo_exponent,
            )
            coach_elo_change = calculate_elo_change(
                coach_elo_before,
                team2_avg_elo_coach,
                outcome1,
                base_k,
                tier,
                current_tier_multiplier_factor,
                current_tier_exponent,
                max_elo,
                elo_midpoint,
                elo_divisor,
                elo_exponent,
            )

            if not is_coach:
                player_elo_after = player_elo_before + player_elo_change
                elo_ratings.loc[
                    elo_ratings["unique_id"] == unique_id, "player_elo"
                ] = player_elo_after
            else:
                player_elo_after = None

            coach_elo_after = coach_elo_before + coach_elo_change
            elo_ratings.loc[
                elo_ratings["unique_id"] == unique_id, "coach_elo"
            ] = coach_elo_after

            ratings_list.append({
                "unique_id": unique_id,
                "match_id": match_id,
                "date": date,
                "player_elo_before": player_elo_before
                if not is_coach
                else None,
                "player_elo_after": player_elo_after,
                "coach_elo_before": coach_elo_before,
                "coach_elo_after": coach_elo_after,
                "is_coach": int(is_coach),
            })

        # Calculate ELO change for all members of team2
        for unique_id in team2_all["unique_id"].unique():
            is_coach = (
                members_df.loc[
                    members_df["unique_id"] == unique_id, "job"
                ].values[0]
                == 1
            )
            player_elo_before = float(
                elo_ratings.loc[
                    elo_ratings["unique_id"] == unique_id, "player_elo"
                ].iloc[0]
            )
            coach_elo_before = float(
                elo_ratings.loc[
                    elo_ratings["unique_id"] == unique_id, "coach_elo"
                ].iloc[0]
            )

            player_elo_change = calculate_elo_change(
                player_elo_before,
                team1_avg_elo_player,
                outcome2,
                base_k,
                tier,
                current_tier_multiplier_factor,
                current_tier_exponent,
                max_elo,
                elo_midpoint,
                elo_divisor,
                elo_exponent,
            )
            coach_elo_change = calculate_elo_change(
                coach_elo_before,
                team1_avg_elo_coach,
                outcome2,
                base_k,
                tier,
                current_tier_multiplier_factor,
                current_tier_exponent,
                max_elo,
                elo_midpoint,
                elo_divisor,
                elo_exponent,
            )

            if not is_coach:
                player_elo_after = player_elo_before + player_elo_change
                elo_ratings.loc[
                    elo_ratings["unique_id"] == unique_id, "player_elo"
                ] = player_elo_after
            else:
                player_elo_after = None

            coach_elo_after = coach_elo_before + coach_elo_change
            elo_ratings.loc[
                elo_ratings["unique_id"] == unique_id, "coach_elo"
            ] = coach_elo_after

            ratings_list.append({
                "unique_id": unique_id,
                "match_id": match_id,
                "date": date,
                "player_elo_before": player_elo_before
                if not is_coach
                else None,
                "player_elo_after": player_elo_after,
                "coach_elo_before": coach_elo_before,
                "coach_elo_after": coach_elo_after,
                "is_coach": int(is_coach),
            })

        # Batch insert ratings into the database
        if len(ratings_list) >= batch_size:
            ratings_df = pd.DataFrame(ratings_list)
            ratings_df.to_sql(
                "Ratings", conn, if_exists="append", index=False, method="multi"
            )
            ratings_list.clear()

    # Insert any remaining records
    if ratings_list:
        ratings_df = pd.DataFrame(ratings_list)
        ratings_df.to_sql(
            "Ratings", conn, if_exists="append", index=False, method="multi"
        )

    conn.commit()
    conn.close()


# Usage
update_elo_ratings("tournaments.db", config)
