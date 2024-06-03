import json
import os
import re
import sqlite3

# List of allowed keys with their types
allowed_tournament_keys = [
    ("liquipediatier", "TEXT"),
    ("name", "TEXT"),
    ("image", "TEXT"),
    ("type", "TEXT"),
    ("organizer", "TEXT"),
    ("team_number", "INTEGER"),
    ("country", "TEXT"),
    ("series", "TEXT"),
    ("format", "TEXT"),
    ("prizepoolusd", "TEXT"),
    ("twitch", "TEXT"),
    ("web", "TEXT"),
    ("bracket", "TEXT"),
    ("shortname", "TEXT"),
    ("liquipediatiertype", "TEXT"),
    ("sponsor", "TEXT"),
    ("date", "TEXT"),
    ("prizepool", "TEXT"),
    ("localcurrency", "TEXT"),
    ("icon", "TEXT"),
    ("edate", "TEXT"),
    ("sdate", "TEXT"),
    ("organizer_link", "TEXT"),
    ("abbreviation", "TEXT"),
    ("tickername", "TEXT"),
    ("city", "TEXT"),
    ("twitter", "TEXT"),
    ("youtube", "TEXT"),
    ("venue", "TEXT"),
    ("patch", "TEXT"),
    ("game", "TEXT"),
    ("rulebook", "TEXT"),
    ("stream", "TEXT"),
    ("format_timezone", "TEXT"),
]


def process_json_files(folder_path):
    tournament_data = []

    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    # Process tournament_info
                    if "tournament_info" in data:
                        for tournament in data["tournament_info"]:
                            tournament_data.append((
                                tournament,
                                data.get("team_cards", []),
                                data.get("match_cards", []),
                                data.get("legacy_match_cards", []),
                                data.get("match_maps", []),
                            ))
    return tournament_data


def setup_database(conn):
    cursor = conn.cursor()
    # Drop existing tables if they exist
    cursor.execute("DROP TABLE IF EXISTS Tournaments")
    cursor.execute("DROP TABLE IF EXISTS Teams")
    cursor.execute("DROP TABLE IF EXISTS Members")
    cursor.execute("DROP TABLE IF EXISTS Matches")
    cursor.execute("DROP TABLE IF EXISTS Maps")

    # Create new tables dynamically based on allowed_tournament_keys
    tournament_table_sql = (
        """CREATE TABLE Tournaments (
                                id INTEGER PRIMARY KEY, """
        + ", ".join([
            f"{key} {type_}" for key, type_ in allowed_tournament_keys
        ])
        + ")"
    )
    cursor.execute(tournament_table_sql)

    cursor.execute("""CREATE TABLE Teams (
                        id INTEGER PRIMARY KEY,
                        tournament_id INTEGER,
                        team TEXT,
                        FOREIGN KEY(tournament_id) REFERENCES Tournaments(id)
                    )""")
    cursor.execute("""CREATE TABLE Members (
                        id INTEGER PRIMARY KEY,
                        team_id INTEGER,
                        name TEXT,
                        role TEXT,
                        flag TEXT,
                        section TEXT,
                        FOREIGN KEY(team_id) REFERENCES Teams(id)
                    )""")
    cursor.execute("""CREATE TABLE Matches (
                        id INTEGER PRIMARY KEY,
                        tournament_id INTEGER,
                        date TEXT,
                        date_timezone TEXT,
                        team1 TEXT,
                        team2 TEXT,
                        score1 INTEGER,
                        score2 INTEGER,
                        winner TEXT,
                        optional TEXT,
                        FOREIGN KEY(tournament_id) REFERENCES Tournaments(id)
                    )""")
    cursor.execute("""CREATE TABLE Maps (
                        id INTEGER PRIMARY KEY,
                        match_id INTEGER,
                        map TEXT,
                        mode TEXT,
                        score1 TEXT,
                        score2 TEXT,
                        winner TEXT,
                        FOREIGN KEY(match_id) REFERENCES Matches(id)
                    )""")
    conn.commit()


def sanitize_keys(data):
    return {key.replace("-", "_"): value for key, value in data.items()}


def filter_tournament_keys(tournament):
    allowed_keys = {key for key, _ in allowed_tournament_keys}
    return {
        key: value
        for key, value in tournament.items()
        if key.replace("-", "_") in allowed_keys
    }


def insert_tournament_data(conn, tournament_data):
    cursor = conn.cursor()
    tournament_ids = {}
    for (
        tournament,
        teams,
        match_cards,
        legacy_match_cards,
        match_maps,
    ) in tournament_data:
        sanitized_tournament = sanitize_keys(tournament)
        filtered_tournament = filter_tournament_keys(sanitized_tournament)
        columns = ", ".join(filtered_tournament.keys())
        placeholders = ", ".join("?" for _ in filtered_tournament)
        sql = f"INSERT INTO Tournaments ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, tuple(filtered_tournament.values()))
        tournament_id = cursor.lastrowid
        tournament_ids[tournament_id] = (
            teams,
            match_cards,
            legacy_match_cards,
            match_maps,
        )
    conn.commit()
    return tournament_ids


def insert_team_and_member_data(conn, tournament_ids):
    cursor = conn.cursor()
    for tournament_id, (teams, _, _, _) in tournament_ids.items():
        for team in teams:
            if not team.get("team"):
                continue  # Skip teams with no name
            cursor.execute(
                """INSERT INTO Teams (tournament_id, team)
                              VALUES (?, ?)""",
                (tournament_id, team.get("team")),
            )
            team_id = cursor.lastrowid

            for key, value in team.items():
                if not value:
                    continue  # Skip members with empty names

                # Match player keys with any number of digits ending the key (p1, p10, etc.)
                if re.match(r"p\d+$", key):
                    index = key[1:]
                    pos_key = f"pos{index}"
                    flag_key = f"{key}flag"
                    cursor.execute(
                        """INSERT INTO Members (team_id, name, role, flag, section)
                                      VALUES (?, ?, ?, ?, ?)""",
                        (
                            team_id,
                            value,
                            team.get(pos_key, None),
                            team.get(flag_key, None),
                            key,
                        ),
                    )

                # Match non-core members with pattern t, digit(s), letter, digit(s) and ensure no more characters
                elif re.match(r"t\d+[a-zA-Z]\d+$", key):
                    if key[2].isalpha() and key[2] == "p":
                        # Handle t2p4 case
                        pos_key = f"{key[:2]}pos{key[3:]}"
                    else:
                        # Handle t2c1 case
                        pos_key = f"{key}pos"
                    flag_key = f"{key}flag"
                    cursor.execute(
                        """INSERT INTO Members (team_id, name, role, flag, section)
                                      VALUES (?, ?, ?, ?, ?)""",
                        (
                            team_id,
                            value,
                            team.get(pos_key, None),
                            team.get(flag_key, None),
                            key,
                        ),
                    )

    conn.commit()


def parse_match_data(match_data, source):
    matches = []
    for match in match_data:
        match_record = {
            "date": match.get("date"),
            "date_timezone": match.get("date_timezone"),
            "team1": match.get(
                "opponent1" if source == "match_cards" else "team1"
            ),
            "team2": match.get(
                "opponent2" if source == "match_cards" else "team2"
            ),
            "score1": 0,
            "score2": 0,
            "winner": "",
            "optional": match.get("optional", ""),
        }
        maps = []  # Initialize maps list for each match
        map_keys = [key for key in match.keys() if re.match(r"map\d+$", key)]
        for map_key in map_keys:
            map_data = (
                match[map_key]
                if source == "match_cards"
                else {
                    "map": match.get(map_key),
                    "mode": "",
                    "score1": parse_score(
                        match.get(f"{map_key}score", ""), "left"
                    ),
                    "score2": parse_score(
                        match.get(f"{map_key}score", ""), "right"
                    ),
                    "winner": match.get(f"{map_key}win", ""),
                }
            )

            if isinstance(map_data, str) or "winner" not in map_data:
                continue

            if map_data["winner"] == "1":
                match_record["score1"] += 1
            elif map_data["winner"] == "2":
                match_record["score2"] += 1
            maps.append({
                "map": map_data.get("map"),
                "mode": map_data.get("mode"),
                "score1": map_data.get("score1"),
                "score2": map_data.get("score2"),
                "winner": map_data.get("winner"),
            })
        if source == "match_maps":
            match_record["winner"] = (
                "team1" if match.get("winner") == "1" else "team2"
            )
        else:
            match_record["winner"] = (
                "team1"
                if match_record["score1"] > match_record["score2"]
                else "team2"
            )
        matches.append((match_record, maps))
    return matches


def parse_score(score, side):
    """Parse the score based on the side (left or right) and handle different delimiters."""
    if not score:
        return ""
    delimiter = "-" if "-" in score else ";" if ";" in score else None
    if delimiter:
        parts = score.split(delimiter)
        if side == "left":
            return parts[0].strip()
        elif side == "right":
            return parts[1].strip()
    return ""


def insert_match_data(conn, tournament_ids):
    cursor = conn.cursor()
    for tournament_id, (
        _,
        match_cards,
        legacy_match_cards,
        match_maps,
    ) in tournament_ids.items():
        for source, match_data in zip(
            ["match_cards", "legacy_match_cards", "match_maps"],
            [match_cards, legacy_match_cards, match_maps],
        ):
            if not match_data:
                continue
            parsed_matches = parse_match_data(match_data, source)
            for match_record, maps in parsed_matches:
                cursor.execute(
                    """INSERT INTO Matches (tournament_id, date, date_timezone, team1, team2, score1, score2, winner, optional)
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        tournament_id,
                        match_record["date"],
                        match_record["date_timezone"],
                        match_record["team1"],
                        match_record["team2"],
                        match_record["score1"],
                        match_record["score2"],
                        match_record["winner"],
                        match_record["optional"],
                    ),
                )
                match_id = cursor.lastrowid
                for map_data in maps:
                    cursor.execute(
                        """INSERT INTO Maps (match_id, map, mode, score1, score2, winner)
                                      VALUES (?, ?, ?, ?, ?, ?)""",
                        (
                            match_id,
                            map_data["map"],
                            map_data["mode"],
                            map_data["score1"],
                            map_data["score2"],
                            map_data["winner"],
                        ),
                    )
    conn.commit()


# Main function to process and load data into SQLite database
def main():
    tournament_data = process_json_files(
        "legacy_tournaments_data"
    ) + process_json_files("tournaments_data")

    conn = sqlite3.connect("tournaments.db")
    setup_database(conn)
    tournament_ids = insert_tournament_data(conn, tournament_data)
    insert_team_and_member_data(conn, tournament_ids)
    insert_match_data(conn, tournament_ids)
    conn.close()


if __name__ == "__main__":
    main()
