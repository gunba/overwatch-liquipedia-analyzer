import json
import logging
import os
import re
import sqlite3

from dateutil import parser

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
                    if "tournament_info" in data:
                        for tournament in data["tournament_info"]:
                            tournament["file_name"] = (
                                file  # Track the file name
                            )
                            tournament_data.append((
                                tournament,
                                data.get("team_cards", []),
                                data.get("match_cards", []),
                            ))
    return tournament_data


def setup_database(conn):
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS Tournaments")
    cursor.execute("DROP TABLE IF EXISTS Teams")
    cursor.execute("DROP TABLE IF EXISTS Members")
    cursor.execute("DROP TABLE IF EXISTS Matches")
    cursor.execute("DROP TABLE IF EXISTS Maps")

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
                        date_time TEXT,
                        date_timezone TEXT,
                        team1 TEXT,
                        team2 TEXT,
                        score1 INTEGER,
                        score2 INTEGER,
                        winner INTEGER,
                        mvp TEXT,
                        comment TEXT,
                        owl TEXT,
                        vod TEXT,
                        format TEXT,
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


def normalize_date(date_str):
    try:
        parsed_date = parser.parse(date_str)
        timezone = (
            parsed_date.tzinfo.tzname(parsed_date)
            if parsed_date.tzinfo
            else None
        )
        date = parsed_date.strftime("%Y-%m-%d")
        time = parsed_date.strftime("%H:%M:%S")
        # We are not changing the timezone value in the process, returning it as is
        return date, time, timezone
    except ValueError:
        logging.warning(f"Failed to parse date: {date_str}")
        return (
            "9999-12-31",
            "00:00:00",
            None,
        )  # Placeholder for unparsed dates


def parse_team_info(team):
    if team:
        match = re.match(r"^(.*?)(?:\|score=(W|FF|DQ|L))?$", team)
        if match:
            return match.group(1), match.group(2)
    return team, None


# Configure logging
logging.basicConfig(
    filename="excluded_tournaments.log",
    filemode="w",  # Overwrite the log file on each run
    level=logging.INFO,
    format="%(message)s",
)


def insert_tournament_data(conn, tournament_data):
    cursor = conn.cursor()
    tournament_ids = {}
    excluded_tournaments = []

    for tournament, teams, match_cards in tournament_data:
        if not match_cards:
            json_file_name = tournament["file_name"]
            txt_file_name = json_file_name.replace(".json", ".txt")
            txt_file_path = os.path.join(
                "C:\\Users\\jorda\\PycharmProjects\\liquipedia_data_miner\\tournaments_text",
                txt_file_name,
            )
            txt_file_size = (
                os.path.getsize(txt_file_path)
                if os.path.exists(txt_file_path)
                else "File not found"
            )
            excluded_tournaments.append((json_file_name, txt_file_size))
            continue

        # Check for date fields
        date = (
            tournament.get("date")
            or tournament.get("sdate")
            or tournament.get("edate")
            or "9999-12-31"
        )

        # if not date:
        #     json_file_name = tournament["file_name"]
        #     txt_file_name = json_file_name.replace(".json", ".txt")
        #     txt_file_path = os.path.join(
        #         "C:\\Users\\jorda\\PycharmProjects\\liquipedia_data_miner\\tournaments_text",
        #         txt_file_name,
        #     )
        #     txt_file_size = (
        #         os.path.getsize(txt_file_path)
        #         if os.path.exists(txt_file_path)
        #         else "File not found"
        #     )
        #     excluded_tournaments.append((json_file_name, txt_file_size))
        #     continue

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
            date,
        )

    conn.commit()

    # Log excluded tournaments sorted by file size
    excluded_tournaments.sort(key=lambda x: x[1], reverse=True)
    for json_file_name, txt_file_size in excluded_tournaments:
        logging.info(
            f"Excluded Tournament: {json_file_name}, Text File Size: {txt_file_size}"
        )

    return tournament_ids


def insert_team_and_member_data(conn, tournament_ids):
    cursor = conn.cursor()
    for tournament_id, (teams, _, _) in tournament_ids.items():
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

                elif re.match(r"t\d+[a-zA-Z]\d+$", key):
                    if key[2].isalpha() and key[2] == "p":
                        pos_key = f"{key[:2]}pos{key[3:]}"
                    else:
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


def parse_match_data(match_data, tournament_date):
    matches = []
    for match in match_data:
        date, date_time, timezone = normalize_date(
            match.get("date") or tournament_date
        )

        team1 = match.get("opponent1")
        team2 = match.get("opponent2")

        timezone = match.get("date_timezone", timezone)

        match_record = {
            "date": date,
            "date_time": date_time,
            "date_timezone": match.get("date_timezone"),
            "team1": team1,
            "team2": team2,
            "score1": match.get("opponent1_score", 0),
            "score2": match.get("opponent2_score", 0),
            "winner": match.get("winner", 0),
            "mvp": match.get("mvp", ""),
            "comment": match.get("comment", ""),
            "owl": match.get("owl", ""),
            "vod": match.get("vod", ""),
            "format": match.get("format", ""),
        }

        if not match_record["team1"] or not match_record["team2"]:
            continue

        maps = []
        map_index = 1
        while f"map{map_index}" in match:
            map_data = match[f"map{map_index}"]
            maps.append({
                "map": map_data.get("map"),
                "mode": map_data.get("mode", ""),
                "score1": map_data.get("score1"),
                "score2": map_data.get("score2"),
                "winner": map_data.get("winner"),
            })
            map_index += 1

        matches.append((match_record, maps))
    return matches


def insert_match_data(conn, tournament_ids):
    cursor = conn.cursor()
    for tournament_id, (
        _,
        match_cards,
        tournament_date,
    ) in tournament_ids.items():
        if not match_cards:
            continue  # Skip if there are no match cards
        parsed_matches = parse_match_data(match_cards, tournament_date)
        for match_record, maps in parsed_matches:
            cursor.execute(
                """INSERT INTO Matches (tournament_id, date, date_time, date_timezone, team1, team2, score1, score2, winner, mvp, comment, owl, vod, format)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    tournament_id,
                    match_record["date"],
                    match_record["date_time"],
                    match_record["date_timezone"],
                    match_record["team1"],
                    match_record["team2"],
                    match_record["score1"],
                    match_record["score2"],
                    match_record["winner"],
                    match_record["mvp"],
                    match_record["comment"],
                    match_record["owl"],
                    match_record["vod"],
                    match_record["format"],
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
    tournament_data = process_json_files("tournaments_data")

    conn = sqlite3.connect("tournaments.db")
    setup_database(conn)
    tournament_ids = insert_tournament_data(conn, tournament_data)
    insert_team_and_member_data(conn, tournament_ids)
    insert_match_data(conn, tournament_ids)
    conn.close()


if __name__ == "__main__":
    main()
