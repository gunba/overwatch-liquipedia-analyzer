import re
import sqlite3
from collections import defaultdict

import pandas as pd


# Step 1: Download the Members table
def download_members_table():
    conn = sqlite3.connect("tournaments.db")
    df = pd.read_sql_query("SELECT * FROM Members", conn)
    conn.close()
    return df


# Step 2: Impute missing role values
def impute_role_values(df):
    # Create a lookup dictionary for the most common role for each name+flag combination
    role_lookup = defaultdict(lambda: defaultdict(list))

    for _, row in df[df["role"].notna() & df["flag"].notna()].iterrows():
        role_lookup[row["name"].lower()][row["flag"].lower()].append(
            row["role"]
        )

    most_common_role = {
        (name, flag): max(set(roles), key=roles.count)
        for name, flags in role_lookup.items()
        for flag, roles in flags.items()
    }

    # Impute missing role values
    for idx, row in df[df["role"].isna() & df["flag"].notna()].iterrows():
        key = (row["name"].lower(), row["flag"].lower())
        if key in most_common_role:
            df.at[idx, "role"] = most_common_role[key]

    return df


# Step 3: Impute missing flag values
def impute_flag_values(df):
    # Create a lookup dictionary for the most common flag for each name+role combination
    flag_lookup = defaultdict(lambda: defaultdict(list))

    for _, row in df[df["flag"].notna() & df["role"].notna()].iterrows():
        flag_lookup[row["name"].lower()][row["role"].lower()].append(
            row["flag"]
        )

    most_common_flag = {
        (name, role): max(set(flags), key=flags.count)
        for name, roles in flag_lookup.items()
        for role, flags in roles.items()
    }

    # Impute missing flag values
    for idx, row in df[df["flag"].isna() & df["role"].notna()].iterrows():
        key = (row["name"].lower(), row["role"].lower())
        if key in most_common_flag:
            df.at[idx, "flag"] = most_common_flag[key]

    return df


# Step 4: Impute missing values based on the name alone
def impute_remaining_values(df):
    # Create lookup dictionaries for the most common role and flag for each name
    role_lookup = defaultdict(list)
    flag_lookup = defaultdict(list)

    for _, row in df[df["role"].notna()].iterrows():
        role_lookup[row["name"].lower()].append(row["role"])

    for _, row in df[df["flag"].notna()].iterrows():
        flag_lookup[row["name"].lower()].append(row["flag"])

    most_common_role = {
        name: max(set(roles), key=roles.count)
        for name, roles in role_lookup.items()
    }
    most_common_flag = {
        name: max(set(flags), key=flags.count)
        for name, flags in flag_lookup.items()
    }

    # Impute missing role values based on name alone
    for idx, row in df[df["role"].isna()].iterrows():
        name = row["name"].lower()
        if name in most_common_role:
            df.at[idx, "role"] = most_common_role[name]

    # Impute missing flag values based on name alone
    for idx, row in df[df["flag"].isna()].iterrows():
        name = row["name"].lower()
        if name in most_common_flag:
            df.at[idx, "flag"] = most_common_flag[name]

    return df


# Step 5: Add the job column based on section and role
def add_job_column(df):
    def determine_job(row):
        if re.match(r"^p\d*$", str(row["section"]).lower()):
            return 0
        elif "coach" in str(row["role"]).lower():
            return 1
        else:
            return 2

    df["job"] = df.apply(determine_job, axis=1)
    return df


# Step 6: Create a unique ID for combinations of NAME, FLAG, and JOB (0 or 1&2)
def create_unique_id(df):
    combination_to_id = {}
    current_id = 1

    def get_combination_id(row):
        nonlocal current_id
        job = 0 if row["job"] == 0 else 1
        key = (
            row["name"].lower(),
            row["flag"].lower() if pd.notna(row["flag"]) else None,
            job,
        )
        if key not in combination_to_id:
            combination_to_id[key] = current_id
            current_id += 1
        return combination_to_id[key]

    df["unique_id"] = df.apply(get_combination_id, axis=1)
    return df


# Step 7: Upload the changes back to the database using parameterized queries
def upload_members_table(df, batch_size=10000):
    conn = sqlite3.connect("tournaments.db")
    cursor = conn.cursor()

    # Begin transaction
    cursor.execute("BEGIN;")

    # Check if the job column exists and drop it if necessary
    cursor.execute("PRAGMA table_info(Members)")
    columns = [info[1] for info in cursor.fetchall()]

    if "job" in columns:
        cursor.execute("ALTER TABLE Members DROP COLUMN job")

    # Add the job column to the Members table
    cursor.execute("ALTER TABLE Members ADD COLUMN job INTEGER")

    # Check if the unique_id column exists and drop it if necessary
    if "unique_id" in columns:
        cursor.execute("ALTER TABLE Members DROP COLUMN unique_id")

    # Add the unique_id column to the Members table
    cursor.execute("ALTER TABLE Members ADD COLUMN unique_id INTEGER")

    # Delete all rows in the Members table
    cursor.execute("DELETE FROM Members;")

    # Insert updated rows in batches
    total_rows = len(df)
    insert_query = "INSERT INTO Members (id, team_id, name, role, flag, section, job, unique_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    for start in range(0, total_rows, batch_size):
        end = start + batch_size
        batch = df[start:end]
        data = [
            (
                row["id"],
                row["team_id"],
                row["name"] if pd.notna(row["name"]) else None,
                row["role"] if pd.notna(row["role"]) else None,
                row["flag"] if pd.notna(row["flag"]) else None,
                row["section"] if pd.notna(row["section"]) else None,
                row["job"],
                row["unique_id"],
            )
            for _, row in batch.iterrows()
        ]
        cursor.executemany(insert_query, data)

    # Commit transaction
    conn.commit()
    conn.close()


# Main script
members_df = download_members_table()

# Impute role and flag values
members_df = impute_role_values(members_df)
members_df = impute_flag_values(members_df)
members_df = impute_remaining_values(
    members_df
)  # New step for final imputation

# Add job column based on section and role
members_df = add_job_column(members_df)

# Create unique ID for each combination of NAME, FLAG, and JOB
members_df = create_unique_id(members_df)

upload_members_table(members_df)

print(
    "Members data imputation, job column creation, and unique ID creation completed."
)
