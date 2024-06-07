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


# Step 5: Upload the changes back to the database using parameterized queries
def upload_members_table(df, batch_size=1000):
    conn = sqlite3.connect("tournaments.db")
    cursor = conn.cursor()

    # Begin transaction
    cursor.execute("BEGIN;")

    # Delete all rows in the Members table
    cursor.execute("DELETE FROM Members;")

    # Insert updated rows in batches
    total_rows = len(df)
    insert_query = "INSERT INTO Members (id, team_id, name, role, flag, section) VALUES (?, ?, ?, ?, ?, ?)"
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

upload_members_table(members_df)

print("Data imputation completed.")
