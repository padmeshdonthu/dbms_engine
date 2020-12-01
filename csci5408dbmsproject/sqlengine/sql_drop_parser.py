import csv
import os
import re
import shutil
from pathlib import Path

import pandas as pd

import csci5408dbmsproject.sqlengine.access_check as access_check

database_selected = "None"


def validate_sql_drop_query(user_query):
    match = re.search("DROP TABLE (.*);", user_query, re.IGNORECASE)
    if match is None:
        match = re.search("DROP DATABASE (.*)", user_query, re.IGNORECASE)
        if match is None:
            return "None", False
        else:
            return "database", True
    else:
        return "table", True


def can_table_be_dropped(table_name):
    global database_selected
    path = "resources/" + database_selected + "/foreign_key_relationship_info.csv"
    if not os.path.isfile(path):
        return "None", True
    else:
        dataframe = pd.read_csv(path)
        for index, row in dataframe.iterrows():
            if row["primary_table"] == table_name:
                return row["foreign_table"], False
            else:
                continue
        return "None", True


def delete_from_relationship_info(table_name):
    path = "resources/" + database_selected + "/foreign_key_relationship_info.csv"
    if os.path.isfile(path):
        df = pd.read_csv(path)
        df = df[df["foreign_table"].str.lower() != table_name]
        if df.empty:
            with open(path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['primary_table', 'primary_table_key', 'foreign_table', 'foreign_table_key'])
        else:
            df.to_csv(path, mode='w')


def delete_from_sql_dump(table_name):
    path = "resources/" + database_selected + "/sql_dump.txt"
    if os.path.isfile(path):
        f = open(path, mode='r')
        data = f.read()
        f.close()
        match = re.search("(CREATE TABLE " + table_name + "(.*))", data, re.IGNORECASE).group(1)
        data = data.replace(match, '')
        f = open(path, mode='w')
        f.write(data)
        f.close()


def update_database_info(database_name, table_name):
    if table_name == "None":
        database_info_path = "resources/database_info.csv"
        if os.path.isfile(database_info_path):
            df = pd.read_csv(database_info_path)
            df_row = df[df["database_name"].str.lower() != database_name.lower()]
            if df_row.empty:
                with open(database_info_path, mode='w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(["database_name", "table_name"])
            else:
                df_row.to_csv(database_info_path, mode='w', index=False, header=True)
    else:
        database_info_path = "resources/database_info.csv"
        if os.path.isfile(database_info_path):
            df = pd.read_csv(database_info_path)
            index_names = df[(df['database_name'].str.lower() == database_name.lower())
                             & (df['table_name'].str.lower() == table_name.lower())].index
            df.drop(index_names, inplace=True)
            if df.empty:
                with open(database_info_path, mode='w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(["database_name", "table_name"])
            else:
                df.to_csv(database_info_path, mode='w', header=True, index=False)


def parse_sql_drop_query(user_query, database, user_name):
    drop_type, valid = validate_sql_drop_query(user_query)
    if not valid:
        print("Invalid Query")
        return "None", False, "None"
    else:
        if drop_type == "table":
            match = re.search("DROP TABLE (.*);", user_query, re.IGNORECASE)
            table_name = match.group(1).strip()
            global database_selected
            database_selected = database
            file = Path("resources/" + database_selected + "/" + table_name + ".csv")
            if file.is_file():
                if not access_check.check_table_access("drop", table_name, database_selected, user_name):
                    print(f"User {user_name} cannot drop {table_name}")
                    return "table", False, table_name
                print(f"Are you sure that you want to delete the {table_name} in {database_selected}?")
                print("All data in the table will be deleted!")
                answer = input("Y|y for Yes and N|n for No\n")
                if answer.upper() == "Y":
                    # Check for foreign key using metadata (To be done)
                    foreign_table, can_drop = can_table_be_dropped(table_name)
                    if not can_drop:
                        print(
                            f"The {table_name} table cannot be deleted because it is referred by {foreign_table} table")
                    else:
                        # Check for access level
                        os.remove("resources/" + database_selected + "/" + table_name + ".csv")
                        print(f"Table {table_name} successfully deleted!")
                        os.remove("resources/" + database_selected + "/" + table_name + "_metadata.csv")
                        delete_from_relationship_info(table_name)
                        delete_from_sql_dump(table_name)
                else:
                    print("Drop operation cancelled!")
            else:
                print(f"Cannot delete {table_name} which does not exist in {database_selected}")
                return "table", False, table_name
            table_access_path = "resources/" + database_selected + "/table_access_level.csv"
            if os.path.isfile(table_access_path):
                df = pd.read_csv(table_access_path)
                df = df[df["table_name"].str.lower() != table_name]
                if df.empty:
                    with open(table_access_path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(['table_name', 'user1', 'user2'])
                else:
                    df.to_csv(table_access_path, mode='w', index=False, header=True)
                update_database_info(database_selected, table_name)
            return "table", True, table_name
        elif drop_type == "database":
            match = re.search("DROP DATABASE (.*);", user_query, re.IGNORECASE)
            if database == "None":
                print("Choose the database before using this query")
                return "None", False, "None"
            database_name = match.group(1).strip()
            directory = Path("resources/" + database_name)
            if directory.is_dir():
                print(f"Are you sure that you want to delete the {database_name}?")
                print("All data in the database will be deleted!")
                answer = input("Y|y for Yes and N|n for No\n")
                if answer.upper() == "Y":
                    # Check for access level
                    shutil.rmtree("resources/" + database_name, ignore_errors=True)
                    print(f"Database {database_name} successfully deleted!")
                    database_selected = "None"
                    database_access_path = "resources/database_access_level.csv"
                    if os.path.isfile(database_access_path):
                        df = pd.read_csv(database_access_path)
                        df = df[df["database_name"].str.lower() != database_name]
                        if df.empty:
                            with open(database_access_path, 'w', newline='', encoding='utf-8') as f:
                                writer = csv.writer(f)
                                writer.writerow(['database_name', 'user1', 'user2'])
                        else:
                            df.to_csv(database_access_path, mode='w', header=True, index=False)
                        update_database_info(database_name, "None")
                    return "database", True, database_name
                else:
                    print("Drop operation cancelled!")
                    return "database", True, database_name
            else:
                print(f"Cannot delete the {database_name} which does not exist")
                return "database", False, database_name
