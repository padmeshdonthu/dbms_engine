import csv
import os
import re

import pandas as pd

database_selected = "None"


class TableDetail:
    def __init__(self, column_name, column_type, is_primary, column_length, is_foreign, foreign_table,
                 foreign_table_key):
        self.column_name = column_name
        self.column_type = column_type
        self.is_primary = is_primary
        self.column_length = column_length  # -1 for int, -2 for boolean, actual for varchar
        self.is_foreign = is_foreign
        self.foreign_table = foreign_table
        self.foreign_table_key = foreign_table_key


class ForeignKeyDetail:
    def __init__(self, column_name, foreign_table, foreign_key_table):
        self.column_name = column_name
        self.foreign_table = foreign_table
        self.foreign_key_table = foreign_key_table


def validate_sql_create_query(user_query):
    match = re.search("CREATE DATABASE (.*);", user_query, re.IGNORECASE)
    if match is None:
        match = re.search("CREATE TABLE (\w+)[\s\n\r]*[(](.*)[)];", user_query, re.IGNORECASE)
        if match is None:
            return "None", False
        else:
            return "table", True
    else:
        return "database", True


def create_sql_dump(user_query):
    global database_selected
    path = "resources/" + database_selected + "/sql_dump.txt"
    if not os.path.isfile(path):
        f = open(path, 'w+')
        f.write("USE DATABASE " + database_selected + ";")
        f.write("\n")
        f.write(user_query)
        f.write("\n")
        f.close()
    else:
        f = open(path, "a+")
        f.write(user_query)
        f.write("\n")
        f.close()


def create_table(table_details, table_name):
    global database_selected
    filename = "resources/" + database_selected + "/" + table_name + ".csv"
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            column_list = []
            for item in table_details:
                column_list.append(item.column_name)
            writer.writerow(column_list)
    except BaseException as e:
        print('BaseException:', filename)
        return False
    return True


def set_primary_and_foreign_keys(table_name, table_details, primary_key_columns, foreign_key_details):
    global database_selected
    dataframe = pd.read_csv(
        "resources/" + database_selected + "/" + table_name + ".csv")
    for primary_key in primary_key_columns:
        if primary_key not in dataframe:
            print(f"Invalid column {primary_key} as primary key")
            os.remove("resources/" + database_selected + "/" + table_name + ".csv")
            return None

    for column in table_details:
        if column.column_name in primary_key_columns:
            column.is_primary = True

    for column in table_details:
        for foreign_key in foreign_key_details:
            if foreign_key.column_name not in dataframe:
                print(f"Invalid column {foreign_key.column_name} as foreign key")
                os.remove("resources/" + database_selected + "/" + table_name + ".csv")
                return None
            if column.column_name == foreign_key.column_name:
                column.is_foreign = True
                column.foreign_table = foreign_key.foreign_table
                column.foreign_table_key = foreign_key.foreign_key_table
    return table_details


def create_table_metadata(table_details, table_name):
    global database_selected
    filename = "resources/" + database_selected + "/" + table_name + "_metadata.csv"
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['column_name', 'column_type', 'column_length', 'is_primary',
                             'is_foreign', 'foreign_table', 'foreign_key_table'])
            for item in table_details:
                writer.writerow([item.column_name, item.column_type, item.column_length, item.is_primary,
                                 item.is_foreign, item.foreign_table, item.foreign_table_key])
    except BaseException as e:
        print('BaseException:', filename)
        return False
    return True


def store_foreign_key_relationship_info(foreign_key_details, table_name):
    global database_selected
    path = "resources/" + database_selected + "/foreign_key_relationship_info.csv"
    if os.path.isfile(path):
        with open(path, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for foreign_key_detail in foreign_key_details:
                writer.writerow([foreign_key_detail.foreign_table, foreign_key_detail.foreign_key_table,
                                 table_name, foreign_key_detail.column_name])
    else:
        with open(path, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["primary_table", "primary_table_key", "foreign_table", "foreign_table_key"])
            for foreign_key_detail in foreign_key_details:
                writer.writerow([foreign_key_detail.foreign_table, foreign_key_detail.foreign_key_table,
                                 table_name, foreign_key_detail.column_name])


def create_database_info(database, table_name):
    database_info_path = "resources/database_info.csv"
    if not os.path.isfile(database_info_path):
        with open(database_info_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["database_name", "table_name", "lock_status"])
            writer.writerow([database, table_name, "N"])
    else:
        with open(database_info_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([database, table_name, "N"])


def create_log_files(database):
    event_logs_path = "resources/" + database + "/event_logs.csv"
    general_logs_path = "resources/" + database + "/general_logs.csv"
    with open(general_logs_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["User Query", "Current time", "Execution time", "Database Name",
                         "Number of tables", "Queried Table Name", "Number of Records"])
    with open(event_logs_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["User Query", "Table/Database Name", "Query Type", "Current time", "Execution time"])


def parse_sql_create_query(user_query, database_name, user_name):
    create_type, valid = validate_sql_create_query(user_query)
    if not valid:
        print("Invalid Query")
        return "None", False, "None"
    else:
        if create_type == "database":
            match = re.search("CREATE DATABASE (.*);", user_query, re.IGNORECASE)
            database = match.group(1).strip()
            try:
                path = "resources/" + database
                if os.path.isdir(path):
                    print("Database already exists!")
                    return "database", False, "None"
                else:
                    os.mkdir(path)
                    create_log_files(database)
                    print(f"Database {database} successfully created!")
            except OSError:
                print("Database creation failed! Please try again")
                return "database", False, "None"
            database_access_path = "resources/database_access_level.csv"
            if os.path.isfile(database_access_path):
                with open(database_access_path, mode='a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    if user_name == "User1":
                        writer.writerow([database, "Y", "N"])
                    else:
                        writer.writerow([database, "N", "Y"])
            else:
                with open(database_access_path, mode='w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(["database_name", "user1", "user2"])
                    if user_name == "User1":
                        writer.writerow([database, "Y", "N"])
                    else:
                        writer.writerow([database, "N", "Y"])
            return "database", True, database
        elif create_type == "table":
            global database_selected
            database_selected = database_name
            if database_selected == "None":
                print("No database selected! Please select the database before using the query!")
                return "table", False, "None"
            else:
                match = re.search("CREATE TABLE (\w+)[\s\n\r]*[(](.*)[)];", user_query, re.IGNORECASE)
                table_name = match.group(1).strip()
                column_list = match.group(2).strip()
                table_details = []
                primary_key = False
                primary_key_columns = []
                foreign_key_details = []
                for column_info in column_list.split(","):
                    column_datatype = ""
                    column_length = -99
                    datatype_allowed = ["int", "boolean", "varchar", "float"]
                    if column_info.strip().upper().startswith("PRIMARY KEY") and not primary_key:
                        match = re.search("PRIMARY KEY[\s\n\r]?[(](.*)[)]", column_info, re.IGNORECASE)
                        if match is None:
                            print("Invalid syntax")
                            return "table", False, table_name
                        match = match.group(1).strip()
                        primary_key_columns = match.split(",")
                        primary_key = True
                    elif column_info.strip().upper().startswith("PRIMARY KEY") and primary_key:
                        print("Incorrect Query. Only one primary key representation allowed!")
                        return "table", False, table_name
                    elif column_info.strip().upper().startswith("FOREIGN KEY"):
                        match = re.search("FOREIGN KEY[\s\n\t]?[(](.*)[)][\s\n\t]+REFERENCES[\s\n\t]+(.*)[(](.*)[)]",
                                          column_info.strip(), re.IGNORECASE)
                        if match is None:
                            print("Invalid syntax!")
                            return "table", False, table_name
                        else:
                            table_column_name = match.group(1).strip()
                            foreign_table = match.group(2).strip()
                            foreign_table_key = match.group(3).strip()

                            path = "resources/" + database_selected + "/" + foreign_table + ".csv"
                            if not os.path.isfile(path):
                                print(f"Foreign table {foreign_table}does not exist.")
                                return "table", False, table_name
                            else:
                                dataframe = pd.read_csv(
                                    "resources/" + database_selected + "/" + foreign_table + ".csv")
                                if foreign_table_key not in dataframe:
                                    print(f"The reference key {foreign_table_key} does not exist in {foreign_table}")
                                    return "table", False, table_name

                            foreign_key_details.append(
                                ForeignKeyDetail(table_column_name, foreign_table, foreign_table_key))
                    else:
                        column_info_list = column_info.strip().split()
                        if len(column_info_list) < 2:
                            print("Incorrect Query")
                            return "table", False, table_name
                        column_name = column_info_list[0]
                        for datatype in datatype_allowed:
                            column_datatype = ""
                            column_length = -99
                            if datatype not in column_info_list[1]:
                                continue
                            else:
                                if datatype == "int" or datatype == "float":
                                    column_length = -1
                                    column_datatype = datatype
                                elif datatype == "boolean":
                                    column_length = -2
                                    column_datatype = datatype
                                else:
                                    column_length = re.search("varchar[(](.*)[)]",
                                                              column_info_list[1], re.IGNORECASE)
                                    column_length = int(column_length.group(1))
                                    column_datatype = datatype
                                break

                        if column_datatype == "":
                            print("Incorrect Query")
                            return "table", False, table_name
                        table_details.append(
                            TableDetail(column_name, column_datatype, False, column_length, False, "None", "None"))
                path = "resources/" + database_selected + "/" + table_name + ".csv"
                if os.path.isfile(path):
                    print(f"Table already exists in the {database_selected}!")
                    return "table", False, table_name
                else:
                    create_sql_dump(user_query)
                    create_table(table_details, table_name)
                    table_details = set_primary_and_foreign_keys(table_name, table_details, primary_key_columns,
                                                                 foreign_key_details)
                    if table_details is not None:
                        create_table_metadata(table_details, table_name)
                        store_foreign_key_relationship_info(foreign_key_details, table_name)
                    print(f"Table {table_name} successfully created!")
                    table_access_path = "resources/" + database_selected + "/table_access_level.csv"
                    if os.path.isfile(table_access_path):
                        with open(table_access_path, mode='a', newline='', encoding='utf-8') as f:
                            writer = csv.writer(f)
                            if user_name == "User1":
                                writer.writerow([table_name, "edit", "view"])
                            else:
                                writer.writerow([table_name, "view", "edit"])
                    else:
                        with open(table_access_path, mode='w', newline='', encoding='utf-8') as f:
                            writer = csv.writer(f)
                            writer.writerow(["table_name", "user1", "user2"])
                            if user_name == "User1":
                                writer.writerow([table_name, "edit", "view"])
                            else:
                                writer.writerow([table_name, "view", "edit"])

                    create_database_info(database_selected, table_name)
                return "table", True, table_name
