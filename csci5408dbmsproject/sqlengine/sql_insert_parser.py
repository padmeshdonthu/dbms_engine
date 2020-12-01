import csv
import os
import re

import pandas as pd

import csci5408dbmsproject.sqlengine.access_check as access_check

database_selected = "None"


def validate_sql_insert_query(user_query):
    match = re.search("INSERT INTO (.*)[\s\n\r]*[(](.*)[)] VALUES[\s\n\r]*[(](.*)[)];", user_query, re.IGNORECASE)
    if match is None:
        return False
    else:
        return True


def check_insert_data(table_name, columns, values):
    global database_selected
    path = "resources/" + database_selected + "/" + table_name + ".csv"
    metadata_path = "resources/" + database_selected + "/" + table_name + "_metadata.csv"

    if os.path.isfile(path):
        df = pd.read_csv(path)
        column_list = columns.strip().split(",")
        for column in column_list:
            column = column.strip()
            if column not in df:
                print(f"Column {column} does not exist in {table_name}")
                return False
        metadata_df = pd.read_csv(metadata_path)
        value_list = values.strip().split(",")
        # Checking if datatypes match
        if len(column_list) != len(value_list):
            print("Query does not have proper columns and values count")
            return False
        if len(column_list) != len(metadata_df["column_name"]):
            print(f"Incorrect number of columns mentioned in insert query for table {table_name}")
            return False

        for column, value in zip(column_list, value_list):
            column = column.strip()
            if column in metadata_df["column_name"].values:
                metadata_row = metadata_df[metadata_df["column_name"].str.lower() == column.lower()]
                datatype = metadata_row["column_type"].values[0]
                length = metadata_row["column_length"].values[0]
                is_primary = metadata_row["is_primary"].any()
                is_foreign = metadata_row["is_foreign"].any()
                foreign_table = metadata_row["foreign_table"].values[0]
                foreign_table_key = metadata_row["foreign_key_table"].values[0]

                if datatype == "float":
                    try:
                        float(value)
                    except ValueError:
                        print(f"Incorrect value for column {column}")
                        return False
                if datatype == "int":
                    try:
                        int(value)
                    except ValueError:
                        print(f"Incorrect value for column {column}")
                        return False
                if datatype == "boolean":
                    if not (value.upper() == "FALSE" or value.upper() == "TRUE"):
                        print(f"Incorrect value for column {column}")
                        return False
                if datatype == "varchar":
                    if len(value) > int(length):
                        print(f"The value is exceeding the column {column}'s length {int(length)}")
                        return False

                if is_foreign:
                    foreign_table_path = "resources/" + database_selected + "/" + foreign_table + ".csv"
                    if os.path.isfile(foreign_table_path):
                        foreign_table_df = pd.read_csv(foreign_table_path)
                        flag = 0
                        for value_in_table in foreign_table_df[foreign_table_key].values:
                            if datatype == "int":
                                if int(value) == value_in_table:
                                    flag = 1
                                    break
                                else:
                                    continue
                            elif datatype == "float":
                                if float(value) == value_in_table:
                                    flag = 1
                                    break
                                else:
                                    continue
                            elif datatype == "boolean":
                                if value_in_table == value.upper() == "FALSE":
                                    flag = 1
                                    break
                                elif value_in_table == value.upper() == "TRUE":
                                    flag = 1
                                    break
                                else:
                                    continue
                        if flag == 0:
                            print(f"The value does not exist in foreign table {foreign_table}")
                            return False
                flag = 0
                if is_primary:
                    for value_in_table in df[column].values:
                        if datatype == "int":
                            if int(value) == value_in_table:
                                flag = 1
                                break
                            else:
                                continue
                        elif datatype == "float":
                            if float(value) == value_in_table:
                                flag = 1
                                break
                            else:
                                continue
                        elif datatype == "boolean":
                            if value_in_table == value.upper() == "FALSE":
                                flag = 1
                                break
                            elif value_in_table == value.upper() == "TRUE":
                                flag = 1
                                break
                            else:
                                continue
                    if flag == 1:
                        print(f"The value {value} already exists in the table {table_name}")
                        return False
            else:
                print(f"The column {column} does not exist in table {table_name}")
                return False
        return True
    else:
        print(f"The table {table_name} does not exist in database {database_selected}")
        return False


def parse_sql_insert_query(user_query, database, user_name, transaction="None"):
    if not validate_sql_insert_query(user_query):
        print("Incorrect syntax!")
        return False, "NA"
    else:
        global database_selected
        database_selected = database
        if database_selected == "None":
            print("No database selected!")
            return False, "NA"
        match = re.search("INSERT INTO (.*)[\s\n\r]*[(](.*)[)] VALUES[\s\n\r]*[(](.*)[)];", user_query, re.IGNORECASE)
        table_name = match.group(1).strip()
        if not access_check.check_table_access("insert", table_name, database_selected, user_name):
            print(f"User {user_name} cannot insert to {table_name}")
            return False, "NA"
        if access_check.get_lock_status(database_selected, table_name) == "N" and transaction == "Y":
            pass
        elif access_check.get_lock_status(database_selected, table_name) == user_name and transaction == "Y":
            pass
        elif transaction == "None":
            pass
        else:
            print("Table locked by another transaction!!")
            return table_name, False
        if transaction == "Y":
            access_check.lock_table(database, table_name, user_name)
        columns = match.group(2).strip()
        values = match.group(3).strip()
        valid = check_insert_data(table_name, columns, values)
        path = "resources/" + database_selected + "/" + table_name + ".csv"
        if valid:
            with open(path, mode="a", newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(values.split(","))
            print(f"Data inserted successfully to table {table_name}")
        else:
            access_check.unlock_table(database, table_name, user_name)
            return False, table_name
    return True, table_name
