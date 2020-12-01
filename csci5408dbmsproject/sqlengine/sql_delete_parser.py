import csv
import os
import re

import pandas as pd

import csci5408dbmsproject.sqlengine.access_check as access_check

database_selected = "None"


def validate_sql_delete_query(user_query):
    if user_query.upper().find("WHERE") > -1:
        match = re.search("DELETE FROM (.*) WHERE (.*);",
                          user_query, re.IGNORECASE)
        if match is None:
            return False, False
        else:
            return True, True
    else:
        match = re.search("DELETE FROM (.*);", user_query, re.IGNORECASE)
        if match is None:
            return False, False
        else:
            return False, True


def extract_select_condition(user_query):
    available_operators = ['<', '>', '<=', '>=', '=', '<>']
    for operator in available_operators:
        data = re.search("WHERE(.*)[\s\n\t]*" + operator + "[\s\n\t]*(.*);", user_query, re.IGNORECASE)
        if data is not None:
            column_name = data.group(1).strip()
            column_value = data.group(2).strip()
            if column_value.startswith("'") and column_value.endswith("'"):
                column_value = column_value[1:]
                column_value = column_value[:-1]
            print(f"{column_name}: {column_value}")
            return {column_name: column_value}, operator
    return None, ''


def check_data_can_be_deleted(df, removed_df, table_name):
    global database_selected
    path = "resources/" + database_selected + "/foreign_key_relationship_info.csv"
    if not os.path.isfile(path):
        return True
    else:
        foreign_key_df = pd.read_csv(path)
        mask = foreign_key_df['primary_table'].str.lower() == table_name.lower()
        foreign_key_df = foreign_key_df[mask]
        for index, row in foreign_key_df.iterrows():
            foreign_table_df = pd.read_csv("resources/" + database_selected + "/" + row['foreign_table'] + ".csv")
            primary_table_column = row['primary_table_key']
            foreign_table_column = row['foreign_table_key']
            if removed_df[primary_table_column].isin(foreign_table_df[foreign_table_column]).any():
                return False
        return True


def delete_data(table_name, dictionary, operator, where_exists):
    global database_selected
    path = "resources/" + database_selected + "/" + table_name + ".csv"
    metadata_path = "resources/" + database_selected + "/" + table_name + "_metadata.csv"
    if not os.path.isfile(path):
        print(f"The table {table_name} does not exist in {database_selected}")
        return False
    else:
        df = pd.read_csv(path)
        metadata_df = pd.read_csv(metadata_path)
        filtered_df = None
        removed_df = df
        if where_exists:
            condition_column = list(dictionary.keys())[0]
            metadata_row = metadata_df[metadata_df["column_name"].str.lower() == condition_column.lower()]
            metadata_datatype = metadata_row["column_type"].values[0]
            if condition_column not in df:
                print("Column doesn't exist!!")
                return False
            try:
                if metadata_datatype == "varchar":
                    if operator == '=':
                        filtered_df = df[df[condition_column].str.lower() != dictionary[condition_column].lower()]
                        removed_df = df[df[condition_column].str.lower() == dictionary[condition_column].lower()]
                    elif operator == '<>':
                        filtered_df = df[df[condition_column].str.lower() == dictionary[condition_column].lower()]
                        removed_df = df[df[condition_column].str.lower() != dictionary[condition_column].lower()]
                    else:
                        print("Invalid conditional operator")
                        return False
                elif metadata_datatype == "int":
                    if operator == '=':
                        filtered_df = df[df[condition_column] != int(dictionary[condition_column])]
                        removed_df = df[df[condition_column] == int(dictionary[condition_column])]
                    elif operator == '<':
                        filtered_df = df[df[condition_column] >= int(dictionary[condition_column])]
                        removed_df = df[df[condition_column] < int(dictionary[condition_column])]
                    elif operator == '<=':
                        filtered_df = df[df[condition_column] > int(dictionary[condition_column])]
                        removed_df = df[df[condition_column] <= int(dictionary[condition_column])]
                    elif operator == '>':
                        filtered_df = df[df[condition_column] <= int(dictionary[condition_column])]
                        removed_df = df[df[condition_column] > int(dictionary[condition_column])]
                    elif operator == '>=':
                        filtered_df = df[df[condition_column] < int(dictionary[condition_column])]
                        removed_df = df[df[condition_column] >= int(dictionary[condition_column])]
                    elif operator == '<>':
                        filtered_df = df[df[condition_column] == int(dictionary[condition_column])]
                        removed_df = df[df[condition_column] != int(dictionary[condition_column])]
                    else:
                        print("Invalid conditional operator")
                        return False
                elif metadata_datatype == "float":
                    if operator == '=':
                        filtered_df = df[df[condition_column] != float(dictionary[condition_column])]
                        removed_df = df[df[condition_column] == float(dictionary[condition_column])]
                    elif operator == '<':
                        filtered_df = df[df[condition_column] >= float(dictionary[condition_column])]
                        removed_df = df[df[condition_column] < float(dictionary[condition_column])]
                    elif operator == '<=':
                        filtered_df = df[df[condition_column] > float(dictionary[condition_column])]
                        removed_df = df[df[condition_column] <= float(dictionary[condition_column])]
                    elif operator == '>':
                        filtered_df = df[df[condition_column] <= float(dictionary[condition_column])]
                        removed_df = df[df[condition_column] > float(dictionary[condition_column])]
                    elif operator == '>=':
                        filtered_df = df[df[condition_column] < float(dictionary[condition_column])]
                        removed_df = df[df[condition_column] >= float(dictionary[condition_column])]
                    elif operator == '<>':
                        filtered_df = df[df[condition_column] == float(dictionary[condition_column])]
                        removed_df = df[df[condition_column] != float(dictionary[condition_column])]
                    else:
                        print("Invalid conditional operator")
                        return False
                else:
                    if dictionary[condition_column].upper == "FALSE":
                        if operator == '=':
                            filtered_df = df[df[condition_column] is True]
                            removed_df = df[df[condition_column] is False]
                        elif operator == '<>':
                            filtered_df = df[df[condition_column] is False]
                            removed_df = df[df[condition_column] is True]
                        else:
                            print("Invalid conditional operator")
                            return False
                    else:
                        if operator == '=':
                            filtered_df = df[df[condition_column] is False]
                            removed_df = df[df[condition_column] is True]
                        elif operator == '<>':
                            filtered_df = df[df[condition_column] is True]
                            removed_df = df[df[condition_column] is False]
                        else:
                            print("Invalid conditional operator")
                            return False

            except ValueError:
                print("Incorrect value in condition")
                return False
        can_delete = check_data_can_be_deleted(df, removed_df, table_name)
        if can_delete:
            path = "resources/" + database_selected + "/" + table_name + ".csv"
            if filtered_df is None or filtered_df.empty:
                try:
                    with open(path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(df.columns)
                except BaseException as e:
                    print('BaseException:', path)
            else:
                try:
                    with open(path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(filtered_df.columns)
                        for index, row in filtered_df.iterrows():
                            writer.writerow(row)
                except BaseException as e:
                    print('BaseException:', path)
            print("Data successfully deleted!")
        else:
            print(f"Data cannot be deleted from {table_name} since it being used by another table")


def parse_sql_delete_query(user_query, database_name, user_name, transaction="None"):
    where_exists, valid = validate_sql_delete_query(str(user_query))
    if not valid:
        print("Invalid syntax!")
        return "NA", False
    else:
        global database_selected
        database_selected = database_name
        if database_selected == "None":
            print("Select a database before using queries!")
            return "NA", False
        if where_exists:
            match = re.search("DELETE FROM (.*) WHERE (.*);", user_query, re.IGNORECASE)
            table_name = match.group(1).strip()
            if not access_check.check_table_access("delete", table_name, database_selected, user_name):
                print(f"User {user_name} cannot delete from {table_name}")
                return "NA", False
            if access_check.get_lock_status(database_name, table_name) == "N" and transaction == "Y":
                pass
            elif access_check.get_lock_status(database_name, table_name) == user_name and transaction == "Y":
                pass
            elif transaction == "None":
                pass
            else:
                print("Table locked by another transaction!!")
                return table_name, False
            if transaction == "Y":
                access_check.lock_table(database_name, table_name, user_name)
            dictionary, operator = extract_select_condition(user_query)
            if dictionary is None:
                print("Error!!!")
                access_check.unlock_table(database_name, table_name, user_name)
                return "NA", False
        else:
            match = re.search("DELETE FROM (.*);", user_query, re.IGNORECASE)
            table_name = match.group(1).strip()
            if not access_check.check_table_access("delete", table_name, database_selected, user_name):
                print(f"User {user_name} cannot delete from {table_name}")
                return "NA", False
            if access_check.get_lock_status(database_name, table_name) == "N" and transaction == "Y":
                pass
            elif access_check.get_lock_status(database_name, table_name) == user_name and transaction == "Y":
                pass
            elif transaction == "None":
                pass
            else:
                print("Table locked by another transaction!!")
                return table_name, False
            if transaction == "Y":
                access_check.lock_table(database_name, table_name, user_name)
            dictionary = None
            operator = ''
        delete_data(table_name, dictionary, operator, where_exists)
    return table_name, True
