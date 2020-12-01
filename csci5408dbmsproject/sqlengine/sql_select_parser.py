import re
from pathlib import Path

import pandas as pd

import csci5408dbmsproject.sqlengine.access_check as access_check

database_selected = "None"


def extract_table_name(user_query):
    if user_query.lower().find("where") != -1:
        table_name = re.search("FROM(.*)WHERE", user_query, re.IGNORECASE)
        print(f"The table name is {table_name.group(1).strip()}")
        return table_name.group(1).strip(), True
    else:
        table_name = re.search("FROM(.*);", user_query, re.IGNORECASE)
        print(f"The table name is {table_name.group(1).strip()}")
        return table_name.group(1).strip(), False


def validate_sql_select_query(user_query):
    match = re.search("select[\s\r\n]+(.*)[\s\r\n]+from[\s\r\n]*(\w+)"
                      "([\s\r\n]+where[\s\r\n]+(\w+)[\s\r\n]*(=|<=|>=|<>|<|>)[\s\r\n]*("
                      "['](\w+)[']|(\w+)))?;",
                      str(user_query).lower())
    if match is None:
        return False
    return True


def extract_select_condition(user_query):
    available_operators = ['<', '>', '<=', '>=', '=', '<>']
    for operator in available_operators:
        data = re.search("WHERE (\w+)[\s\n\t]*" + operator + "[\s\n\t]*(\w+);", user_query, re.IGNORECASE)
        if data is not None:
            column_name = data.group(1).strip()
            column_value = data.group(2).strip()
            if column_value.startswith("'") and column_value.endswith("'"):
                column_value = column_value[1:]
                column_value = column_value[:-1]
            print(f"{column_name}: {column_value}")
            return {column_name: column_value}, operator
    return None, ''


def extract_selected_columns(user_query):
    data = re.search("SELECT(.*)FROM", user_query, re.IGNORECASE)
    raw_columns = data.group(1).strip()
    if re.search(".*[*].*", raw_columns) is not None:
        columns = "ALL"
    else:
        columns = raw_columns.split(",")
        refined_columns = []
        for column in columns:
            column = column.strip()
            if column != "":
                refined_columns.append(column)
        columns = refined_columns
    return columns


def select_data(table_name, dictionary, operator, columns_needed):
    global database_selected
    file = Path("resources/" + database_selected + "/" + table_name + ".csv")
    if file.is_file():
        df = pd.read_csv("resources/" + database_selected + "/" + table_name + ".csv")
        if dictionary is not None:
            condition_column = list(dictionary.keys())[0]
            condition_column_value = dictionary[condition_column]
            if condition_column not in df:
                print("Column doesn't exist!!")
                return False
            metadata_path = "resources/" + database_selected + "/" + table_name + "_metadata.csv"
            metadata_df = pd.read_csv(metadata_path)
            metadata_row = metadata_df[metadata_df["column_name"].str.lower() == condition_column.lower()]
            datatype = metadata_row["column_type"].values[0]
            if datatype == "float":
                try:
                    float(condition_column_value)
                    if operator == '=':
                        filtered_df = df[df[condition_column] == float(condition_column_value)]
                    elif operator == '<':
                        filtered_df = df[df[condition_column] < float(condition_column_value)]
                    elif operator == '<=':
                        filtered_df = df[df[condition_column] <= float(condition_column_value)]
                    elif operator == '>':
                        filtered_df = df[df[condition_column] > float(condition_column_value)]
                    elif operator == '>=':
                        filtered_df = df[df[condition_column] >= float(condition_column_value)]
                    elif operator == '<>':
                        filtered_df = df[df[condition_column] != float(condition_column_value)]
                    else:
                        print("Invalid conditional operator")
                        return False
                except ValueError:
                    print(f"Incorrect value for column {condition_column}")
                    return False
            if datatype == "int":
                try:
                    int(condition_column_value)
                    if operator == '=':
                        filtered_df = df[df[condition_column] == int(condition_column_value)]
                    elif operator == '<':
                        filtered_df = df[df[condition_column] < int(condition_column_value)]
                    elif operator == '<=':
                        filtered_df = df[df[condition_column] <= int(condition_column_value)]
                    elif operator == '>':
                        filtered_df = df[df[condition_column] > int(condition_column_value)]
                    elif operator == '>=':
                        filtered_df = df[df[condition_column] >= int(condition_column_value)]
                    elif operator == '<>':
                        filtered_df = df[df[condition_column] != int(condition_column_value)]
                    else:
                        print("Invalid conditional operator")
                        return False
                except ValueError:
                    print(f"Incorrect value for column {condition_column}")
                    return False
            if datatype == "boolean":
                if not (condition_column_value.upper() == "FALSE" or condition_column_value.upper() == "TRUE"):
                    print(f"Incorrect value for column {condition_column}")
                    return False
                if condition_column_value.upper == "FALSE":
                    if operator == '=':
                        filtered_df = df[df[condition_column] == False]
                    elif operator == '<>':
                        filtered_df = df[df[condition_column] == True]
                    else:
                        print("Invalid conditional operator")
                        return False
                else:
                    if operator == '=':
                        filtered_df = df[df[condition_column] == True]
                    elif operator == '<>':
                        filtered_df = df[df[condition_column] == False]
                    else:
                        print("Invalid conditional operator")
                        return False
            if datatype == "varchar":
                if operator == '=':
                    filtered_df = df[df[condition_column].str.lower() == dictionary[condition_column].lower()]
                elif operator == '<>':
                    filtered_df = df[df[condition_column].str.lower() != dictionary[condition_column].lower()]
                else:
                    print("Invalid conditional operator")
                    return False
        else:
            filtered_df = df
        if columns_needed != 'ALL':
            for column in columns_needed:
                if column in filtered_df:
                    continue
                else:
                    print("The specified column does not exist")
                    return False
            filtered_df = filtered_df[columns_needed]
        print(filtered_df)
        return True
    else:
        print("Table does not exist!")
        return False


def parse_sql_select_query(user_query, database_name, user_name, transaction="None"):
    global database_selected
    database_selected = database_name
    if database_selected == "None":
        print("Database is not selected!")
        return False, "NA"
    if not validate_sql_select_query(user_query):
        print("Invalid query!")
        return False, "NA"
    user_query = str(user_query).strip()
    table_name, where_exists = extract_table_name(user_query)
    path = "resources/" + database_selected + "/" + table_name + ".csv"
    if not Path(path).is_file():
        print(f"Table {table_name} does not exist in the database {database_selected}")
        return False, table_name
    if not access_check.check_table_access("select", table_name, database_selected, user_name):
        print(f"User {user_name} cannot access {table_name}")
        return False, "NA"
    if access_check.get_lock_status(database_selected, table_name) == "N" and transaction == "Y":
        pass
    elif access_check.get_lock_status(database_selected, table_name) == user_name and transaction == "Y":
        pass
    elif transaction == "None":
        pass
    else:
        print("Table locked by another transaction!!")
        return False, table_name

    dictionary = None
    operator = ''
    if where_exists:
        dictionary, operator = extract_select_condition(user_query)
        if dictionary is None:
            print("Error!!!")
            return False, "NA"

    columns_needed = extract_selected_columns(user_query)
    select_data(table_name, dictionary, operator, columns_needed)
    return True, table_name
