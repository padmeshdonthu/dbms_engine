import os
import re

import pandas as pd

import csci5408dbmsproject.sqlengine.access_check as access_check

database_selected = "None"


def validate_sql_update_query(user_query):
    if user_query.upper().find("WHERE") > -1:
        match = re.search("UPDATE (.*) SET (.*) WHERE (.*);", user_query, re.IGNORECASE)
        if match is None:
            return True, False
        else:
            return True, True
    else:
        match = re.search("UPDATE (.*) SET (.*);", user_query, re.IGNORECASE)
        if match is None:
            return False, False
        else:
            return False, True


def extract_update_condition(user_query):
    available_operators = ['=']
    for operator in available_operators:
        data = re.search("WHERE (\w+)[\s\n\t]*" + operator + "[\s\n\t]*(.*);", user_query, re.IGNORECASE)
        if data is not None:
            column_name = data.group(1).strip()
            column_value = data.group(2).strip()
            if column_value.startswith("'") and column_value.endswith("'"):
                column_value = column_value[1:]
                column_value = column_value[:-1]
            print(f"{column_name}: {column_value}")
            return {column_name: column_value}, operator
    return None, ''


def extract_setting_column(setting_column_info):
    data = re.search("(.*)[\s\n\t]*" + "=" + "[\s\n\t]*(.*)", setting_column_info, re.IGNORECASE)
    if data is not None:
        column_name = data.group(1).strip()
        column_value = data.group(2).strip()
        if column_value.startswith("'") and column_value.endswith("'"):
            column_value = column_value[1:]
            column_value = column_value[:-1]
        print(f"{column_name}: {column_value}")
        return {column_name: column_value}
    else:
        return None


def check_for_setting_column_value(table_name, table_df, metadata_df, setting_column, setting_column_value):
    setting_metadata_row = metadata_df[metadata_df["column_name"].str.lower() == setting_column.lower()]
    datatype = setting_metadata_row["column_type"].values[0]
    length = setting_metadata_row["column_length"].values[0]
    is_primary = setting_metadata_row["is_primary"].any()
    is_foreign = setting_metadata_row["is_foreign"].any()
    foreign_table = setting_metadata_row["foreign_table"].values[0]
    foreign_table_key = setting_metadata_row["foreign_key_table"].values[0]

    if datatype == "float":
        try:
            float(setting_column_value)
        except ValueError:
            print(f"Incorrect value for column {setting_column}")
            return False
    if datatype == "int":
        try:
            int(setting_column_value)
        except ValueError:
            print(f"Incorrect value for column {setting_column}")
            return False
    if datatype == "boolean":
        if not (setting_column_value.upper() == "FALSE" or setting_column_value.upper() == "TRUE"):
            print(f"Incorrect value for column {setting_column}")
            return False
    if datatype == "varchar":
        if len(setting_column_value) > int(length):
            print(f"The value is exceeding the column {setting_column}'s length {int(length)}")
            return False

    if is_foreign:
        foreign_table_path = "resources/" + database_selected + "/" + foreign_table + ".csv"
        if os.path.isfile(foreign_table_path):
            foreign_table_df = pd.read_csv(foreign_table_path)
            flag = 0
            for value_in_table in foreign_table_df[foreign_table_key].values:
                if datatype == "int":
                    if int(setting_column_value) == value_in_table:
                        flag = 1
                        break
                    else:
                        continue
                elif datatype == "float":
                    if float(setting_column_value) == value_in_table:
                        flag = 1
                        break
                    else:
                        continue
                elif datatype == "boolean":
                    if value_in_table == setting_column_value.upper() == "FALSE":
                        flag = 1
                        break
                    elif value_in_table == setting_column_value.upper() == "TRUE":
                        flag = 1
                        break
                    else:
                        continue
            if flag == 0:
                print(f"The value does not exist in foreign table {foreign_table}")
                return False
    flag = 0
    if is_primary:
        for value_in_table in table_df[setting_column].values:
            if datatype == "int":
                if int(setting_column_value) == value_in_table:
                    flag = 1
                    break
                else:
                    continue
            elif datatype == "float":
                if float(setting_column_value) == value_in_table:
                    flag = 1
                    break
                else:
                    continue
            elif datatype == "boolean":
                if value_in_table == setting_column_value.upper() == "FALSE":
                    flag = 1
                    break
                elif value_in_table == setting_column_value.upper() == "TRUE":
                    flag = 1
                    break
                else:
                    continue
        if flag == 1:
            print(f"The value {setting_column_value} already exists in the table {table_name}")
            return False

    return True


def check_update_data_with_where(table_name, table_df, update_dictionary_condition, setting_column_dictionary):
    conditional_column = list(update_dictionary_condition.keys())[0].strip()
    setting_column = list(setting_column_dictionary.keys())[0].strip()
    conditional_column_value = update_dictionary_condition[conditional_column].strip()
    setting_column_value = setting_column_dictionary[setting_column].strip()
    metadata_path = "resources/" + database_selected + "/" + table_name + "_metadata.csv"
    metadata_df = pd.read_csv(metadata_path)

    conditional_metadata_row = metadata_df[metadata_df["column_name"].str.lower()
                                           == conditional_column.lower()]
    datatype = conditional_metadata_row["column_type"].values[0]
    length = conditional_metadata_row["column_length"].values[0]

    valid = check_for_datatype_conditional_column(conditional_column, conditional_column_value, datatype, length)
    if not valid:
        return False

    valid = check_for_setting_column_value(table_name, table_df, metadata_df, setting_column, setting_column_value)

    if not valid:
        return False
    else:
        return True


def check_for_datatype_conditional_column(conditional_column, conditional_column_value, datatype, length):
    if datatype == "float":
        try:
            float(conditional_column_value)
        except ValueError:
            print(f"Incorrect value for column {conditional_column}")
            return False
    if datatype == "int":
        try:
            int(conditional_column_value)
        except ValueError:
            print(f"Incorrect value for column {conditional_column}")
            return False
    if datatype == "boolean":
        if not (conditional_column_value.upper() == "FALSE" or conditional_column_value.upper() == "TRUE"):
            print(f"Incorrect value for column {conditional_column}")
            return False
    if datatype == "varchar":
        if len(conditional_column_value) > int(length):
            print(f"The value is exceeding the column {conditional_column}'s length {int(length)}")
            return False
    return True


def check_update_data_without_where(table_name, table_df, setting_column_dictionary):
    setting_column = list(setting_column_dictionary.keys())[0].strip()
    setting_column_value = setting_column_dictionary[setting_column].strip()
    metadata_path = "resources/" + database_selected + "/" + table_name + "_metadata.csv"
    metadata_df = pd.read_csv(metadata_path)

    valid = check_for_setting_column_value(table_name, table_df, metadata_df, setting_column, setting_column_value)

    if not valid:
        return False
    else:
        return True


def parse_sql_update_query(user_query, database, user_name, transaction="None"):
    where_exists, valid = validate_sql_update_query(user_query)
    if not valid:
        print("Incorrect Update Query Syntax!")
        return "NA", False
    else:
        global database_selected
        database_selected = database
        if database_selected == "None":
            print("No database is selected!")
            return "NA", False
        if where_exists:
            match = re.search("UPDATE (.*) SET (.*) WHERE (.*);", user_query, re.IGNORECASE)
            table_name = match.group(1).strip()
            setting_info = match.group(2).strip()
            conditional_info = match.group(3).strip()
            if table_name == "" or setting_info == "" or conditional_info == "":
                print("Incorrect Update Query")
                return "NA", False
            path = "resources/" + database_selected + "/" + table_name + ".csv"
            if os.path.isfile(path):
                if not access_check.check_table_access("update", table_name, database_selected, user_name):
                    print(f"User {user_name} cannot access {table_name}")
                    return "NA", False
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
                table_df = pd.read_csv(path)
                update_dictionary_condition, operator = extract_update_condition(user_query)
                setting_column_dictionary = extract_setting_column(setting_info)
                if update_dictionary_condition is None or setting_column_dictionary is None:
                    print("Incorrect syntax in update query!")
                    access_check.unlock_table(database, table_name, user_name)
                    return "NA", False
                if list(update_dictionary_condition.keys())[0].strip() not in table_df:
                    print(f"The conditional column in update query does not exist in {table_name} table")
                    access_check.unlock_table(database, table_name, user_name)
                    return "NA", False
                if list(setting_column_dictionary.keys())[0].strip() not in table_df:
                    print(
                        f"The column for which value must be changed in update query does not exist in "
                        f"{table_name} table")
                    access_check.unlock_table(database, table_name, user_name)
                    return "NA", False
                valid = check_update_data_with_where(table_name, table_df,
                                                     update_dictionary_condition,
                                                     setting_column_dictionary)
                if not valid:
                    access_check.unlock_table(database, table_name, user_name)
                    return "NA", False
                else:
                    update_table(update_dictionary_condition, setting_column_dictionary,
                                 table_name, table_df)
                    return table_name, True
            else:
                print(f"The table {table_name} does not exist in database {database_selected}")
                return "NA", False
        else:
            match = re.search("UPDATE (.*) SET (.*);", user_query, re.IGNORECASE)
            table_name = match.group(1).strip()
            setting_info = match.group(2).strip()
            if table_name == "" or setting_info == "":
                print("Incorrect Update Query")
                return "NA", False
            if not access_check.check_table_access("update", table_name, database_selected, user_name):
                print(f"User {user_name} cannot update {table_name}")
                return "NA", False
            path = "resources/" + database_selected + "/" + table_name + ".csv"
            if os.path.isfile(path):
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
                table_df = pd.read_csv(path)
                setting_column_dictionary = extract_setting_column(setting_info)
                if setting_column_dictionary is None:
                    print("Incorrect syntax in update query!")
                    access_check.unlock_table(database, table_name, user_name)
                    return "NA", False

                if list(setting_column_dictionary.keys())[0].strip() not in table_df:
                    print(
                        f"The column for which value must be changed in update query does not exist in "
                        f"{table_name} table")
                    access_check.unlock_table(database, table_name, user_name)
                    return "NA", False

                valid = check_update_data_without_where(table_name, table_df, setting_column_dictionary)

                if not valid:
                    access_check.unlock_table(database, table_name, user_name)
                    return "NA", False
                else:
                    update_table_without_where(setting_column_dictionary, table_df, table_name)
                    return table_name, True
            else:
                print(f"The table {table_name} does not exist in database {database_selected}")
                return "NA", False
        print("Data updated successfully")
    return table_name, True


def update_table_without_where(setting_column_dictionary, table_df, table_name):
    setting_column = list(setting_column_dictionary.keys())[0].strip()
    setting_column_value = setting_column_dictionary[setting_column].strip()
    global database_selected
    metadata_path = "resources/" + database_selected + "/" + table_name + "_metadata.csv"
    metadata_df = pd.read_csv(metadata_path)
    setting_metadata_row = metadata_df[metadata_df["column_name"].str.lower()
                                       == setting_column.lower()]
    setting_datatype = setting_metadata_row["column_type"].values[0]

    if setting_datatype == "int":
        setting_column_value = int(setting_column_value)
    elif setting_datatype == "float":
        setting_column_value = float(setting_column_value)
    elif setting_datatype == "boolean":
        if setting_column_value.upper() == "FALSE":
            setting_column_value = False
        else:
            setting_column_value = True
    else:
        setting_column_value = str(setting_column_value)

    table_df[setting_column] = setting_column_value
    print(table_df)
    path = "resources/" + database_selected + "/" + table_name + ".csv"
    table_df.to_csv(path, mode='w', header=True, index=False)


def update_table(update_dictionary_condition, setting_column_dictionary,
                 table_name, table_df):
    conditional_column = list(update_dictionary_condition.keys())[0].strip()
    setting_column = list(setting_column_dictionary.keys())[0].strip()
    conditional_column_value = update_dictionary_condition[conditional_column].strip()
    setting_column_value = setting_column_dictionary[setting_column].strip()
    global database_selected
    metadata_path = "resources/" + database_selected + "/" + table_name + "_metadata.csv"
    metadata_df = pd.read_csv(metadata_path)
    conditional_metadata_row = metadata_df[metadata_df["column_name"].str.lower()
                                           == conditional_column.lower()]
    conditional_datatype = conditional_metadata_row["column_type"].values[0]
    setting_metadata_row = metadata_df[metadata_df["column_name"].str.lower()
                                       == setting_column.lower()]
    setting_datatype = setting_metadata_row["column_type"].values[0]

    if conditional_datatype == "int":
        conditional_column_value = int(conditional_column_value)
    elif conditional_datatype == "float":
        conditional_column_value = float(conditional_column_value)
    elif conditional_datatype == "boolean":
        if conditional_column_value.upper() == "FALSE":
            conditional_column_value = False
        else:
            conditional_column_value = True
    else:
        conditional_column_value = str(conditional_column_value)

    if setting_datatype == "int":
        setting_column_value = int(setting_column_value)
    elif setting_datatype == "float":
        setting_column_value = float(setting_column_value)
    elif setting_datatype == "boolean":
        if setting_column_value.upper() == "FALSE":
            setting_column_value = False
        else:
            setting_column_value = True
    else:
        setting_column_value = str(setting_column_value)

    table_df[setting_column] = table_df.apply(
        lambda x: setting_column_value if x[conditional_column] == conditional_column_value
        else x[setting_column], axis=1)

    print(table_df)

    path = "resources/" + database_selected + "/" + table_name + ".csv"
    table_df.to_csv(path, mode='w', header=True, index=False)
