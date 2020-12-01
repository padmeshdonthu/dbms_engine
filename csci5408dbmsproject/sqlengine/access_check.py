import os
from pathlib import Path

import pandas as pd

import csci5408dbmsproject.sqlengine.keywords as keywords


def check_table_access(keyword, table_name, database_name, user_name):
    table_access_level_path = "resources/" + database_name + "/table_access_level.csv"
    table_access_df = pd.read_csv(table_access_level_path)
    table_access_row = table_access_df[table_access_df["table_name"].str.lower() == table_name.lower()]
    access_value = table_access_row[user_name.lower()].values[0]
    if keyword == keywords.select_keyword:
        if access_value == "view" or access_value == "edit":
            return True
        else:
            return False
    else:
        if access_value == "edit":
            return True
        else:
            return False


def change_database_access(user_name):
    database_name = input("Choose a database\n")
    database_name = database_name.strip().lower()
    directory = Path("resources/" + database_name)
    if directory.is_dir():
        for_user = input(f"Enter the user for which you want to change the database {database_name}"
                         f" access\n")
        for_user = for_user.strip().lower()
        if user_name.lower() == for_user:
            print("Cannot change access for yourself!")
            return
        database_access_level = "resources/database_access_level.csv"
        changed_access = input("Choose the access level. 1 for Yes 2 for No\n")
        if int(changed_access) == 1:
            changed_access = "Y"
        elif int(changed_access) == 2:
            changed_access = "N"
        else:
            print("Invalid choice for changing access")
            return
        if os.path.isfile(database_access_level):
            database_access_df = pd.read_csv(database_access_level)
            database_access_row = database_access_df[database_access_df["database_name"].str.lower() ==
                                                     database_name.lower()]
            user_has_access = database_access_row[user_name].values[0]
            if user_has_access == "Y":
                if for_user.lower() not in database_access_df:
                    print(f"User {for_user} doesn't exist in the system")
                    return
                database_access_df[for_user.lower()] = database_access_df.apply(
                    lambda x: changed_access if x["database_name"] == database_name
                    else x[for_user.lower()], axis=1)
                database_access_df.to_csv(database_access_level, mode='w', header=True, index=False)
                print("Access changed successfully")
            else:
                print("You cannot change access for a database for which you do not have access for!")
                return

    else:
        print("The specified database does not exist!")
        return


def change_table_access(user_name):
    database_name = input("Choose a database\n")
    database_name = database_name.strip().lower()
    directory = Path("resources/" + database_name)
    if directory.is_dir():
        for_user = input(f"Enter the user for which you want to change the database {database_name}"
                         f" access\n")
        for_user = for_user.strip().lower()
        if user_name.lower() == for_user:
            print("Cannot change access for yourself!")
            return
        database_access_level = "resources/database_access_level.csv"

        if os.path.isfile(database_access_level):
            database_access_df = pd.read_csv(database_access_level)
            database_access_row = database_access_df[database_access_df["database_name"].str.lower() ==
                                                     database_name.lower()]
            user_has_access = database_access_row[user_name].values[0]
            if user_has_access == "Y":
                if for_user.lower() not in database_access_df:
                    print(f"User {for_user} doesn't exist in the system")
                    return
                table_access_level = "resources/" + database_name + "/table_access_level.csv"
                if os.path.isfile(table_access_level):
                    table_name = input("Choose a table for which you want to change access\n")
                    table_name = table_name.strip().lower()
                    changed_access = input("Choose the access level for tables. 1 for view 2 for view/edit\n")
                    if int(changed_access) == 1:
                        changed_access = "view"
                    elif int(changed_access) == 2:
                        changed_access = "edit"
                    else:
                        print("Invalid choice for changing access")
                        return
                    if not os.path.isfile("resources/" + database_name + "/" + table_name + ".csv"):
                        print(f"The table {table_name} does not exist in database {database_name}")
                        return
                    table_access_df = pd.read_csv(table_access_level)
                    table_access_row = table_access_df[table_access_df["table_name"].str.lower() ==
                                                       table_name.lower()]
                    user_has_access = table_access_row[user_name].values[0]
                    if user_has_access == "edit":
                        table_access_df[for_user.lower()] = table_access_df.apply(
                            lambda x: changed_access if x["table_name"] == table_name
                            else x[for_user.lower()], axis=1)
                        table_access_df.to_csv(table_access_level, mode='w', header=True, index=False)
                        print("Access changed successfully")

                    else:
                        print("You cannot change access for a table for which you do not have access for!")
                        return
            else:
                print("You cannot change access for a database content for which you do not have access for!")
                return
    else:
        print("The specified database does not exist!")
        return


def lock_table(database_name, table_name, user_name):
    database_info_path = "resources/database_info.csv"
    database_name = str(database_name).replace("_backup", '')
    file_exists = Path(database_info_path).is_file()
    if file_exists:
        df = pd.read_csv(database_info_path)
        df["lock_status"] = df.apply(
            lambda x: user_name if (x["database_name"] == database_name) & (x["table_name"] == table_name)
            else x["lock_status"], axis=1)
        df.to_csv(database_info_path, mode='w', header=True, index=False)


def unlock_table(database_name, table_name, user_name):
    database_info_path = "resources/database_info.csv"
    database_name = str(database_name).replace("_backup", '')
    file_exists = Path(database_info_path).is_file()
    if file_exists:
        df = pd.read_csv(database_info_path)
        df["lock_status"] = df.apply(
            lambda x: "N" if (x["database_name"] == database_name) & (x["table_name"] == table_name) & (
                    x["lock_status"] == user_name)
            else x["lock_status"], axis=1)
        df.to_csv(database_info_path, mode='w', header=True, index=False)


def get_lock_status(database_name, table_name):
    database_info_path = "resources/database_info.csv"
    database_name = str(database_name).replace("_backup", '')
    file_exists = Path(database_info_path).is_file()
    if file_exists:
        df = pd.read_csv(database_info_path)
        df_row = df[
            (df["database_name"].str.lower() == database_name.lower()) & (df["table_name"].str.lower() == table_name)]
        lock_status = df_row["lock_status"].values[0]
        return lock_status
