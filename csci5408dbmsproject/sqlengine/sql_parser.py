import datetime
import os
import re
import shutil
import time
from distutils.dir_util import copy_tree
from pathlib import Path

import pandas as pd

import csci5408dbmsproject.sqlengine.access_check as access_check
import csci5408dbmsproject.sqlengine.keywords as keywords
import csci5408dbmsproject.sqlengine.sql_create_parser as create_parser
import csci5408dbmsproject.sqlengine.sql_delete_parser as delete_parser
import csci5408dbmsproject.sqlengine.sql_drop_parser as drop_parser
import csci5408dbmsproject.sqlengine.sql_insert_parser as insert_parser
import csci5408dbmsproject.sqlengine.sql_logger as logger
import csci5408dbmsproject.sqlengine.sql_select_parser as select_parser
import csci5408dbmsproject.sqlengine.sql_update_parser as update_parser

database_selected = "None"


def validate_sql_use_query(user_query):
    match = re.search("USE[\s\r\n]+(.*);", user_query, re.IGNORECASE)
    if match is None:
        return False
    else:
        return True


def can_use_database(database, user_name):
    database_access_level_path = "resources/database_access_level.csv"
    df = pd.read_csv(database_access_level_path)
    df_row = df[df["database_name"].str.lower() == database.lower()]
    access_value = df_row[user_name.lower()].values[0]
    if access_value == "Y":
        return True
    else:
        return False


def parse_sql_use_query(user_query, user_name):
    if not validate_sql_use_query(user_query):
        print("Invalid query!")
        return False, "None"
    database = re.search("USE (.*);", user_query, re.IGNORECASE)
    database = database.group(1).strip()
    directory_exists = Path("resources/" + database).is_dir()
    global database_selected
    if directory_exists:
        if not can_use_database(database, user_name):
            print(f"User {user_name} cannot access {database}")
            return False, database
        database_selected = database
        print(f"Selected database is {database_selected}")
    else:
        print(f"The database {database} is not present!")
    return True, database


def parse_sql_query(user_query, user_name, transaction="None"):
    global database_selected
    user_query = str(user_query).lower()
    if str(user_query).lower().startswith(keywords.select_keyword):
        start_time = datetime.datetime.now()
        valid, table_name = select_parser.parse_sql_select_query(user_query, database_selected, user_name, transaction)
        end_time = datetime.datetime.now()
        execution_time = int((end_time - start_time).total_seconds() * 1000)
        logger.update_event_logs(database_selected, user_query, table_name, keywords.select_keyword,
                                 execution_time)
        logger.update_general_logs(database_selected, user_query, execution_time, table_name)
        return valid, table_name
    elif str(user_query).lower().startswith(keywords.update_keyword):
        start_time = datetime.datetime.now()
        table_name, valid = update_parser.parse_sql_update_query(user_query, database_selected, user_name, transaction)
        end_time = datetime.datetime.now()
        execution_time = int((end_time - start_time).total_seconds() * 1000)
        logger.update_event_logs(database_selected, user_query, table_name, keywords.update_keyword, execution_time)
        logger.update_general_logs(database_selected, user_query, execution_time, table_name)
        return valid, table_name
    elif str(user_query).lower().startswith(keywords.delete_keyword):
        start_time = datetime.datetime.now()
        table_name, valid = delete_parser.parse_sql_delete_query(user_query, database_selected, user_name, transaction)
        end_time = datetime.datetime.now()
        execution_time = int((end_time - start_time).total_seconds() * 1000)
        logger.update_event_logs(database_selected, user_query, table_name, keywords.delete_keyword, execution_time)
        logger.update_general_logs(database_selected, user_query, execution_time, table_name)
        return valid, table_name
    elif str(user_query).lower().startswith(keywords.drop_keyword):
        start_time = datetime.datetime.now()
        drop_type, valid, table_name = drop_parser.parse_sql_drop_query(user_query, database_selected, user_name)
        end_time = datetime.datetime.now()
        execution_time = int((end_time - start_time).total_seconds() * 1000)
        if valid and drop_type == "table":
            logger.update_event_logs(database_selected, user_query, table_name, keywords.drop_keyword, execution_time)
            logger.update_general_logs(database_selected, user_query, execution_time, table_name)
        if valid and drop_type == "database":
            database_selected = "None"
            logger.update_event_logs(database_selected, user_query, table_name, keywords.drop_keyword, execution_time)
            logger.update_general_logs(database_selected, user_query, execution_time, table_name)
        return valid, database_selected
    elif str(user_query).lower().startswith(keywords.insert_keyword):
        start_time = datetime.datetime.now()
        valid, table_name = insert_parser.parse_sql_insert_query(user_query, database_selected, user_name, transaction)
        end_time = datetime.datetime.now()
        execution_time = int((end_time - start_time).total_seconds() * 1000)
        logger.update_event_logs(database_selected, user_query, table_name, keywords.insert_keyword, execution_time)
        logger.update_general_logs(database_selected, user_query, execution_time, table_name)
        return valid, table_name
    elif str(user_query).lower().startswith(keywords.create_keyword):
        start_time = datetime.datetime.now()
        create_type, valid, table_name = create_parser.parse_sql_create_query(user_query, database_selected, user_name)
        if valid and create_type == "database":
            match = re.search("CREATE DATABASE (.*);", user_query, re.IGNORECASE)
            database = match.group(1).strip()
            database_selected = database.lower()
            table_name = ''
        end_time = datetime.datetime.now()
        execution_time = int((end_time - start_time).total_seconds() * 1000)
        logger.update_event_logs(database_selected, user_query, table_name, keywords.create_keyword, execution_time)
        logger.update_general_logs(database_selected, user_query, execution_time, table_name)
        return valid, table_name
    elif str(user_query).lower().startswith(keywords.use_keyword):
        start_time = datetime.datetime.now()
        valid, database_name = parse_sql_use_query(user_query, user_name)
        end_time = datetime.datetime.now()
        logger.update_event_logs(database_selected, user_query, database_name, keywords.use_keyword,
                                 int((end_time - start_time).total_seconds() * 1000))
        return valid, database_name
    else:
        print("Invalid Query")
        return False


def sql_transaction(user_name):
    global database_selected
    list_of_queries = []
    while True:
        try:
            user_choice_query = input("Choose 1 to enter the query and 2 to stop creating the transaction\n")
            if int(user_choice_query) == 1:
                user_query = input("Enter your sql query\n")
                list_of_queries.append(user_query)
            elif int(user_choice_query) == 2:
                break
            else:
                print("Incorrect choice!")
        except ValueError:
            print("Invalid choice! Only choose numbers!")

    if len(list_of_queries) == 0:
        print("No query added!")
        return False
    start_query = list_of_queries[0]
    end_query = list_of_queries[len(list_of_queries) - 1]
    match = re.search("START TRANSACTION;", start_query, re.IGNORECASE)
    commit_status = False
    tables_locked = []

    if match is None:
        print("Transactions should begin with START TRANSACTION !!")
        return False
    else:
        match = re.search("COMMIT;", end_query, re.IGNORECASE)
        if match is None:
            commit_status = False
        else:
            commit_status = True

        if database_selected == "None":
            print("Select database before starting the transaction using the USE <database>; "
                  "command!! ")
            return False
        if not can_use_database(database_selected, user_name):
            print(f"User {user_name} cannot access {database_selected}")
            return False
        original_database = database_selected
        original_database_path = "resources/" + original_database
        database_selected = database_selected + "_backup"
        path = "resources/" + database_selected
        if not Path(path).is_dir():
            os.mkdir(path)
            copy_tree(original_database_path, path)
        print("The transaction is:")
        for query in list_of_queries:
            print(query)
        for query in list_of_queries:
            if query.upper() == "COMMIT;":
                status = True
                break
            elif query.upper() == "START TRANSACTION;":
                print("Transaction started!")
            else:
                status, table_name = parse_sql_query(query, user_name, "Y")
                tables_locked.append(table_name)
                if status is False:
                    break
        if commit_status:
            for table_name in tables_locked:
                access_check.unlock_table(database_selected, table_name, user_name)
        database_selected = database_selected.replace("_backup", "")
        if status and commit_status:
            print("Transaction executed successfully!!")
            shutil.rmtree(original_database_path, ignore_errors=True)
            os.rename(path, original_database_path)
            return True
        else:
            if commit_status and status is False:
                print("Transaction rolling back!")
                shutil.rmtree(path, ignore_errors=True)
                return False
            else:
                time.sleep(30)
                unlock_tables(tables_locked, user_name, path)
                return False


def unlock_tables(table_list, user_name, path):
    for table_name in table_list:
        access_check.unlock_table(database_selected, table_name, user_name)
    print("Transaction rolling back!")
    shutil.rmtree(path, ignore_errors=True)
    return False
