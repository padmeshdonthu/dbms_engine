import csv
import os
from datetime import datetime
from pathlib import Path

import pandas as pd


def update_event_logs(database_selected, user_query, table_name, select_keyword, excution_time):
    path = "resources/" + database_selected + "/event_logs.csv"
    database_selected = database_selected.replace("_backup", "")
    if table_name == '':
        table_name = database_selected
    if os.path.isfile(path):
        with open(path, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([user_query, table_name, select_keyword, datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                             str(excution_time) + " ms"])


def update_general_logs(database_selected, user_query, execution_time, table_name=''):
    path = "resources/" + database_selected + "/general_logs.csv"
    number_of_records = 0
    number_of_tables = 0
    database_selected = database_selected.replace("_backup", "")
    if table_name != '':
        number_of_records = get_number_of_records(database_selected, table_name)
        number_of_tables = get_number_of_tables(database_selected)
    if os.path.isfile(path):
        with open(path, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([user_query, datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                             str(execution_time) + " ms", database_selected, number_of_tables,
                             table_name, number_of_records])


def get_number_of_tables(database_selected):
    database_info_path = "resources/database_info.csv"
    number_of_tables = 0
    if os.path.isfile(database_info_path):
        df = pd.read_csv(database_info_path)
        number_of_tables = len(df[df['database_name'] == database_selected])
    return number_of_tables


def get_number_of_records(database_selected, table_name):
    path = "resources/" + database_selected + "/" + table_name + ".csv"
    if os.path.isfile(path):
        df = pd.read_csv(path)
        return df.shape[0]
    return 0


def generate_event_logs(database):
    file_path = "resources/" + database + "/event_logs.csv"
    file_exists = Path(file_path).is_file()
    if file_exists:
        print(f"The event logs for database {database} is")
        f = open(file_path, mode='r')
        data = f.read()
        print(data)
    return


def generate_general_logs(database):
    file_path = "resources/" + database + "/general_logs.csv"
    file_exists = Path(file_path).is_file()
    if file_exists:
        print(f"The event logs for database {database} is")
        f = open(file_path, mode='r')
        data = f.read()
        print(data)
    return
