import os

import pandas as pd

import csci5408dbmsproject.sqlengine.sql_logger as logger


def generate_erd(database):
    number_of_tables = logger.get_number_of_tables(database)
    tables = get_tables(database)
    path = "resources/" + database + "/erd.txt"
    f = open(path, "w+", )
    print(f"The database {database} has {number_of_tables} entities!")
    f.write("The database " + database + " has " + str(number_of_tables) + " entities!\n")
    print("The entity information is mentioned below:\n")
    f.write("The entity information is mentioned below:\n")
    print("\n")
    f.write("\n")

    for table in tables:
        metadata_path = "resources/" + database + "/" + table + "_metadata.csv"
        metadata_df = pd.read_csv(metadata_path)
        print(f"The table {table} information is:")
        f.write("The table " + table + " information is:\n")
        print("The columns are:")
        f.write("The columns are:\n")
        for index, row in metadata_df.iterrows():
            column_name = row['column_name']
            datatype = row["column_type"]
            if datatype == "varchar":
                datatype = datatype + "(" + str(row["column_length"]) + ")"
            print(f"Column name: {column_name}, Datatype: {datatype}")
            f.write("Column name: " + column_name + ", Datatype: " + datatype + "\n")

        for index, row in metadata_df.iterrows():
            primary_key = ""
            foreign_key = ""
            foreign_table = ""
            foreign_table_key = ""
            cardinality = ""
            column_name = row['column_name']
            is_primary = row["is_primary"]
            if is_primary:
                primary_key = column_name
            is_foreign = row["is_foreign"]
            if is_foreign:
                foreign_key = column_name
                foreign_table = row["foreign_table"]
                foreign_table_key = row["foreign_key_table"]
            if cardinality == "":
                if is_foreign and is_primary:
                    cardinality = "1:1"
                elif is_foreign:
                    cardinality = "1:N"
                else:
                    cardinality = ""
            if primary_key != "":
                print(f"The Primary Key is {primary_key}")
                f.write("The Primary Key is " + primary_key + "\n")
            if foreign_key != "":
                print(f"The Foreign Key is {foreign_key}")
                f.write("The Foreign Key is " + foreign_key + "\n")
            if cardinality != "":
                print(f"Referenced Table is {foreign_table} and Referenced Table Key is {foreign_table_key} ")
                print(f"The cardinality from {foreign_table} to {table} is {cardinality}")
                f.write(
                    "References Table is " + foreign_table + " and Referenced Table Key is " + foreign_table_key + "\n")
                f.write("The Cardinality from " + foreign_table + " to " + table + " is " + cardinality + "\n")
        print("\n")
        f.write("\n")
    f.close()


def get_tables(database_name):
    database_info_path = "resources/database_info.csv"
    if os.path.isfile(database_info_path):
        df = pd.read_csv(database_info_path)
        tables = df[df['database_name'] == database_name]['table_name'].values
    return tables
