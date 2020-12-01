from pathlib import Path

import csci5408dbmsproject.sqlengine.access_check as access_check
import csci5408dbmsproject.sqlengine.erd_generator as erd
import csci5408dbmsproject.sqlengine.sql_logger as logger
import csci5408dbmsproject.sqlengine.sql_parser as sp
import csci5408dbmsproject.usercontrol.credentials as credentials


def sql_query(user_name):
    while True:
        try:
            user_choice_query = input("Choose 1 to enter the query and 2 to quit from the choice\n")
            if int(user_choice_query) == 1:
                user_query = input("Enter your sql query\n")
                sp.parse_sql_query(user_query, user_name)
            elif int(user_choice_query) == 2:
                break
            else:
                print("Incorrect choice!")
        except ValueError:
            print("Invalid choice! Only choose numbers!")
    return True


def change_access(user_name):
    print("Changing Access")

    while True:
        try:
            type_of_access = input("What access level type do you want to change?\n"
                                   " 1 for Database\n 2 for Table\n 3 for Quitting the Access Change Option\n")
            if int(type_of_access) == 1:
                access_check.change_database_access(user_name)
            elif int(type_of_access) == 2:
                access_check.change_table_access(user_name)
            elif int(type_of_access) == 3:
                break
            else:
                print("Incorrect Choice")
        except ValueError:
            print("Invalid choice! Only choose numbers!")

    return True


def retrieve_sql_dump(user_name):
    database = input("Select a database for which you want SQL Dump\n")
    database = database.strip().lower()
    directory_exists = Path("resources/" + database).is_dir()
    if directory_exists:
        if not sp.can_use_database(database, user_name):
            print(f"User {user_name} cannot access {database}")
            return False
        file_path = "resources/" + database + "/sql_dump.txt"
        file_exists = Path(file_path).is_file()
        if file_exists:
            print(f"The SQL Dump for database {database} is")
            f = open(file_path, mode='r')
            data = f.read()
            print(data)
        else:
            print(f"There are no tables in the database {database} yet! Hence, no SQL Dump yet!")
        return
    else:
        print(f"The database {database} is not present!")
        return


def retrieve_erd(user_name):
    database = input("Enter a database for which you want generate ERD\n")
    database = database.strip().lower()
    directory_exists = Path("resources/" + database).is_dir()
    if directory_exists:
        if not sp.can_use_database(database, user_name):
            print(f"User {user_name} cannot access {database}")
            return False
        erd.generate_erd(database)
    else:
        print(f"The database {database} is not present!")
        return


def retrieve_logs(user_name):
    database = input("Enter a database for which you want logs\n")
    database = database.strip().lower()
    directory_exists = Path("resources/" + database).is_dir()
    if directory_exists:
        if not sp.can_use_database(database, user_name):
            print(f"User {user_name} cannot access {database}")
            return False

        log_type = input("A for Event Logs OR B for General logs")
        if log_type.capitalize() == "A":
            logger.generate_event_logs(database)
        elif log_type.capitalize() == "B":
            logger.generate_general_logs(database)
        else:
            print("Invalid choice. Try again!")
    else:
        print(f"The database {database} is not present!")
        return


def change_user_password(user_name):
    old_password = input("Enter current password\n")
    if credentials.validate_user(user_name, old_password):
        new_password = input("Enter new password\n")
        confirm_password = input("Confirm password\n")
        if new_password != confirm_password:
            print("The passwords do not match!")
            print("Retry again!")
            return
        if new_password == old_password:
            print("The new password cannot be the current password!")
            print("Retry with a new password!")
            return
        credentials.change_password(user_name, new_password)
    else:
        print("The username/password is invalid. Please try again!")
        return


def take_input():
    print("Please login to our system to access our services")

    while True:
        try:
            username = input("Type your username\n")
            password = input("Type your password\n")
            if credentials.validate_user(username, password):
                print("You are successfully logged in!!")
                break
            else:
                print("The username/password is invalid. Please try again!!")
        except Exception as e:
            print(e)

    while True:
        try:
            print("Please choose one among the following choices")
            print("A for SQL Query\nB for Changing Accesses\nC for SQL Dump\n"
                  "D for Log Retrieval\nE for ERD\nF for Transactions\nG for Changing Password\n"
                  "Q for Quitting the System")
            user_choice = input("Enter your choice\n")

            if user_choice.capitalize() == "A":
                sql_query(username)
            elif user_choice.capitalize() == "B":
                change_access(username.lower())
            elif user_choice.capitalize() == "C":
                retrieve_sql_dump(username.lower())
            elif user_choice.capitalize() == "D":
                retrieve_logs(username.lower())
            elif user_choice.capitalize() == "E":
                retrieve_erd(username.lower())
            elif user_choice.capitalize() == "F":
                sp.sql_transaction(username)
            elif user_choice.capitalize() == "G":
                change_user_password(username)
            elif user_choice.capitalize() == "Q":
                break
            else:
                print("Invalid choice. Try again!")
        except Exception as e:
            print(e)
    return
