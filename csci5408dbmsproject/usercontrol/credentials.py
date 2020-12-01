import pandas as pd
from cryptography.fernet import Fernet

key = b'a-wK-6s2bkQjagpuy-erVFmQB0FrizcUddW7CgvaJYU='
cipher_suite = Fernet(key)
ciphered_password1 = cipher_suite.encrypt(b"Admin@123")
ciphered_password2 = cipher_suite.encrypt(b"User@123")


def change_password(user_name, new_password):
    ciphered_password = cipher_suite.encrypt(bytes(new_password, "utf-8"))
    path = "resources/credentials.csv"
    df = pd.read_csv(path)
    if (df["Name"] == user_name).any():
        df["Password"] = df.apply(
            lambda x: ciphered_password.decode("utf-8") if x["Name"] == user_name
            else x["Password"], axis=1)
        df.to_csv(path, mode="w", header=True, index=False)
        print("Password changed successfully!")


def validate_user(user_name, password):
    path = "resources/credentials.csv"
    df = pd.read_csv(path)
    if (df["Name"] == user_name).any():
        df_row = df[df["Name"] == user_name]
        password_encrypted = df_row["Password"].values[0]
        if password == cipher_suite.decrypt(bytes(password_encrypted, "utf-8")).decode("utf-8"):
            return True
        else:
            return False
    else:
        return False
