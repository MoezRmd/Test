import os
from dotenv import load_dotenv

load_dotenv()

# Sql server
MSSQL_SERVER = ""
MSSQL_DB = ""
PORT = ""
MSSQL_USER = os.environ["MSSQL_USER"]
MSSQL_PW = os.environ["MSSQL_PW"]
