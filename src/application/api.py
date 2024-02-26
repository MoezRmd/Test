import logging
from flask import Flask, jsonify
from sqlalchemy.exc import SQLAlchemyError
from src.data_access.databasehelper import DatabaseHelper
from src.config import (MSSQL_DB, MSSQL_PW, MSSQL_USER, MSSQL_SERVER, PORT)

logging.basicConfig(filename='api.log', level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
db_connector = DatabaseHelper(MSSQL_SERVER, MSSQL_DB, PORT, MSSQL_USER, MSSQL_PW)
table_name = ""   #add table name


@app.route('/read/first-chunk')
def read_first_chunk():
    try:
        results = db_connector.get_from_table(table_name)
        logging.info("Fetched data with success")
        return jsonify([dict(row) for row in results])
    except SQLAlchemyError as e:
        logging.error("There was an error", e)


if __name__ == '__main__':
    app.run(debug=True)
