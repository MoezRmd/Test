import logging

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(filename='etl_pipeline.log', level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s - %(message)s')


def load_raw_data(file_paths, table_name, db_connector):
    for file_path in file_paths:
        data = pd.DataFrame()
        try:
            data = pd.read_csv(file_path, encoding='utf-8')
            logging.info(f"data extracted with sucess.")
        except Exception as e:
            logging.error(f"Error reading CSV file {file_path}: {e}")
        try:
            db_connector.insert_dataframe(data, table_name)
            logging.info(f"Data inserted into Table '{table_name}' successfully.")
        except SQLAlchemyError as e:
            logging.error("There was an error", e)


def load_transformed_data(file_paths, table_name, db_connector):
    for file_path in file_paths:
        data = pd.DataFrame()
        try:
            data = pd.read_csv(file_path, encoding='utf-8')
            logging.info(f"data extracted with sucess.")
        except Exception as e:
            logging.error(f"Error reading CSV file {file_path}: {e}")

        # Insert the transformation code as you please
        # Cleaning/sorting/filtering ...
        # logging.info("Phase Done successfully (transforming phases).")

        try:
            db_connector.insert_dataframe(data, table_name)
            logging.info(f"Data inserted into Table '{table_name}' successfully.")
        except SQLAlchemyError as e:
            logging.error("There was an error", e)
