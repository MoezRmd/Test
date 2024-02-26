import logging

from sqlalchemy import create_engine
import pandas as pd
# Configure logging
logging.basicConfig(filename='database_helper.log', level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s - %(message)s')
class DatabaseHelper:
    def __init__(self, sql_sever, sql_db, port, user_sql, password_sql):
        self.connection_string = 'mssql+pyodbc://{}:{}@{}:{}/{}?driver=SQL+Server'.format(
            user_sql, password_sql, sql_sever, port, sql_db)
        self.engine = create_engine(self.connection_string, use_setinputsizes=False)



    def create_table(self, dataframe, table_name, if_exists='append'):
        try:
            dataframe.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
            logging.info(f"Table '{table_name}' created successfully.")
        except Exception as e:
            logging.error(f"Error creating table '{table_name}': {e}")

    def read_data(self, table_name, selected_columns=None):
        try:
            query = f"SELECT * FROM {table_name}" if selected_columns is None else \
                f"SELECT {', '.join(selected_columns)} FROM {table_name}"
            data = pd.read_sql(query, self.engine)
            logging.info(f"Data read successfully from table '{table_name}'.")
            return data
        except Exception as e:
            logging.error(f"Error reading data from table '{table_name}': {e}")
            return pd.DataFrame()

    def insert_dataframe(self, dataframe, table_name):
        try:
            dataframe.to_sql(table_name, self.engine, if_exists='append', index=False)
            logging.info(f"Data inserted successfully into table '{table_name}'.")
        except Exception as e:
            logging.error(f"Error inserting data into table '{table_name}': {e}")

    def get_from_table(self, table_name, columns_names=[], number_rows=10, condition=dict):
        try:
            condition_string = " AND ".join([f"[{col_name}] = '{col_value}'" for col_name, col_value in condition.items()])
            columns_names = ", ".join([f"[{col_name}]" for col_name in columns_names])
            with self.engine.begin() as conn:
                stmt = f"SELECT {columns_names} FROM {table_name} "
                if condition_string:
                    stmt += f"WHERE {condition_string} "
                stmt += f"LIMIT {number_rows}"
                result = conn.execute(stmt)
                result = result.fetchall()
                if result:
                    logging.info(f"Data fetched successfully from table '{table_name}'.")
                    return result
                else:
                    logging.warning(f"No data found in table '{table_name}' with given condition.")
                    return []
        except Exception as e:
            logging.error(f"Error fetching data from table '{table_name}': {e}")
            return []