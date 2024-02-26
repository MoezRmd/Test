import logging
import unittest
import pandas as pd
from sqlalchemy import inspect

from src.data_access.databasehelper import DatabaseHelper
from src.config import (MSSQL_DB, MSSQL_PW, MSSQL_USER, MSSQL_SERVER, PORT)

logging.basicConfig(filename='database_helper.log', level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s - %(message)s')
class TestETLPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_connector = DatabaseHelper(MSSQL_SERVER, MSSQL_DB, PORT, MSSQL_USER, MSSQL_PW)

    def test_create_table(self):
        # Test creating a table
        df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
        self.db_connector.create_table(df, 'test_table')
        # Add assertions to check if the table was created successfully
        inspector = inspect(self.db_connector.engine)
        tables = inspector.get_table_names()
        self.assertIn('test_table', tables)

    def test_read_data(self):
        mock_data = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
        # Read data from the table
        read_data = self.db_connector.read_data('test_table')
        self.assertTrue(read_data.equals(mock_data))
        # Assert that the data read matches the inserted mock data
        self.assertTrue(read_data.equals(mock_data))

    def test_get_from_table(self):
        # Test getting data from a table with specific conditions
        result = self.db_helper.get_from_table('test_table', columns_names=['A'], number_rows=2, condition={'B': 'd'})
        # Add assertions to check if the result matches the expected data
        self.assertEqual(len(result), 10)


if __name__ == '__main__':
    unittest.main()
