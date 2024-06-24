import unittest
import os
import sqlite3
from project.pipeline import execute_pipeline

class TestDataPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up the test environment before running any tests."""
        cls.db_path = '../data/MADE.sqlite'
        
        # Ensure the database file is removed before running tests
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)
        
        # Run the main function to execute the data pipeline once for all tests
        execute_pipeline()
        
        # Set up the database connection and cursor for reuse in tests
        cls.conn = sqlite3.connect(cls.db_path)
        cls.cursor = cls.conn.cursor()

    def test_database_creation(self):
        """Test if the database file is created."""
        self.assertTrue(os.path.exists(self.db_path), "Database file was not created.")

    def test_table_creation(self):
        """Test if the necessary tables are created."""
        tables = ['accident', 'weather']
        for table in tables:
            with self.subTest(table=table):
                self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
                self.assertIsNotNone(self.cursor.fetchone(), f"Table '{table}' was not created in the database.")

    def test_accident_table_schema(self):
        """Test the schema of the accident table."""
        self.cursor.execute("PRAGMA table_info(accident);")
        columns = {column[1]: column[2] for column in self.cursor.fetchall()}
        expected_columns = {
            'id': 'INTEGER',
            'month': 'TEXT',
            'incidents': 'INTEGER'
        }
        self.assertDictEqual(columns, expected_columns, "Accident table schema is incorrect.")

    def test_weather_table_schema(self):
        """Test the schema of the weather table."""
        self.cursor.execute("PRAGMA table_info(weather);")
        columns = {column[1]: column[2] for column in self.cursor.fetchall()}
        expected_columns = {
            'id': 'INTEGER',
            'month': 'TEXT',
            'avg_temp': 'REAL',
            'snowfall': 'REAL',
            'precipitation': 'REAL',
            'wind_speed': 'REAL'
        }
        self.assertDictEqual(columns, expected_columns, "Weather table schema is incorrect.")

    def test_accident_table_data(self):
        """Test that the accident table contains data."""
        self.cursor.execute("SELECT COUNT(*) FROM accident;")
        self.assertGreater(self.cursor.fetchone()[0], 0, "Accident table does not contain any data.")

    def test_weather_table_data(self):
        """Test that the weather table contains data."""
        self.cursor.execute("SELECT COUNT(*) FROM weather;")
        self.assertGreater(self.cursor.fetchone()[0], 0, "Weather table does not contain any data.")

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        cls.conn.close()
        # Ensure the database file is removed after all tests
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)

if __name__ == "__main__":
    unittest.main()
