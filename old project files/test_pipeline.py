import unittest
import os
import sqlite3
from pipeline import main


class TestDataPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up the test environment before running any tests."""
        cls.db_path = '../data/MADE.sqlite'
        
        # Ensure the database file is removed before running tests
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)
        
        # Run the main function to execute the data pipeline once for all tests
        main()
        
        # Set up the database connection and cursor for reuse in tests
        cls.conn = sqlite3.connect(cls.db_path)
        cls.cursor = cls.conn.cursor()

        # List to store the messages indicating passing of each test case
        cls.passed_messages = []

    def test_pipeline_execution(self):
        """Test the execution of the data pipeline."""
        # Check if the database file is created
        self.assertTrue(os.path.exists(self.db_path), "Database file was not created.")

        # Check if the expected tables are created in the database
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='accident';")
        accident_table = self.cursor.fetchone()
        self.assertIsNotNone(accident_table, "Table 'accident' was not created in the database.")

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='weather';")
        weather_table = self.cursor.fetchone()
        self.assertIsNotNone(weather_table, "Table 'weather' was not created in the database.")

    def test_table_schema(self):
        """Test that the tables have the correct schema."""
        # Check accident table schema
        self.cursor.execute("PRAGMA table_info(accident);")
        accident_schema = self.cursor.fetchall()
        expected_accident_schema = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'month', 'TEXT', 0, None, 0),
            (2, 'crashes', 'INTEGER', 0, None, 0)
        ]
        self.assertEqual(accident_schema, expected_accident_schema, "Accident table schema is incorrect.")

        # Check weather table schema
        self.cursor.execute("PRAGMA table_info(weather);")
        weather_schema = self.cursor.fetchall()
        expected_weather_schema = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'month', 'TEXT', 0, None, 0),
            (2, 'tavg', 'REAL', 0, None, 0),
            (3, 'snow', 'REAL', 0, None, 0),
            (4, 'prcp', 'REAL', 0, None, 0),
            (5, 'wspd', 'REAL', 0, None, 0)
        ]
        self.assertEqual(weather_schema, expected_weather_schema, "Weather table schema is incorrect.")

    def test_data_content(self):
        """Test that the tables contain data."""
        # Check accident table data
        self.cursor.execute("SELECT COUNT(*) FROM accident;")
        accident_count = self.cursor.fetchone()[0]
        self.assertGreater(accident_count, 0, "Accident table does not contain any data.")

        # Check weather table data
        self.cursor.execute("SELECT COUNT(*) FROM weather;")
        weather_count = self.cursor.fetchone()[0]
        self.assertGreater(weather_count, 0, "Weather table does not contain any data.")

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        cls.conn.close()
        # Ensure the database file is removed after all tests
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)

if __name__ == "__main__":
    unittest.main()
