import unittest
import os
import json
import sqlite3
import pandas as pd
from unittest.mock import patch, MagicMock
from get_jobs import (
    initialize_logging,
    ConfigReader,
    JobAggregator,
    DEFAULT_HOURS_OLD,
    DEFAULT_RESULTS_WANTED,
)
from collections import namedtuple


class TestConfigReader(unittest.TestCase):
    def setUp(self):
        self.test_config_path = "./test_config.json"
        self.test_config = {
            "scraper_config": {
                "site_names": ["indeed"],
                "search_terms": ["python"],
                "google_search_terms": ["python developer"],
                "locations": ["London"],
                "countries_indeed": ["GB"],
            }
        }

    def tearDown(self):
        if os.path.exists(self.test_config_path):
            os.remove(self.test_config_path)

    @patch("get_jobs.json.load")
    def test_read_config_file_success(self, mock_json_load):
        mock_json_load.return_value = self.test_config
        config_reader = ConfigReader("./config.json")
        self.assertEqual(config_reader.config_dict, self.test_config)

    @patch("get_jobs.open", side_effect=FileNotFoundError)
    def test_read_config_file_filenotfound(self, mock_open):
        with self.assertRaises(FileNotFoundError):
            ConfigReader("./config.json")

    @patch("get_jobs.json.load", side_effect=json.JSONDecodeError("error", "", 0))
    def test_read_config_file_jsondecodeerror(self, mock_json_load):
        with self.assertRaises(json.JSONDecodeError):
            ConfigReader("./config.json")


class TestJobAggregator(unittest.TestCase):
    def setUp(self):
        self.test_db_path = "./test_job_scraper.db"
        self.mock_config = {
            "scraper_config": {
                "site_names": ["indeed"],
                "search_terms": ["python"],
                "google_search_terms": ["python developer"],
                "locations": ["London"],
                "countries_indeed": ["GB"],
            }
        }
        self.config_reader = ConfigReader("./config.json")
        self.config_reader.config_dict = self.mock_config
        self.job_aggregator = JobAggregator(self.config_reader)
        self.job_aggregator.SQLITE_DB_PATH = self.test_db_path

    def tearDown(self):
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)
        if os.path.exists(f"{self.test_db_path}.backup"):
            os.remove(f"{self.test_db_path}.backup")

    def test_initialize_db(self):
        self.job_aggregator.initialize_db()
        self.assertTrue(os.path.exists(self.test_db_path))

    def test_validate_job_data(self):
        test_data = pd.DataFrame(
            {
                "title": ["Test Job", None, "Dev"],
                "company": ["Corp", "Inc", None],
                "location": ["NY", "SF", "LA"],
                "description": ["desc1", "desc2", "desc3"],
            }
        )
        result = self.job_aggregator.validate_job_data(test_data)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["title"], "Test Job")

    @patch("sqlite3.connect")
    def test_save_to_db(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        test_df = pd.DataFrame(
            {
                "title": ["Test Job"],
                "company": ["Test Corp"],
                "location": ["Test Location"],
                "description": ["Test Description"],
            }
        )
        self.job_aggregator.save_to_db(test_df)
        mock_conn.executemany.assert_called_once()

    def test_batch_processing(self):
        large_df = pd.DataFrame(
            {
                "title": ["Job"] * 2500,
                "company": ["Company"] * 2500,
                "location": ["Location"] * 2500,
                "description": ["Description"] * 2500,
            }
        )
        with patch("sqlite3.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            self.job_aggregator.save_to_db(large_df)
            self.assertEqual(mock_connect.call_count, 3)

    def test_database_backup(self):
        with patch("sqlite3.connect") as mock_connect:
            mock_connect.side_effect = sqlite3.Error
            with self.assertRaises(sqlite3.Error):
                self.job_aggregator.backup_database()

    @patch("get_jobs.scrape_jobs")
    def test_run_search_query(self, mock_scrape_jobs):
        mock_df = pd.DataFrame({"title": ["test"]})
        mock_scrape_jobs.return_value = mock_df
        search_query = namedtuple(
            "scraper_query",
            [
                "site_name",
                "search_term",
                "google_search_term",
                "location",
                "country_indeed",
            ],
        )(
            site_name="indeed",
            search_term="python",
            google_search_term="python developer",
            location="London",
            country_indeed="GB",
        )
        result = self.job_aggregator.run_search_query(search_query)
        self.assertEqual(result.shape, mock_df.shape)

    def test_combine_results_empty(self):
        result = self.job_aggregator.combine_results([])
        self.assertTrue(result.empty)

    def test_combine_results_success(self):
        df1 = pd.DataFrame({"title": ["job1"]})
        df2 = pd.DataFrame({"title": ["job2"]})
        result = self.job_aggregator.combine_results([df1, df2])
        self.assertEqual(len(result), 2)


if __name__ == "__main__":
    unittest.main()
