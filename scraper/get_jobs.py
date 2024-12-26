import os
import json
import sqlite3
import pandas as pd
from tqdm import tqdm
from loguru import logger
from datetime import datetime
from jobspy import scrape_jobs
from collections import namedtuple
from typing import List, Dict, Any
from time import sleep
from contextlib import contextmanager
from sqlite3.dbapi2 import Connection

CONFIG_PATH = os.getenv("CONFIG_PATH", "./config.json")
LOGS_DIR_PATH = os.getenv("LOGS_DIR_PATH", "./logs")
DEFAULT_HOURS_OLD = 24
DEFAULT_RESULTS_WANTED = 20
BATCH_SIZE = 1000


def initialize_logging(log_dir: str) -> None:
    os.makedirs(log_dir, exist_ok=True)
    log_file_name = os.path.join(log_dir, f"{datetime.now().strftime('%Y%m%d')}.log")
    logger.add(
        log_file_name,
        rotation="00:00",
        retention="7 days",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    )
    logger.info("Logging initialized successfully.")


class ConfigReader:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config_dict = self.read_config_file()
        self.validate_config(self.config_dict)

    def read_config_file(self) -> Dict[str, Any]:
        try:
            with open(self.config_path, "r") as f:
                config = json.load(f)
            logger.info("Config parsed successfully.")
            return config
        except FileNotFoundError:
            logger.error(f"The file {self.config_path} was not found.")
            raise
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON from the file {self.config_path}.")
            raise

    def validate_config(self, config: Dict[str, Any]) -> None:
        required_types = {
            "site_names": list,
            "search_terms": list,
            "google_search_terms": list,
            "locations": list,
            "countries_indeed": list,
        }
        scraper_config = config.get("scraper_config", {})
        for key, expected_type in required_types.items():
            if not isinstance(scraper_config.get(key), expected_type):
                raise ValueError(f"{key} must be a {expected_type.__name__}")


class JobAggregator:
    SQLITE_DB_PATH = "./job_scraper.db"

    def __init__(self, config_reader: ConfigReader):
        self.config_reader = config_reader
        self.scraper_query_sequence = self.parse_search_query_sequence()
        self.initialize_db()

    @contextmanager
    def get_db_connection(self) -> Any:
        conn = sqlite3.connect(self.SQLITE_DB_PATH)
        try:
            yield conn
        finally:
            conn.close()

    def initialize_db(self):
        with self.get_db_connection() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS job_scraper 
                        (id INTEGER PRIMARY KEY, 
                         title TEXT, 
                         company TEXT, 
                         location TEXT, 
                         description TEXT,
                         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"""
            )
            conn.execute(
                """CREATE INDEX IF NOT EXISTS idx_job_date 
                          ON job_scraper(created_at)"""
            )
            conn.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_job 
                          ON job_scraper(title, company, location)"""
            )

    def backup_database(self) -> None:
        backup_path = f"{self.SQLITE_DB_PATH}.backup"
        with sqlite3.connect(self.SQLITE_DB_PATH) as source:
            backup = sqlite3.connect(backup_path)
            source.backup(backup)
        logger.info(f"Database backed up to {backup_path}")

    def parse_search_query_sequence(self) -> List[namedtuple]:
        scraper_config = self.config_reader.config_dict.get("scraper_config")
        if not scraper_config:
            logger.error("Missing 'scraper_config' in configuration file.")
            raise ValueError("Invalid configuration: 'scraper_config' is missing.")

        required_keys = [
            "site_names",
            "search_terms",
            "google_search_terms",
            "locations",
            "countries_indeed",
        ]
        for key in required_keys:
            if key not in scraper_config:
                logger.error(f"Missing key '{key}' in 'scraper_config'.")
                raise ValueError(
                    f"Invalid configuration: '{key}' is missing in 'scraper_config'."
                )

        site_names = scraper_config["site_names"]
        search_terms = scraper_config["search_terms"]
        google_search_terms = scraper_config["google_search_terms"]
        locations = scraper_config["locations"]
        countries_indeed = scraper_config["countries_indeed"]

        param_size = max(
            len(site_names),
            len(search_terms),
            len(google_search_terms),
            len(locations),
            len(countries_indeed),
        )

        if not param_size > 0:
            logger.error("No search parameters found in 'scraper_config'.")
            raise ValueError("Invalid configuration: No search parameters found.")

        search_query = namedtuple(
            "scraper_query",
            [
                "site_name",
                "search_term",
                "google_search_term",
                "location",
                "country_indeed",
            ],
        )
        queries = [
            search_query(
                site_name=site_names[0] if site_names else None,
                search_term=search_terms[0] if search_terms else None,
                google_search_term=(
                    google_search_terms[i] if i < len(google_search_terms) else None
                ),
                location=locations[i] if i < len(locations) else None,
                country_indeed=(
                    countries_indeed[i] if i < len(countries_indeed) else None
                ),
            )
            for i in range(param_size - 1)
        ]
        logger.info("Search query sequence parsed successfully.")
        return queries

    def run_search_query(
        self,
        search_query,
        hours_old: int = DEFAULT_HOURS_OLD,
        results_wanted: int = DEFAULT_RESULTS_WANTED,
    ) -> pd.DataFrame:
        try:
            results = scrape_jobs(
                site_name=search_query.site_name,
                search_term=search_query.search_term,
                google_search_term=search_query.google_search_term,
                location=search_query.location,
                results_wanted=results_wanted,
                hours_old=hours_old,
                country_indeed=search_query.country_indeed,
            )
            sleep(1)
            return results
        except Exception as e:
            logger.error(f"Failed to scrape jobs: {str(e)}")
            return pd.DataFrame()

    def validate_job_data(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.dropna(subset=["title", "company", "location"])
            .drop_duplicates()
            .replace({r"[^\w\s-]": ""}, regex=True)
        )

    def run(self) -> pd.DataFrame:
        results = self.combine_results(self.collect_search_query_results())
        return self.validate_job_data(results)

    def collect_search_query_results(self) -> List[pd.DataFrame]:
        results = [
            self.run_search_query(search_query)
            for search_query in tqdm(
                self.scraper_query_sequence, "Running search queries"
            )
        ]
        return results

    def combine_results(self, results: List[pd.DataFrame]) -> pd.DataFrame:
        if not results:
            return pd.DataFrame()
        return pd.concat(results, axis=0, ignore_index=True)

    def save_to_db(self, df: pd.DataFrame) -> None:
        if df.empty:
            logger.info("No new jobs to save to database")
            return

        for i in range(0, len(df), BATCH_SIZE):
            batch = df[i : i + BATCH_SIZE]
            with self.get_db_connection() as conn:
                jobs_to_insert = batch[
                    ["title", "company", "location", "description"]
                ].to_dict("records")
                conn.executemany(
                    """INSERT OR IGNORE INTO job_scraper (title, company, location, description)
                       VALUES (:title, :company, :location, :description)""",
                    jobs_to_insert,
                )
            logger.info(f"Saved batch of {len(jobs_to_insert)} jobs to database")


def main(config_path: str, log_dir_path: str) -> None:
    initialize_logging(log_dir=log_dir_path)
    config_reader = ConfigReader(config_path=config_path)
    job_aggregator = JobAggregator(config_reader=config_reader)

    # Create database backup before starting
    job_aggregator.backup_database()

    # Run the job scraping process
    scraped_jobs = job_aggregator.run()
    job_aggregator.save_to_db(scraped_jobs)


if __name__ == "__main__":
    main(config_path=CONFIG_PATH, log_dir_path=LOGS_DIR_PATH)
