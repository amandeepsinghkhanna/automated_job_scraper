# Job Scraper

A robust job scraping system that aggregates job listings from multiple platforms, including Indeed, Glassdoor, and Google Jobs.

## Overview

This system scrapes job postings based on configurable search parameters and stores them in a SQLite database. It includes logging capabilities, error handling, and database backup functionality.

## Components

### 1. Configuration (`config.json`)

- Defines scraping parameters, including:
  - Target job sites (Indeed, Glassdoor, Google)
  - Search terms
  - Google-specific search queries
  - Target locations
  - Country codes for Indeed searches
- Supports multiple locations across different countries.
- Currently configured for Data Scientist positions across various locations in:
  - India
  - Canada
  - Australia
  - New Zealand
  - Ireland

### 2. Main Script (`get_jobs.py`)

#### Key Classes

- `ConfigReader`: Handles configuration file parsing and validation.
- `JobAggregator`: Main class managing the job scraping workflow.

#### Features

- Database initialization and management.
- Automated database backups.
- Batch processing for large datasets.
- Data validation and deduplication.
- Configurable search parameters.
- Rate limiting between requests.
- Comprehensive logging system.

#### Workflow

1. Initializes the logging system.
2. Reads and validates the configuration.
3. Sets up the SQLite database.
4. Creates a database backup.
5. Executes job searches.
6. Validates and processes results.
7. Saves results to the database.

### 3. Tests (`test_get_jobs.py`)

A comprehensive test suite covering:

- Configuration file reading.
- Database operations.
- Data validation.
- Search query execution.
- Results processing.
- Error handling.

## Setup and Usage

1. Install dependencies:

   ```bash
   pip install jobspy pandas loguru tqdm
   ```

2. Configure environment variables:

   ```bash
   export CONFIG_PATH="./config.json"
   export LOGS_DIR_PATH="./logs"
   ```

3. Run the scraper:

   ```bash
   python get_jobs.py
   ```

4. Run tests:

   ```bash
   python -m unittest test_get_jobs.py
   ```

## Database Schema

The SQLite database (`job_scraper.db`) contains a single table with the following structure:

| Column      | Type        | Description                |
| ----------- | ----------- | -------------------------- |
| id          | PRIMARY KEY | Unique identifier for jobs |
| title       | TEXT        | Job title                  |
| company     | TEXT        | Company name               |
| location    | TEXT        | Job location               |
| description | TEXT        | Job description            |
| created_at  | TIMESTAMP   | Job posting creation date  |

### Includes indexes for:

- Job creation date.
- Unique job entries (title, company, location).

## Features

- Automated daily job scraping.
- Deduplication of job listings.
- Error handling and logging.
- Database backups.
- Batch processing for large datasets.
- Configurable search parameters.
- Multi-platform support.
- Location-based searching.
- Test coverage.

## Logging

Logs are stored in the `./logs` directory with:

- Daily rotation.
- 7-day retention.
- Timestamp and level-based formatting.
- Detailed operation logging.

## Error Handling

The system includes comprehensive error handling for:

- Configuration file issues.
- Database operations.
- Network requests.
- Data validation.
- File operations.

## Performance Considerations

- Batch processing for large datasets (1000 records per batch).
- Rate limiting between requests.
- Database indexing for efficient queries.
- Memory-efficient data processing.
