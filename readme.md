# Data Quality Checks Script

## Overview
This Python script automates data quality checks between raw and ingested data. It validates row counts, column counts, and grouped data consistency with tolerance checks for numerical fields. Note that this is a framework code so it would need to be adjusted according to your needs

## Setup
1. Clone the repository and install dependencies
2. Configure your database in `config/config.ini`.
3. Update file paths and SQL queries in the script.

## Usage
Run the script with:
```bash
python QA_Report.py
```

## Key Features
- Row and column validation
- Grouped data checks with tolerance
- Comprehensive logging to `logs/qa_report.log`

