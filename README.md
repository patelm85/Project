# Big Data Project: Chicago Building Permits Analysis with MongoDB

## Overview
This project implements a big data pipeline using MongoDB to process the Chicago Building Permits dataset. The pipeline ingests data via a REST API, stores and cleans it in MongoDB, performs aggregations, and generates visualizations using Matplotlib.

## Dataset
- **Source**: Chicago Data Portal (https://data.cityofchicago.org/resource/ydr8-5enu.json)
- **Size**: 100,000 rows, 70 columns
- **Format**: JSON

## Project Structure
- `data/raw/`: Temporary storage for raw JSON files
- `notebooks/exploration.ipynb`: Initial data exploration
- `scripts/`:
  - `ingest_data.py`: Fetches 100,000 records via REST API and stores in MongoDB (Bronze layer)
  - `bronze_layer.py`: Verifies row and column counts
  - `silver_layer.py`: Cleans data using MongoDB aggregation
  - `gold_layer.py`: Creates aggregated datasets for analysis
  - `visualizations.py`: Generates three Matplotlib visualizations
- `pyproject.toml`: Poetry configuration
- `requirements.txt`: Exported dependencies
- `.gitignore`: Excludes `.env`, raw data, and Python artifacts
- `permit_distribution.png`, `community_distribution.png`, `issue_trend.png`: Output visualizations

## Setup
1. Install Poetry:
   ```bash
   pip install poetry
   ```
2. Install dependencies:
   ```bash
   poetry install
   ```
3. Set up MongoDB (local or Atlas) and create `.env` in the project root:
   ```plaintext
   MONGO_URI=mongodb://127.0.0.1:27017
   DB_NAME=chicago_permits
   ```
4. Run scripts in order:
   ```bash
   poetry run python scripts/ingest_data.py
   poetry run python scripts/bronze_layer.py
   poetry run python scripts/silver_layer.py
   poetry run python scripts/gold_layer.py
   poetry run python scripts/visualizations.py
   ```
5. Verify data in MongoDB:
   ```bash
   mongosh
   use chicago_permits
   db.bronze_permits.countDocuments({})  # Expect 100,000
   db.silver_permits.countDocuments({})  # Expect ~100,000
   db.gold_permit_distribution.find().limit(5)
   db.gold_community_distribution.find().limit(5)
   db.gold_issue_trend.find().limit(5)
   ```

## Visualizations
- **Permit Distribution**: Bar chart of top 10 permit types (e.g., "PERMIT - EASY PERMIT PROCESS")
- **Permits by Community Area**: Bar chart of permits per community area (77 areas)
- **Issue Trend**: Line chart of permits issued per year (2006–2018)

## Video
- **Unlisted YouTube URL**: [Insert URL after uploading]
- **Duration**: 5-6 minutes
- **Content**: Demonstrates data ingestion, verification, cleaning, aggregations, and visualizations

## Experience
At the start of the semester, big data concepts like NoSQL databases and large-scale processing felt overwhelming. This project taught me how to build a robust data pipeline using MongoDB, handle REST API ingestion, and create meaningful visualizations. I now feel confident working with big data and appreciate the flexibility of NoSQL for scalable applications.

## Requirements
- **Python**: 3.9+
- **MongoDB**: Local or Atlas
- **Poetry**: For dependency management
- **Dependencies**: See `requirements.txt`

## Rubrics Addressed
- **Big Data Application**: MongoDB for storage and processing
- **Data Size**: 100,000 rows, 70 columns
- **Process**: REST API ingestion, MongoDB aggregation, Matplotlib visualizations
- **Visualizations**: 3 charts (permit types, community areas, yearly trend)
- **Code Quality**: Python best practices, logging, error handling
- **Video**: 5-6 minutes, clear and readable