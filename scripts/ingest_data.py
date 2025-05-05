import requests
import json
import os
from pymongo import MongoClient
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_data(api_url, limit=1000, target_rows=100000):
    """Fetch data from the REST API with pagination until target rows are reached."""
    all_data = []
    offset = 0
    while len(all_data) < target_rows:
        try:
            query = f"{api_url}?$limit={limit}&$offset={offset}&$order=permit_"
            response = requests.get(query, timeout=10)
            response.raise_for_status()
            data = response.json()
            if not data:
                logger.info("No more data to fetch.")
                break
            all_data.extend(data)
            logger.info(f"Fetched {len(data)} records, total: {len(all_data)}")
            offset += limit
            if len(data) < limit:
                logger.info("Reached end of dataset.")
                break
        except requests.RequestException as e:
            logger.error(f"Error fetching data: {e}")
            break
    return all_data[:target_rows]

def save_to_mongodb(data, collection):
    """Save data to MongoDB."""
    try:
        result = collection.insert_many(data, ordered=False)
        logger.info(f"Inserted {len(result.inserted_ids)} records into MongoDB")
    except Exception as e:
        logger.error(f"Error inserting data into MongoDB: {e}")

def main():
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("DB_NAME")
    collection_name = "bronze_permits"

    api_url = "https://data.cityofchicago.org/resource/ydr8-5enu.json"

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Drop existing collection to avoid duplicates
    collection.drop()

    # Fetch and save data
    data = fetch_data(api_url)
    if data:
        save_to_mongodb(data, collection)
        logger.info(f"Total rows: {collection.count_documents({})}")
        logger.info(f"Sample record: {collection.find_one()}")

    client.close()

if __name__ == "__main__":
    main()