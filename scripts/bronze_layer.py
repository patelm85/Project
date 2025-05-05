import os
from pymongo import MongoClient
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_data(db_name, collection_name):
    """Verify the number of rows and columns in the bronze layer."""
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI")
    
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Count rows
    row_count = collection.count_documents({})
    logger.info(f"Total rows in {collection_name}: {row_count}")

    # Get columns (fields) from a sample document
    sample_doc = collection.find_one()
    if sample_doc:
        columns = list(sample_doc.keys())
        logger.info(f"Total columns: {len(columns)}")
        logger.info(f"Columns: {columns}")

    client.close()

def main():
    db_name = os.getenv("DB_NAME")
    collection_name = "bronze_permits"
    verify_data(db_name, collection_name)

if __name__ == "__main__":
    main()