import os
from pymongo import MongoClient
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_data(db_name, bronze_collection_name, silver_collection_name):
    """Clean data and store in silver layer using MongoDB aggregation."""
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI")
    
    client = MongoClient(mongo_uri)
    db = client[db_name]
    bronze_collection = db[bronze_collection_name]
    silver_collection = db[silver_collection_name]

    # Drop existing silver collection
    silver_collection.drop()

    # Aggregation pipeline for cleaning
    pipeline = [
        # Filter out records with missing critical fields
        {
            "$match": {
                "permit_": {"$exists": True, "$ne": None},
                "permit_type": {"$exists": True, "$ne": None},
                "issue_date": {"$exists": True, "$ne": None}
                # Removed community_area filter to retain more records
            }
        },
        # Standardize fields
        {
            "$set": {
                "permit_type": {"$trim": {"input": "$permit_type"}},
                "work_description": {
                    "$cond": {
                        "if": {"$ne": ["$work_description", ""]},
                        "then": {"$trim": {"input": "$work_description"}},
                        "else": None
                    }
                },
                "issue_date": {
                    "$cond": {
                        "if": {"$ne": ["$issue_date", ""]},
                        "then": {"$dateFromString": {"dateString": "$issue_date"}},
                        "else": None
                    }
                },
                "community_area": {
                    "$cond": {
                        "if": {"$ne": ["$community_area", ""]},
                        "then": {"$toInt": "$community_area"},
                        "else": None
                    }
                }
            }
        },
        # Remove duplicates based on permit_
        {
            "$group": {
                "_id": "$permit_",
                "doc": {"$first": "$$ROOT"}
            }
        },
        # Project cleaned fields
        {
            "$replaceRoot": {"newRoot": "$doc"}
        },
        # Write to silver collection
        {
            "$out": silver_collection_name
        }
    ]

    try:
        bronze_collection.aggregate(pipeline)
        row_count = silver_collection.count_documents({})
        logger.info(f"Total rows in silver layer: {row_count}")
        logger.info(f"Sample cleaned record: {silver_collection.find_one()}")
    except Exception as e:
        logger.error(f"Error cleaning data: {e}")

    client.close()

def main():
    db_name = os.getenv("DB_NAME")
    bronze_collection_name = "bronze_permits"
    silver_collection_name = "silver_permits"
    clean_data(db_name, bronze_collection_name, silver_collection_name)

if __name__ == "__main__":
    main()