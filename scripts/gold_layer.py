import os
from pymongo import MongoClient
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_gold_layer(db_name, silver_collection_name):
    """Create aggregated datasets for gold layer."""
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI")
    
    client = MongoClient(mongo_uri)
    db = client[db_name]
    silver_collection = db[silver_collection_name]

    # Aggregation 1: Permit Distribution by Permit Type
    pipeline_permit_dist = [
        {
            "$group": {
                "_id": "$permit_type",
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"count": -1}},
        {"$out": "gold_permit_distribution"}
    ]

    # Aggregation 2: Permits by Community Area
    pipeline_community_dist = [
        {
            "$match": {"community_area": {"$ne": None}}
        },
        {
            "$group": {
                "_id": "$community_area",
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"_id": 1}},
        {"$out": "gold_community_distribution"}
    ]

    # Aggregation 3: Permit Issue Trend by Year
    pipeline_trend = [
        {
            "$match": {"issue_date": {"$ne": None}}
        },
        {
            "$group": {
                "_id": {"$year": "$issue_date"},
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"_id": 1}},
        {"$out": "gold_issue_trend"}
    ]

    try:
        silver_collection.aggregate(pipeline_permit_dist)
        silver_collection.aggregate(pipeline_community_dist)
        silver_collection.aggregate(pipeline_trend)
        logger.info("Gold layer aggregations created successfully")
    except Exception as e:
        logger.error(f"Error creating gold layer: {e}")

    client.close()

def main():
    db_name = os.getenv("DB_NAME")
    silver_collection_name = "silver_permits"
    create_gold_layer(db_name, silver_collection_name)

if __name__ == "__main__":
    main()