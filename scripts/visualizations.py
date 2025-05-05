import os
import matplotlib.pyplot as plt
from pymongo import MongoClient
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def plot_permit_distribution(db, output_file):
    """Plot permit distribution by permit type."""
    data = list(db.gold_permit_distribution.find().limit(10))  # Top 10
    labels = [item["_id"] for item in data]
    counts = [item["count"] for item in data]

    plt.figure(figsize=(10, 6))
    plt.barh(labels, counts, color='skyblue')
    plt.xlabel("Number of Permits")
    plt.ylabel("Permit Type")
    plt.title("Top 10 Permit Types Distribution")
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()
    logger.info(f"Saved permit distribution plot to {output_file}")

def plot_community_distribution(db, output_file):
    """Plot permits by community area."""
    data = list(db.gold_community_distribution.find())
    areas = [item["_id"] for item in data]
    counts = [item["count"] for item in data]

    plt.figure(figsize=(12, 6))
    plt.bar(areas, counts, color='lightgreen')
    plt.xlabel("Community Area")
    plt.ylabel("Number of Permits")
    plt.title("Permits by Community Area")
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()
    logger.info(f"Saved community distribution plot to {output_file}")

def plot_issue_trend(db, output_file):
    """Plot permit issue trend by year."""
    data = list(db.gold_issue_trend.find())
    years = [item["_id"] for item in data]
    counts = [item["count"] for item in data]

    plt.figure(figsize=(10, 6))
    plt.plot(years, counts, marker='o', color='coral')
    plt.xlabel("Year")
    plt.ylabel("Number of Permits Issued")
    plt.title("Permit Issue Trend Over Time")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()
    logger.info(f"Saved issue trend plot to {output_file}")

def main():
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("DB_NAME")
    
    client = MongoClient(mongo_uri)
    db = client[db_name]

    plot_permit_distribution(db, "permit_distribution.png")
    plot_community_distribution(db, "community_distribution.png")
    plot_issue_trend(db, "issue_trend.png")

    client.close()

if __name__ == "__main__":
    main()