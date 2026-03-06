import os
import logging
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

logger = logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGO_URI", "")
MONGO_USER = os.getenv("MONGO_USER", "")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "")


def get_mongo_uri():
    """Build MongoDB connection string.

    Handles two formats for MONGO_URI:
    - Full connection string (starts with "mongodb"): used as-is
    - Cluster hostname only: assembled with MONGO_USER/MONGO_PASSWORD
    """
    if MONGO_URI.startswith("mongodb"):
        return MONGO_URI
    return (
        f"mongodb+srv://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_URI}"
        f"/{MONGO_DB_NAME}?retryWrites=true&w=majority"
    )


def get_mongo_client(**kwargs):
    """Return a MongoClient using the resolved connection string."""
    uri = get_mongo_uri()
    return MongoClient(uri, **kwargs)


def get_db(**kwargs):
    """Return the default database handle."""
    client = get_mongo_client(**kwargs)
    return client[MONGO_DB_NAME]
