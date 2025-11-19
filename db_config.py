import os
import pymongo
from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("Missing MONGO_URI in .env")

# Apify setup
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
if not APIFY_API_TOKEN:
    raise ValueError("Missing APIFY_API_TOKEN in .env")

_client = None
_db = None

def get_database():
    """Singleton pattern for MongoDB connection"""
    global _client, _db
    if _db is None:
        _client = pymongo.MongoClient(MONGO_URI)
        _db = _client["cosmetics_app"]
    return _db

def get_clients_collection():
    return get_database()["clients"]

def get_audience_collection():
    return get_database()["audience_data"]

# Backward compatibility
audience_collection = get_audience_collection()
clients_collection = get_clients_collection()
db = get_database()

# APIFY CLIENT
apify_client = ApifyClient(APIFY_API_TOKEN)
