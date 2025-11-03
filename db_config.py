# db_config.py
from dotenv import load_dotenv
from apify_client import ApifyClient
import pymongo
import os
from apscheduler.schedulers.background import BackgroundScheduler

load_dotenv()

APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
if not APIFY_API_TOKEN:
    raise ValueError("Missing APIFY_API_TOKEN in .env")

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("Missing MONGO_URI in .env")

client = pymongo.MongoClient(MONGO_URI)
db = client["cosmetics_app"]

clients_collection = db["clients"]
audience_collection = db["audience_data"]

apify_client = ApifyClient(APIFY_API_TOKEN)

scheduler = BackgroundScheduler()
scheduler.start()

# Global in-memory task tracker
tasks = {}
