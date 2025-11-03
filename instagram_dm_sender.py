# instagram_dm_sender.py
import os
from apify_client import ApifyClient
from dotenv import load_dotenv
import pymongo
from datetime import datetime

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
client = pymongo.MongoClient(MONGO_URI)
db = client["cosmetics_app"]
dm_logs_collection = db["dm_logs"]

def send_instagram_dm(username: str, message: str, client_id: str):
    try:
        apify_client = ApifyClient(os.getenv("APIFY_API_TOKEN"))
        sessionid = os.getenv("INSTA_SESSION_ID")

        if not sessionid:
            raise ValueError("Missing INSTA_SESSION_ID in environment variables")

        run_input = {
            "sessionid": sessionid,
            "target_usernames": [username],
            "message": message,
            "delay_between_messages": 10,
            "proxy": {"useApifyProxy": True, "apifyProxyGroups": ["RESIDENTIAL"]}
        }

        print(f"📤 Sending Instagram DM to @{username}")
        run = apify_client.actor("deepanshusharm/instagram-dms-automation").call(run_input=run_input)

        log_entry = {
            "client_id": client_id,
            "username": username,
            "platform": "instagram",
            "message": message,
            "status": "sent",
            "apify_run_id": run.get("id"),
            "sent_at": datetime.utcnow()
        }
        dm_logs_collection.insert_one(log_entry)

        print(f"✅ DM sent successfully to @{username}")
        return {"success": True, "username": username}

    except Exception as e:
        log_entry = {
            "client_id": client_id,
            "username": username,
            "platform": "instagram",
            "message": message,
            "status": "failed",
            "error": str(e),
            "sent_at": datetime.utcnow()
        }
        dm_logs_collection.insert_one(log_entry)
        print(f"❌ Failed to send DM to @{username}: {e}")
        return {"success": False, "error": str(e)}
