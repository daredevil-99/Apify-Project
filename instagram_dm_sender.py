import os
import json
import re
from apify_client import ApifyClient
from pymongo import MongoClient
from datetime import datetime

# MongoDB Connection
mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise ValueError("❌ MONGO_URI not found in environment variables")

mongo_client = MongoClient(mongo_uri)
db = mongo_client["cosmetics_app"]
clients_collection = db["clients"]
audience_collection = db["audience_data"]


def sanitize_message(text: str) -> str:
    """Remove ALL emojis and special characters"""
    if not text:
        return ""
    
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F1E0-\U0001F1FF"
        "\U00002500-\U00002BEF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "\U0001f926-\U0001f937"
        "\U00010000-\U0010ffff"
        "\u2640-\u2642"
        "\u2600-\u2B55"
        "\u200d"
        "\u23cf"
        "\u23e9"
        "\u231a"
        "\ufe0f"
        "\u3030"
        "]+",
        flags=re.UNICODE
    )
    
    clean_text = emoji_pattern.sub(' ', text)
    clean_text = ' '.join(clean_text.split())
    return clean_text.strip()


def get_instagram_cookies_from_db(client_id: str):
    """
    Fetch Instagram cookies from MongoDB for the given client_id.
    Validates expiry and returns cookie data.
    """
    client = clients_collection.find_one(
        {"client_id": client_id},
        {"cookies": 1, "_id": 0}
    )
    
    if not client:
        raise ValueError(f"❌ Client {client_id} not found in database")
    
    if "cookies" not in client:
        raise ValueError(f"❌ No cookies found for client {client_id}")
    
    instagram_cookies = client["cookies"].get("instagram")
    
    if not instagram_cookies:
        raise ValueError(
            f"❌ No Instagram cookies found for client {client_id}\n"
            "👉 Upload cookies via: POST /pipeline/cookies-file/{client_id}"
        )
    
    # Check if cookies are expired
    expires_at = instagram_cookies.get("expires_at")
    if expires_at:
        expiry_date = datetime.fromisoformat(expires_at)
        if datetime.utcnow() > expiry_date:
            raise ValueError(
                f"❌ Instagram cookies expired on {expires_at}\n"
                "👉 Please re-upload cookies via: POST /pipeline/cookies-file/{client_id}"
            )
    
    # Check if cookies are marked as inactive
    if not instagram_cookies.get("is_active", True):
        raise ValueError(
            f"❌ Instagram cookies are marked as inactive\n"
            "👉 Please re-upload cookies via: POST /pipeline/cookies-file/{client_id}"
        )
    
    cookies_data = instagram_cookies.get("data")
    
    if not cookies_data:
        raise ValueError(f"❌ Cookie data is empty for client {client_id}")
    
    print(f"✅ Loaded {len(cookies_data)} Instagram cookies from database")
    return cookies_data


def send_instagram_dm(recipient_username: str, message: str, client_id: str = None):
    """
    Send Instagram DM using bhansalisoft/instagram-bulk-message-sender
    Fetches cookies from MongoDB instead of local file.
    """
    if not recipient_username:
        raise ValueError("Recipient username is missing")
    
    if not client_id:
        raise ValueError("❌ client_id is required to fetch cookies from database")

    print(f"\n📤 Sending Instagram DM to @{recipient_username}")

    # Clean emojis
    message = sanitize_message(message)

    # Load APIFY token
    apify_token = os.getenv("APIFY_API_TOKEN")
    if not apify_token:
        raise ValueError("❌ APIFY_API_TOKEN not found")

    client = ApifyClient(apify_token)

    # ✅ Fetch cookies from MongoDB
    try:
        cookies_data = get_instagram_cookies_from_db(client_id)
    except Exception as e:
        raise ValueError(f"Failed to load cookies: {str(e)}")

    # Actor input
    run_input = {
        "Instagram_UserName_List": [recipient_username],
        "Message": message,
        "Delay": "5",
        "Cookies": cookies_data
    }

    try:
        print(f"🚀 Starting Apify actor: bhansalisoft/instagram-bulk-message-sender")
        run = client.actor("bhansalisoft/instagram-bulk-message-sender").call(
            run_input=run_input
        )

        print(f"📊 Status: {run.get('status')}")
        print(f"🆔 Run ID: {run.get('id')}")

        if run.get("status") == "SUCCEEDED":
            print(f"✅ DM sent successfully to @{recipient_username}")
            return {
                "status": "success",
                "recipient": recipient_username,
                "message": message,
                "run_id": run.get("id")
            }
        else:
            raise Exception(f"Actor failed: {run.get('status')}")

    except Exception as e:
        error_msg = str(e)
        print(f"❌ Failed to send DM: {error_msg}")

        if "trial has expired" in error_msg.lower():
            raise Exception(
                "❌ APIFY ACTOR TRIAL EXPIRED\n"
                "👉 Rent actor: https://console.apify.com/actors/bhansalisoft~instagram-bulk-message-sender"
            )
        elif "insufficient credit" in error_msg.lower():
            raise Exception(
                "❌ INSUFFICIENT APIFY CREDITS\n"
                "👉 Add credits: https://console.apify.com/billing"
            )
        else:
            raise Exception(f"Failed to send Instagram DM: {error_msg}")