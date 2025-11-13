import os
import json
import re
from apify_client import ApifyClient
from pymongo import MongoClient

# MongoDB Connection
mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise ValueError("❌ MONGO_URI not found in environment variables")

mongo_client = MongoClient(mongo_uri)
db = mongo_client["cosmetics_app"]
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


def send_instagram_dm(recipient_username: str, message: str, client_id: str = None):
    """
    ✅ Send Instagram DM using bhansalisoft/instagram-bulk-message-sender
    """
    if not recipient_username:
        raise ValueError("Recipient username is missing")

    print(f"\n📤 Sending Instagram DM to @{recipient_username}")

    # Clean emojis
    message = sanitize_message(message)

    # Load APIFY token
    apify_token = os.getenv("APIFY_API_TOKEN")
    if not apify_token:
        raise ValueError("❌ APIFY_API_TOKEN not found")

    client = ApifyClient(apify_token)

    # ✅ Load cookies from JSON file
    cookies_path = os.getenv("INSTAGRAM_COOKIES_PATH", "cookies.json")
    if not os.path.exists(cookies_path):
        raise FileNotFoundError(f"Instagram cookies file not found: {cookies_path}")

    with open(cookies_path, "r", encoding="utf-8") as f:
        cookies_data = json.load(f)

    # ✅ Prepare actor input for bhansalisoft actor
    run_input = {
        "Instagram_UserName_List": [recipient_username],
        "Message": message,
        "Delay": "5",  # seconds between messages
        "Cookies": cookies_data
    }

    try:
        # ✅ Run bhansalisoft actor
        print(f"🚀 Starting Apify actor: bhansalisoft/instagram-bulk-message-sender")
        run = client.actor("bhansalisoft/instagram-bulk-message-sender").call(
            run_input=run_input
        )

        print(f"📊 Actor run status: {run.get('status')}")
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
            error_msg = f"Actor run failed with status: {run.get('status')}"
            print(f"❌ {error_msg}")
            raise Exception(error_msg)

    except Exception as e:
        error_msg = str(e)
        print(f"❌ Failed to send DM: {error_msg}")
        
        # Handle known errors
        if "trial has expired" in error_msg.lower():
            raise Exception(
                "❌ APIFY ACTOR TRIAL EXPIRED\n"
                "👉 Rent the actor at: https://console.apify.com/actors/bhansalisoft~instagram-bulk-message-sender"
            )
        elif "insufficient credit" in error_msg.lower():
            raise Exception(
                "❌ INSUFFICIENT APIFY CREDITS\n"
                "👉 Add credits at: https://console.apify.com/billing"
            )
        else:
            raise Exception(f"Failed to send Instagram DM: {error_msg}")

