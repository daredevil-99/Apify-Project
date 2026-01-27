# services/webhook_manager.py

import requests
import os
from dotenv import load_dotenv
from db_config import clients_collection

# Load environment variables
load_dotenv()

# Get config from environment
UNIPILE_API_TOKEN = os.getenv("UNIPILE_API_TOKEN")
UNIPILE_DSN = os.getenv("UNIPILE_DSN")

# ✅ Fix: Ensure DSN has https:// protocol
if UNIPILE_DSN and not UNIPILE_DSN.startswith("http"):
    UNIPILE_DSN = f"https://{UNIPILE_DSN}"

def get_ngrok_url():
    """Auto-detect current ngrok URL"""
    try:
        response = requests.get("http://127.0.0.1:4040/api/tunnels", timeout=3)
        data = response.json()
        
        for tunnel in data.get("tunnels", []):
            if tunnel.get("proto") == "https":
                public_url = tunnel["public_url"]
                print(f"🌐 Detected ngrok URL: {public_url}")
                return public_url
    except Exception as e:
        print(f"⚠️ Ngrok not running: {e}")
    
    # Fallback to .env
    return os.getenv("APP_BASE_URL")


def register_unipile_webhooks(base_url: str):
    """
    Register webhooks for Instagram & LinkedIn message tracking.
    Called automatically on server startup.
    """
    
    if not UNIPILE_API_TOKEN:
        print("❌ ERROR: UNIPILE_API_TOKEN not found in .env file!")
        return {"registered": [], "failed": ["all - missing API token"]}
    
    if not UNIPILE_DSN:
        print("❌ ERROR: UNIPILE_DSN not found in .env file!")
        return {"registered": [], "failed": ["all - missing DSN"]}
    
    print(f"\n🔗 Using Unipile DSN: {UNIPILE_DSN}")
    
    headers = {
        "X-API-KEY": UNIPILE_API_TOKEN,
        "accept": "application/json",
        "content-type": "application/json"
    }
    
    # ✅ FIXED: Webhook configurations with correct Unipile API format
    # services/webhook_manager.py

    # services/webhook_manager.py - Around line 65-80

    webhook_configs = [
        {
            "source": "account_status",
            "request_url": f"{base_url}/pipeline/webhook/account-status",
            "format": "json",
            "enabled": True,
            "events": [
                "creation_success",
                "creation_fail",
                "deleted",
                "reconnected",
                "error",
                "credentials",
                "permissions"
            ]
        },
        {
            "source": "messaging",
            "request_url": f"{base_url}/pipeline/webhook/messaging",
            "format": "json",
            "enabled": True,
            "events": [
                "message_received",
                "message_read",
                "message_delivered",
                "message_reaction"
            ]
        },
        {
            "source": "users",
            "request_url": f"{base_url}/pipeline/webhook/users",  # ✅ FIXED: Changed from /chat to /users
            "format": "json",
            "enabled": True,
            "events": [
                "new_relation"
            ]
        }
    ]
    
    print("\n" + "="*70)
    print("🔔 REGISTERING UNIPILE WEBHOOKS")
    print("="*70)
    
    # Step 1: List and delete existing webhooks to avoid duplicates
    try:
        print(f"\n🔍 Fetching existing webhooks...")
        
        list_response = requests.get(
            f"{UNIPILE_DSN}/api/v1/webhooks",
            headers=headers,
            timeout=10
        )
        
        if list_response.status_code == 200:
            existing_webhooks = list_response.json().get("items", [])
            print(f"   Found {len(existing_webhooks)} existing webhooks")
            
            for webhook in existing_webhooks:
                webhook_id = webhook.get("id")
                webhook_source = webhook.get("source")
                
                print(f"   🗑️  Deleting: {webhook_source} (ID: {webhook_id})")
                
                delete_response = requests.delete(
                    f"{UNIPILE_DSN}/api/v1/webhooks/{webhook_id}",
                    headers=headers,
                    timeout=10
                )
                
                if delete_response.status_code in [200, 204]:
                    print(f"      ✅ Deleted")
                else:
                    print(f"      ⚠️ Delete failed: {delete_response.status_code}")
        else:
            print(f"   ⚠️ Could not list webhooks: {list_response.status_code}")
            
    except Exception as e:
        print(f"⚠️ Error cleaning old webhooks: {e}")
    
    # Step 2: Register new webhooks
    registered = []
    failed = []
    
    for config in webhook_configs:
        try:
            print(f"\n📍 Registering: {config['source']}")
            print(f"   URL: {config['request_url']}")
            print(f"   Events: {', '.join(config['events'])}")
            
            response = requests.post(
                f"{UNIPILE_DSN}/api/v1/webhooks",
                json=config,
                headers=headers,
                timeout=10
            )
            
            if response.status_code in [200, 201]:
                webhook_data = response.json()
                webhook_id = webhook_data.get("id", "unknown")
                print(f"   ✅ Success - ID: {webhook_id}")
                registered.append(config["source"])
            else:
                print(f"   ❌ Failed: {response.status_code}")
                print(f"   Error: {response.text[:300]}")
                failed.append(config["source"])
                
        except Exception as e:
            print(f"   ❌ Exception: {str(e)}")
            failed.append(config["source"])
    
    print("\n" + "="*70)
    print(f"✅ Registered: {len(registered)}/{len(webhook_configs)} webhooks")
    if failed:
        print(f"⚠️ Failed: {', '.join(failed)}")
    print("="*70 + "\n")
    
    return {
        "registered": registered,
        "failed": failed
    }


def verify_webhooks():
    """Check currently registered webhooks"""
    
    if not UNIPILE_API_TOKEN or not UNIPILE_DSN:
        print("❌ Missing Unipile credentials in .env")
        return []
    
    headers = {
        "X-API-KEY": UNIPILE_API_TOKEN,
        "accept": "application/json"
    }
    
    try:
        response = requests.get(
            f"{UNIPILE_DSN}/api/v1/webhooks",
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            webhooks = response.json().get("items", [])
            
            if len(webhooks) > 0:
                print(f"\n📋 Currently registered webhooks: {len(webhooks)}")
                
                for wh in webhooks:
                    webhook_id = wh.get("id")
                    source = wh.get("source")
                    url = wh.get("request_url")
                    enabled = wh.get("enabled", False)
                    events = wh.get("events", [])
                    
                    status = "✅" if enabled else "⏸️"
                    print(f"   {status} {source}")
                    print(f"      ID: {webhook_id}")
                    print(f"      URL: {url}")
                    print(f"      Events: {', '.join(events)}")
            else:
                print(f"\n📋 No webhooks registered yet")
            
            return webhooks
        else:
            print(f"❌ Failed to list webhooks: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return []
            
    except Exception as e:
        print(f"❌ Error verifying webhooks: {str(e)}")
        return []