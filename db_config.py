# db_config.py
import os
import pymongo
from apify_client import ApifyClient
from dotenv import load_dotenv
import httpx
import logging

load_dotenv()

logger = logging.getLogger(__name__)

# MongoDB setup
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("Missing MONGO_URI in .env")

# Apify setup
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
if not APIFY_API_TOKEN:
    raise ValueError("Missing APIFY_API_TOKEN in .env")

# Unipile setup
UNIPILE_API_TOKEN = os.getenv("UNIPILE_API_TOKEN")
UNIPILE_DSN = os.getenv("UNIPILE_DSN")

# ✅ FIX: Ensure UNIPILE_DSN has proper format
if not UNIPILE_DSN:
    raise ValueError("Missing UNIPILE_DSN in .env")

# ✅ Add protocol if missing
if not UNIPILE_DSN.startswith(("http://", "https://")):
    UNIPILE_DSN = f"https://{UNIPILE_DSN}"
    logger.warning(f"⚠️ Added https:// protocol to UNIPILE_DSN: {UNIPILE_DSN}")

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


# ═══════════════════════════════════════════════════════════════════════
# UNIPILE ACCOUNT MANAGEMENT FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════

async def list_all_unipile_accounts():
    """
    List all Instagram/LinkedIn accounts connected to Unipile
    Returns: List of account dictionaries
    """
    if not UNIPILE_API_TOKEN:
        logger.error("❌ UNIPILE_API_TOKEN not set in environment")
        return []
    
    if not UNIPILE_DSN:
        logger.error("❌ UNIPILE_DSN not set in environment")
        return []
    
    url = f"{UNIPILE_DSN}/api/v1/accounts"
    headers = {"X-API-KEY": UNIPILE_API_TOKEN}
    
    logger.info(f"🔗 Calling Unipile API: {url}")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            # ✅ FIX: Handle different response formats
            # Unipile can return either:
            # 1. Direct list: [{"id": "...", "type": "..."}]
            # 2. Wrapped object: {"items": [...]} or {"accounts": [...]}
            # 3. Dict with account_id as keys: {"account_id_1": {...}, "account_id_2": {...}}
            
            accounts = []
            
            if isinstance(data, list):
                # Direct list format
                accounts = data
            elif isinstance(data, dict):
                # Check for common wrapper keys
                if "items" in data:
                    accounts = data["items"]
                elif "accounts" in data:
                    accounts = data["accounts"]
                elif "data" in data:
                    accounts = data["data"]
                else:
                    # Dict with account_id as keys - convert to list
                    accounts = []
                    for key, value in data.items():
                        if isinstance(value, dict):
                            # Add the key as 'id' if not present
                            if 'id' not in value:
                                value['id'] = key
                            accounts.append(value)
                        elif isinstance(value, str):
                            # Simple string value, create minimal dict
                            accounts.append({"id": key, "raw_value": value})
            
            logger.info(f"🔍 Found {len(accounts)} Unipile accounts")
            
            # Log account details
            for acc in accounts:
                if isinstance(acc, dict):
                    acc_id = acc.get('id', 'UNKNOWN')
                    acc_type = acc.get('type', acc.get('provider', 'UNKNOWN'))
                    username = acc.get('username', acc.get('name', 'N/A'))
                    status = acc.get('status', 'N/A')
                    logger.info(f"   • {acc_type}: {acc_id} - {username} (Status: {status})")
                else:
                    logger.warning(f"   ⚠️ Unexpected account format: {type(acc)} - {acc}")
            
            return accounts
            
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ HTTP error listing Unipile accounts: {e.response.status_code} - {e.response.text}")
            return []
        except Exception as e:
            logger.error(f"❌ Error listing Unipile accounts: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []


async def delete_unipile_account(account_id: str):
    """
    Delete a specific account from Unipile
    Args:
        account_id: The Unipile account ID to delete
    Returns: True if successful, False otherwise
    """
    if not UNIPILE_API_TOKEN:
        raise ValueError("UNIPILE_API_KEY not set in environment")
    
    url = f"{UNIPILE_DSN}/api/v1/accounts/{account_id}"
    headers = {"X-API-KEY": UNIPILE_API_TOKEN}
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.delete(url, headers=headers)
            
            if response.status_code == 200:
                logger.info(f"✅ Deleted Unipile account: {account_id}")
                return True
            else:
                logger.error(f"❌ Failed to delete account {account_id}: Status {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"❌ Error deleting account {account_id}: {e}")
            return False


async def audit_client_accounts():
    """
    Audit all clients' Instagram/LinkedIn accounts
    Checks if account_ids exist in Unipile and cleans up orphaned entries
    """
    logger.info("🔍 Starting client account audit...")
    
    # Get all Unipile accounts
    unipile_accounts = await list_all_unipile_accounts()
    valid_account_ids = {acc['id'] for acc in unipile_accounts}
    
    logger.info(f"📊 Valid Unipile account IDs: {valid_account_ids}")
    
    # Audit Instagram accounts
    instagram_clients = clients_collection.find({"instagram.account_id": {"$exists": True, "$ne": None}})
    
    for client in instagram_clients:
        client_id = client["client_id"]
        instagram_account_id = client.get("instagram", {}).get("account_id")
        
        if instagram_account_id and instagram_account_id not in valid_account_ids:
            logger.warning(f"⚠️ Client {client_id} has orphaned Instagram account: {instagram_account_id}")
            logger.info(f"   Cleaning up database entry...")
            
            clients_collection.update_one(
                {"client_id": client_id},
                {
                    "$set": {
                        "instagram.status": "orphaned",
                        "instagram.is_active": False,
                        "instagram.orphaned_at": pymongo.datetime.datetime.utcnow().isoformat()
                    }
                }
            )
            logger.info(f"✅ Marked Instagram account as orphaned for client {client_id}")
        elif instagram_account_id:
            logger.info(f"✅ Client {client_id} Instagram account is valid: {instagram_account_id}")
    
    # Audit LinkedIn accounts
    linkedin_clients = clients_collection.find({"linkedin.account_id": {"$exists": True, "$ne": None}})
    
    for client in linkedin_clients:
        client_id = client["client_id"]
        linkedin_account_id = client.get("linkedin", {}).get("account_id")
        
        if linkedin_account_id and linkedin_account_id not in valid_account_ids:
            logger.warning(f"⚠️ Client {client_id} has orphaned LinkedIn account: {linkedin_account_id}")
            logger.info(f"   Cleaning up database entry...")
            
            clients_collection.update_one(
                {"client_id": client_id},
                {
                    "$set": {
                        "linkedin.status": "orphaned",
                        "linkedin.is_active": False,
                        "linkedin.orphaned_at": pymongo.datetime.datetime.utcnow().isoformat()
                    }
                }
            )
            logger.info(f"✅ Marked LinkedIn account as orphaned for client {client_id}")
        elif linkedin_account_id:
            logger.info(f"✅ Client {client_id} LinkedIn account is valid: {linkedin_account_id}")
    
    logger.info("✅ Account audit complete")


async def cleanup_duplicate_accounts(keep_account_id: str, delete_account_ids: list):
    """
    Delete duplicate Unipile accounts and keep only one
    
    Args:
        keep_account_id: The account ID to keep
        delete_account_ids: List of account IDs to delete
    """
    logger.info(f"🧹 Starting cleanup - keeping {keep_account_id}")
    logger.info(f"   Deleting: {', '.join(delete_account_ids)}")
    
    deleted_count = 0
    failed_count = 0
    
    for account_id in delete_account_ids:
        success = await delete_unipile_account(account_id)
        if success:
            deleted_count += 1
        else:
            failed_count += 1
    
    logger.info(f"✅ Cleanup complete: {deleted_count} deleted, {failed_count} failed")
    
    return {
        "kept": keep_account_id,
        "deleted": deleted_count,
        "failed": failed_count
    }