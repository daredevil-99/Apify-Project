"""
LinkedIn DM Sender - FIXED VERSION with Complete Tracking
==========================================================
This version ensures ALL fields are properly tracked:
- provider_id ✅
- name ✅
- profile_url ✅
- status ✅
"""

import os
import requests
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import time
import re
import logging
from typing import Dict, Any, Tuple, Optional

# Import tracking
from linkedin_tracking import track_linkedin_invitation_sent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("❌ MONGO_URI missing in .env")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["cosmetics_app"]
clients_collection = db["clients"]


def get_unipile_credentials() -> Tuple[str, str]:
    """Get Unipile API credentials"""
    token = os.getenv("UNIPILE_API_TOKEN")
    dsn = os.getenv("UNIPILE_DSN")

    if not token:
        raise ValueError("❌ Missing UNIPILE_API_TOKEN in .env")
    if not dsn:
        raise ValueError("❌ Missing UNIPILE_DSN in .env")
    
    if not dsn.startswith(('http://', 'https://')):
        dsn = f"https://{dsn}"

    return token, dsn


def get_linkedin_account_id(client_id: str) -> Dict[str, Any]:
    """Get LinkedIn account_id from OAuth connection"""
    try:
        client = clients_collection.find_one({"client_id": client_id})
        if not client:
            return {"status": "error", "error": f"Client not found: {client_id}"}

        linkedin_data = client.get("linkedin", {})
        
        if not linkedin_data:
            return {"status": "error", "error": f"LinkedIn not connected"}
        
        account_id = linkedin_data.get("account_id")
        
        if not account_id:
            return {"status": "error", "error": f"No LinkedIn account_id found"}
        
        # Verify account
        try:
            token, dsn = get_unipile_credentials()
            headers = {"X-API-KEY": token, "accept": "application/json"}
            
            check_url = f"{dsn}/api/v1/accounts/{account_id}"
            response = requests.get(check_url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                return {
                    "status": "error",
                    "error": f"LinkedIn account verification failed (status {response.status_code})"
                }
            
            print(f"✅ Using LinkedIn account: {account_id}")
            return {"status": "success", "account_id": account_id}
            
        except requests.RequestException as e:
            return {"status": "error", "error": f"Failed to verify account: {str(e)}"}
            
    except Exception as e:
        logger.error(f"Error getting LinkedIn account: {str(e)}", exc_info=True)
        return {"status": "error", "error": f"Database error: {str(e)}"}


def extract_linkedin_public_identifier(profile_url: str) -> Dict[str, Any]:
    """Extract LinkedIn identifier from profile URL"""
    try:
        if not profile_url:
            return {"status": "error", "error": "Profile URL is empty"}
        
        profile_url = profile_url.strip().rstrip('/')
        
        if ',' in profile_url:
            profile_url = profile_url.split(',')[0]
        
        # Sales Navigator
        match = re.search(r'/sales/lead/([^/?]+)', profile_url)
        if match:
            identifier = match.group(1)
            print(f"🔑 Sales Navigator ID: {identifier}")
            return {"status": "success", "identifier": identifier, "is_sales_nav": True}
        
        # Regular profile
        match = re.search(r'/in/([^/?]+)', profile_url)
        if match:
            identifier = match.group(1)
            print(f"🔑 Public identifier: {identifier}")
            return {"status": "success", "identifier": identifier, "is_sales_nav": False}
        
        return {"status": "error", "error": f"Invalid LinkedIn URL format: {profile_url}"}
        
    except Exception as e:
        logger.error(f"Error parsing LinkedIn URL: {str(e)}", exc_info=True)
        return {"status": "error", "error": f"URL parsing error: {str(e)}"}


def get_provider_id_from_profile(client_id: str, identifier: str, is_sales_nav: bool = False) -> Dict[str, Any]:
    """
    Convert LinkedIn identifier to provider_id
    ⭐ FIXED: Now returns complete profile data
    """
    try:
        if is_sales_nav or identifier.startswith('ACwAA'):
            print(f"✅ Using provider_id directly: {identifier}")
            return {
                "status": "success",
                "provider_id": identifier,
                "first_name": "",
                "last_name": "",
                "public_identifier": identifier
            }
        
        account_result = get_linkedin_account_id(client_id)
        if account_result["status"] != "success":
            return {"status": "error", "error": account_result.get("error")}
        
        account_id = account_result["account_id"]
        token, dsn = get_unipile_credentials()
        
        headers = {"X-API-KEY": token, "accept": "application/json"}
        url = f"{dsn}/api/v1/users/{identifier}"
        params = {"account_id": account_id}
        
        print(f"🔍 Fetching provider_id for: {identifier}")
        
        response = requests.get(url, headers=headers, params=params, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            provider_id = data.get("provider_id")
            
            if not provider_id:
                return {"status": "error", "error": "No provider_id in API response"}
            
            print(f"✅ Provider ID: {provider_id}")
            
            # ⭐ RETURN ALL FIELDS
            return {
                "status": "success",
                "provider_id": provider_id,
                "first_name": data.get("first_name", ""),
                "last_name": data.get("last_name", ""),
                "public_identifier": data.get("public_identifier", identifier),
                "headline": data.get("headline", ""),
                "location": data.get("location", "")
            }
        else:
            data = response.json() if response.text else {}
            error_msg = data.get("message", "Unknown error")
            return {"status": "error", "error": f"Profile lookup failed: {error_msg}"}
                    
    except Exception as e:
        logger.error(f"Error getting provider_id: {str(e)}", exc_info=True)
        return {"status": "error", "error": f"Unexpected error: {str(e)}"}


def send_linkedin_invitation(
    client_id: str, 
    provider_id: str, 
    message: str,
    recipient_name: str = "",
    recipient_url: str = ""
) -> Dict[str, Any]:
    """
    Send LinkedIn connection invitation
    ⭐ FIXED: Now includes tracking with complete data
    """
    try:
        account_result = get_linkedin_account_id(client_id)
        if account_result["status"] != "success":
            return {
                "status": "error",
                "method": "invitation",
                "error": account_result.get("error")
            }
        
        account_id = account_result["account_id"]
        token, dsn = get_unipile_credentials()
        
        headers = {
            "X-API-KEY": token,
            "accept": "application/json",
            "Content-Type": "application/json"
        }
        
        # Truncate message
        MAX_INVITATION_LENGTH = 300
        if len(message) > MAX_INVITATION_LENGTH:
            print(f"⚠️ Truncating message from {len(message)} to {MAX_INVITATION_LENGTH} chars")
            message = message[:MAX_INVITATION_LENGTH - 3] + "..."
        
        url = f"{dsn}/api/v1/users/invite"
        payload = {
            "provider_id": provider_id,
            "account_id": account_id,
            "message": message
        }
        
        print(f"📤 Sending connection invitation to {recipient_name or provider_id}...")
        print(f"   Message: {len(message)} chars")
        
        response = requests.post(url, json=payload, headers=headers, timeout=15)
        
        if response.status_code in [200, 201]:
            data = response.json()
            invitation_id = data.get("id") or data.get("invitation_id")
            
            print(f"✅ Invitation sent!")
            
            # ⭐ TRACK WITH COMPLETE DATA
            track_linkedin_invitation_sent(
                client_id=client_id,
                provider_id=provider_id,
                recipient_name=recipient_name or "Unknown",
                recipient_url=recipient_url,
                message=message,
                invitation_id=invitation_id
            )
            
            return {
                "status": "success",
                "method": "invitation",
                "provider_id": provider_id,
                "invitation_id": invitation_id,
                "details": data
            }
        else:
            try:
                data = response.json()
            except:
                data = {"raw_response": response.text}
            
            error_msg = data.get("message") or data.get("error", "Unknown error")
            print(f"❌ Invitation failed: {error_msg}")
            return {
                "status": "error",
                "method": "invitation",
                "error": f"Status {response.status_code}: {error_msg}",
                "details": data
            }
            
    except Exception as e:
        logger.error(f"Error sending invitation: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "method": "invitation",
            "error": f"Unexpected error: {str(e)}"
        }


def send_linkedin_direct_message(client_id: str, provider_id: str, message: str) -> Dict[str, Any]:
    """Send direct message to connected LinkedIn user"""
    try:
        account_result = get_linkedin_account_id(client_id)
        if account_result["status"] != "success":
            return {
                "status": "error",
                "method": "direct_message",
                "error": account_result.get("error")
            }
        
        account_id = account_result["account_id"]
        token, dsn = get_unipile_credentials()
        
        headers = {
            "X-API-KEY": token,
            "accept": "application/json",
            "Content-Type": "application/json"
        }
        
        url = f"{dsn}/api/v1/chats"
        payload = {
            "account_id": account_id,
            "attendees_ids": [provider_id],
            "text": message
        }
        
        print(f"📤 Sending direct message...")
        
        response = requests.post(url, json=payload, headers=headers, timeout=15)
        
        if response.status_code in [200, 201]:
            data = response.json()
            print(f"✅ Direct message sent!")
            return {
                "status": "success",
                "method": "direct_message",
                "provider_id": provider_id,
                "chat_id": data.get("chat_id"),
                "details": data
            }
        else:
            try:
                data = response.json()
            except:
                data = {"raw_response": response.text}
            
            error_msg = data.get("message") or data.get("error", "Unknown error")
            print(f"❌ Direct message failed: {error_msg}")
            return {
                "status": "error",
                "method": "direct_message",
                "error": f"Status {response.status_code}: {error_msg}",
                "status_code": response.status_code,
                "details": data
            }
            
    except Exception as e:
        logger.error(f"Error sending direct message: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "method": "direct_message",
            "error": f"Unexpected error: {str(e)}"
        }


def send_linkedin_dm_smart(client_id: str, profile_url: str, message: str) -> Dict[str, Any]:
    """
    Smart LinkedIn messaging
    ⭐ FIXED: Now captures and passes all profile data to tracking
    """
    print(f"\n{'='*60}")
    print(f"📤 Smart LinkedIn messaging")
    print(f"   Profile: {profile_url}")
    print(f"{'='*60}\n")
    
    try:
        # Extract identifier
        identifier_result = extract_linkedin_public_identifier(profile_url)
        
        if identifier_result["status"] != "success":
            return {
                "status": "error",
                "error": identifier_result.get("error"),
                "step": "url_parsing"
            }
        
        identifier = identifier_result["identifier"]
        is_sales_nav = identifier_result["is_sales_nav"]
        
        # ⭐ Get provider_id AND profile data
        profile_result = get_provider_id_from_profile(client_id, identifier, is_sales_nav)
        
        if profile_result["status"] != "success":
            return {
                "status": "error",
                "error": profile_result.get("error"),
                "step": "profile_lookup"
            }
        
        provider_id = profile_result["provider_id"]
        
        # ⭐ EXTRACT NAME FROM PROFILE
        first_name = profile_result.get("first_name", "")
        last_name = profile_result.get("last_name", "")
        recipient_name = f"{first_name} {last_name}".strip()
        
        if not recipient_name:
            recipient_name = "Unknown"
        
        print(f"📋 Recipient: {recipient_name}")
        print(f"🔑 Provider ID: {provider_id}")
        
        # Try direct message first
        print("🔄 Attempting direct message (for connected users)...")
        dm_result = send_linkedin_direct_message(client_id, provider_id, message)
        
        if dm_result["status"] == "success":
            return dm_result
        
        # If not connected, send invitation
        error_str = str(dm_result.get("error", ""))
        status_code = dm_result.get("status_code", 0)
        
        if status_code in [403, 422] or "422" in error_str or "cannot be reached" in error_str.lower():
            print("⚠️ Not connected - sending invitation instead...")
            
            # ⭐ PASS COMPLETE DATA TO INVITATION
            invitation_result = send_linkedin_invitation(
                client_id=client_id,
                provider_id=provider_id,
                message=message,
                recipient_name=recipient_name,  # ⭐ NOW PASSING NAME
                recipient_url=profile_url       # ⭐ NOW PASSING URL
            )
            
            return invitation_result
        
        # Other error
        return dm_result
        
    except Exception as e:
        logger.error(f"Error in smart send: {str(e)}", exc_info=True)
        return {"status": "error", "error": f"Unexpected error: {str(e)}"}


def send_linkedin_dm(recipient_url: str, message: str, client_id: str) -> Dict[str, Any]:
    """
    Main wrapper function
    ⭐ FIXED: Now properly tracks all data
    """
    print(f"\n{'='*60}")
    print(f"📤 Sending LinkedIn Message")
    print(f"   Recipient: {recipient_url}")
    print(f"   Client: {client_id}")
    print(f"{'='*60}\n")
    
    try:
        result = send_linkedin_dm_smart(client_id, recipient_url, message)
        
        if not isinstance(result, dict):
            return {
                "status": "error",
                "error": f"Internal error: Invalid return type {type(result)}"
            }
        
        if "status" not in result:
            return {
                "status": "error",
                "error": "Internal error: Missing status field",
                "raw_result": result
            }
        
        # Log result
        if result.get("status") == "success":
            method = result.get("method", "unknown")
            if method == "invitation":
                print(f"✅ Connection invitation sent!")
            else:
                print(f"✅ Direct message sent!")
        else:
            print(f"❌ Failed: {result.get('error', 'Unknown error')}")
        
        return result
        
    except Exception as e:
        logger.error(f"Exception in send_linkedin_dm: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": f"Unexpected error: {str(e)}",
            "exception_type": type(e).__name__
        }


def verify_linkedin_connection(client_id: str) -> Dict[str, Any]:
    """Verify LinkedIn account connection"""
    try:
        account_result = get_linkedin_account_id(client_id)
        
        if account_result["status"] != "success":
            return {
                "status": "error",
                "error": account_result.get("error"),
                "oauth_required": True
            }
        
        account_id = account_result["account_id"]
        token, dsn = get_unipile_credentials()
        headers = {"X-API-KEY": token, "accept": "application/json"}
        
        check_url = f"{dsn}/api/v1/accounts/{account_id}"
        response = requests.get(check_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            return {
                "status": "connected",
                "account_id": account_id,
                "provider": data.get("provider", "LINKEDIN"),
                "is_active": True,
                "oauth_method": True
            }
        else:
            return {
                "status": "error",
                "error": f"Account verification failed: {response.status_code}",
                "account_id": account_id
            }
            
    except Exception as e:
        logger.error(f"Error verifying connection: {str(e)}", exc_info=True)
        return {"status": "error", "error": f"Unexpected error: {str(e)}"}


if __name__ == "__main__":
    print("\n" + "="*70)
    print("🔍 LINKEDIN DM SENDER - FIXED VERSION")
    print("="*70)
    print("\nNow properly tracks:")
    print("  ✅ provider_id")
    print("  ✅ recipient name")
    print("  ✅ profile URL")
    print("  ✅ invitation status")
    print("\n" + "="*70)