import os
import requests
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import time
import re
import logging
from typing import Dict, Any, Tuple, Optional

# ------------------ SETUP LOGGING ------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------ LOAD ENV ------------------
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("❌ MONGO_URI missing in .env")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["cosmetics_app"]
clients_collection = db["clients"]

# ------------------ UNIPILE CREDENTIALS ------------------
def get_unipile_credentials() -> Tuple[str, str]:
    """
    Returns Unipile API token and DSN safely.
    
    Returns:
        tuple: (token, dsn)
    
    Raises:
        ValueError: If credentials are missing
    """
    token = os.getenv("UNIPILE_API_TOKEN")
    dsn = os.getenv("UNIPILE_DSN")

    if not token:
        raise ValueError("❌ Missing UNIPILE_API_TOKEN in .env")
    if not dsn:
        raise ValueError("❌ Missing UNIPILE_DSN in .env")
    
    # Ensure DSN has https://
    if not dsn.startswith(('http://', 'https://')):
        dsn = f"https://{dsn}"

    return token, dsn


# ------------------ GET LINKEDIN ACCOUNT (OAUTH ONLY) ------------------
def get_linkedin_account_id(client_id: str) -> Dict[str, Any]:
    """
    Get LinkedIn account_id from OAuth connection.
    
    ⚠️ This now ONLY works with OAuth flow, not cookies.
    User must connect via /pipeline/connect-linkedin/{client_id} first.
    
    Args:
        client_id: Client identifier
    
    Returns:
        dict with 'status', 'account_id', and optional 'error'
    """
    try:
        client = clients_collection.find_one({"client_id": client_id})
        if not client:
            return {
                "status": "error",
                "error": f"Client not found: {client_id}"
            }

        # Check for OAuth-connected LinkedIn account
        linkedin_data = client.get("linkedin", {})
        
        if not linkedin_data:
            return {
                "status": "error",
                "error": f"LinkedIn not connected for client {client_id}. "
                        f"Please connect at: /pipeline/connect-linkedin/{client_id}"
            }
        
        account_id = linkedin_data.get("account_id")
        is_active = linkedin_data.get("is_active", False)
        oauth_completed = linkedin_data.get("oauth_completed", False)
        
        if not account_id:
            return {
                "status": "error",
                "error": f"No LinkedIn account_id found. "
                        f"Please complete OAuth at: /pipeline/connect-linkedin/{client_id}"
            }
        
        if not is_active:
            return {
                "status": "error",
                "error": f"LinkedIn account is inactive for client {client_id}. "
                        f"Please reconnect at: /pipeline/connect-linkedin/{client_id}"
            }
        
        if not oauth_completed:
            return {
                "status": "error",
                "error": f"OAuth flow not completed. "
                        f"Please complete connection at: /pipeline/connect-linkedin/{client_id}"
            }
        
        # Verify account is still valid with Unipile
        try:
            token, dsn = get_unipile_credentials()
            headers = {
                "X-API-KEY": token,
                "accept": "application/json"
            }
            
            check_url = f"{dsn}/api/v1/accounts/{account_id}"
            response = requests.get(check_url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                return {
                    "status": "error",
                    "error": f"LinkedIn account verification failed (status {response.status_code}). "
                            f"Please reconnect at: /pipeline/connect-linkedin/{client_id}"
                }
            
            print(f"✅ Using LinkedIn account: {account_id}")
            return {
                "status": "success",
                "account_id": account_id
            }
            
        except requests.RequestException as e:
            return {
                "status": "error",
                "error": f"Failed to verify LinkedIn account: {str(e)}"
            }
            
    except Exception as e:
        logger.error(f"Error getting LinkedIn account: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": f"Database error: {str(e)}"
        }


# ------------------ EXTRACT LINKEDIN IDENTIFIER ------------------
def extract_linkedin_public_identifier(profile_url: str) -> Dict[str, Any]:
    """
    Extract LinkedIn public identifier from profile URL.
    
    Supports multiple formats:
    - https://www.linkedin.com/in/john-doe-123456 → john-doe-123456
    - https://www.linkedin.com/sales/lead/ACwAAD... → ACwAAD...
    - https://www.linkedin.com/in/ACwAAAi2RPMBVfsioBgWbqxbdifADUvM3f44igE
    
    Args:
        profile_url: LinkedIn profile URL
    
    Returns:
        dict with 'status', 'identifier', 'is_sales_nav', and optional 'error'
    """
    try:
        if not profile_url:
            return {
                "status": "error",
                "error": "Profile URL is empty"
            }
        
        profile_url = profile_url.strip().rstrip('/')
        
        # Remove any trailing parameters after comma (Sales Navigator format)
        if ',' in profile_url:
            profile_url = profile_url.split(',')[0]
        
        # Try Sales Navigator format first: /sales/lead/{identifier}
        match = re.search(r'/sales/lead/([^/?]+)', profile_url)
        if match:
            identifier = match.group(1)
            print(f"🔑 Sales Navigator ID: {identifier}")
            return {
                "status": "success",
                "identifier": identifier,
                "is_sales_nav": True
            }
        
        # Try regular profile URL format: /in/{identifier}
        match = re.search(r'/in/([^/?]+)', profile_url)
        if match:
            identifier = match.group(1)
            print(f"🔑 Public identifier: {identifier}")
            return {
                "status": "success",
                "identifier": identifier,
                "is_sales_nav": False
            }
        
        return {
            "status": "error",
            "error": f"Invalid LinkedIn URL format: {profile_url}"
        }
        
    except Exception as e:
        logger.error(f"Error parsing LinkedIn URL: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": f"URL parsing error: {str(e)}"
        }


def get_provider_id_from_profile(client_id: str, identifier: str, is_sales_nav: bool = False) -> Dict[str, Any]:
    """
    Convert LinkedIn identifier to provider_id using Unipile API.
    
    Args:
        client_id: Client identifier
        identifier: Either public_identifier (john-doe-123) or provider_id (ACwAAD...)
        is_sales_nav: True if identifier is already a provider_id from Sales Navigator
    
    Returns:
        dict with 'status', 'provider_id', profile info, and optional 'error'
    """
    try:
        # If it's already a Sales Navigator provider_id, use it directly
        if is_sales_nav or identifier.startswith('ACwAA'):
            print(f"✅ Using Sales Navigator provider_id: {identifier}")
            return {
                "status": "success",
                "provider_id": identifier,
                "first_name": "",
                "last_name": "",
                "public_identifier": identifier
            }
        
        # Get account_id first
        account_result = get_linkedin_account_id(client_id)
        if account_result["status"] != "success":
            return {
                "status": "error",
                "error": account_result.get("error", "Failed to get account_id")
            }
        
        account_id = account_result["account_id"]
        
        # Get credentials
        token, dsn = get_unipile_credentials()
        
        headers = {
            "X-API-KEY": token,
            "accept": "application/json"
        }
        
        # Get user profile to extract provider_id
        url = f"{dsn}/api/v1/users/{identifier}"
        params = {"account_id": account_id}
        
        print(f"🔍 Fetching provider_id for: {identifier}")
        
        # Retry logic with exponential backoff
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"   Attempt {attempt + 1}/{max_retries}...")
                response = requests.get(url, headers=headers, params=params, timeout=60)
                data = response.json()
                
                if response.status_code == 200:
                    provider_id = data.get("provider_id")
                    if not provider_id:
                        return {
                            "status": "error",
                            "error": "No provider_id in API response"
                        }
                    
                    print(f"✅ Provider ID: {provider_id}")
                    
                    return {
                        "status": "success",
                        "provider_id": provider_id,
                        "first_name": data.get("first_name", ""),
                        "last_name": data.get("last_name", ""),
                        "public_identifier": data.get("public_identifier", identifier)
                    }
                else:
                    error_msg = data.get("message", "Unknown error")
                    return {
                        "status": "error",
                        "error": f"Profile lookup failed: {error_msg}"
                    }
                    
            except requests.Timeout:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    print(f"⏱️ Timeout, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    return {
                        "status": "error",
                        "error": f"Profile fetch timed out after {max_retries} attempts"
                    }
                    
            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    print(f"🔌 Connection error, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    return {
                        "status": "error",
                        "error": f"Network error: {str(e)}"
                    }
                    
    except Exception as e:
        logger.error(f"Error getting provider_id: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": f"Unexpected error: {str(e)}"
        }


# ------------------ SEND CONNECTION INVITATION ------------------
def send_linkedin_invitation(client_id: str, provider_id: str, message: str) -> Dict[str, Any]:
    """
    Send LinkedIn connection invitation with message.
    LinkedIn has a 300-character limit for invitation messages.
    
    Args:
        client_id: Client identifier
        provider_id: LinkedIn provider ID
        message: Invitation message
    
    Returns:
        dict with 'status', 'method', and optional 'error'
    """
    try:
        # Get account_id
        account_result = get_linkedin_account_id(client_id)
        if account_result["status"] != "success":
            return {
                "status": "error",
                "method": "invitation",
                "error": account_result.get("error", "Failed to get account_id")
            }
        
        account_id = account_result["account_id"]
        
        # Get credentials
        token, dsn = get_unipile_credentials()
        
        headers = {
            "X-API-KEY": token,
            "accept": "application/json",
            "Content-Type": "application/json"
        }
        
        # LinkedIn enforces 300 character limit
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
        
        print(f"📤 Sending connection invitation...")
        print(f"   Message: {len(message)} chars")
        
        response = requests.post(url, json=payload, headers=headers, timeout=15)
        data = response.json()
        
        if response.status_code in [200, 201]:
            print(f"✅ Invitation sent!")
            return {
                "status": "success",
                "method": "invitation",
                "provider_id": provider_id,
                "details": data
            }
        else:
            error_msg = data.get("message") or data.get("error", "Unknown error")
            print(f"❌ Invitation failed: {error_msg}")
            return {
                "status": "error",
                "method": "invitation",
                "error": f"Status {response.status_code}: {error_msg}",
                "details": data
            }
            
    except requests.RequestException as e:
        error_msg = f"Network error: {str(e)}"
        print(f"❌ {error_msg}")
        return {
            "status": "error",
            "method": "invitation",
            "error": error_msg
        }
    except Exception as e:
        logger.error(f"Error sending invitation: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "method": "invitation",
            "error": f"Unexpected error: {str(e)}"
        }


# ------------------ SEND DIRECT MESSAGE ------------------
def send_linkedin_direct_message(client_id: str, provider_id: str, message: str) -> Dict[str, Any]:
    """
    Send direct message to already-connected LinkedIn user.
    Only works for 1st degree connections.
    
    Args:
        client_id: Client identifier
        provider_id: LinkedIn provider ID
        message: Message text
    
    Returns:
        dict with 'status', 'method', and optional 'error'
    """
    try:
        # Get account_id
        account_result = get_linkedin_account_id(client_id)
        if account_result["status"] != "success":
            return {
                "status": "error",
                "method": "direct_message",
                "error": account_result.get("error", "Failed to get account_id")
            }
        
        account_id = account_result["account_id"]
        
        # Get credentials
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
        data = response.json()
        
        if response.status_code in [200, 201]:
            print(f"✅ Direct message sent!")
            return {
                "status": "success",
                "method": "direct_message",
                "provider_id": provider_id,
                "chat_id": data.get("chat_id"),
                "details": data
            }
        else:
            error_msg = data.get("message") or data.get("error", "Unknown error")
            print(f"❌ Direct message failed: {error_msg}")
            return {
                "status": "error",
                "method": "direct_message",
                "error": f"Status {response.status_code}: {error_msg}",
                "status_code": response.status_code,
                "details": data
            }
            
    except requests.RequestException as e:
        error_msg = f"Network error: {str(e)}"
        print(f"❌ {error_msg}")
        return {
            "status": "error",
            "method": "direct_message",
            "error": error_msg
        }
    except Exception as e:
        logger.error(f"Error sending direct message: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "method": "direct_message",
            "error": f"Unexpected error: {str(e)}"
        }


# ------------------ SMART SEND (TRIES DIRECT MESSAGE, FALLS BACK TO INVITATION) ------------------
def send_linkedin_dm_smart(client_id: str, profile_url: str, message: str) -> Dict[str, Any]:
    """
    Smart LinkedIn messaging:
    1. Try to send direct message (if already connected)
    2. If that fails (422 - not connected), send invitation instead
    
    This is the recommended approach.
    
    Args:
        client_id: Client identifier
        profile_url: LinkedIn profile URL
        message: Message text to send
    
    Returns:
        dict with 'status', 'method', and optional 'error'
    """
    print(f"\n{'='*60}")
    print(f"📤 Smart LinkedIn messaging")
    print(f"   Profile: {profile_url}")
    print(f"{'='*60}\n")
    
    try:
        # Step 1: Extract identifier and determine type
        identifier_result = extract_linkedin_public_identifier(profile_url)
        
        if identifier_result["status"] != "success":
            return {
                "status": "error",
                "error": identifier_result.get("error", "Failed to parse URL"),
                "step": "url_parsing"
            }
        
        identifier = identifier_result["identifier"]
        is_sales_nav = identifier_result["is_sales_nav"]
        
        # Step 2: Get provider_id
        profile_result = get_provider_id_from_profile(client_id, identifier, is_sales_nav)
        
        if profile_result["status"] != "success":
            return {
                "status": "error",
                "error": profile_result.get("error", "Failed to get provider_id"),
                "step": "profile_lookup"
            }
        
        provider_id = profile_result["provider_id"]
        
        # Step 3: Try direct message first
        print("🔄 Attempting direct message (for connected users)...")
        dm_result = send_linkedin_direct_message(client_id, provider_id, message)
        
        if dm_result["status"] == "success":
            return dm_result
        
        # Step 4: If direct message failed with 422 (not connected), try invitation
        error_str = str(dm_result.get("error", ""))
        status_code = dm_result.get("status_code", 0)
        
        if status_code == 422 or "422" in error_str or "cannot be reached" in error_str.lower():
            print("⚠️ Not connected - sending invitation instead...")
            invitation_result = send_linkedin_invitation(client_id, provider_id, message)
            return invitation_result
        
        # Other error - return the direct message error
        return dm_result
        
    except Exception as e:
        logger.error(f"Error in smart send: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": f"Unexpected error: {str(e)}",
            "step": "exception"
        }


# ------------------ HIGH-LEVEL WRAPPER FOR DM_SERVICE.PY ------------------
def send_linkedin_dm(recipient_url: str, message: str, client_id: str) -> Dict[str, Any]:
    """
    Main wrapper function for dm_service.py
    Uses smart sending (tries direct message, falls back to invitation)
    
    ⭐ GUARANTEED to return a dict with 'status' key
    
    Args:
        recipient_url: LinkedIn profile URL
        message: Message text to send
        client_id: Client identifier
    
    Returns:
        dict with keys:
        - status: "success" or "error"
        - method: "direct_message" or "invitation" (if success)
        - error: Error message (if failed)
        - Additional details depending on method
    """
    print(f"\n{'='*60}")
    print(f"📤 Sending LinkedIn Message")
    print(f"   Recipient: {recipient_url}")
    print(f"   Client: {client_id}")
    print(f"{'='*60}\n")
    
    try:
        result = send_linkedin_dm_smart(client_id, recipient_url, message)
        
        # ⭐ CRITICAL: Ensure result is always a dict
        if not isinstance(result, dict):
            logger.error(f"send_linkedin_dm_smart returned non-dict: {type(result)}")
            return {
                "status": "error",
                "error": f"Internal error: Invalid return type {type(result)}"
            }
        
        # ⭐ CRITICAL: Ensure status key exists
        if "status" not in result:
            logger.error(f"send_linkedin_dm_smart returned dict without status: {result}")
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
        logger.error(f"Unexpected exception in send_linkedin_dm: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": f"Unexpected error: {str(e)}",
            "exception_type": type(e).__name__
        }


# ------------------ VERIFY LINKEDIN CONNECTION ------------------
def verify_linkedin_connection(client_id: str) -> Dict[str, Any]:
    """
    Verify that LinkedIn account is properly connected via OAuth.
    
    Args:
        client_id: Client identifier
    
    Returns:
        dict with connection status and details
    """
    try:
        account_result = get_linkedin_account_id(client_id)
        
        if account_result["status"] != "success":
            return {
                "status": "error",
                "error": account_result.get("error", "Failed to get account_id"),
                "oauth_required": True
            }
        
        account_id = account_result["account_id"]
        
        token, dsn = get_unipile_credentials()
        headers = {
            "X-API-KEY": token,
            "accept": "application/json"
        }
        
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
        return {
            "status": "error",
            "error": f"Unexpected error: {str(e)}"
        }


# ------------------ TESTING FUNCTIONS ------------------
def test_url_parsing():
    """Test URL parsing with various formats"""
    test_urls = [
        "https://www.linkedin.com/in/johnsmith/",
        "https://linkedin.com/in/ACwAAAi2RPMBVfsioBgWbqxbdifADUvM3f44igE",
        "linkedin.com/in/someone123",
        "www.linkedin.com/in/test",
        "https://www.linkedin.com/sales/lead/ACwAAD123456",
        "https://linkedin.com/in/",  # Invalid
        "not-a-url",  # Invalid
    ]
    
    print("\n" + "="*60)
    print("🧪 TESTING URL PARSING")
    print("="*60)
    
    for url in test_urls:
        result = extract_linkedin_public_identifier(url)
        status_icon = "✅" if result["status"] == "success" else "❌"
        
        print(f"\n{status_icon} URL: {url}")
        if result["status"] == "success":
            print(f"   Identifier: {result['identifier']}")
            print(f"   Sales Nav: {result['is_sales_nav']}")
        else:
            print(f"   Error: {result['error']}")


if __name__ == "__main__":
    # Run tests
    test_url_parsing()