# fb_dm_sender.py
"""
Facebook DM Sender via Ayrshare API
Handles Facebook account connection and message sending through Ayrshare
"""

import os
import requests
import logging
from typing import Dict, Optional
from dotenv import load_dotenv
from db_config import clients_collection

load_dotenv()

# ============================================================
# CONFIGURATION
# ============================================================

AYRSHARE_API_KEY = os.getenv("AYRSHARE_API_KEY")
AYRSHARE_BASE_URL = "https://app.ayrshare.com/api"

if not AYRSHARE_API_KEY:
    raise ValueError("❌ Missing AYRSHARE_API_KEY in .env file")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================
# AYRSHARE API HELPER FUNCTIONS
# ============================================================

def get_ayrshare_headers(profile_key: Optional[str] = None) -> Dict:
    """
    Get headers for Ayrshare API requests.
    
    Args:
        profile_key: Optional profile key for multi-profile accounts
    
    Returns:
        Headers dictionary
    """
    headers = {
        "Authorization": f"Bearer {AYRSHARE_API_KEY}",
        "Content-Type": "application/json"
    }
    
    if profile_key:
        headers["Profile-Key"] = profile_key
    
    return headers


def get_jwt_url(redirect_url: str, private_key: Optional[str] = None) -> Dict:
    """
    Generate JWT URL for connecting Facebook account.
    
    ⚠️ REQUIRES AYRSHARE BUSINESS PLAN
    
    This JWT URL is used to connect a Facebook account to Ayrshare.
    User clicks this URL → Logs into Facebook → Account gets connected
    
    Args:
        redirect_url: URL to redirect after successful connection
        private_key: Optional private key for specific profile
    
    Returns:
        Dictionary with JWT URL and details
    
    Docs: https://www.ayrshare.com/docs/dashboard/connect-social-accounts/facebook
    """
    try:
        endpoint = f"{AYRSHARE_BASE_URL}/profiles/generateJWT"
        
        payload = {
            "domain": redirect_url,
            "platforms": ["facebook"]  # Only Facebook for now
        }
        
        if private_key:
            payload["privateKey"] = private_key
        
        response = requests.post(
            endpoint,
            json=payload,
            headers=get_ayrshare_headers(),
            timeout=15
        )
        
        if response.status_code in [200, 201]:
            result = response.json()
            
            return {
                "status": "success",
                "jwt_url": result.get("url"),
                "redirect_url": redirect_url,
                "platforms": ["facebook"],
                "expires_in": "1 hour",
                "instructions": [
                    "1. Open the jwt_url in a browser",
                    "2. Click 'Connect Facebook'",
                    "3. Log in with Facebook credentials",
                    "4. Grant permissions",
                    "5. You'll be redirected back to redirect_url"
                ]
            }
        else:
            error_data = response.json() if response.content else {}
            error_msg = error_data.get("message", "Failed to generate JWT URL")
            
            # Check for Business Plan requirement
            if "Business Plan" in error_msg or response.status_code == 403:
                logger.error(f"⚠️ Business Plan required for JWT generation")
                return {
                    "status": "error",
                    "error": "Ayrshare Business Plan required for automatic OAuth",
                    "error_type": "business_plan_required",
                    "status_code": response.status_code,
                    "alternative": "Use manual connection method instead"
                }
            
            logger.error(f"Ayrshare JWT generation failed: {response.status_code} - {error_msg}")
            
            return {
                "status": "error",
                "error": error_msg,
                "status_code": response.status_code
            }
    
    except requests.RequestException as e:
        logger.error(f"Network error generating JWT: {str(e)}")
        return {
            "status": "error",
            "error": f"Network error: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Unexpected error generating JWT: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }


def get_manual_connection_instructions() -> Dict:
    """
    Get instructions for manually connecting Facebook via Ayrshare dashboard.
    
    This is the FREE alternative that works without Business Plan.
    
    Returns:
        Dictionary with manual connection instructions
    """
    return {
        "status": "manual_required",
        "method": "ayrshare_dashboard",
        "cost": "Free (no Business Plan required)",
        "instructions": [
            "1. Go to Ayrshare Dashboard: https://app.ayrshare.com",
            "2. Click 'Social Accounts' in sidebar",
            "3. Click 'Add Account' button",
            "4. Select 'Facebook' from platform options",
            "5. Click 'Connect Facebook'",
            "6. Log in with Facebook credentials",
            "7. Grant permissions (pages_messaging, pages_manage_metadata)",
            "8. Your Facebook account is now connected!",
            "9. Return here and check status with GET /facebook-status/{client_id}"
        ],
        "dashboard_url": "https://app.ayrshare.com",
        "note": "After connecting via dashboard, your API key will work for sending messages",
        "why_manual": "JWT generation requires Ayrshare Business Plan. Manual connection is free and works the same way.",
        "next_step": "Once connected, use POST /facebook/send-messages to send DMs"
    }


def get_connected_profiles(profile_key: Optional[str] = None) -> Dict:
    """
    Get list of connected social media profiles.
    
    Returns information about all connected accounts including Facebook.
    
    Args:
        profile_key: Optional profile key for specific profile
    
    Returns:
        Dictionary with connected profiles
    """
    try:
        endpoint = f"{AYRSHARE_BASE_URL}/profiles"
        
        response = requests.get(
            endpoint,
            headers=get_ayrshare_headers(profile_key),
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            
            # Filter for Facebook profiles only
            facebook_profiles = [
                profile for profile in result.get("profiles", [])
                if profile.get("platform") == "facebook"
            ]
            
            return {
                "status": "success",
                "facebook_profiles": facebook_profiles,
                "total_profiles": len(result.get("profiles", [])),
                "all_profiles": result.get("profiles", [])
            }
        else:
            error_msg = response.json().get("message", "Failed to fetch profiles")
            return {
                "status": "error",
                "error": error_msg
            }
    
    except Exception as e:
        logger.error(f"Error fetching profiles: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }


def send_facebook_dm(
    recipient_id: str,
    message: str,
    profile_key: Optional[str] = None
) -> Dict:
    """
    Send a direct message via Facebook using Ayrshare.
    
    Args:
        recipient_id: Facebook user ID or page ID to send message to
        message: Message text to send
        profile_key: Optional profile key for multi-profile accounts
    
    Returns:
        Dictionary with send result
    
    Docs: https://www.ayrshare.com/docs/apis/messages/send-message
    """
    try:
        endpoint = f"{AYRSHARE_BASE_URL}/send-message"
        
        payload = {
            "platforms": ["facebook"],
            "recipientId": recipient_id,
            "message": message
        }
        
        logger.info(f"📤 Sending Facebook DM to {recipient_id}")
        logger.info(f"Message: {message[:100]}...")
        
        response = requests.post(
            endpoint,
            json=payload,
            headers=get_ayrshare_headers(profile_key),
            timeout=20
        )
        
        logger.info(f"Response status: {response.status_code}")
        logger.info(f"Response body: {response.text}")
        
        if response.status_code in [200, 201]:
            result = response.json()
            
            # Check if message was successfully sent
            if result.get("status") == "success":
                return {
                    "status": "success",
                    "method": "ayrshare_api",
                    "platform": "facebook",
                    "recipient_id": recipient_id,
                    "message_id": result.get("id"),
                    "response": result
                }
            else:
                return {
                    "status": "error",
                    "error": result.get("errors", ["Unknown error"])[0] if result.get("errors") else "Send failed",
                    "response": result
                }
        else:
            error_data = response.json()
            error_msg = error_data.get("message") or error_data.get("errors", ["Unknown error"])[0]
            
            logger.error(f"Ayrshare send failed: {response.status_code} - {error_msg}")
            
            return {
                "status": "error",
                "error": error_msg,
                "status_code": response.status_code,
                "response": error_data
            }
    
    except requests.RequestException as e:
        logger.error(f"Network error sending Facebook DM: {str(e)}")
        return {
            "status": "error",
            "error": f"Network error: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Unexpected error sending Facebook DM: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }


# ============================================================
# DATABASE INTEGRATION
# ============================================================

def save_facebook_connection(client_id: str, profile_data: Dict) -> bool:
    """
    Save Facebook connection details to database.
    
    Args:
        client_id: Client ID
        profile_data: Profile data from Ayrshare
    
    Returns:
        True if successful, False otherwise
    """
    try:
        clients_collection.update_one(
            {"client_id": client_id},
            {
                "$set": {
                    "facebook_connection": {
                        "profile_key": profile_data.get("profileKey"),
                        "profile_id": profile_data.get("id"),
                        "name": profile_data.get("name"),
                        "platform": "facebook",
                        "is_active": True,
                        "connected_at": profile_data.get("connectedAt"),
                        "provider": "ayrshare"
                    }
                }
            }
        )
        
        logger.info(f"✅ Saved Facebook connection for client {client_id}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to save Facebook connection: {str(e)}")
        return False


def get_facebook_connection(client_id: str) -> Optional[Dict]:
    """
    Get Facebook connection details from database.
    
    Args:
        client_id: Client ID
    
    Returns:
        Facebook connection data or None
    """
    try:
        client = clients_collection.find_one({"client_id": client_id})
        
        if not client:
            return None
        
        return client.get("facebook_connection")
    
    except Exception as e:
        logger.error(f"Failed to get Facebook connection: {str(e)}")
        return None


# ============================================================
# HIGH-LEVEL FUNCTIONS FOR ROUTES
# ============================================================

def initiate_facebook_connection(client_id: str, redirect_url: str) -> Dict:
    """
    HIGH-LEVEL: Initiate Facebook account connection for a client.
    
    ⚠️ NOTE: JWT generation requires Ayrshare Business Plan.
    For free/standard plans, use manual dashboard connection instead.
    
    Args:
        client_id: Client ID
        redirect_url: URL to redirect after successful connection
    
    Returns:
        Dictionary with JWT URL or manual instructions
    """
    from services.db_service import get_client_data
    
    # Verify client exists
    client = get_client_data(client_id)
    if not client:
        return {
            "status": "error",
            "error": "Client not found"
        }
    
    # Try to generate JWT URL (requires Business Plan)
    result = get_jwt_url(redirect_url)
    
    if result["status"] == "success":
        logger.info(f"✅ Generated Facebook connection URL for client {client_id}")
        return result
    
    # If Business Plan error, return manual instructions
    if result.get("error_type") == "business_plan_required":
        logger.info(f"💡 Returning manual connection instructions for client {client_id}")
        manual_instructions = get_manual_connection_instructions()
        manual_instructions["client_id"] = client_id
        return manual_instructions
    
    # Other error
    return result


def verify_facebook_connection(client_id: str) -> Dict:
    """
    HIGH-LEVEL: Verify if Facebook account is connected and active.
    
    Args:
        client_id: Client ID
    
    Returns:
        Dictionary with connection status
    """
    from services.db_service import get_client_data
    
    # Check database
    client = get_client_data(client_id)
    if not client:
        return {
            "status": "error",
            "error": "Client not found"
        }
    
    fb_connection = client.get("facebook_connection", {})
    
    if not fb_connection or not fb_connection.get("is_active"):
        return {
            "connected": False,
            "status": "not_connected",
            "message": "Facebook not connected"
        }
    
    # Verify with Ayrshare API
    profile_key = fb_connection.get("profile_key")
    profiles_result = get_connected_profiles(profile_key)
    
    if profiles_result["status"] == "error":
        return {
            "connected": True,
            "status": "verification_failed",
            "message": "Could not verify with Ayrshare",
            "error": profiles_result.get("error")
        }
    
    # Check if Facebook is still connected
    facebook_profiles = profiles_result.get("facebook_profiles", [])
    
    if not facebook_profiles:
        return {
            "connected": False,
            "status": "disconnected",
            "message": "Facebook account disconnected from Ayrshare"
        }
    
    return {
        "connected": True,
        "status": "active",
        "message": "✅ Facebook connected and active",
        "profile_name": fb_connection.get("name"),
        "profile_id": fb_connection.get("profile_id"),
        "connected_at": fb_connection.get("connected_at"),
        "facebook_profiles": facebook_profiles,
        "ready_to_send": True
    }


def send_facebook_message_to_prospect(
    client_id: str,
    recipient_id: str,
    message: str
) -> Dict:
    """
    HIGH-LEVEL: Send Facebook message to a prospect.
    
    This is the main function to call from routes.py
    
    Args:
        client_id: Client ID
        recipient_id: Facebook user/page ID
        message: Message text
    
    Returns:
        Dictionary with send result
    """
    # Get Facebook connection
    fb_connection = get_facebook_connection(client_id)
    
    if not fb_connection or not fb_connection.get("is_active"):
        return {
            "status": "error",
            "error": "Facebook not connected. Connect Facebook account first."
        }
    
    profile_key = fb_connection.get("profile_key")
    
    # Send message
    result = send_facebook_dm(
        recipient_id=recipient_id,
        message=message,
        profile_key=profile_key
    )
    
    return result


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def extract_facebook_id_from_url(url: str) -> Optional[str]:
    """
    Extract Facebook user/page ID from profile URL.
    
    Facebook URLs can be:
    - https://www.facebook.com/username
    - https://www.facebook.com/profile.php?id=123456789
    - https://www.facebook.com/pages/PageName/123456789
    
    Args:
        url: Facebook profile URL
    
    Returns:
        Facebook ID or username, or None if extraction fails
    """
    import re
    
    # Remove trailing slash
    url = url.rstrip("/")
    
    # Pattern 1: /profile.php?id=123456789
    match = re.search(r'profile\.php\?id=(\d+)', url)
    if match:
        return match.group(1)
    
    # Pattern 2: /pages/PageName/123456789
    match = re.search(r'/pages/[^/]+/(\d+)', url)
    if match:
        return match.group(1)
    
    # Pattern 3: /username
    match = re.search(r'facebook\.com/([^/?]+)', url)
    if match:
        username = match.group(1)
        # Exclude common paths
        if username not in ['pages', 'profile.php', 'groups']:
            return username
    
    return None


# ============================================================
# TESTING FUNCTIONS
# ============================================================

def test_ayrshare_connection():
    """
    Test Ayrshare API connection and configuration.
    """
    print(f"\n{'='*70}")
    print("🧪 TESTING AYRSHARE CONNECTION")
    print(f"{'='*70}\n")
    
    print(f"API Key: {AYRSHARE_API_KEY[:10]}...{AYRSHARE_API_KEY[-4:]}")
    print(f"Base URL: {AYRSHARE_BASE_URL}")
    
    # Test fetching profiles
    print("\n📡 Fetching connected profiles...")
    result = get_connected_profiles()
    
    if result["status"] == "success":
        print(f"✅ Successfully connected to Ayrshare")
        print(f"Total profiles: {result['total_profiles']}")
        print(f"Facebook profiles: {len(result['facebook_profiles'])}")
        
        for profile in result['facebook_profiles']:
            print(f"\n  📱 {profile.get('name')}")
            print(f"     Platform: {profile.get('platform')}")
            print(f"     Connected: {profile.get('connectedAt')}")
    else:
        print(f"❌ Connection failed: {result.get('error')}")
    
    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    # Run tests when file is executed directly
    test_ayrshare_connection()