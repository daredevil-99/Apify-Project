"""
Instagram DM Sender via Unipile API - Integrated with existing workflow
Sends pre-generated messages to Instagram prospects
Updated to use provider_messaging_id for Instagram platform
"""
import os
import logging
import requests
from typing import Dict, Optional

# Configuration
UNIPILE_API_TOKEN = os.getenv("UNIPILE_API_TOKEN")
UNIPILE_DSN = os.getenv("UNIPILE_DSN")

# Ensure UNIPILE_DSN has https:// prefix
if UNIPILE_DSN and not UNIPILE_DSN.startswith(('http://', 'https://')):
    UNIPILE_DSN = f"https://{UNIPILE_DSN}"

logger = logging.getLogger(__name__)


def send_instagram_dm_via_unipile(
    username: str,
    message: str,
    account_id: str
) -> Dict:
    """
    Send Instagram DM using Unipile's chat API with pre-generated message.
    
    Flow:
    1. Get provider_messaging_id from username (identify user)
    2. Send message to new chat using account_id and provider_messaging_id
    
    Args:
        username: Instagram username (without @)
        message: Pre-generated message text
        account_id: Unipile account ID (from Instagram connection)
    
    Returns:
        dict: {
            "status": "success" | "error",
            "message_id": str (if success),
            "provider_messaging_id": str,
            "error": str (if error)
        }
    """
    try:
        # Step 1: Get provider_messaging_id from username
        logger.info(f"Step 1: Getting provider_messaging_id for @{username}")
        provider_messaging_id = get_instagram_provider_messaging_id(username, account_id)
        
        if not provider_messaging_id:
            return {
                "status": "error",
                "error": f"Failed to find Instagram user @{username}. User may not exist or profile is private."
            }
        
        logger.info(f"✅ Resolved provider_messaging_id: {provider_messaging_id}")
        
        # Step 2: Send message to new chat
        logger.info(f"Step 2: Sending message to @{username}")
        result = _send_chat_message(
            account_id=account_id,
            provider_messaging_id=provider_messaging_id,
            message=message,
            username=username
        )
        
        return result
    
    except Exception as e:
        logger.error(f"❌ Unexpected error sending DM to @{username}: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": f"Unexpected error: {str(e)}"
        }


def get_instagram_provider_messaging_id(username: str, account_id: str) -> Optional[str]:
    """
    Get Instagram provider_messaging_id from username using Unipile API.
    
    For Instagram, provider_messaging_id is required for messaging functionality.
    
    Endpoint: GET /api/v1/users/{username}?account_id={account_id}
    
    Args:
        username: Instagram username (without @)
        account_id: Unipile account ID
    
    Returns:
        str: Provider messaging ID or None if failed
    """
    try:
        headers = {
            "X-API-KEY": UNIPILE_API_TOKEN,
            "accept": "application/json"
        }
        
        url = f"{UNIPILE_DSN}/api/v1/users/{username}"
        params = {"account_id": account_id}
        
        logger.info(f"Getting provider_messaging_id for @{username}")
        logger.debug(f"URL: {url}, Params: {params}")
        
        response = requests.get(url, headers=headers, params=params, timeout=10)
        
        logger.info(f"Response status: {response.status_code}")
        logger.debug(f"Response body: {response.text}")
        
        if response.status_code == 200:
            user_data = response.json()
            provider_messaging_id = user_data.get("provider_messaging_id")
            
            if provider_messaging_id:
                logger.info(f"✅ Found provider_messaging_id for @{username}: {provider_messaging_id}")
                return provider_messaging_id
            else:
                logger.error(f"No provider_messaging_id in response for @{username}")
                logger.debug(f"Available fields: {list(user_data.keys())}")
                return None
        else:
            logger.error(f"Failed to get provider_messaging_id: {response.status_code} - {response.text}")
            return None
    
    except Exception as e:
        logger.error(f"Error getting Instagram provider_messaging_id: {str(e)}")
        return None


def _send_chat_message(
    account_id: str,
    provider_messaging_id: str,
    message: str,
    username: str
) -> Dict:
    """
    Send message to new Instagram chat using Unipile API.
    
    Uses provider_messaging_id for Instagram platform compatibility.
    
    Endpoint: POST /api/v1/chats
    
    Args:
        account_id: Unipile account ID
        provider_messaging_id: Instagram provider messaging ID (from get_instagram_provider_messaging_id)
        message: Pre-generated message text
        username: Instagram username (for logging)
    
    Returns:
        dict: Result with status, message_id, or error
    """
    try:
        url = f"{UNIPILE_DSN}/api/v1/chats"
        
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "X-API-KEY": UNIPILE_API_TOKEN
        }
        
        payload = {
            "account_id": account_id,
            "text": message,
            "attendees_ids": [provider_messaging_id]
        }
        
        logger.info(f"Sending DM to @{username}")
        logger.debug(f"Payload: {payload}")
        
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        
        logger.info(f"DM response status: {response.status_code}")
        logger.debug(f"DM response body: {response.text}")
        
        if response.status_code in [200, 201]:
            result = response.json()
            
            # Extract message_id from various possible structures
            message_id = None
            if "object" in result:
                if isinstance(result["object"], dict):
                    message_id = result["object"].get("id")
                elif isinstance(result["object"], str):
                    message_id = result["object"]
            
            if not message_id:
                message_id = result.get("id", "unknown")
            
            logger.info(f"✅ DM sent successfully to @{username}: message_id={message_id}")
            
            return {
                "status": "success",
                "message_id": message_id,
                "provider_messaging_id": provider_messaging_id,
                "recipient": username,
                "provider": "INSTAGRAM"
            }
        else:
            try:
                error_data = response.json()
                error_msg = error_data.get("message", error_data.get("error", "Unknown error"))
                error_detail = error_data.get("detail", "")
            except:
                error_msg = response.text or "Failed to send message"
                error_detail = ""
            
            logger.error(f"❌ DM failed: {response.status_code} - {error_msg}")
            
            return {
                "status": "error",
                "error": error_msg,
                "status_code": response.status_code,
                "detail": error_detail if error_detail else None
            }
    
    except requests.RequestException as e:
        logger.error(f"❌ Network error sending DM: {str(e)}")
        return {
            "status": "error",
            "error": f"Network error: {str(e)}"
        }
    
    except Exception as e:
        logger.error(f"❌ Unexpected error sending DM: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": f"Unexpected error: {str(e)}"
        }


def extract_username_from_url(profile_url_or_username: str) -> str:
    """
    Extract Instagram username from URL or return username as-is.
    
    Args:
        profile_url_or_username: Instagram URL or username
    
    Returns:
        str: Clean username without @ or URL parts
    
    Examples:
        "https://www.instagram.com/johndoe/" → "johndoe"
        "instagram.com/janedoe/" → "janedoe"
        "@username" → "username"
        "username" → "username"
    """
    url = profile_url_or_username.strip()
    
    # Remove @ if present
    if url.startswith("@"):
        url = url[1:]
    
    # Handle Instagram URLs
    if "instagram.com" in url.lower():
        parts = url.rstrip("/").split("/")
        
        # Get the last non-empty part
        for part in reversed(parts):
            if part and part not in ["www.instagram.com", "instagram.com"]:
                username = part.strip()
                # Remove any query parameters
                if "?" in username:
                    username = username.split("?")[0]
                return username
    
    # Return as-is (already a username)
    return url.strip()


# Example usage for integration with your pipeline
if __name__ == "__main__":
    # Example: Send a pre-generated message
    username = "target_username"
    generated_message = "Hey! Your content on sustainable fashion is amazing. I'm building a network of eco-conscious creators and would love to connect!"
    account_id = "your_instagram_account_id"  # From Instagram connection
    
    result = send_instagram_dm_via_unipile(
        username=username,
        message=generated_message,
        account_id=account_id
    )
    
    if result["status"] == "success":
        print(f"✅ Message sent successfully!")
        print(f"Message ID: {result['message_id']}")
        print(f"Provider Messaging ID: {result['provider_messaging_id']}")
    else:
        print(f"❌ Failed to send message: {result['error']}")