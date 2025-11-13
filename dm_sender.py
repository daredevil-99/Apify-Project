# dm_sender.py - Multi-Platform DM Automation Module

import os
import time
import random
from typing import Dict, List, Optional
from datetime import datetime
import pymongo
from dotenv import load_dotenv

load_dotenv()

# MongoDB Setup
MONGO_URI = os.getenv("MONGO_URI")
client = pymongo.MongoClient(MONGO_URI)
db = client["cosmetics_app"]
dm_logs_collection = db["dm_logs"]
clients_collection = db["clients"]

# ==========================================
# OPTION 1: Using Apify Actors (Recommended)
# ==========================================

from apify_client import ApifyClient

class ApifyDMSender:
    """Send DMs using Apify's messaging actors"""
    
    def __init__(self):
        self.apify_client = ApifyClient(os.getenv("APIFY_API_TOKEN"))
    
    def send_instagram_dm(self, username: str, message: str) -> Dict:
        """
        Send Instagram DM using Apify's Instagram DM sender
        Actor: bhansalisoft/instagram-bulk-message-sender or similar
        """
        try:
            actor_id = "bhansalisoft/instagram-bulk-message-sender"  # Check Apify store for exact actor
            
            run_input = {
                "username": username,
                "message": message,
                "proxy": {
                    "useApifyProxy": True,
                    "apifyProxyGroups": ["RESIDENTIAL"]
                }
            }
            
            print(f"📤 Sending Instagram DM to @{username}")
            run = self.apify_client.actor(actor_id).call(run_input=run_input)
            
            return {
                "success": True,
                "platform": "instagram",
                "username": username,
                "run_id": run.get("id"),
                "status": "sent"
            }
            
        except Exception as e:
            print(f"❌ Instagram DM failed: {e}")
            return {
                "success": False,
                "platform": "instagram",
                "username": username,
                "error": str(e)
            }
    
    def send_linkedin_message(self, profile_url: str, message: str) -> Dict:
        """
        Send LinkedIn message using Apify
        Actor: apify/linkedin-message-sender
        """
        try:
            actor_id = "apify/linkedin-message-sender"
            
            run_input = {
                "profileUrls": [profile_url],
                "message": message,
                "proxy": {
                    "useApifyProxy": True
                }
            }
            
            print(f"📤 Sending LinkedIn message to {profile_url}")
            run = self.apify_client.actor(actor_id).call(run_input=run_input)
            
            return {
                "success": True,
                "platform": "linkedin",
                "profile_url": profile_url,
                "run_id": run.get("id"),
                "status": "sent"
            }
            
        except Exception as e:
            print(f"❌ LinkedIn message failed: {e}")
            return {
                "success": False,
                "platform": "linkedin",
                "profile_url": profile_url,
                "error": str(e)
            }
    
    def send_facebook_message(self, profile_url: str, message: str) -> Dict:
        """
        Send Facebook message using Apify
        Actor: apify/facebook-messenger
        """
        try:
            actor_id = "apify/facebook-messenger"
            
            run_input = {
                "profileUrl": profile_url,
                "message": message,
                "proxy": {
                    "useApifyProxy": True,
                    "apifyProxyGroups": ["RESIDENTIAL"]
                }
            }
            
            print(f"📤 Sending Facebook message to {profile_url}")
            run = self.apify_client.actor(actor_id).call(run_input=run_input)
            
            return {
                "success": True,
                "platform": "facebook",
                "profile_url": profile_url,
                "run_id": run.get("id"),
                "status": "sent"
            }
            
        except Exception as e:
            print(f"❌ Facebook message failed: {e}")
            return {
                "success": False,
                "platform": "facebook",
                "profile_url": profile_url,
                "error": str(e)
            }


# ==========================================
# OPTION 2: Using Platform APIs (If Available)
# ==========================================

class DirectAPISender:
    """Send DMs using official platform APIs (requires API keys)"""
    
    def send_instagram_business_dm(self, recipient_id: str, message: str) -> Dict:
        """
        Send Instagram DM via Instagram Graph API
        Requires: Instagram Business Account + Facebook App
        """
        import requests
        
        access_token = os.getenv("INSTAGRAM_ACCESS_TOKEN")
        instagram_account_id = os.getenv("INSTAGRAM_ACCOUNT_ID")
        
        url = f"https://graph.facebook.com/v18.0/{instagram_account_id}/messages"
        
        payload = {
            "recipient": {"id": recipient_id},
            "message": {"text": message},
            "access_token": access_token
        }
        
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            
            return {
                "success": True,
                "platform": "instagram",
                "recipient_id": recipient_id,
                "response": response.json()
            }
        except Exception as e:
            return {
                "success": False,
                "platform": "instagram",
                "error": str(e)
            }
    
    def send_linkedin_api_message(self, recipient_urn: str, message: str) -> Dict:
        """
        Send LinkedIn message via LinkedIn API
        Requires: LinkedIn Developer Account + OAuth2
        """
        import requests
        
        access_token = os.getenv("LINKEDIN_ACCESS_TOKEN")
        
        url = "https://api.linkedin.com/v2/messages"
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0"
        }
        
        payload = {
            "recipients": [recipient_urn],
            "subject": "Connection Request",
            "body": message
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            
            return {
                "success": True,
                "platform": "linkedin",
                "recipient_urn": recipient_urn,
                "response": response.json()
            }
        except Exception as e:
            return {
                "success": False,
                "platform": "linkedin",
                "error": str(e)
            }


# ==========================================
# DM Campaign Manager
# ==========================================

class DMCampaignManager:
    """Manage DM sending campaigns with rate limiting and logging"""
    
    def __init__(self, use_apify: bool = True):
        self.use_apify = use_apify
        self.sender = ApifyDMSender() if use_apify else DirectAPISender()
    
    def send_dm_to_prospect(
        self, 
        prospect: Dict, 
        message: str, 
        client_id: str,
        platform: str
    ) -> Dict:
        """Send a DM to a single prospect and log the result"""
        
        try:
            # Rate limiting - avoid spam detection
            self._apply_rate_limit(platform)
            
            # Send based on platform
            if platform == "instagram":
                username = prospect.get("username", "")
                result = self.sender.send_instagram_dm(username, message)
            elif platform == "linkedin":
                profile_url = prospect.get("profile_url", "")
                result = self.sender.send_linkedin_message(profile_url, message)
            elif platform == "facebook":
                profile_url = prospect.get("profile_url", "")
                result = self.sender.send_facebook_message(profile_url, message)
            else:
                return {"success": False, "error": f"Unsupported platform: {platform}"}
            
            # Log the DM attempt
            self._log_dm_attempt(client_id, prospect, message, result)
            
            return result
            
        except Exception as e:
            error_result = {"success": False, "error": str(e)}
            self._log_dm_attempt(client_id, prospect, message, error_result)
            return error_result
    
    def send_bulk_dms(
        self, 
        client_id: str, 
        max_messages: int = 10
    ) -> Dict:
        """
        Send DMs to multiple prospects for a client
        Returns summary of sent messages
        """
        try:
            # Get client data and generated messages
            client_data = clients_collection.find_one({"client_id": client_id})
            if not client_data:
                return {"error": "Client not found"}
            
            platform = client_data.get("platform")
            generated_message = client_data.get("generated_messages", {}).get("raw_message", "")
            
            if not generated_message:
                return {"error": "No generated message found. Generate messages first."}
            
            # Get prospects from audience_data
            from pymongo import MongoClient
            audience_collection = db["audience_data"]
            
            prospects = list(
                audience_collection.find(
                    {
                        "client_id": client_id,
                        "platform": platform,
                        "has_valid_content": True
                    }
                )
                .sort("location_relevance_score", -1)
                .limit(max_messages)
            )
            
            if not prospects:
                return {"error": "No prospects found"}
            
            # Send DMs with rate limiting
            results = {
                "total_prospects": len(prospects),
                "sent": 0,
                "failed": 0,
                "details": []
            }
            
            for prospect in prospects:
                result = self.send_dm_to_prospect(
                    prospect, 
                    generated_message, 
                    client_id, 
                    platform
                )
                
                if result.get("success"):
                    results["sent"] += 1
                else:
                    results["failed"] += 1
                
                results["details"].append({
                    "username": prospect.get("username", "unknown"),
                    "success": result.get("success"),
                    "error": result.get("error")
                })
                
                print(f"✅ Sent {results['sent']}/{len(prospects)} messages")
            
            # Update client status
            clients_collection.update_one(
                {"client_id": client_id},
                {
                    "$set": {
                        "dm_campaign_status": "completed",
                        "messages_sent": results["sent"],
                        "campaign_completed_at": datetime.utcnow()
                    }
                }
            )
            
            return results
            
        except Exception as e:
            return {"error": str(e)}
    
    def _apply_rate_limit(self, platform: str):
        """Apply platform-specific rate limiting"""
        delays = {
            "instagram": random.uniform(5, 10),  # 5-10 seconds
            "linkedin": random.uniform(10, 15),  # 10-15 seconds
            "facebook": random.uniform(7, 12)    # 7-12 seconds
        }
        delay = delays.get(platform, 5)
        print(f"⏳ Waiting {delay:.1f}s to avoid rate limits...")
        time.sleep(delay)
    
    def _log_dm_attempt(
        self, 
        client_id: str, 
        prospect: Dict, 
        message: str, 
        result: Dict
    ):
        """Log DM attempt to MongoDB"""
        log_entry = {
            "client_id": client_id,
            "prospect_username": prospect.get("username", "unknown"),
            "prospect_profile_url": prospect.get("profile_url", ""),
            "platform": prospect.get("platform"),
            "message_sent": message,
            "success": result.get("success", False),
            "error": result.get("error"),
            "sent_at": datetime.utcnow(),
            "apify_run_id": result.get("run_id")
        }
        
        dm_logs_collection.insert_one(log_entry)
        print(f"📝 Logged DM attempt to {prospect.get('username', 'unknown')}")


# ==========================================
# Usage Example
# ==========================================

if __name__ == "__main__":
    # Initialize campaign manager
    dm_manager = DMCampaignManager(use_apify=True)
    
    # Send to a single prospect
    prospect = {
        "username": "photographer_delhi",
        "profile_url": "https://instagram.com/photographer_delhi"
    }
    message = "Hey! 👋 Love your work! Let's collaborate!"
    
    result = dm_manager.send_dm_to_prospect(
        prospect, 
        message, 
        client_id="test-client-123",
        platform="instagram"
    )
    print(result)