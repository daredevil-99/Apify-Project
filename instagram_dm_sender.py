# instagram_dm_sender.py
# UPDATE YOUR EXISTING FILE WITH BETTER ERROR HANDLING

import os
from apify_client import ApifyClient

def send_instagram_dm(recipient_username: str, message: str, client_id: str = None):
    """
    Send Instagram DM using Apify Actor with improved error handling
    """
    print(f"📤 Sending Instagram DM to @{recipient_username}")
    
    # Initialize Apify client
    apify_token = os.getenv("APIFY_API_TOKEN")
    if not apify_token:
        error_msg = "APIFY_API_TOKEN not found in environment variables"
        print(f"❌ {error_msg}")
        raise ValueError(error_msg)
    
    client = ApifyClient(apify_token)
    
    # Prepare actor input
    run_input = {
        "username": recipient_username,
        "message": message,
        # Add any other required parameters for your Instagram DM actor
    }
    
    try:
        # Run the Instagram DM actor
        # Replace 'YOUR_ACTOR_ID' with your actual actor ID
        run = client.actor("HCeZTYRGtlUT8Sxx6").call(run_input=run_input)
        
        # Check if run was successful
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
        print(f"❌ Failed to send DM to @{recipient_username}: {error_msg}")
        
        # Check for specific error types
        if "free trial has expired" in error_msg or "rent a paid Actor" in error_msg:
            raise Exception(
                f"❌ APIFY ACTOR TRIAL EXPIRED: You need to rent the Instagram DM Actor. "
                f"Visit: https://console.apify.com/actors/HCeZTYRGtlUT8Sxx6"
            )
        elif "insufficient credit" in error_msg.lower():
            raise Exception(
                f"❌ INSUFFICIENT APIFY CREDITS: Please add credits to your Apify account. "
                f"Visit: https://console.apify.com/billing"
            )
        else:
            raise Exception(f"Failed to send Instagram DM: {error_msg}")


def send_bulk_instagram_dms(prospects: list, message_template: str, client_id: str = None):
    """
    Send Instagram DMs to multiple prospects
    """
    results = {
        "success": [],
        "failed": [],
        "total": len(prospects)
    }
    
    for prospect in prospects:
        username = prospect.get("username")
        
        # Personalize message if needed
        personalized_message = message_template.replace("{name}", prospect.get("full_name", username))
        
        try:
            send_instagram_dm(username, personalized_message, client_id)
            results["success"].append(username)
        except Exception as e:
            results["failed"].append({
                "username": username,
                "error": str(e)
            })
            print(f"⚠️  Skipping @{username} due to error")
    
    print(f"\n📊 Bulk DM Results:")
    print(f"   ✅ Successful: {len(results['success'])}")
    print(f"   ❌ Failed: {len(results['failed'])}")
    
    return results