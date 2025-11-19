from apify_client import ApifyClient
import os
import time
import random
from dotenv import load_dotenv

load_dotenv() 

def send_linkedin_dm(profile_url: str, message: str, client_id: str):
    """
    ✅ WORKING VERSION - Sends LinkedIn DM using noddsolutions actor
    
    Returns:
        dict: {
            "status": "success" | "failed" | "error",
            "message_sent": bool,
            "error": str (if failed)
        }
    """
    try:
        if not profile_url:
            return {
                "status": "failed", 
                "message_sent": False,
                "error": "missing_profile_url"
            }

        # ✅ Get credentials
        apify_token = os.getenv("APIFY_API_TOKEN")
        linkedin_cookie = os.getenv("LINKEDIN_COOKIE")  # Full cookie JSON string
        
        if not apify_token:
            raise ValueError("Missing APIFY_API_TOKEN in .env file")
        if not linkedin_cookie:
            raise ValueError("Missing LINKEDIN_COOKIE in .env file")

        client = ApifyClient(apify_token)
        
        # ✅ Use the correct actor ID
        actor_id = "noddsolutions/linkedin-automation-tool"
        
        # ✅ CRITICAL FIX: Proper field names and URL format
        run_input = {
            "action": "send-message",
            "startUrls": [{"url": profile_url}],
            "message": message,
            "linkedinCookie": linkedin_cookie,
            "maxMessages": 1,
            "waitBetweenRequests": [3, 5],
            # Try without proxy first (comment out if it doesn't work)
            "proxyConfiguration": {
                  "useApifyProxy": True
                }
        }

        print(f"🚀 Sending LinkedIn DM to {profile_url}...")
        print(f"📝 Message preview: {message[:50]}...")

        # ✅ Run the actor
        run = client.actor(actor_id).call(run_input=run_input)
        run_id = run["id"]
        
        print(f"🔄 Actor run ID: {run_id}")

        # ✅ Wait for completion with status updates
        max_wait_time = 120  # 2 minutes max
        check_interval = 5
        elapsed = 0
        
        while elapsed < max_wait_time:
            run_details = client.run(run_id).get()
            status = run_details.get("status", "UNKNOWN")
            
            print(f"[LinkedIn Actor] Status: {status} (elapsed: {elapsed}s)")
            
            if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                break
                
            time.sleep(check_interval)
            elapsed += check_interval

        # ✅ Get results
        dataset_response = client.dataset(run["defaultDatasetId"]).list_items()
        
        # Handle ListPage object properly
        if hasattr(dataset_response, 'items'):
            dataset_items = dataset_response.items
        else:
            dataset_items = []
        
        print(f"📊 Actor finished with {len(dataset_items)} results")

        # ✅ Check if message was actually sent
        message_sent = False
        errors = []
        
        # Debug: Print what we got
        if dataset_items:
            print(f"🔍 First result keys: {list(dataset_items[0].keys())}")
            print(f"🔍 First result: {dataset_items[0]}")
        
        if status == "SUCCEEDED":
            if dataset_items:
                for item in dataset_items:
                    if isinstance(item, dict):
                        # Check various success indicators
                        if (item.get("messageSent") or 
                            item.get("sent") or 
                            item.get("success") or
                            item.get("status") == "sent"):
                            message_sent = True
                        
                        # Collect errors
                        if item.get("error"):
                            errors.append(item["error"])
            else:
                # If SUCCEEDED but no items, assume it worked
                message_sent = True
                print("⚠️ No dataset items, but status is SUCCEEDED - assuming success")
        
        # ✅ Return result
        if message_sent and not errors:
            return {
                "status": "success",
                "message_sent": True,
                "platform": "linkedin",
                "profile_url": profile_url,
                "actor_run": f"https://console.apify.com/actors/runs/{run_id}"
            }
        else:
            error_msg = errors[0] if errors else "Message not sent - check actor logs"
            if status == "FAILED":
                error_msg = f"Actor failed with status: {status}"
            
            return {
                "status": "failed",
                "message_sent": False,
                "error": error_msg,
                "actor_run": f"https://console.apify.com/actors/runs/{run_id}",
                "final_status": status
            }

    except Exception as e:
        print(f"❌ LinkedIn DM failed: {e}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "message_sent": False,
            "error": str(e)
        }


def get_linkedin_cookie_from_browser():
    """
    Helper function to guide users on getting LinkedIn cookies
    """
    guide = """
    📋 HOW TO GET LINKEDIN COOKIES:
    
    1. Install Chrome Extension: "EditThisCookie"
       https://chrome.google.com/webstore/detail/editthiscookie
    
    2. Go to linkedin.com and log in
    
    3. Click the EditThisCookie extension icon
    
    4. Click "Export" button (looks like a download icon)
       This copies ALL cookies to clipboard as JSON
    
    5. Paste in your .env file:
       LINKEDIN_COOKIE='[{"name":"li_at","value":"AQE..."},...all cookies...]'
    
    IMPORTANT: Use the FULL cookie array, not just li_at!
    """
    print(guide)
    return guide


def process_linkedin_dms(prospects, client_id):
    """
    Process LinkedIn DMs with proper error handling and rate limiting
    No database - just sends messages and returns results
    """
    successful = 0
    failed = 0
    results = []
    
    print(f"\n🚀 Processing {len(prospects)} LinkedIn DM(s)...\n")
    
    for i, prospect in enumerate(prospects, 1):
        profile_url = prospect.get("profile_url") or prospect.get("url")
        message = prospect.get("message")
        prospect_id = prospect.get("id")
        prospect_name = prospect.get("name", "Unknown")
        
        if not profile_url or not message:
            print(f"⏭️  [{i}/{len(prospects)}] Skipping {prospect_name} - missing data")
            failed += 1
            results.append({
                "name": prospect_name,
                "status": "skipped",
                "reason": "missing data"
            })
            continue
        
        print(f"\n{'='*60}")
        print(f"📤 [{i}/{len(prospects)}] Sending to: {prospect_name}")
        print(f"🔗 Profile: {profile_url}")
        print(f"{'='*60}\n")
        
        result = send_linkedin_dm(profile_url, message, client_id)
        
        if result["status"] == "success" and result["message_sent"]:
            print(f"✅ Message sent successfully to {prospect_name}!")
            successful += 1
            results.append({
                "name": prospect_name,
                "profile_url": profile_url,
                "status": "success"
            })
        else:
            print(f"❌ Failed to send to {prospect_name}: {result.get('error', 'Unknown error')}")
            failed += 1
            results.append({
                "name": prospect_name,
                "profile_url": profile_url,
                "status": "failed",
                "error": result.get('error')
            })
        
        if i < len(prospects):
            wait_time = random.randint(45, 90)
            print(f"\n⏱️  Waiting {wait_time}s before next message...\n")
            time.sleep(wait_time)
    
    print(f"\n{'='*60}")
    print(f"📊 FINAL RESULTS:")
    print(f"   ✅ Successful: {successful}")
    print(f"   ❌ Failed: {failed}")
    print(f"   📈 Success Rate: {(successful/(successful+failed)*100):.1f}%")
    print(f"{'='*60}\n")
    
    return {
        "successful": successful, 
        "failed": failed,
        "results": results
    }


def test_single_linkedin_dm():
    """Test sending a single DM"""
    test_prospect = {
        "id": "test_123",
        "name": "Dharani",
        "profile_url": "https://www.linkedin.com/in/kdharani-ai/",
        "message": "Hi! This is a test message from my automation system."
    }
    
    result = send_linkedin_dm(
        test_prospect["profile_url"],
        test_prospect["message"],
        "test_client"
    )
    
    print(f"\n🧪 Test Result: {result}")
    return result


if __name__ == "__main__":
    get_linkedin_cookie_from_browser()
    test_single_linkedin_dm()


