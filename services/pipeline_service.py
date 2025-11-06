# services/pipeline_service.py
from datetime import datetime
from pipeline_utils import kickoff_message_generation, auto_send_gmail
from instagram_dm_sender import send_instagram_dm
from services import db_service
from services.scraping_service import scrape_and_store
from utils.serialization_utils import convert_objectid_to_str
from services.db_service import (
    get_client_data, 
    get_prospects_from_audience, 
    update_client_generated_message,
    update_prospect_status
)


def safe_serialize(obj):
    """Convert CrewOutput to JSON-friendly format"""
    if hasattr(obj, "json_dict") and obj.json_dict:
        return obj.json_dict
    elif hasattr(obj, "raw"):
        return obj.raw
    elif isinstance(obj, dict):
        return obj
    elif isinstance(obj, list):
        return [safe_serialize(i) for i in obj]
    return str(obj)


def generate_message(client_id: str):
    """
    🧠 AGENTIC CONTROLLER: Main orchestrator
    Extract prospects → Generate → Send
    """
    print(f"\n{'='*60}")
    print(f"🚀 Pipeline started for client: {client_id}")
    print(f"{'='*60}\n")

    # 1️⃣ Fetch client data
    client_data = db_service.get_client_data(client_id)
    if not client_data:
        return {"error": f"Client {client_id} not found"}

    platform = client_data.get("platform", "").lower()
    print(f"📱 Platform: {platform}")

    # 2️⃣ CHECK IF PROSPECTS EXIST (NEW WAY)
    prospects_data = get_prospects_from_audience(client_id, platform)
    
    if not prospects_data or not prospects_data.get('prospects'):
        print(f"⚠️  No extracted prospects found.")
        print(f"🔍 Checking if scraped data exists...")
        
        # Check if we have scraped posts
        scraped_count = db_service.count_prospects(client_id, platform)
        
        if scraped_count == 0:
            print(f"🔍 No scraped data found. Triggering scraper...")
            
            # INTERNAL SCRAPING (not exposed as endpoint)
            scrape_result = scrape_and_store(
                client_id=client_id,
                platform=platform,
                search_terms=client_data.get("search_terms_with_location", []),
                profession=client_data.get("preferred_profession"),
                location=client_data.get("preferred_location")
            )

            if scrape_result["status"] != "success":
                return {
                    "status": "scraping_failed",
                    "error": scrape_result.get("message"),
                    "platform": platform
                }

            print(f"✅ Scraping completed: {scrape_result['profiles_count']} profiles")
        
        print(f"❌ Prospects not extracted yet. Please run /extract-prospects first.")
        return {
            "status": "no_prospects_extracted",
            "message": "Scraped data exists but prospects not extracted. Run /extract-prospects endpoint first.",
            "scraped_posts": scraped_count
        }
    
    # Get all prospects
    all_prospects = prospects_data.get('prospects', [])
    print(f"📊 Total prospects: {len(all_prospects)}")
    
    # Filter only NEW prospects (not yet contacted)
    new_prospects = [p for p in all_prospects if p.get('status') == 'new']
    print(f"🆕 New prospects to contact: {len(new_prospects)}")
    
    if not new_prospects:
        print("⚠️  All prospects have already been contacted")
        return {
            "status": "all_contacted",
            "message": "All prospects have already been contacted",
            "total_prospects": len(all_prospects)
        }

    # 3️⃣ Get first new prospect to contact
    target_prospect = new_prospects[0]
    print(f"🎯 Target prospect: @{target_prospect.get('username')}")

    # 4️⃣ Generate message via CrewAI
    print(f"🤖 Generating personalized message...")
    safe_client_data = convert_objectid_to_str(client_data)
    
    # Add prospect info to client data for message generation
    safe_client_data['target_prospect'] = target_prospect
    
    result = kickoff_message_generation(safe_client_data)
    cleaned_message = safe_serialize(result)

    # 5️⃣ Save message to DB
    db_service.update_client_generated_message(client_id, {
        "generated_messages": cleaned_message,
        "generated_at": datetime.utcnow(),
        "target_prospect_username": target_prospect.get('username')
    })

    # 6️⃣ Auto-send based on platform
    if platform == "gmail":
        print(f"📧 Auto-sending Gmail...")
        auto_send_gmail(client_id)
        
        # Update prospect status
        update_prospect_status(
            client_id, 
            platform, 
            target_prospect.get('username'), 
            'contacted'
        )

    elif platform == "instagram":
        print(f"💬 Auto-sending Instagram DM...")
        
        recipient_username = target_prospect.get("username")
        message_content = (
            cleaned_message.get("message")
            if isinstance(cleaned_message, dict)
            else str(cleaned_message)
        )
        
        try:
            result = send_instagram_dm(recipient_username, message_content, client_id)
            
            # Only update status if DM actually sent successfully
            if result and result.get("status") == "success":
                update_prospect_status(
                    client_id, 
                    platform, 
                    recipient_username, 
                    'contacted'
                )
                print(f"✅ DM sent and status updated for @{recipient_username}")
            else:
                print(f"⚠️  DM send status unclear, not updating prospect status")
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ DM FAILED: {error_msg}")
            
            # Mark prospect as 'failed' instead of 'contacted'
            update_prospect_status(
                client_id, 
                platform, 
                recipient_username, 
                'failed'
            )
            
            return {
                "status": "dm_failed", 
                "error": error_msg,
                "prospect": recipient_username,
                "action_required": "Check Apify account - Actor may need payment or credits"
            }
    
    elif platform == "linkedin":
        print(f"💼 LinkedIn messaging not yet implemented")
        # TODO: Implement LinkedIn messaging
        
    elif platform == "facebook":
        print(f"📘 Facebook messaging not yet implemented")
        # TODO: Implement Facebook messaging

    print(f"\n✅ Pipeline completed for {client_id}\n")
    return {
        "status": "success",
        "platform": platform,
        "prospect_contacted": target_prospect.get('username'),
        "remaining_new_prospects": len(new_prospects) - 1,
        "result": cleaned_message
    }