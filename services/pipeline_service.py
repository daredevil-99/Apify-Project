# services/pipeline_service.py
from datetime import datetime
from pipeline_utils import kickoff_message_generation, auto_send_gmail
from instagram_dm_sender import send_instagram_dm
from services import db_service
from services.scraping_service import scrape_and_store
from utils.serialization_utils import convert_objectid_to_str


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
    Scrape (if needed) → Generate → Send
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

    # 2️⃣ CHECK IF PROSPECTS EXIST (Agentic Decision)
    prospect_count = db_service.count_prospects(client_id, platform)
    print(f"📊 Existing prospects: {prospect_count}")

    if prospect_count == 0:
        print(f"🔍 No prospects found. Triggering scraper...")

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

    # 3️⃣ Generate message via CrewAI
    print(f"🤖 Generating personalized message...")
    safe_client_data = convert_objectid_to_str(client_data)
    result = kickoff_message_generation(safe_client_data)
    cleaned_message = safe_serialize(result)

    # 4️⃣ Save message to DB
    db_service.update_client_generated_message(client_id, {
        "generated_messages": cleaned_message,
        "generated_at": datetime.utcnow(),
    })

    # 5️⃣ Auto-send based on platform
    if platform == "gmail":
        print(f"📧 Auto-sending Gmail...")
        auto_send_gmail(client_id)

    elif platform == "instagram":
        print(f"💬 Auto-sending Instagram DM...")
        prospects = db_service.get_prospect_profiles(client_id, platform, limit=1)
        
        if prospects:
            recipient_username = prospects[0].get("username")
            message_content = (
                cleaned_message.get("message")
                if isinstance(cleaned_message, dict)
                else str(cleaned_message)
            )
            
            try:
                send_instagram_dm(recipient_username, message_content, client_id)
            except Exception as e:
                print(f"❌ DM failed: {e}")
                return {"status": "dm_failed", "error": str(e)}
        else:
            print("❌ No prospects found")
            return {"status": "no_prospects"}

    print(f"\n✅ Pipeline completed for {client_id}\n")
    return {
        "status": "success",
        "platform": platform,
        "result": cleaned_message
    }