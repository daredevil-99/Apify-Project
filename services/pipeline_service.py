# services/pipeline_service.py
from datetime import datetime
from pipeline_utils import kickoff_message_generation
from utils.serialization_utils import convert_objectid_to_str
from services.db_service import (
    get_client_data, 
    get_prospects_from_audience, 
    update_client_generated_message,
    get_prospects_by_status
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


def generate_messages_for_prospects(client_id: str):
    """
    🧠 Generate personalized messages for NEW prospects
    This function ONLY generates messages and saves them to DB
    It does NOT send messages - that's handled by send_dm_message()
    """
    print(f"\n{'='*60}")
    print(f"🤖 Message Generation started for client: {client_id}")
    print(f"{'='*60}\n")

    # 1️⃣ Fetch client data
    client_data = get_client_data(client_id)
    if not client_data:
        print(f"❌ Client {client_id} not found")
        return {"error": f"Client {client_id} not found"}

    platform = client_data.get("platform", "").lower()
    print(f"📱 Platform: {platform}")

    # 2️⃣ CHECK IF PROSPECTS EXIST
    prospects_data = get_prospects_from_audience(client_id, platform)
    
    if not prospects_data or not prospects_data.get('prospects'):
        print(f"❌ No extracted prospects found.")
        return {
            "status": "no_prospects_extracted",
            "message": "No prospects found. Run /extract-prospects first."
        }
    
    # 3️⃣ Get NEW prospects only (not yet contacted)
    new_prospects = get_prospects_by_status(client_id, platform, 'new')
    print(f"📊 Total prospects: {len(prospects_data.get('prospects', []))}")
    print(f"🆕 New prospects to contact: {len(new_prospects)}")
    
    if not new_prospects:
        print("⚠️  All prospects have already been contacted")
        return {
            "status": "all_contacted",
            "message": "All prospects have already been contacted",
            "total_prospects": len(prospects_data.get('prospects', []))
        }

    # 4️⃣ Get first new prospect to generate message for
    target_prospect = new_prospects[0]
    print(f"🎯 Target prospect: @{target_prospect.get('username')}")

    # 5️⃣ Generate message via CrewAI
    print(f"🤖 Generating personalized message...")
    safe_client_data = convert_objectid_to_str(client_data)
    
    # Add prospect info to client data for message generation
    safe_client_data['target_prospect'] = target_prospect
    
    result = kickoff_message_generation(safe_client_data)
    cleaned_message = safe_serialize(result)

    # 6️⃣ Save message to DB (but DON'T send yet)
    update_client_generated_message(client_id, {
        "generated_messages": cleaned_message,
        "generated_at": datetime.utcnow(),
        "target_prospect_username": target_prospect.get('username'),
        "message_status": "generated_not_sent"  # Track that message is ready but not sent
    })

    print(f"✅ Message generated and saved for @{target_prospect.get('username')}")
    print(f"💬 Message preview: {str(cleaned_message)[:100]}...")
    print(f"\n✅ Message generation completed for {client_id}\n")
    
    return {
        "status": "success",
        "platform": platform,
        "prospect_username": target_prospect.get('username'),
        "message_generated": True,
        "remaining_new_prospects": len(new_prospects) - 1,
        "result": cleaned_message
    }


# LEGACY FUNCTION - Keep for backwards compatibility but mark as deprecated
def generate_message(client_id: str):
    """
    ⚠️  DEPRECATED: This function generates AND sends messages
    Use generate_messages_for_prospects() + send_dm_message() instead
    
    This function is kept for backwards compatibility only
    """
    print("⚠️  WARNING: Using deprecated generate_message() function")
    print("⚠️  Please use generate_messages_for_prospects() + send_dm_message() instead")
    
    # Just call the new function
    return generate_messages_for_prospects(client_id)