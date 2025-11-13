#dm_service.py


from datetime import datetime
from pipeline_utils import auto_send_gmail
from instagram_dm_sender import send_instagram_dm
from services.db_service import (
    get_client_data,
    update_client_status,
)
from db_config import audience_collection, clients_collection
import re


def extract_username_from_message(crew_output: str) -> str:
    """
    Extract username from crew output.
    Handles formats like:
    - Target: username
    - Target: @username
    """
    if not crew_output:
        return None
    
    # Try to find "Target: username" or "Target: @username"
    match = re.search(r'Target:\s*@?(\w+(?:\.\w+)*(?:_\w+)*)', crew_output, re.IGNORECASE)
    if match:
        username = match.group(1)
        print(f"✅ Extracted username from crew output: {username}")
        return username
    
    print("⚠️ Could not extract username from crew output")
    return None


def send_dm_message(client_id: str):
    """
    ✅ FIXED: Send DMs to prospects with proper username extraction
    """
    print(f"\n{'=' * 60}")
    print(f"📤 DM Sending started for client: {client_id}")
    print(f"{'=' * 60}\n")

    # 1️⃣ Get client data
    client_data = get_client_data(client_id)
    if not client_data:
        print(f"❌ Client {client_id} not found")
        return {"error": f"Client {client_id} not found"}

    platform = client_data.get("platform", "").lower()
    print(f"📱 Platform: {platform}")

    # 2️⃣ Check if message was generated
    generated_messages = client_data.get("generated_messages")
    if not generated_messages:
        print(f"❌ No generated messages found")
        return {
            "status": "no_messages",
            "error": "No generated messages found. Run /generate-messages first.",
        }

    # 3️⃣ Extract message and username from generated_messages
    # ✅ Handle different formats
    if isinstance(generated_messages, dict):
        message_content = generated_messages.get("message")
        target_username = generated_messages.get("username")
        
        # Fallback to old format if new format not found
        if not message_content:
            crew_output = generated_messages.get("final_output", "")
            if crew_output:
                # Extract from old format
                message_match = re.search(r'Message:\s*(.+?)(?:\n\n|\Z)', crew_output, re.DOTALL)
                if message_match:
                    message_content = message_match.group(1).strip()
                
                username_match = re.search(r'Target:\s*@?([\w\.\-_]+)', crew_output, re.IGNORECASE)
                if username_match:
                    target_username = username_match.group(1)
    else:
        message_content = None
        target_username = None

    if not message_content or not target_username:
        print(f"❌ Could not extract message or username")
        print(f"📊 Generated messages content: {generated_messages}")
        return {"error": "Invalid message format"}

    print(f"🎯 Target username: @{target_username}")
    print(f"💬 Message: {message_content[:100]}...")

    # 4️⃣ Get prospects document from audience collection
    prospects_doc = audience_collection.find_one({
        "client_id": client_id,
        "platform": platform,
        "type": "prospects"
    })

    if not prospects_doc:
        print("❌ No prospects document found")
        return {
            "status": "error",
            "error": "No prospects found. Run /scrape and /generate-messages first.",
        }

    # 5️⃣ Find the target prospect in the array
    prospects_array = prospects_doc.get("prospects", [])
    target_prospect = None
    target_index = None

    for idx, prospect in enumerate(prospects_array):
        p_username = prospect.get("username") or prospect.get("ownerUsername")
        if p_username == target_username:
            target_prospect = prospect
            target_index = idx
            break

    if not target_prospect:
        print(f"❌ Prospect @{target_username} not found in prospects array")
        print(f"📊 Available prospects ({len(prospects_array)}):")
        for p in prospects_array[:5]:
            p_user = p.get("username") or p.get("ownerUsername")
            print(f"   - {p_user}")
        
        return {
            "status": "error",
            "error": f"Prospect @{target_username} not found in database",
        }

    print(f"✅ Found prospect at index {target_index}: @{target_username}")

    # 6️⃣ Send based on platform
    result = None

    try:
        if platform == "gmail":
            print(f"📧 Sending via Gmail...")
            result = auto_send_gmail(client_id)
            
            # Update prospect status in array
            audience_collection.update_one(
                {
                    "client_id": client_id,
                    "platform": platform,
                    "type": "prospects"
                },
                {
                    "$set": {
                        f"prospects.{target_index}.status": "contacted",
                        f"prospects.{target_index}.last_contacted": datetime.utcnow().isoformat(),
                        f"prospects.{target_index}.dm_sent": True
                    }
                }
            )
            
            result = {
                "status": "success",
                "platform": "gmail",
                "prospect": target_username,
                "message": "Email sent successfully",
            }

        elif platform == "instagram":
            print(f"📸 Sending via Instagram DM...")
            
            # ✅ Send Instagram DM using bhansalisoft actor
            ig_result = send_instagram_dm(target_username, message_content, client_id)

            if ig_result and ig_result.get("status") == "success":
                # ✅ Update prospect status in array
                audience_collection.update_one(
                    {
                        "client_id": client_id,
                        "platform": platform,
                        "type": "prospects"
                    },
                    {
                        "$set": {
                            f"prospects.{target_index}.status": "contacted",
                            f"prospects.{target_index}.last_contacted": datetime.utcnow().isoformat(),
                            f"prospects.{target_index}.dm_sent": True,
                            f"prospects.{target_index}.dm_message": message_content
                        }
                    }
                )
                
                print(f"✅ Instagram DM sent to @{target_username}")
                result = {
                    "status": "success",
                    "platform": "instagram",
                    "prospect": target_username,
                    "message": "Instagram DM sent successfully",
                    "apify_result": ig_result,
                }
            else:
                print(f"⚠️ Instagram DM status unclear")
                result = {
                    "status": "unclear",
                    "platform": "instagram",
                    "prospect": target_username,
                    "message": "DM send status unclear",
                    "apify_result": ig_result,
                }

        elif platform == "linkedin":
            from linkedin_dm_sender import send_linkedin_dm
            
            print(f"💼 Sending via LinkedIn DM...")
            
            recipient_url = target_prospect.get("profile_url")
            if not recipient_url:
                raise ValueError("Missing LinkedIn profile URL for prospect")
            
            li_result = send_linkedin_dm(recipient_url, message_content, client_id)
            
            if li_result.get("status") == "success":
                audience_collection.update_one(
                    {
                        "client_id": client_id,
                        "platform": platform,
                        "type": "prospects"
                    },
                    {
                        "$set": {
                            f"prospects.{target_index}.status": "contacted",
                            f"prospects.{target_index}.last_contacted": datetime.utcnow().isoformat()
                        }
                    }
                )
            
            result = li_result

        else:
            print(f"❌ Unsupported platform: {platform}")
            result = {"status": "error", "error": f"Unsupported platform: {platform}"}

    except Exception as e:
        error_msg = str(e)
        print(f"❌ DM FAILED: {error_msg}")
        
        # Update prospect as failed
        audience_collection.update_one(
            {
                "client_id": client_id,
                "platform": platform,
                "type": "prospects"
            },
            {
                "$set": {
                    f"prospects.{target_index}.status": "failed",
                    f"prospects.{target_index}.last_contacted": datetime.utcnow().isoformat(),
                    f"prospects.{target_index}.dm_error": error_msg
                }
            }
        )
        
        result = {
            "status": "failed",
            "error": error_msg,
            "prospect": target_username,
            "platform": platform,
        }

    # 7️⃣ Update client with DM result
    clients_collection.update_one(
        {"client_id": client_id},
        {
            "$set": {
                "last_dm_result": result,
                "last_dm_sent_at": datetime.utcnow(),
                "message_status": "sent" if result.get("status") == "success" else "send_failed",
            }
        },
    )

    print(f"\n✅ DM sending completed for {client_id}\n")
    return result