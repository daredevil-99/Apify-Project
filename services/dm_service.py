# services/dm_service.py
from datetime import datetime
from pipeline_utils import auto_send_gmail
from instagram_dm_sender import send_instagram_dm
from services.db_service import (
    get_client_data,
    get_prospects_by_status,
    update_prospect_status,
    update_client_status
)
from db_config import clients_collection


def send_dm_message(client_id: str):
    """
    Send DMs to NEW prospects based on platform
    Supports: Instagram (Apify), Gmail (email)
    TODO: LinkedIn, Facebook
    """
    print(f"\n{'='*60}")
    print(f"📤 DM Sending started for client: {client_id}")
    print(f"{'='*60}\n")

    # 1️⃣ Get client data
    client_data = get_client_data(client_id)
    if not client_data:
        print(f"❌ Client {client_id} not found")
        return {"error": f"Client {client_id} not found"}

    platform = client_data.get("platform", "").lower()
    print(f"📱 Platform: {platform}")

    # 2️⃣ Check if message was generated
    if not client_data.get("generated_messages"):
        print(f"❌ No generated messages found")
        return {
            "status": "no_messages",
            "error": "No generated messages found. Run /generate-messages first."
        }

    # 3️⃣ Get NEW prospects (not contacted yet)
    new_prospects = get_prospects_by_status(client_id, platform, "new")
    
    if not new_prospects:
        print("⚠️  No new prospects to contact")
        return {
            "status": "no_new_prospects",
            "message": "All prospects have already been contacted"
        }

    # 4️⃣ Get target prospect (should match the one message was generated for)
    target_prospect_username = client_data.get("target_prospect_username")
    target_prospect = next(
        (p for p in new_prospects if p.get('username') == target_prospect_username),
        new_prospects[0]  # Fallback to first new prospect
    )
    
    print(f"🎯 Target prospect: @{target_prospect.get('username')}")

    # 5️⃣ Get message content
    message_data = client_data.get("generated_messages")
    message_content = (
        message_data.get("message") 
        if isinstance(message_data, dict) 
        else str(message_data)
    )
    
    print(f"💬 Message: {message_content[:100]}...")

    # 6️⃣ Send based on platform
    result = None
    
    try:
        if platform == "gmail":
            print(f"📧 Sending via Gmail...")
            result = auto_send_gmail(client_id)
            
            # Update prospect status
            update_prospect_status(
                client_id, 
                platform, 
                target_prospect.get('username'), 
                'contacted',
                datetime.utcnow().isoformat()
            )
            
            print(f"✅ Gmail sent successfully")
            result = {
                "status": "success",
                "platform": "gmail",
                "prospect": target_prospect.get('username'),
                "message": "Email sent successfully"
            }

        elif platform == "instagram":
            print(f"📸 Sending via Instagram DM...")
            recipient_username = target_prospect.get("username")
            
            ig_result = send_instagram_dm(recipient_username, message_content, client_id)
            
            # Only update if DM sent successfully
            if ig_result and ig_result.get("status") == "success":
                update_prospect_status(
                    client_id, 
                    platform, 
                    recipient_username, 
                    'contacted',
                    datetime.utcnow().isoformat()
                )
                print(f"✅ Instagram DM sent to @{recipient_username}")
                
                result = {
                    "status": "success",
                    "platform": "instagram",
                    "prospect": recipient_username,
                    "message": "Instagram DM sent successfully",
                    "apify_result": ig_result
                }
            else:
                print(f"⚠️  Instagram DM status unclear")
                result = {
                    "status": "unclear",
                    "platform": "instagram",
                    "prospect": recipient_username,
                    "message": "DM send status unclear",
                    "apify_result": ig_result
                }

        elif platform == "linkedin":
            print(f"💼 LinkedIn messaging not yet implemented")
            result = {
                "status": "not_implemented",
                "platform": "linkedin",
                "message": "LinkedIn messaging coming soon"
            }
            
        elif platform == "facebook":
            print(f"📘 Facebook messaging not yet implemented")
            result = {
                "status": "not_implemented",
                "platform": "facebook",
                "message": "Facebook messaging coming soon"
            }
        
        else:
            print(f"❌ Unsupported platform: {platform}")
            result = {
                "status": "error",
                "error": f"Unsupported platform: {platform}"
            }

    except Exception as e:
        error_msg = str(e)
        print(f"❌ DM FAILED: {error_msg}")
        
        # Mark prospect as 'failed'
        update_prospect_status(
            client_id, 
            platform, 
            target_prospect.get('username'), 
            'failed',
            datetime.utcnow().isoformat()
        )
        
        result = {
            "status": "failed",
            "error": error_msg,
            "prospect": target_prospect.get('username'),
            "platform": platform,
            "action_required": "Check platform credentials or API limits"
        }

    # 7️⃣ Update client with DM campaign result
    clients_collection.update_one(
        {"client_id": client_id},
        {"$set": {
            "last_dm_result": result,
            "last_dm_sent_at": datetime.utcnow(),
            "message_status": "sent" if result.get("status") == "success" else "send_failed"
        }}
    )

    print(f"\n✅ DM sending completed for {client_id}\n")
    return result


def send_bulk_dms(client_id: str, max_prospects: int = 10):
    """
    Send DMs to multiple NEW prospects in bulk
    Useful for campaigns
    
    Args:
        client_id: Client UUID
        max_prospects: Maximum number of prospects to contact (default: 10)
    """
    print(f"\n{'='*60}")
    print(f"📤 BULK DM Campaign started for client: {client_id}")
    print(f"{'='*60}\n")

    client_data = get_client_data(client_id)
    if not client_data:
        return {"error": f"Client {client_id} not found"}

    platform = client_data.get("platform", "").lower()
    
    # Get NEW prospects
    new_prospects = get_prospects_by_status(client_id, platform, "new")
    
    if not new_prospects:
        return {
            "status": "no_new_prospects",
            "message": "All prospects have already been contacted"
        }

    # Limit to max_prospects
    prospects_to_contact = new_prospects[:max_prospects]
    print(f"📊 Contacting {len(prospects_to_contact)} prospects")

    results = {
        "total_attempted": len(prospects_to_contact),
        "successful": 0,
        "failed": 0,
        "details": []
    }

    # Send to each prospect
    for prospect in prospects_to_contact:
        print(f"\n--- Processing @{prospect.get('username')} ---")
        
        # You would need to generate individual messages for each prospect
        # For now, this is a placeholder - you'd call generate_messages_for_prospects
        # for each one, then send
        
        # TODO: Implement per-prospect message generation + sending
        print(f"⚠️  Bulk sending requires individual message generation per prospect")
        
    return results