# routes.py - CLEANED UP VERSION

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from services.db_service import register_client, get_client_data, get_prospects_from_audience
from services.scraping_service import scrape_and_store, extract_and_store_prospects
from services.pipeline_service import generate_messages_for_prospects
from services.dm_service import send_dm_message
from db_config import audience_collection

router = APIRouter(prefix="/pipeline", tags=["Pipeline"])


# ============================================================
# 🧾 Models
# ============================================================
class ClientRegistration(BaseModel):
    name: str
    role: str
    email: str
    platform: str  # "instagram", "linkedin", "facebook"
    search_terms_with_location: List[str]
    preferred_profession: str
    preferred_location: str


class LinkedInInput(BaseModel):
    searchQuery: Optional[str] = None
    profileScraperMode: str = "Full"
    startPage: int = 1


class SelectiveMessageRequest(BaseModel):
    prospect_usernames: List[str]
    send_immediately: bool = False


# ============================================================
# 1️⃣ Register Client
# ============================================================
@router.post("/register")
async def register_client_endpoint(client_data: ClientRegistration):
    """
    Register a new client and store their configuration (platform, keywords, etc.)
    """
    client_dict = client_data.dict()
    client_dict["search_terms"] = client_dict.pop("search_terms_with_location", [])
    client_dict["profession"] = client_dict.pop("preferred_profession", "")
    client_dict["location"] = client_dict.pop("preferred_location", "")

    client_id = register_client(client_dict)
    return {
        "message": "✅ Client registered successfully",
        "client_id": client_id,
        "note": "Next step: call /pipeline/scrape/{client_id}"
    }


# ============================================================
# 2️⃣ Scrape Platform Data
# ============================================================
@router.post("/scrape/{client_id}")
async def scrape_endpoint(
    client_id: str,
    background_tasks: BackgroundTasks,
    linkedin_input: Optional[LinkedInInput] = None
):
    """
    Scrape posts/profiles from the client's target platform and save results in DB.
    """
    client_data = get_client_data(client_id)
    if not client_data:
        raise HTTPException(status_code=404, detail="Client not found")

    platform = client_data.get("platform", "").lower()
    search_terms = client_data.get("search_terms", [])
    profession = client_data.get("profession", "")
    location = client_data.get("location", "")

    print(f"🔍 Platform: {platform}, Terms: {search_terms}, Profession: {profession}, Location: {location}")

    if platform == "linkedin":
        background_tasks.add_task(scrape_and_store, client_id, platform, search_terms, profession, location)
        return {
            "message": "🚀 LinkedIn scraping started",
            "client_id": client_id,
            "platform": "LinkedIn",
            "search_params": {"search_terms": search_terms, "profession": profession, "location": location}
        }

    if not search_terms:
        raise HTTPException(status_code=400, detail="No search terms found for this client")

    background_tasks.add_task(scrape_and_store, client_id, platform, search_terms, profession, location)

    return {
        "message": f"🚀 Scraping started for {platform}",
        "client_id": client_id,
        "platform": platform,
        "info": "Once completed, run /pipeline/extract-prospects/{client_id}"
    }


# ============================================================
# 3️⃣ Extract Prospects from Scraped Data
# ============================================================
@router.post("/extract-prospects/{client_id}")
async def extract_prospects_endpoint(client_id: str):
    """
    Extract potential leads/prospects from scraped data.
    """
    try:
        client_data = get_client_data(client_id)
        if not client_data:
            raise HTTPException(status_code=404, detail="Client not found")

        platform = client_data.get("platform", "instagram")
        count = extract_and_store_prospects(client_id, platform)
        
        if count == 0:
            return {"message": "❌ No scraped posts found. Run /scrape first."}
        
        return {
            "message": f"✅ Extracted and stored {count} prospects.",
            "next": f"/pipeline/generate-messages-all/{client_id}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 4️⃣ Generate Messages for ALL Prospects
# ============================================================
@router.post("/generate-messages-all/{client_id}")
async def generate_messages_all(client_id: str, background_tasks: BackgroundTasks):
    """
    Generate personalized messages for ALL filtered prospects.
    """
    try:
        from services.pipeline_service import kickoff_message_generation
        
        client_data = get_client_data(client_id)
        if not client_data:
            raise HTTPException(status_code=404, detail="Client not found")
        
        platform = client_data.get("platform", "")
        
        prospects_doc = audience_collection.find_one({
            "client_id": client_id,
            "platform": platform,
            "type": "prospects"
        })
        
        if not prospects_doc:
            raise HTTPException(status_code=404, detail="No prospects found. Run extract-prospects first.")
        
        all_prospects = prospects_doc.get("prospects", [])
        
        if not all_prospects:
            return {"message": "❌ No prospects available", "count": 0}
        
        # Filter prospects that need messages
        prospects_needing_messages = [
            p for p in all_prospects 
            if not p.get("generated_message")
        ]
        
        if not prospects_needing_messages:
            return {
                "message": "✅ All prospects already have messages",
                "total": len(all_prospects),
                "already_generated": len(all_prospects)
            }
        
        print(f"📝 Found {len(prospects_needing_messages)} prospects needing messages")
        
        # Background task to generate messages for all prospects
        def generate_all_messages():
            success_count = 0
            failed_count = 0
            
            for idx, prospect in enumerate(prospects_needing_messages):
                username = (
                    prospect.get("username") or 
                    prospect.get("ownerUsername") or 
                    prospect.get("pageName", "unknown")
                )
                
                try:
                    print(f"\n🔄 [{idx+1}/{len(prospects_needing_messages)}] Generating message for @{username}...")
                    
                    # ✅ FIX: Call message generation (it will pick a random prospect without a message)
                    result = kickoff_message_generation(client_data)
                    
                    if result.get("success") or result.get("username"):
                        success_count += 1
                        generated_username = result.get("username")
                        print(f"✅ Message generated for @{generated_username}")
                    else:
                        failed_count += 1
                        print(f"❌ Failed to generate message")
                        
                except Exception as e:
                    print(f"❌ Error generating message: {e}")
                    failed_count += 1
                
                # Add delay between generations to avoid rate limits
                import time
                time.sleep(2)
            
            print(f"\n✅ Batch generation complete:")
            print(f"   ✅ Success: {success_count}")
            print(f"   ❌ Failed: {failed_count}")
            
            return {"success": success_count, "failed": failed_count}
        
        background_tasks.add_task(generate_all_messages)
        
        return {
            "message": f"🚀 Generating messages for {len(prospects_needing_messages)} prospects",
            "total_prospects": len(prospects_needing_messages),
            "already_have_messages": len(all_prospects) - len(prospects_needing_messages),
            "status": "processing",
            "next_step": "Use GET /pipeline/prospects/{client_id}/ready to view generated messages"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 5️⃣ View Prospects with Generated Messages
# ============================================================
@router.get("/prospects/{client_id}/ready")
async def get_ready_prospects(client_id: str):
    """
    Get all prospects that have generated messages.
    Users can review these before sending.
    """
    try:
        client_data = get_client_data(client_id)
        if not client_data:
            raise HTTPException(status_code=404, detail="Client not found")
        
        platform = client_data.get("platform", "")
        
        prospects_doc = audience_collection.find_one({
            "client_id": client_id,
            "platform": platform,
            "type": "prospects"
        })
        
        if not prospects_doc:
            return {
                "message": "No prospects found",
                "count": 0,
                "prospects": []
            }
        
        all_prospects = prospects_doc.get("prospects", [])
        
        # Separate prospects by status
        ready_to_send = []
        already_sent = []
        no_message = []
        
        for prospect in all_prospects:
            username = (
                prospect.get("username") or 
                prospect.get("ownerUsername") or 
                prospect.get("pageName", "unknown")
            )
            
            prospect_info = {
                "username": username,
                "full_name": prospect.get("ownerFullName") or prospect.get("full_name", ""),
                "bio": prospect.get("bio", "")[:100],
                "generated_message": prospect.get("generated_message"),
                "profile_url": prospect.get("profile_url", ""),
                "status": prospect.get("status", "pending"),
                "message_sent": prospect.get("message_sent", False),
                "last_sent_at": prospect.get("last_sent_at")
            }
            
            if prospect.get("message_sent"):
                already_sent.append(prospect_info)
            elif prospect.get("generated_message"):
                ready_to_send.append(prospect_info)
            else:
                no_message.append({
                    "username": username,
                    "full_name": prospect.get("ownerFullName") or prospect.get("full_name", ""),
                    "bio": prospect.get("bio", "")[:100],
                    "status": "no_message"
                })
        
        return {
            "platform": platform,
            "summary": {
                "ready_to_send": len(ready_to_send),
                "already_sent": len(already_sent),
                "no_message_yet": len(no_message),
                "total": len(all_prospects)
            },
            "ready_to_send": ready_to_send,
            "already_sent": already_sent,
            "no_message_yet": no_message
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 6️⃣ Send to Selected Prospects
# ============================================================
@router.post("/send-selected/{client_id}")
async def send_to_selected_prospects(
    client_id: str,
    request: SelectiveMessageRequest,
    background_tasks: BackgroundTasks
):
    """
    Mark specific prospects as ready to send, then optionally send immediately.
    
    Request body:
    {
      "prospect_usernames": ["user1", "user2", "user3"],
      "send_immediately": true
    }
    """
    try:
        client_data = get_client_data(client_id)
        if not client_data:
            raise HTTPException(status_code=404, detail="Client not found")
        
        if not request.prospect_usernames:
            raise HTTPException(status_code=400, detail="No prospects specified")
        
        platform = client_data.get("platform", "")
        
        prospects_doc = audience_collection.find_one({
            "client_id": client_id,
            "platform": platform,
            "type": "prospects"
        })
        
        if not prospects_doc:
            raise HTTPException(status_code=404, detail="No prospects found")
        
        all_prospects = prospects_doc.get("prospects", [])
        selected_count = 0
        
        # Mark selected prospects as "ready_to_send"
        for idx, prospect in enumerate(all_prospects):
            p_username = (
                prospect.get("username") or 
                prospect.get("ownerUsername") or 
                prospect.get("pageName", "")
            ).strip().lower().lstrip('@')
            
            if any(u.strip().lower().lstrip('@') == p_username for u in request.prospect_usernames):
                if prospect.get("generated_message"):
                    audience_collection.update_one(
                        {
                            "client_id": client_id,
                            "platform": platform,
                            "type": "prospects"
                        },
                        {
                            "$set": {
                                f"prospects.{idx}.status": "ready_to_send",
                                f"prospects.{idx}.selected_at": datetime.utcnow().isoformat()
                            }
                        }
                    )
                    selected_count += 1
        
        if selected_count == 0:
            return {
                "message": "⚠️ No valid prospects found or messages not generated",
                "selected": 0
            }
        
        # If send_immediately, trigger the send-dms endpoint
        if request.send_immediately:
            background_tasks.add_task(send_dm_message, client_id)
            return {
                "message": f"🚀 Sending messages to {selected_count} selected prospects",
                "selected_count": selected_count,
                "status": "sending",
                "usernames": request.prospect_usernames
            }
        else:
            return {
                "message": f"✅ Marked {selected_count} prospects as ready to send",
                "selected_count": selected_count,
                "status": "ready",
                "usernames": request.prospect_usernames,
                "next_step": "Call POST /pipeline/send-dms/{client_id} to send messages"
            }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 7️⃣ Send DMs (Your existing endpoint - works with selection)
# ============================================================
@router.post("/send-dms/{client_id}")
async def send_dms_endpoint(client_id: str, background_tasks: BackgroundTasks):
    """
    Send messages to prospects marked as "ready_to_send".
    """
    background_tasks.add_task(send_dm_message, client_id)
    return {
        "message": "📤 Sending messages in background.",
        "tip": "Use /pipeline/prospects/{client_id}/ready to track statuses."
    }


# ============================================================
# 8️⃣ View All Prospects
# ============================================================
@router.get("/prospects/{client_id}")
async def get_prospects_endpoint(client_id: str, status: str = None):
    """
    Retrieve all prospects for a client (optionally filter by status).
    """
    prospects_data = get_prospects_from_audience(client_id, platform=None)
    prospects = prospects_data.get("prospects", [])
    
    if status:
        prospects = [p for p in prospects if p.get("status") == status]

    return {
        "count": len(prospects),
        "prospects": prospects
    }


# ============================================================
# 9️⃣ Get Sending Statistics
# ============================================================
@router.get("/stats/{client_id}")
async def get_sending_stats(client_id: str):
    """
    Get statistics about message sending for a client.
    """
    try:
        client_data = get_client_data(client_id)
        if not client_data:
            raise HTTPException(status_code=404, detail="Client not found")
        
        platform = client_data.get("platform", "")
        
        prospects_doc = audience_collection.find_one({
            "client_id": client_id,
            "platform": platform,
            "type": "prospects"
        })
        
        if not prospects_doc:
            return {
                "total_prospects": 0,
                "ready_to_send": 0,
                "sent": 0,
                "failed": 0,
                "pending": 0,
                "no_message": 0
            }
        
        all_prospects = prospects_doc.get("prospects", [])
        
        stats = {
            "total_prospects": len(all_prospects),
            "ready_to_send": 0,
            "sent": 0,
            "failed": 0,
            "pending": 0,
            "no_message": 0
        }
        
        for prospect in all_prospects:
            status = prospect.get("status", "pending")
            has_message = bool(prospect.get("generated_message"))
            
            if not has_message:
                stats["no_message"] += 1
            elif status == "ready_to_send":
                stats["ready_to_send"] += 1
            elif status == "sent" or prospect.get("message_sent"):
                stats["sent"] += 1
            elif status == "failed":
                stats["failed"] += 1
            else:
                stats["pending"] += 1
        
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

 