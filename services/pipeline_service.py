# services/pipeline_service.py - FIXED VERSION

from datetime import datetime
from pipeline_utils import kickoff_message_generation
from utils.serialization_utils import convert_objectid_to_str
from services.db_service import (
    get_client_data, 
    get_prospects_from_audience, 
    update_client_generated_message,
    get_prospects_by_status
)
from db_config import audience_collection
from typing import List, Optional, Any, Union


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


def generate_message_for_specific_prospect(
    client_data: dict,
    prospect: dict,
    prospect_index: int,
    campaign_message: str = None
):
    """
    🎯 Generate a personalized message for a SPECIFIC user-selected prospect.
    
    Args:
        client_data: Client information from database
        prospect: The specific prospect dict to generate message for
        prospect_index: Index position in the prospects array (for DB update)
        campaign_message: Optional custom campaign message from user
    
    Returns:
        dict with success status and generated message
    """
    print(f"\n{'='*60}")
    print(f"🎯 Generating message for SELECTED prospect")
    print(f"{'='*60}\n")
    
    client_id = str(client_data.get("client_id", client_data.get("_id", "")))
    platform = client_data.get("platform", "").lower()
    
    # Get prospect identifier
    if platform == "linkedin":
        username = (
            prospect.get("full_name") or 
            prospect.get("fullName") or
            f"{prospect.get('firstName', '')} {prospect.get('lastName', '')}".strip()
        )
    else:
        username = (
            prospect.get("username") or 
            prospect.get("ownerUsername") or 
            prospect.get("pageName", "unknown")
        )
    
    print(f"👤 Prospect: {username}")
    print(f"📱 Platform: {platform}")
    print(f"📍 Index: {prospect_index}")
    
    # Use provided campaign message or get from client data
    if not campaign_message:
        campaign_message = client_data.get("campaign_message") or \
                          "I'd love to connect and explore potential collaboration opportunities."
    
    print(f"📝 Campaign message: {campaign_message[:80]}...")
    
    # Prepare client data with selected prospect info
    safe_client_data = convert_objectid_to_str(client_data)
    
    # Generate message via CrewAI pipeline
    try:
        print(f"🤖 Starting CrewAI message generation...")
        
        # ✅ FIX: Only pass parameters that kickoff_message_generation accepts
        result = kickoff_message_generation(
            client_data=safe_client_data,
            campaign_message=campaign_message,
            target_prospect=prospect,           # ✅ Pass prospect
            target_prospect_index=prospect_index # ✅ Pass index
        )
        
        if result.get("success") or result.get("username"):
            generated_username = result.get("username")
            message = result.get("message")
            
            print(f"✅ Message generated successfully for {generated_username}")
            print(f"💬 Message preview: {message[:100]}...")
            
            # Update client record
            update_client_generated_message(client_id, {
                "generated_messages": result,
                "generated_at": datetime.utcnow(),
                "target_prospect_username": generated_username,
                "message_status": "generated_not_sent",
                "campaign_message": campaign_message
            })
            
            return {
                "status": "success",
                "success": True,
                "username": generated_username,
                "message": message,
                "platform": platform,
                "prospect_index": prospect_index
            }
        else:
            error = result.get("error", "Unknown error")
            print(f"❌ Message generation failed: {error}")
            
            return {
                "status": "failed",
                "success": False,
                "error": error,
                "platform": platform
            }
    
    except Exception as e:
        print(f"❌ Exception during message generation: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            "status": "error",
            "success": False,
            "error": str(e),
            "platform": platform
        }


def generate_messages_for_prospects(client_id: str):
    """
    🧠 Generate personalized messages for NEW prospects (RANDOM MODE)
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
    
    # ✅ Call WITHOUT target_prospect for random mode
    result = kickoff_message_generation(safe_client_data)
    cleaned_message = safe_serialize(result)

    # 6️⃣ Save message to DB
    update_client_generated_message(client_id, {
        "generated_messages": cleaned_message,
        "generated_at": datetime.utcnow(),
        "target_prospect_username": target_prospect.get('username'),
        "message_status": "generated_not_sent"
    })

    print(f"✅ Message generated and saved for @{target_prospect.get('username')}")
    print(f"💬 Message preview: {str(cleaned_message)[:100]}...")
    print(f"\n✅ Message generation completed for {client_id}\n")

    return {
        "status": "success",
        "success": True,
        "platform": platform,
        "prospect_username": target_prospect.get('username'),
        "username": target_prospect.get('username'),
        "message_generated": True,
        "remaining_new_prospects": len(new_prospects) - 1,
        "result": cleaned_message
    }
def enrich_and_generate_messages(
    client_id: str,
    profile_urls: List[str],
    campaign_message: str = None
):
    """
    🎯 NEW FLOW: Enrich LinkedIn profiles with full data, then generate messages.
    
    Flow:
    1. User selects LinkedIn profile URLs from harvested prospects
    2. Enrich profiles using apimaestro/linkedin-profile-posts (gets posts, engagement)
    3. Generate personalized messages using enriched data
    
    Args:
        client_id: Client ID
        profile_urls: List of LinkedIn profile URLs to enrich and message
        campaign_message: Custom campaign message
        
    Returns:
        dict with enrichment and generation results
    """
    try:
        from utils.apify_utils import enrich_linkedin_profiles
        
        print(f"\n{'='*60}")
        print(f"🎯 ENRICHED MESSAGE GENERATION FLOW")
        print(f"{'='*60}\n")
        
        # 1️⃣ Get client data
        client_data = get_client_data(client_id)
        if not client_data:
            return {"error": "Client not found", "success": False}
        
        platform = client_data.get("platform", "").lower()
        if platform != "linkedin":
            return {
                "error": "This endpoint only works for LinkedIn",
                "success": False
            }
        
        print(f"👤 Client: {client_id}")
        print(f"📱 Platform: {platform}")
        print(f"🔗 Profiles to enrich: {len(profile_urls)}")
        
        # 2️⃣ Enrich profiles with apimaestro actor
        print(f"\n🔍 Step 1: Enriching LinkedIn profiles...")
        enriched_profiles = enrich_linkedin_profiles(profile_urls)
        
        if not enriched_profiles:
            return {
                "error": "Failed to enrich profiles",
                "success": False,
                "enriched_count": 0
            }
        
        print(f"✅ Successfully enriched {len(enriched_profiles)} profiles")
        
        # 3️⃣ Store enriched profiles in audience collection
        from db_config import audience_collection
        
        # Get or create prospects document
        prospects_doc = audience_collection.find_one({
            "client_id": client_id,
            "platform": platform,
            "type": "prospects"
        })
        
        if prospects_doc:
            existing_prospects = prospects_doc.get("prospects", [])
        else:
            existing_prospects = []
        
        # Add enriched profiles to prospects
        enriched_count = 0
        for enriched_profile in enriched_profiles:
            # Check if already exists (by profile URL)
            profile_url = enriched_profile.get("profileUrl") or enriched_profile.get("url")
            
            existing_index = None
            for idx, p in enumerate(existing_prospects):
                p_url = p.get("profile_url") or p.get("profileUrl") or p.get("url")
                if p_url and profile_url and p_url.lower() == profile_url.lower():
                    existing_index = idx
                    break
            
            # Update or add enriched data
            enriched_data = {
                "firstName": enriched_profile.get("firstName", ""),
                "lastName": enriched_profile.get("lastName", ""),
                "full_name": f"{enriched_profile.get('firstName', '')} {enriched_profile.get('lastName', '')}".strip(),
                "headline": enriched_profile.get("headline", ""),
                "about": enriched_profile.get("about", ""),
                "location": enriched_profile.get("location", ""),
                "profile_url": profile_url,
                "experience": enriched_profile.get("experience", []),
                "skills": enriched_profile.get("skills", []),
                "posts": enriched_profile.get("posts", []),
                "engagement": enriched_profile.get("engagement", {}),
                "enriched": True,
                "enriched_at": datetime.utcnow().isoformat(),
                "has_valid_content": True,
                "status": "new"
            }
            
            if existing_index is not None:
                # Update existing
                existing_prospects[existing_index].update(enriched_data)
            else:
                # Add new
                existing_prospects.append(enriched_data)
            
            enriched_count += 1
        
        # Save to DB
        audience_collection.update_one(
            {
                "client_id": client_id,
                "platform": platform,
                "type": "prospects"
            },
            {
                "$set": {
                    "prospects": existing_prospects,
                    "updated_at": datetime.utcnow()
                }
            },
            upsert=True
        )
        
        print(f"✅ Saved {enriched_count} enriched profiles to database")
        
        # 4️⃣ Generate messages for each enriched profile
        print(f"\n🤖 Step 2: Generating personalized messages...")
        
        results = []
        success_count = 0
        failed_count = 0
        
        for idx, enriched_profile in enumerate(enriched_profiles):
            try:
                # Find the prospect in the updated array
                profile_url = enriched_profile.get("profileUrl") or enriched_profile.get("url")
                
                prospect_index = None
                prospect = None
                for p_idx, p in enumerate(existing_prospects):
                    p_url = p.get("profile_url") or p.get("profileUrl")
                    if p_url and profile_url and p_url.lower() == profile_url.lower():
                        prospect_index = p_idx
                        prospect = p
                        break
                
                if not prospect:
                    print(f"⚠️ Prospect not found for {profile_url}")
                    continue
                
                full_name = prospect.get("full_name") or f"{prospect.get('firstName', '')} {prospect.get('lastName', '')}".strip()
                
                print(f"\n[{idx + 1}/{len(enriched_profiles)}] Generating message for {full_name}...")
                
                # Generate message using enriched data
                result = generate_message_for_specific_prospect(
                    client_data=client_data,
                    prospect=prospect,
                    prospect_index=prospect_index,
                    campaign_message=campaign_message
                )
                
                if result.get("success"):
                    success_count += 1
                    results.append({
                        "username": full_name,
                        "profile_url": profile_url,
                        "status": "success",
                        "message": result.get("message")
                    })
                    print(f"✅ Message generated successfully")
                else:
                    failed_count += 1
                    results.append({
                        "username": full_name,
                        "profile_url": profile_url,
                        "status": "failed",
                        "error": result.get("error")
                    })
                    print(f"❌ Failed: {result.get('error')}")
                
                # Rate limiting
                import time
                time.sleep(2)
                
            except Exception as e:
                print(f"❌ Error processing {enriched_profile.get('firstName', 'unknown')}: {e}")
                failed_count += 1
                results.append({
                    "username": enriched_profile.get("firstName", "unknown"),
                    "status": "error",
                    "error": str(e)
                })
        
        print(f"\n{'='*60}")
        print(f"✅ ENRICHMENT & MESSAGE GENERATION COMPLETE")
        print(f"{'='*60}")
        print(f"📊 Enriched profiles: {enriched_count}")
        print(f"✅ Messages generated: {success_count}")
        print(f"❌ Failed: {failed_count}")
        
        return {
            "success": True,
            "enriched_count": enriched_count,
            "messages_generated": success_count,
            "failed": failed_count,
            "results": results,
            "next_step": "Use GET /pipeline/prospects/{client_id}/ready to view messages"
        }
        
    except Exception as e:
        print(f"❌ Enrichment flow error: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e)
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