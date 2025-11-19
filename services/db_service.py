

# services/db_service.py
from datetime import datetime
from typing import Optional, Dict, List
from uuid import uuid4
from pydantic import BaseModel
from db_config import get_clients_collection, get_audience_collection

clients_collection = get_clients_collection()
audience_collection = get_audience_collection()


class ClientRegistration(BaseModel):
    name: str
    role: str
    email: str
    platform: str
    search_terms_with_location: List[str]
    preferred_profession: str
    preferred_location: str


def register_client(data: dict) -> str:
    """Register new client and return UUID"""
    platform = data.get("platform", "").lower()
    if platform not in ["instagram", "linkedin", "facebook", "gmail"]:
        raise ValueError("Invalid platform")

    client_id = str(uuid4())
    client_info = data.copy()
    client_info["client_id"] = client_id
    client_info["platform"] = platform
    client_info["status"] = "registered"
    client_info["created_at"] = datetime.utcnow()

    clients_collection.insert_one(client_info)
    print(f"✅ Registered client: {client_id}")
    return client_id


def get_client_data(client_id: str) -> Optional[Dict]:
    """Fetch client data by UUID"""
    return clients_collection.find_one({"client_id": client_id}, {"_id": 0})


def update_client_status(client_id: str, updates: Dict = None, **kwargs):
    """Update client status and metadata"""
    if updates is None:
        updates = {}
    updates.update(kwargs)
    updates["updated_at"] = datetime.utcnow()
    
    clients_collection.update_one(
        {"client_id": client_id},
        {"$set": updates}
    )


def update_client_generated_message(client_id: str, message_data: Dict):
    """Save generated message to client record"""
    clients_collection.update_one(
        {"client_id": client_id},
        {"$set": message_data}
    )


# ============================================================
# COMPATIBILITY LAYER FOR SCRAPING SERVICE
# ============================================================

def save_scraped_data(client_id: str, platform: str, profiles: List[Dict]) -> int:
    """
    Save scraped profiles - COMPATIBILITY WRAPPER
    Maps to your existing save_scraped_profiles function
    """
    if not profiles:
        print("⚠️  No items to save")
        return 0

    saved_count = 0
    for i, profile in enumerate(profiles):
        if not profile:
            continue

        profile["client_id"] = client_id
        profile["platform"] = platform
        profile["fetched_at"] = datetime.utcnow()
        
        # Generate unique key
        unique_key = (
            profile.get("linkedinUrl")
            or profile.get("profileUrl")
            or profile.get("publicIdentifier")
            or profile.get("username")
            or profile.get("ownerUsername")
            or f"{platform}_{i}_{datetime.utcnow().timestamp()}"
        )
        profile["unique_key"] = unique_key
        
        # Mark if profile has content
        profile["has_valid_content"] = bool(
            profile.get("bio") or 
            profile.get("summary") or
            profile.get("headline") or
            profile.get("description") or 
            profile.get("caption") or
            profile.get("firstName") or
            profile.get("fullName")
        )

        # Upsert to avoid duplicates
        audience_collection.update_one(
            {
                "client_id": client_id,
                "platform": platform,
                "unique_key": unique_key
            },
            {"$set": profile},
            upsert=True
        )
        saved_count += 1

    # Update client fetch stats
    update_client_status(client_id, {
        "status": "data_fetched" if saved_count else "data_fetch_attempted",
        "data_fetched_at": datetime.utcnow(),
        "last_fetch_count": saved_count
    })

    print(f"💾 Saved {saved_count} {platform} profiles for {client_id}")
    return saved_count


def get_scraped_posts(client_id: str, platform: str) -> List[Dict]:
    """
    Retrieve scraped profiles - COMPATIBILITY WRAPPER
    Returns profiles from audience collection
    """
    cursor = audience_collection.find({
        "client_id": client_id,
        "platform": platform,
        "unique_key": {"$exists": True}  # Only get scraped profiles
    })
    
    posts = list(cursor)
    
    # Remove MongoDB _id
    for post in posts:
        post.pop("_id", None)
    
    print(f"📊 Retrieved {len(posts)} scraped posts for {client_id}")
    return posts


def save_prospects(client_id: str, platform: str, prospects: List[Dict]):
    """
    Save extracted prospects - COMPATIBILITY WRAPPER
    Stores prospects in the same collection with a special marker
    """
    if not prospects:
        print("⚠️  No prospects to save")
        return
    
    # Create a prospects document
    prospects_doc = {
        "client_id": client_id,
        "platform": platform,
        "type": "prospects",
        "created_at": datetime.utcnow(),
        "prospects": []
    }
    
    # Check if prospects doc already exists
    existing = audience_collection.find_one({
        "client_id": client_id,
        "platform": platform,
        "type": "prospects"
    })
    
    if existing:
        # Update existing
        existing_prospects = existing.get("prospects", [])
        existing_usernames = {p.get("username") for p in existing_prospects}
        
        new_prospects = []
        for prospect in prospects:
            if prospect.get("username") not in existing_usernames:
                new_prospects.append(prospect)
        
        if new_prospects:
            audience_collection.update_one(
                {
                    "client_id": client_id,
                    "platform": platform,
                    "type": "prospects"
                },
                {"$push": {"prospects": {"$each": new_prospects}}}
            )
            print(f"💾 Added {len(new_prospects)} new prospects for client {client_id}")
    else:
        # Create new prospects document
        prospects_doc["prospects"] = prospects
        audience_collection.insert_one(prospects_doc)
        print(f"💾 Saved {len(prospects)} prospects for client {client_id}")


# ========================================
# ORIGINAL PROSPECT MANAGEMENT FUNCTIONS
# ========================================

def get_prospects_from_audience(client_id: str, platform: str = None) -> Dict:
    """
    Get prospects for a specific client from audience collection
    """
    query = {
        "client_id": client_id,
        "type": "prospects"
    }
    
    if platform:
        query["platform"] = platform
    
    prospects_doc = audience_collection.find_one(query, {"_id": 0})
    
    if not prospects_doc:
        return {
            "client_id": client_id,
            "platform": platform,
            "count": 0,
            "prospects": []
        }
    
    prospects = prospects_doc.get("prospects", [])
    
    return {
        "client_id": client_id,
        "platform": platform,
        "count": len(prospects),
        "prospects": prospects
    }


def update_prospect_status(client_id: str, platform: str, username: str, 
                          status: str, contacted_date: str = None) -> bool:
    """
    Update the status of a specific prospect
    """
    update_data = {
        "prospects.$.status": status,
        "prospects.$.last_contacted": contacted_date or datetime.utcnow().isoformat()
    }
    
    result = audience_collection.update_one(
        {
            "client_id": client_id,
            "platform": platform,
            "type": "prospects",
            "prospects.username": username
        },
        {"$set": update_data}
    )
    
    if result.modified_count > 0:
        print(f"✅ Updated @{username} status to: {status}")
    else:
        print(f"⚠️  No prospect found to update: @{username}")
    
    return result.modified_count > 0


def get_prospects_by_status(client_id: str, platform: str, status: str = "new") -> List[Dict]:
    """
    Get prospects filtered by status (new, contacted, replied, etc.)
    """
    prospects_data = get_prospects_from_audience(client_id, platform)
    prospects = prospects_data.get('prospects', [])
    
    # Filter by status
    filtered = [p for p in prospects if p.get('status') == status]
    
    print(f"🎯 Found {len(filtered)} {status} prospects for {client_id}")
    return filtered


def get_all_prospects(client_id: str, platform: str) -> List[Dict]:
    """
    Get all prospects for a client regardless of status
    """
    prospects_data = get_prospects_from_audience(client_id, platform)
    return prospects_data.get('prospects', [])


def count_prospects(client_id: str, platform: str) -> int:
    """Count audience profiles for client"""
    return audience_collection.count_documents({
        "client_id": client_id,
        "platform": platform
    })


def get_prospect_profiles(client_id: str, platform: str, limit: int = 10) -> List[Dict]:
    """Get top prospect profiles sorted by location relevance"""
    return list(
        audience_collection.find(
            {
                "client_id": client_id,
                "platform": platform,
                "has_valid_content": True
            },
            {"_id": 0}
        )
        .sort("location_relevance_score", -1)
        .limit(limit)
    )


def get_prospects_statistics(client_id: str, platform: str) -> Dict:
    """
    Get statistics about prospects for a client
    """
    prospects_data = get_prospects_from_audience(client_id, platform)
    prospects = prospects_data.get('prospects', [])
    
    stats = {
        "total": len(prospects),
        "new": sum(1 for p in prospects if p.get('status') == 'new'),
        "contacted": sum(1 for p in prospects if p.get('status') == 'contacted'),
        "replied": sum(1 for p in prospects if p.get('status') == 'replied'),
        "verified": sum(1 for p in prospects if p.get('is_verified', False)),
        "private": sum(1 for p in prospects if p.get('is_private', False)),
        "with_email": sum(1 for p in prospects if p.get('email'))
    }
    
    return stats


def get_client_stats(client_id: str) -> Dict:
    """Get overall client statistics"""
    client = get_client_data(client_id)
    if not client:
        return {}
    
    platform = client.get("platform")
    stats = get_prospects_statistics(client_id, platform)
    
    return {
        "client_id": client_id,
        "platform": platform,
        **stats,
        "conversion_rate": (stats["replied"] / stats["contacted"] * 100) if stats["contacted"] > 0 else 0
    }