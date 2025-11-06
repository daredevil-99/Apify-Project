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


def register_client(data: ClientRegistration) -> Dict:
    """Register new client and return UUID"""
    platform = data.platform.lower()
    if platform not in ["instagram", "linkedin", "facebook"]:
        return {"error": "Invalid platform"}

    client_id = str(uuid4())
    client_info = data.dict()
    client_info["client_id"] = client_id
    client_info["platform"] = platform
    client_info["status"] = "registered"
    client_info["created_at"] = datetime.utcnow()

    clients_collection.insert_one(client_info)
    return {
        "message": f"Client registered for {platform.upper()}",
        "client_id": client_id
    }


def get_client_data(client_id: str) -> Optional[Dict]:
    """Fetch client data by UUID"""
    return clients_collection.find_one({"client_id": client_id}, {"_id": 0})


def update_client_status(client_id: str, updates: Dict):
    """Update client status and metadata"""
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


def save_scraped_profiles(client_id: str, platform: str, profiles: List[Dict]) -> int:
    """Save Apify scraped profiles to MongoDB"""
    if not profiles:
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
            profile.get("profileUrl")
            or profile.get("publicIdentifier")
            or profile.get("username")
            or f"{platform}_{i}_{datetime.utcnow().timestamp()}"
        )
        profile["unique_key"] = unique_key
        
        # Mark if profile has content
        profile["has_valid_content"] = bool(
            profile.get("bio") or 
            profile.get("description") or 
            profile.get("posts") or
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


# ========================================
# PROSPECT MANAGEMENT FUNCTIONS
# ========================================

def get_prospects_from_audience(client_id: str, platform: str) -> Optional[Dict]:
    """
    Get prospects for a specific client from audience collection
    """
    # Find the prospects document
    prospects_doc = audience_collection.find_one(
        {
            "client_id": client_id,
            "platform": platform,
            "type": "prospects"
        },
        {"_id": 0}
    )
    
    return prospects_doc


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
    
    return result.modified_count > 0


def get_prospects_by_status(client_id: str, platform: str, status: str = "new") -> List[Dict]:
    """
    Get prospects filtered by status (new, contacted, replied, etc.)
    """
    prospects_doc = get_prospects_from_audience(client_id, platform)
    
    if not prospects_doc:
        return []
    
    prospects = prospects_doc.get('prospects', [])
    
    # Filter by status
    filtered = [p for p in prospects if p.get('status') == status]
    
    return filtered


def get_all_prospects(client_id: str, platform: str) -> List[Dict]:
    """
    Get all prospects for a client regardless of status
    """
    prospects_doc = get_prospects_from_audience(client_id, platform)
    
    if not prospects_doc:
        return []
    
    return prospects_doc.get('prospects', [])


def get_prospects_statistics(client_id: str, platform: str) -> Dict:
    """
    Get statistics about prospects for a client
    """
    prospects_doc = get_prospects_from_audience(client_id, platform)
    
    if not prospects_doc:
        return {
            "total": 0,
            "new": 0,
            "contacted": 0,
            "replied": 0,
            "verified": 0,
            "private": 0,
            "with_email": 0
        }
    
    prospects = prospects_doc.get('prospects', [])
    
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