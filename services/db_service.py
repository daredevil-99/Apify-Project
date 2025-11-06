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