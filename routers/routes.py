# routers/routes.py
from fastapi import APIRouter, BackgroundTasks, HTTPException
from services.pipeline_service import generate_message
from services.scraping_service import extract_and_store_prospects
from services.db_service import (
    register_client, 
    get_client_data, 
    ClientRegistration,
    get_audience_collection,
    get_prospects_from_audience
)
from utils.serialization_utils import convert_objectid_to_str

router = APIRouter(prefix="/pipeline", tags=["Pipeline"])

# 0️⃣ Register client
@router.post("/register")
async def register_client_endpoint(data: ClientRegistration):
    return register_client(data)

# 1️⃣ Extract prospects from scraped data and store in DB
@router.post("/extract-prospects/{client_id}")
async def extract_prospects_endpoint(client_id: str, background_tasks: BackgroundTasks):
    """
    Extract prospect profiles from scraped posts and store in audience DB
    """
    client_data = get_client_data(client_id)
    if not client_data:
        raise HTTPException(status_code=404, detail="Client not found")
    
    platform = client_data["platform"]
    
    # Run extraction in background
    background_tasks.add_task(extract_and_store_prospects, client_id, platform)
    
    return {
        "message": "Prospect extraction started",
        "client_id": client_id,
        "platform": platform,
        "info": "Prospects will be extracted from scraped data and stored in audience collection"
    }

# 2️⃣ REMOVED - Display results endpoint removed as requested

# 2️⃣ Generate messages for prospects
@router.post("/generate-messages/{client_id}")
async def generate_messages_endpoint(client_id: str, background_tasks: BackgroundTasks):
    """
    Generate personalized messages for extracted prospects
    """
    client_data = get_client_data(client_id)
    if not client_data:
        raise HTTPException(status_code=404, detail="Client not found")
    
    # Check if prospects exist
    prospects_data = get_prospects_from_audience(client_id, client_data["platform"])
    if not prospects_data or not prospects_data.get('prospects'):
        raise HTTPException(
            status_code=400, 
            detail="No prospects found. Run /extract-prospects first."
        )
    
    # Generate messages in background
    background_tasks.add_task(generate_message, client_id)
    
    return {
        "message": "Message generation started",
        "client_id": client_id,
        "prospects_count": len(prospects_data.get('prospects', [])),
        "info": "Messages will be generated for all prospects"
    }

# 3️⃣ Get client status
@router.get("/client/{client_id}")
async def get_client_details(client_id: str):
    data = get_client_data(client_id)
    if not data:
        raise HTTPException(status_code=404, detail="Client not found")
    return convert_objectid_to_str(data)

# 4️⃣ Get scraped audience data (posts/content)
@router.get("/audience/{client_id}")
async def get_audience(client_id: str):
    """
    Get scraped posts/content data (not prospects)
    """
    client_data = get_client_data(client_id)
    if not client_data:
        raise HTTPException(status_code=404, detail="Client not found")

    platform = client_data["platform"]
    audience_collection = get_audience_collection()
    
    profiles = list(
        audience_collection.find(
            {"client_id": client_id, "platform": platform},
            {"_id": 0, "prospects": 0}  # Exclude prospects from this view
        )
        .sort("location_relevance_score", -1)
        .limit(10)
    )

    return {
        "client_id": client_id,
        "platform": platform,
        "total_profiles": len(profiles),
        "profiles": profiles
    }