# routers/routes.py
from fastapi import APIRouter, BackgroundTasks, HTTPException
from services.pipeline_service import generate_message
from services.db_service import (
    register_client, 
    get_client_data, 
    ClientRegistration,
    get_audience_collection
)
from utils.serialization_utils import convert_objectid_to_str

router = APIRouter(prefix="/pipeline", tags=["Pipeline"])

# 0️⃣ Register client
@router.post("/register")
async def register_client_endpoint(data: ClientRegistration):
    return register_client(data)

# 1️⃣ Main pipeline: Scrape → Generate → Send
@router.post("/generate/{client_id}")
async def generate_pipeline(client_id: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(generate_message, client_id)
    return {
        "message": "Pipeline started",
        "client_id": client_id,
        "info": "Scraping will happen automatically if no data exists"
    }

# 2️⃣ Get client status
@router.get("/client/{client_id}")
async def get_client_details(client_id: str):
    data = get_client_data(client_id)
    if not data:
        raise HTTPException(status_code=404, detail="Client not found")
    return convert_objectid_to_str(data)

# 3️⃣ Get audience data
@router.get("/audience/{client_id}")
async def get_audience(client_id: str):
    client_data = get_client_data(client_id)
    if not client_data:
        raise HTTPException(status_code=404, detail="Client not found")

    platform = client_data["platform"]
    audience_collection = get_audience_collection()
    
    profiles = list(
        audience_collection.find(
            {"client_id": client_id, "platform": platform},
            {"_id": 0}
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
