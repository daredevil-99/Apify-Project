from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import List
from services.db_service import register_client, get_client_data, get_prospects_from_audience
from services.scraping_service import scrape_and_store, extract_and_store_prospects
from services.pipeline_service import generate_messages_for_prospects
from services.dm_service import send_dm_message


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
    searchQuery: str
    profileScraperMode: str = "Full"
    startPage: int = 1


# ============================================================
# 1️⃣ Register Client
# ============================================================
@router.post("/register")
async def register_client_endpoint(client_data: ClientRegistration):
    """
    Register a new client and store their configuration (platform, keywords, etc.)
    """
    client_dict = client_data.dict()

    # Normalize field names for scraper compatibility
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
# 2️⃣ Scrape Platform Data (Instagram, LinkedIn, Facebook)
# ============================================================
@router.post("/scrape/{client_id}")
async def scrape_endpoint(
    client_id: str,
    background_tasks: BackgroundTasks,
    linkedin_input: LinkedInInput | None = None
):
    """
    Scrape posts/profiles from the client's target platform and save results in DB.
    Supports Instagram / LinkedIn / Facebook.
    """
    client_data = get_client_data(client_id)
    if not client_data:
        raise HTTPException(status_code=404, detail="Client not found")

    platform = client_data.get("platform", "").lower()

    # 🟣 Platform-specific logic
    if platform == "linkedin":
        if not linkedin_input:
            raise HTTPException(
                status_code=400,
                detail="LinkedIn scraping requires 'searchQuery' and optional mode/page"
            )
        background_tasks.add_task(
            scrape_and_store,
            client_id,
            platform,
            linkedin_input.searchQuery,
            linkedin_input.profileScraperMode,
            linkedin_input.startPage
        )
        return {
            "message": "🚀 LinkedIn scraping started",
            "client_id": client_id,
            "platform": "LinkedIn",
            "query": linkedin_input.searchQuery,
        }

    # 🟣 Instagram / Facebook logic
    search_terms = client_data.get("search_terms", [])
    profession = client_data.get("profession", "")
    location = client_data.get("location", "")

    if not search_terms:
        raise HTTPException(status_code=400, detail="No search terms found for this client")

    background_tasks.add_task(
        scrape_and_store,
        client_id,
        platform,
        search_terms,
        profession,
        location
    )

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

        platform = client_data.get("platform", "instagram")  # default if missing

        count = extract_and_store_prospects(client_id, platform)
        if count == 0:
            return {"message": "❌ No scraped posts found. Run /scrape first."}
        return {
            "message": f"✅ Extracted and stored {count} prospects.",
            "next": f"/pipeline/generate-messages/{client_id}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 4️⃣ Generate Outreach Messages
# ============================================================
@router.post("/generate-messages/{client_id}")
def generate_messages(client_id: str):
    """
    Step 4️⃣ - Generate personalized outreach messages for extracted prospects.
    """
    return generate_messages_for_prospects(client_id)


# ============================================================
# 5️⃣ Send DMs or Emails
# ============================================================
@router.post("/send-dms/{client_id}")
async def send_dms_endpoint(client_id: str, background_tasks: BackgroundTasks):
    """
    Automatically send generated messages (Instagram DM, LinkedIn, or Gmail).
    """
    background_tasks.add_task(send_dm_message, client_id)
    return {
        "message": "📤 Sending messages in background.",
        "tip": "Use /pipeline/prospects/{client_id} to track statuses."
    }


# ============================================================
# 6️⃣ View All Prospects (Status / Filter Optional)
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
