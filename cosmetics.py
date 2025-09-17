from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from apify_client import ApifyClient
import os
import pymongo
from bson import ObjectId
from datetime import datetime
from pipeline_utils import kickoff_message_generation # ✅ Keep crew for message generation
from bson import ObjectId
from fastapi import FastAPI, HTTPException

# -----------------------------
# 1. ENV + Mongo Setup
# -----------------------------
load_dotenv()

APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
if not APIFY_API_TOKEN:
    raise ValueError("Missing APIFY_API_TOKEN. Please set it in .env file")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = pymongo.MongoClient(MONGO_URI)
db = client["cosmetics_app"]

clients_collection = db["clients"]
audience_collection = db["audience_data"]

apify_client = ApifyClient(APIFY_API_TOKEN)

app = FastAPI()
scheduler = BackgroundScheduler()
scheduler.start()

# -----------------------------
# 2. Models
# -----------------------------
class ClientRegistration(BaseModel):
    name: str
    role: str
    email: str
    platform: str  # linkedin / instagram / facebook
    search_terms_with_location: list[str]
    preferred_profession: str
    preferred_location: str

# -----------------------------
# 3. Helper: Run Apify Actor
# -----------------------------
def run_apify(platform, search_terms, profession=None, preferred_location=None):
    if profession or preferred_location:
        enriched_terms = []
        for term in search_terms:
            combined = term
            if profession:
                combined += f" {profession}"
            if preferred_location:
                combined += f" {preferred_location}"
            enriched_terms.append(combined)
        search_terms = enriched_terms

    if platform == "instagram":
        actor_id = "apify/instagram-hashtag-scraper"
        payload = {"hashtags": search_terms, "resultsLimit": 5, "addParentData": False}
    elif platform == "linkedin":
        actor_id = "curious_coder~linkedin-profile-scraper"
        payload = {"startUrls": [{"url": term} for term in search_terms], "maxItems": 10}
    elif platform == "facebook":
        actor_id = "scrapestorm~facebook-profiles-people-scraper"
        payload = {"startUrls": [{"url": term} for term in search_terms], "maxPosts": 10}
    else:
        raise ValueError(f"Unsupported platform: {platform}")

    try:
        print(f"🚀 Running actor {actor_id} with payload: {payload}")
        actor_client = apify_client.actor(actor_id)
        run = actor_client.call(run_input=payload)
        if not run:
            raise RuntimeError("Actor run returned no data")

        dataset_client = apify_client.dataset(run["defaultDatasetId"])
        items = dataset_client.list_items().items
        print(f"✅ Retrieved {len(items)} results from Apify dataset")
        return items
    except Exception as e:
        print(f"❌ Error running Apify actor: {e}")
        raise HTTPException(status_code=500, detail=f"Apify client error: {e}")

# -----------------------------
# 4. Background Job
# -----------------------------
def fetch_and_store_audience_data():
    clients = clients_collection.find({})
    for client_data in clients:
        try:
            results = run_apify(
                client_data["platform"],
                client_data["search_terms"],
                client_data.get("preferred_profession"),
                client_data.get("preferred_location")
            )
            stored_count = 0
            for r in results:
                r["client_id"] = str(client_data["_id"])
                r["platform"] = client_data["platform"]
                r["fetched_at"] = datetime.utcnow()
                unique_key = r.get("username") or r.get("profileId") or r.get("id")
                if not unique_key:
                    continue

                if not audience_collection.find_one(
                    {"client_id": str(client_data["_id"]), "unique_key": unique_key}
                ):
                    r["unique_key"] = unique_key
                    audience_collection.insert_one(r)
                    stored_count += 1
            print(f"✅ Stored {stored_count} new results for {client_data['name']}")
        except Exception as e:
            print(f"❌ Error fetching data for {client_data['name']}: {e}")

scheduler.add_job(fetch_and_store_audience_data, "interval", hours=6)

# -----------------------------
# 5. API Endpoints
# -----------------------------
@app.post("/register")
def register_client(data: ClientRegistration):
    """Register a new client with platform + search terms + profession + location."""
    client_info = data.dict()
    client_info["status"] = "registered"
    inserted_id = clients_collection.insert_one(client_info).inserted_id
    return {
        "message": "Client registered successfully",
        "client_id": str(inserted_id)
    }

@app.post("/fetch-audience/{client_id}")
def fetch_audience(client_id: str):
    """Run Apify to fetch audience data and store in MongoDB."""
    client_data = clients_collection.find_one({"_id": ObjectId(client_id)})
    if not client_data:
        raise HTTPException(status_code=404, detail="Client not found")

    results = run_apify(client_data["platform"], client_data["search_terms_with_location"])
    stored_count = 0
    for r in results:
        r["client_id"] = str(client_data["_id"])
        r["fetched_at"] = datetime.utcnow()
        unique_key = r.get("username") or r.get("profileId") or r.get("id")
        if not unique_key:
            continue
        if not audience_collection.find_one(
            {"client_id": str(client_data["_id"]), "unique_key": unique_key}
        ):
            r["unique_key"] = unique_key
            audience_collection.insert_one(r)
            stored_count += 1

    clients_collection.update_one({"_id": client_data["_id"]}, {"$set": {"status": "data_fetched"}})
    return {"message": f"Fetched and stored {stored_count} new results"}


# ✅ Helper to recursively convert ObjectIds to strings
def convert_objectids(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, dict):
        return {k: convert_objectids(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectids(item) for item in obj]
    return obj

@app.post("/generate-messages/{client_id}")
def generate_messages(client_id: str):
    """Generate personalized DMs AFTER data is stored in MongoDB."""
    client_data = clients_collection.find_one({"_id": ObjectId(client_id)})
    if not client_data:
        raise HTTPException(status_code=404, detail="Client not found")
    if client_data.get("status") != "data_fetched":
        raise HTTPException(status_code=400, detail="Audience data not fetched yet")

    # ✅ Fix: convert ObjectId to string before sending to crew
    client_data = convert_objectids(client_data)

    crew_result = kickoff_message_generation(client_data)

    final_message = None
    if isinstance(crew_result, dict):
        final_message = crew_result.get("final_output")
    elif hasattr(crew_result, "output"):
        final_message = crew_result.output

    clients_collection.update_one(
        {"_id": ObjectId(client_id)}, {"$set": {"status": "messages_generated"}}
    )

    return {"client_id": client_id, "final_message": final_message}


@app.get("/")
def root():
    return {
        "message": "🚀 Personalized Outreach Engine is Running",
        "pipeline": "3-Step Flow → Register Client → Fetch Audience → Generate Messages",
        "endpoints": [
            "/register (POST) - Register a new client with platform, profession, location, and search terms",
            "/fetch-audience/{client_id} (POST) - Run Apify and store audience data in MongoDB",
            "/generate-messages/{client_id} (POST) - Generate personalized DMs after data is stored",
            "/audience/{client_id} (GET) - View stored audience data for a client"
        ],
    }
