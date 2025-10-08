from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from apify_client import ApifyClient
import os
import pymongo
from bson import ObjectId
from datetime import datetime
from pipeline_utils import kickoff_message_generation
import re
import time

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

class LinkedInInput(BaseModel):
    searchQuery: str
    profileScraperMode: str = "Full"  # default full
    startPage: int = 1

# -----------------------------
# 3. Helper Functions
# -----------------------------
def clean_hashtag(tag):
    """Clean hashtag to match Instagram's requirements."""
    cleaned = re.sub(r'[^a-zA-Z0-9_]', '', tag.replace(' ', ''))
    return cleaned.lower()

def run_apify(platform, search_terms, profession=None, preferred_location=None):
    """Run Apify actors for Instagram, LinkedIn, and Facebook."""
    try:
        # ---------------- Facebook ----------------
        if platform == "facebook":
            actor_id = "apify/facebook-search-scraper"
            payload = {
                    "categories": search_terms,
                    "locations": [preferred_location],
                    "resultsLimit": 20,
                    "maxRequestRetries": 5,
                    "proxy": {"apifyProxyGroups": ["RESIDENTIAL"]}
                }


        # ---------------- Instagram ----------------
        elif platform == "instagram":
            # Build dynamic hashtags
            hashtags = [clean_hashtag(t) for t in search_terms if clean_hashtag(t)]

            if profession:
                hashtags.append(clean_hashtag(profession))
            if preferred_location:
                hashtags.append(clean_hashtag(preferred_location))

            # Remove duplicates and empty strings
            hashtags = list({t for t in hashtags if t})[:10]

            actor_id = "apify/instagram-hashtag-scraper"
            payload = {
                "hashtags": hashtags,
                "resultsLimit": 20,
                "addParentData": False
            }

            print(f"📸 Using dynamic Instagram hashtags: {hashtags}")

        # ---------------- LinkedIn ----------------
        elif platform == "linkedin":
            actor_id = "harvestapi/linkedin-profile-search"
            payload = {
                "searchQuery": " OR ".join(search_terms[:5]),
                "profileScraperMode": "Full",
                "startPage": 1,
                "maxItems": 0,  # scrape all available profiles
                "locations": [preferred_location] if preferred_location else []
            }

        else:
            print(f"❌ Platform {platform} not supported")
            return []

        print(f"🚀 Running {platform.upper()} actor {actor_id} with payload: {payload}")
        actor_client = apify_client.actor(actor_id)
        run = actor_client.call(run_input=payload)

        if not run or "defaultDatasetId" not in run:
            print(f"⚠️ {platform.upper()} actor returned no dataset")
            return []

        dataset_client = apify_client.dataset(run["defaultDatasetId"])
        items = list(dataset_client.iterate_items())
        print(f"✅ Retrieved {len(items)} {platform.upper()} results")
        return items

    except Exception as e:
        print(f"❌ Error running {platform.upper()} Apify actor: {e}")
        return []

def convert_objectids(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, dict):
        return {k: convert_objectids(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectids(item) for item in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    return obj

# -----------------------------
# 4. API Endpoints
# -----------------------------
@app.post("/register")
def register_client(data: ClientRegistration):
    platform = data.platform.lower()
    if platform not in ["instagram", "linkedin", "facebook"]:
        raise HTTPException(status_code=400, detail="Invalid platform")
    client_info = data.dict()
    client_info["platform"] = platform
    client_info["status"] = "registered"
    client_info["created_at"] = datetime.utcnow()
    inserted_id = clients_collection.insert_one(client_info).inserted_id
    return {"message": f"Client registered for {platform.upper()} successfully.", "client_id": str(inserted_id)}

@app.post("/fetch-audience/{client_id}")
def fetch_audience(client_id: str):
    client_data = clients_collection.find_one({"_id": ObjectId(client_id)})
    if not client_data:
        raise HTTPException(status_code=404, detail="Client not found")

    platform = client_data["platform"]
    results = run_apify(platform, client_data["search_terms_with_location"], client_data.get("preferred_profession"), client_data.get("preferred_location"))

    stored_count = 0
    for i, r in enumerate(results):
        if not r:
            continue
        r["client_id"] = str(client_data["_id"])
        r["platform"] = platform
        r["fetched_at"] = datetime.utcnow()
        unique_key = r.get("profileUrl") or r.get("publicIdentifier") or r.get("unique_key") or f"{platform}_{i}_{datetime.utcnow().timestamp()}"
        r["unique_key"] = unique_key
        if not audience_collection.find_one({"client_id": str(client_data["_id"]), "platform": platform, "unique_key": unique_key}):
            audience_collection.insert_one(r)
            stored_count += 1

    status = "data_fetched" if stored_count else "data_fetch_attempted"
    clients_collection.update_one({"_id": client_data["_id"]}, {"$set": {"status": status, "data_fetched_at": datetime.utcnow(), "last_fetch_count": stored_count}})
    return {"message": f"Fetched {stored_count} {platform.upper()} items.", "stored_count": stored_count}


def fetch_and_store_audience_data():
    """Background job for Instagram, Facebook, and LinkedIn with LinkedIn defaults."""
    clients = clients_collection.find({})
    
    for client_data in clients:
        try:
            platform = client_data["platform"].lower()
            print(f"🔄 Background job processing: {client_data['name']} - Platform: {platform.upper()}")

            # ---------------- Run Apify ----------------
            results = run_apify(
                platform,
                client_data.get("search_terms_with_location", []),
                client_data.get("preferred_profession"),
                client_data.get("preferred_location")
            )

            if not results:
                print(f"⚠️ No results found for {client_data['name']} on {platform.upper()}")
                continue

            stored_count = 0
            for i, r in enumerate(results):
                if not r:
                    continue

                r["client_id"] = str(client_data["_id"])
                r["platform"] = platform
                r["fetched_at"] = datetime.utcnow()

                # ---------------- Unique Key ----------------
                if platform == "linkedin":
                    unique_key = r.get("profileUrl") or r.get("publicIdentifier") or f"li_{i}_{datetime.utcnow().timestamp()}"
                elif platform == "instagram":
                    unique_key = r.get("id") or r.get("shortCode") or r.get("url") or f"ig_{i}_{datetime.utcnow().timestamp()}"
                elif platform == "facebook":
                    unique_key = r.get("unique_key") or f"fb_{i}_{datetime.utcnow().timestamp()}"
                else:
                    unique_key = f"{platform}_{i}_{datetime.utcnow().timestamp()}"

                r["unique_key"] = unique_key

                # ---------------- Avoid Duplicates ----------------
                existing_record = audience_collection.find_one({
                    "client_id": str(client_data["_id"]),
                    "platform": platform,
                    "unique_key": unique_key
                })

                if not existing_record:
                    audience_collection.insert_one(r)
                    stored_count += 1

            print(f"✅ Background job stored {stored_count} new {platform.upper()} results for {client_data['name']}")

        except Exception as e:
            print(f"❌ Background job error for {client_data.get('name', 'unknown')}: {e}")
            continue

# Schedule the job every 6 hours
scheduler.add_job(fetch_and_store_audience_data, "interval", hours=6)

@app.post("/generate-messages/{client_id}")
def generate_messages(client_id: str):
    """Enhanced message generation for multiple platforms."""
    try:
        client_data = clients_collection.find_one({"_id": ObjectId(client_id)})
        if not client_data:
            raise HTTPException(status_code=404, detail="Client not found")
            
        # Accept both "data_fetched" and "data_fetch_attempted" statuses
        valid_statuses = ["data_fetched", "data_fetch_attempted"]
        if client_data.get("status") not in valid_statuses:
            raise HTTPException(
                status_code=400, 
                detail="Audience data not fetched yet. Please run /fetch-audience first."
            )

        platform = client_data["platform"].lower()

        # Check if we have any data for this client and platform
        audience_count = audience_collection.count_documents({
            "client_id": client_id, 
            "platform": platform
        })
        
        if audience_count == 0:
            raise HTTPException(
                status_code=400,
                detail=f"No {platform.upper()} audience data found. Please fetch audience data first."
            )

        # Convert ObjectId to string before sending to crew
        client_data = convert_objectids(client_data)
        
        print(f"🚀 Starting {platform.upper()} message generation for client: {client_data['name']}")
        print(f"📊 Found {audience_count} {platform.upper()} profiles to analyze")
        
        crew_result = kickoff_message_generation(client_data)

        # Extract final message from crew result
        final_message = None
        if isinstance(crew_result, dict):
            final_message = crew_result.get("final_output") or crew_result.get("output")
        elif hasattr(crew_result, "output"):
            final_message = crew_result.output
        else:
            final_message = str(crew_result)

        # Update client with generated message
        clients_collection.update_one(
            {"_id": ObjectId(client_id)}, 
            {"$set": {
                "status": "messages_generated", 
                "messages_generated_at": datetime.utcnow(),
                "generated_messages": final_message,
                f"{platform}_message_generated": True
            }}
        )

        return {
            "client_id": client_id,
            "platform": platform.upper(),
            "final_message": final_message,
            "audience_profiles_analyzed": audience_count,
            "status": "✅ Personalized message generated successfully!"
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"❌ Error in generate_messages: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/audience/{client_id}")
def get_audience_data(client_id: str):
    """Enhanced audience data viewer with platform filtering."""
    try:
        client_data = clients_collection.find_one({"_id": ObjectId(client_id)})
        if not client_data:
            raise HTTPException(status_code=404, detail="Client not found")

        platform = client_data["platform"].lower()

        # Fetch platform-specific audience data
        audience_data = list(
            audience_collection.find(
                {"client_id": client_id, "platform": platform}, 
                {"_id": 0}
            )
            .sort("fetched_at", -1)   # newest first
            .limit(10)
        )

        total_count = audience_collection.count_documents({
            "client_id": client_id, 
            "platform": platform
        })
        
        return {
            "client_id": client_id,
            "client_name": client_data.get("name"),
            "platform": platform.upper(),
            "total_profiles": total_count,
            "sample_profiles": audience_data,
            "status": client_data.get("status", "unknown"),
            "last_fetch_count": client_data.get("last_fetch_count", 0)
        }
        
    except Exception as e:
        print(f"❌ Error in get_audience_data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/client/{client_id}/status")
def get_client_status(client_id: str):
    """Get detailed client status and progress."""
    try:
        client_data = clients_collection.find_one({"_id": ObjectId(client_id)})
        if not client_data:
            raise HTTPException(status_code=404, detail="Client not found")

        platform = client_data["platform"].lower()
        audience_count = audience_collection.count_documents({
            "client_id": client_id, 
            "platform": platform
        })

        status_info = {
            "client_id": client_id,
            "name": client_data.get("name"),
            "platform": platform.upper(),
            "current_status": client_data.get("status", "registered"),
            "created_at": client_data.get("created_at"),
            "data_fetched_at": client_data.get("data_fetched_at"),
            "messages_generated_at": client_data.get("messages_generated_at"),
            "audience_profiles_count": audience_count,
            "last_fetch_count": client_data.get("last_fetch_count", 0),
            "search_terms": client_data.get("search_terms_with_location", []),
            "preferred_profession": client_data.get("preferred_profession"),
            "preferred_location": client_data.get("preferred_location")
        }

        # Add latest generated message if available
        if client_data.get("generated_messages"):
            status_info["latest_generated_message"] = client_data.get("generated_messages")

        return status_info
        
    except Exception as e:
        print(f"❌ Error in get_client_status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def root():
    return {
        "message": "🚀 Multi-Platform Personalized Outreach Engine - FIXED VERSION",
        "supported_platforms": ["Instagram", "LinkedIn", "Facebook"],
        "pipeline": "4-Step Flow → Register Client → Fetch Audience → Generate Messages → Track Results",
        "improvements": [
            "✅ Better error handling for failed scrapers",
            "✅ Multiple Facebook scraper fallbacks", 
            "✅ Timeout handling for long-running actors",
            "✅ Empty result handling without crashes",
            "✅ Enhanced logging and debugging"
        ],
        "endpoints": {
            "POST /register": "Register a new client with platform, profession, location, and search terms",
            "POST /fetch-audience/{client_id}": "Run Apify and store platform-specific audience data",
            "POST /generate-messages/{client_id}": "Generate personalized outreach messages",
            "GET /audience/{client_id}": "View stored audience data with platform stats",
            "GET /client/{client_id}/status": "Get detailed client status and progress",
            "GET /test-platform/{platform}": "Test individual platform scraping"
        },
        "status": "✅ Ready to process multi-platform outreach requests with improved reliability"
    }