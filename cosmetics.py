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
# 3. Helper Functions
# -----------------------------
def clean_hashtag(tag):
    """Clean hashtag to match Instagram's requirements: no spaces, special chars, etc."""
    # Remove spaces and convert to lowercase
    cleaned = re.sub(r'[^a-zA-Z0-9_]', '', tag.replace(' ', ''))
    return cleaned.lower()

def run_apify(platform, search_terms, profession=None, preferred_location=None):
    """Enhanced multi-platform Apify runner"""
    
    if platform == "instagram":
        # For Instagram, we need clean hashtags without spaces or special characters
        hashtags = []
        
        # Add base search terms as hashtags
        for term in search_terms:
            clean_tag = clean_hashtag(term)
            if clean_tag and len(clean_tag) > 2:
                hashtags.append(clean_tag)
        
        # Add profession-based hashtags
        if profession:
            prof_tag = clean_hashtag(profession)
            if prof_tag and len(prof_tag) > 2:
                hashtags.append(prof_tag)
        
        # Add location-based hashtags
        if preferred_location:
            loc_tag = clean_hashtag(preferred_location)
            if loc_tag and len(loc_tag) > 2:
                hashtags.append(loc_tag)
        
        # Add some cosmetic/beauty related hashtags
        beauty_hashtags = ["makeup", "beauty", "cosmetics", "skincare", "makeupartist"]
        for tag in beauty_hashtags:
            if tag not in hashtags:
                hashtags.append(tag)
        
        # Remove duplicates and limit to 10
        hashtags = list(set(hashtags))[:10]
        
        actor_id = "apify/instagram-hashtag-scraper"
        payload = {
            "hashtags": hashtags, 
            "resultsLimit": 20,  # Increased for better selection
            "addParentData": False
        }
        
    elif platform == "linkedin":
        # For LinkedIn, construct search URLs or use profile scraper
        search_queries = []
        
        # Build comprehensive search terms
        for term in search_terms:
            query = term
            if profession:
                query += f" {profession}"
            if preferred_location:
                query += f" {preferred_location}"
            search_queries.append(query)
        
        # Add beauty industry specific terms
        beauty_terms = ["cosmetics industry", "beauty marketing", "skincare specialist"]
        for term in beauty_terms:
            combined_term = term
            if preferred_location:
                combined_term += f" {preferred_location}"
            search_queries.append(combined_term)
        
        actor_id = "curious_coder~linkedin-profile-scraper" 
        payload = {
            "startUrls": [{"url": f"https://www.linkedin.com/search/people/?keywords={query}"} for query in search_queries[:5]],
            "maxItems": 15
        }
        
    elif platform == "facebook":
        # For Facebook, use people search with enhanced terms
        search_queries = []
        
        for term in search_terms:
            query = term
            if profession:
                query += f" {profession}"
            if preferred_location:
                query += f" {preferred_location}"
            search_queries.append(query)
        
        # Add beauty-related searches
        beauty_searches = ["beauty blogger", "makeup artist", "skincare enthusiast"]
        for search in beauty_searches:
            combined_search = search
            if preferred_location:
                combined_search += f" {preferred_location}"
            search_queries.append(combined_search)
            
        actor_id = "scrapestorm~facebook-profiles-people-scraper"
        payload = {
            "startUrls": [{"url": f"https://www.facebook.com/search/people/?q={query}"} for query in search_queries[:5]],
            "maxPosts": 15
        }
        
    else:
        raise ValueError(f"Unsupported platform: {platform}")

    try:
        print(f"🚀 Running {platform.upper()} actor {actor_id}")
        print(f"📋 Payload: {payload}")
        
        actor_client = apify_client.actor(actor_id)
        run = actor_client.call(run_input=payload)
        
        if not run:
            raise RuntimeError("Actor run returned no data")

        dataset_client = apify_client.dataset(run["defaultDatasetId"])
        items = dataset_client.list_items().items
        
        print(f"✅ Retrieved {len(items)} {platform.upper()} results from Apify")
        return items
        
    except Exception as e:
        print(f"❌ Error running {platform.upper()} Apify actor: {e}")
        raise HTTPException(status_code=500, detail=f"Apify client error for {platform}: {e}")

# -----------------------------
# 4. Background Job
# -----------------------------
def fetch_and_store_audience_data():
    """Enhanced background job to fetch data from all platforms"""
    clients = clients_collection.find({})
    
    for client_data in clients:
        try:
            platform = client_data["platform"].lower()
            print(f"🔄 Processing client: {client_data['name']} - Platform: {platform.upper()}")
            
            results = run_apify(
                platform,
                client_data["search_terms_with_location"],
                client_data.get("preferred_profession"),
                client_data.get("preferred_location")
            )
            
            stored_count = 0
            for r in results:
                if not r:  # Skip empty results
                    continue
                    
                r["client_id"] = str(client_data["_id"])
                r["platform"] = platform
                r["fetched_at"] = datetime.utcnow()
                
                # Create platform-specific unique identifier
                unique_key = None
                
                if platform == "instagram":
                    unique_key = r.get("id") or r.get("shortCode") or r.get("url")
                elif platform == "linkedin":
                    unique_key = r.get("profileUrl") or r.get("publicIdentifier") or r.get("fullName")
                elif platform == "facebook":
                    unique_key = r.get("profileUrl") or r.get("id") or r.get("name")
                
                if not unique_key:
                    print(f"⚠️ Skipping {platform} record without unique identifier")
                    continue

                # Check for duplicates
                existing_record = audience_collection.find_one({
                    "client_id": str(client_data["_id"]), 
                    "platform": platform,
                    "unique_key": unique_key
                })
                
                if not existing_record:
                    r["unique_key"] = unique_key
                    audience_collection.insert_one(r)
                    stored_count += 1
                    
            print(f"✅ Stored {stored_count} new {platform.upper()} results for {client_data['name']}")
            
        except Exception as e:
            print(f"❌ Error fetching {client_data.get('platform', 'unknown')} data for {client_data['name']}: {e}")

# Schedule the job to run every 6 hours
scheduler.add_job(fetch_and_store_audience_data, "interval", hours=6)

# -----------------------------
# 5. Helper Functions
# -----------------------------
def convert_objectids(obj):
    """Helper to recursively convert ObjectIds to strings"""
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
# 6. API Endpoints
# -----------------------------
@app.post("/register")
def register_client(data: ClientRegistration):
    """Register a new client with platform + search terms + profession + location."""
    
    # Validate platform
    valid_platforms = ["instagram", "linkedin", "facebook"]
    if data.platform.lower() not in valid_platforms:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid platform. Must be one of: {', '.join(valid_platforms)}"
        )
    
    client_info = data.dict()
    client_info["platform"] = data.platform.lower()  # Normalize platform
    client_info["status"] = "registered"
    client_info["created_at"] = datetime.utcnow()
    
    inserted_id = clients_collection.insert_one(client_info).inserted_id
    
    return {
        "message": f"Client registered successfully for {data.platform.upper()}",
        "client_id": str(inserted_id),
        "platform": data.platform.lower(),
        "next_steps": "Use /fetch-audience/{client_id} to collect prospect data"
    }

@app.post("/fetch-audience/{client_id}")
def fetch_audience(client_id: str):
    """Enhanced audience fetching for multiple platforms."""
    try:
        client_data = clients_collection.find_one({"_id": ObjectId(client_id)})
        if not client_data:
            raise HTTPException(status_code=404, detail="Client not found")

        platform = client_data["platform"].lower()
        print(f"🎯 Fetching {platform.upper()} audience for client: {client_data['name']}")

        results = run_apify(
            platform, 
            client_data["search_terms_with_location"],
            client_data.get("preferred_profession"),
            client_data.get("preferred_location")
        )
        
        stored_count = 0
        skipped_count = 0
        
        for r in results:
            if not r:  # Skip empty results
                skipped_count += 1
                continue
                
            r["client_id"] = str(client_data["_id"])
            r["platform"] = platform
            r["fetched_at"] = datetime.utcnow()
            
            # Create platform-specific unique key
            unique_key = None
            if platform == "instagram":
                unique_key = r.get("id") or r.get("shortCode") or r.get("url")
            elif platform == "linkedin":
                unique_key = r.get("profileUrl") or r.get("publicIdentifier") or r.get("fullName")
            elif platform == "facebook":
                unique_key = r.get("profileUrl") or r.get("id") or r.get("name")
            
            if not unique_key:
                skipped_count += 1
                continue
                
            # Check for duplicates
            if not audience_collection.find_one(
                {"client_id": str(client_data["_id"]), "platform": platform, "unique_key": unique_key}
            ):
                r["unique_key"] = unique_key
                audience_collection.insert_one(r)
                stored_count += 1
            else:
                skipped_count += 1

        # Update client status
        clients_collection.update_one(
            {"_id": client_data["_id"]}, 
            {"$set": {
                "status": "data_fetched", 
                "data_fetched_at": datetime.utcnow(),
                f"{platform}_profiles_count": stored_count
            }}
        )
        
        return {
            "message": f"Successfully processed {platform.upper()} audience data",
            "platform": platform,
            "stored_new": stored_count,
            "skipped_duplicates": skipped_count,
            "total_processed": len(results),
            "next_step": "Use /generate-messages/{client_id} to create personalized outreach"
        }
    
    except Exception as e:
        print(f"❌ Error in fetch_audience: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/generate-messages/{client_id}")
def generate_messages(client_id: str):
    """Enhanced message generation for multiple platforms."""
    try:
        client_data = clients_collection.find_one({"_id": ObjectId(client_id)})
        if not client_data:
            raise HTTPException(status_code=404, detail="Client not found")
            
        if client_data.get("status") != "data_fetched":
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
        audience_data = list(audience_collection.find(
            {"client_id": client_id, "platform": platform}, 
            {"_id": 0}
        ).limit(10))  # Limit to 10 for display
        
        total_count = audience_collection.count_documents({
            "client_id": client_id, 
            "platform": platform
        })
        
        # Get platform-specific stats
        platform_stats = {}
        if platform == "instagram":
            platform_stats = {
                "avg_likes": audience_collection.aggregate([
                    {"$match": {"client_id": client_id, "platform": platform}},
                    {"$group": {"_id": None, "avg_likes": {"$avg": "$likesCount"}}}
                ]),
                "total_hashtags": len(set([tag for doc in audience_data for tag in doc.get("hashtags", [])]))
            }
        elif platform == "linkedin":
            platform_stats = {
                "industries": list(set([doc.get("industry", "") for doc in audience_data if doc.get("industry")])),
                "avg_connections": audience_collection.aggregate([
                    {"$match": {"client_id": client_id, "platform": platform}},
                    {"$group": {"_id": None, "avg_connections": {"$avg": "$connectionsCount"}}}
                ])
            }
        elif platform == "facebook":
            platform_stats = {
                "locations": list(set([doc.get("location", "") for doc in audience_data if doc.get("location")])),
                "avg_friends": audience_collection.aggregate([
                    {"$match": {"client_id": client_id, "platform": platform}},
                    {"$group": {"_id": None, "avg_friends": {"$avg": "$friendsCount"}}}
                ])
            }
        
        return {
            "client_id": client_id,
            "client_name": client_data.get("name"),
            "platform": platform.upper(),
            "total_profiles": total_count,
            "sample_profiles": audience_data,
            "platform_stats": platform_stats,
            "status": client_data.get("status", "unknown")
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
        "message": "🚀 Multi-Platform Personalized Outreach Engine",
        "supported_platforms": ["Instagram", "LinkedIn", "Facebook"],
        "pipeline": "4-Step Flow → Register Client → Fetch Audience → Generate Messages → Track Results",
        "endpoints": {
            "POST /register": "Register a new client with platform, profession, location, and search terms",
            "POST /fetch-audience/{client_id}": "Run Apify and store platform-specific audience data",
            "POST /generate-messages/{client_id}": "Generate personalized outreach messages",
            "GET /audience/{client_id}": "View stored audience data with platform stats",
            "GET /client/{client_id}/status": "Get detailed client status and progress"
        },
        "status": "✅ Ready to process multi-platform outreach requests"
    }