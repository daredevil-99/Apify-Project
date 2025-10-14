from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from apify_client import ApifyClient
import os
import pymongo
from bson import ObjectId
from datetime import datetime
from uuid import uuid4
import re
import time
import threading
import uuid
from pipeline_utils import kickoff_message_generation
import asyncio
from concurrent.futures import ThreadPoolExecutor


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
# 2. Global Task Tracker
# -----------------------------
tasks = {}  # store running background jobs

# -----------------------------
# 3. Models
# -----------------------------
class ClientRegistration(BaseModel):
    name: str
    role: str
    email: str
    platform: str
    search_terms_with_location: list[str]
    preferred_profession: str
    preferred_location: str

class LinkedInInput(BaseModel):
    searchQuery: str
    profileScraperMode: str = "Full"  # default full
    startPage: int = 1

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

# -----------------------------
# 4. Register Client with UUID
# -----------------------------
@app.post("/register")
def register_client(data: ClientRegistration):
    platform = data.platform.lower()
    if platform not in ["instagram", "linkedin", "facebook"]:
        raise HTTPException(status_code=400, detail="Invalid platform")

    client_id = str(uuid4())  # generate permanent unique client_id
    client_info = data.dict()
    client_info["client_id"] = client_id
    client_info["platform"] = platform
    client_info["status"] = "registered"
    client_info["created_at"] = datetime.utcnow()

    clients_collection.insert_one(client_info)
    return {"message": f"Client registered for {platform.upper()} successfully.", "client_id": client_id}


# -----------------------------
# 5. Background Task Function
# -----------------------------
def run_fetch_audience_task(task_id: str, client_id: str):
    try:
        tasks[task_id]["status"] = "running"
        client_data = clients_collection.find_one({"client_id": client_id})
        if not client_data:
            raise Exception("Client not found")

        platform = client_data["platform"]
        results = run_apify(
            platform,
            client_data["search_terms_with_location"],
            client_data.get("preferred_profession"),
            client_data.get("preferred_location"),
        )

        stored_count = 0
        for i, r in enumerate(results):
            if not r:
                continue
            r["client_id"] = client_id
            r["platform"] = platform
            r["fetched_at"] = datetime.utcnow()
            unique_key = (
                r.get("profileUrl")
                or r.get("publicIdentifier")
                or r.get("unique_key")
                or f"{platform}_{i}_{datetime.utcnow().timestamp()}"
            )
            r["unique_key"] = unique_key
            if not audience_collection.find_one(
                {"client_id": client_id, "platform": platform, "unique_key": unique_key}
            ):
                audience_collection.insert_one(r)
                stored_count += 1

        status = "data_fetched" if stored_count else "data_fetch_attempted"
        clients_collection.update_one(
            {"client_id": client_id},
            {
                "$set": {
                    "status": status,
                    "data_fetched_at": datetime.utcnow(),
                    "last_fetch_count": stored_count,
                }
            },
        )

        tasks[task_id]["status"] = "completed"
        tasks[task_id]["result"] = {"stored_count": stored_count, "platform": platform}

    except Exception as e:
        tasks[task_id]["status"] = f"failed: {str(e)}"

    finally:
        # Clean up old task data
        print(f"🧹 Cleaning up task {task_id}")
        time.sleep(2)  # slight delay for visibility
        del tasks[task_id]


# -----------------------------
# 6. Async Endpoint with Task ID
# -----------------------------
@app.post("/fetch-audience/{client_id}")
async def fetch_audience(client_id: str, background_tasks: BackgroundTasks):
    task_id = str(uuid4())
    tasks[task_id] = {
        "status": "pending",
        "client_id": client_id,
        "start_time": datetime.utcnow(),
    }
    background_tasks.add_task(run_fetch_audience_task, task_id, client_id)
    return {"task_id": task_id, "message": "Audience fetch started in background."}


# -----------------------------
# 7. Task Status Checker
# -----------------------------
@app.get("/task-status/{task_id}")
def get_task_status(task_id: str):
    task_info = tasks.get(task_id)
    if not task_info:
        return {"task_id": task_id, "status": "not found or completed (cleaned up)"}
    return {"task_id": task_id, "status": task_info["status"], "details": task_info}


# -----------------------------
# Background task to generate message and update DB
# -----------------------------
def convert_objectid_to_str(data):
    """Recursively convert ObjectId fields to strings in dictionaries and lists."""
    if isinstance(data, dict):
        return {key: convert_objectid_to_str(value) for key, value in data.items() if key != '_id'}
    elif isinstance(data, list):
        return [convert_objectid_to_str(item) for item in data]
    elif isinstance(data, ObjectId):
        return str(data)
    elif isinstance(data, datetime):
        return data.isoformat()
    else:
        return data

def convert_datetime_to_str(obj):
    """Recursively convert datetime objects to ISO format strings."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: convert_datetime_to_str(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime_to_str(item) for item in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj


def serialize_crew_output(crew_output):
    """
    Convert CrewOutput object to a JSON-serializable dictionary.
    Handles nested objects like TaskOutput and UsageMetrics.
    """
    try:
        result = {
            "raw_message": crew_output.raw if hasattr(crew_output, 'raw') else str(crew_output),
            "generated_at": datetime.utcnow().isoformat()
        }
        
        # Extract token usage if available
        if hasattr(crew_output, 'token_usage') and crew_output.token_usage:
            result["token_usage"] = {
                "total_tokens": crew_output.token_usage.total_tokens,
                "prompt_tokens": crew_output.token_usage.prompt_tokens,
                "completion_tokens": crew_output.token_usage.completion_tokens,
                "successful_requests": crew_output.token_usage.successful_requests
            }
        
        # Extract task outputs if available
        if hasattr(crew_output, 'tasks_output') and crew_output.tasks_output:
            result["tasks_output"] = []
            for task in crew_output.tasks_output:
                task_data = {
                    "agent": task.agent if hasattr(task, 'agent') else "Unknown",
                    "summary": task.summary if hasattr(task, 'summary') else "",
                    "raw": task.raw if hasattr(task, 'raw') else ""
                }
                result["tasks_output"].append(task_data)
        
        return result
    except Exception as e:
        print(f"⚠️ Error serializing CrewOutput: {e}")
        # Fallback: return just the string representation
        return {
            "raw_message": str(crew_output),
            "generated_at": datetime.utcnow().isoformat(),
            "error": f"Partial serialization: {str(e)}"
        }


def convert_datetime_to_str(obj):
    """Recursively convert datetime objects to ISO format strings."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: convert_datetime_to_str(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime_to_str(item) for item in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj


async def generate_message_task_async(client_id: str, task_id: str):
    """Generate message for client asynchronously using ThreadPoolExecutor."""
    tasks[task_id] = {
        "status": "running", 
        "client_id": client_id, 
        "start_time": datetime.utcnow()
    }
    
    try:
        # ✅ Exclude _id field to avoid ObjectId serialization issues
        client_data = clients_collection.find_one(
            {"client_id": client_id},
            {"_id": 0}  # Exclude MongoDB's _id field
        )
        
        if not client_data:
            tasks[task_id]["status"] = "failed: client not found"
            print(f"❌ Client {client_id} not found in database")
            return

        # ✅ Convert all datetime objects to strings before processing
        client_data = convert_datetime_to_str(client_data)

        print(f"🎯 Generating message for client {client_id} - {client_data.get('platform')}")
        
        # Run kickoff_message_generation in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as pool:
            generated_message = await loop.run_in_executor(
                pool, 
                kickoff_message_generation, 
                client_data
            )

        print(f"✅ Message generation completed for client {client_id}")
        
        # ✅ CRITICAL: Serialize the CrewOutput object before saving to MongoDB
        serialized_message = serialize_crew_output(generated_message)
        
        print(f"📝 Serialized message data: {serialized_message.get('raw_message', '')[:100]}...")
        
        # Update MongoDB with serializable data
        update_result = clients_collection.update_one(
            {"client_id": client_id},
            {
                "$set": {
                    "generated_messages": serialized_message,
                    "messages_generated_at": datetime.utcnow(),
                    "current_status": "message_generated"
                }
            }
        )
        
        if update_result.modified_count > 0:
            print(f"✅ Message saved to database for client {client_id}")
        else:
            print(f"⚠️ Database update returned 0 modified documents for client {client_id}")

        # Update task status
        tasks[task_id]["status"] = "completed"
        tasks[task_id]["result"] = serialized_message
        
    except Exception as e:
        error_msg = str(e)
        tasks[task_id]["status"] = f"failed: {error_msg}"
        tasks[task_id]["result"] = {"error": error_msg}
        print(f"❌ Error generating message for client {client_id}: {error_msg}")
        
        # Try to update the client status to reflect the error
        try:
            clients_collection.update_one(
                {"client_id": client_id},
                {
                    "$set": {
                        "current_status": "message_generation_failed",
                        "last_error": error_msg,
                        "error_at": datetime.utcnow()
                    }
                }
            )
        except Exception as db_error:
            print(f"❌ Failed to update error status in database: {db_error}")
            
# -----------------------------
# Endpoint to trigger async message generation
# -----------------------------
@app.post("/generate-messages/{client_id}")
async def generate_messages(client_id: str, background_tasks: BackgroundTasks):
    client_data = clients_collection.find_one({"client_id": client_id})
    if not client_data:
        raise HTTPException(status_code=404, detail="Client not found")

    task_id = str(uuid4())
    tasks[task_id] = {"status": "pending", "client_id": client_id, "start_time": datetime.utcnow()}

    # Add async-safe background task
    background_tasks.add_task(generate_message_task_async, client_id, task_id)

    return {
        "client_id": client_id,
        "task_id": task_id,
        "message": "Message generation started in background. Check /task-status/<task_id> or /client/<client_id>/message"
    }


# -----------------------------
# Endpoint to fetch generated message
# -----------------------------
@app.get("/client/{client_id}/message")
async def get_generated_message(client_id: str):
    client_data = clients_collection.find_one({"client_id": client_id})
    if not client_data:
        raise HTTPException(status_code=404, detail="Client not found")

    if client_data.get("generated_messages"):
        return {
            "client_id": client_id,
            "generated_message": client_data["generated_messages"],
            "messages_generated_at": client_data.get("messages_generated_at"),
            "current_status": client_data.get("current_status"),
        }
    else:
        return {
            "client_id": client_id,
            "message": "Message not generated yet. Please try again in a few seconds.",
            "current_status": client_data.get("current_status"),
        }



@app.get("/audience/{client_id}")
def get_audience_data(client_id: str):
    """View stored audience data with platform filtering (UUID-safe)."""
    try:
        # ✅ Use client_id string instead of ObjectId
        client_data = clients_collection.find_one({"client_id": client_id})
        if not client_data:
            raise HTTPException(status_code=404, detail="Client not found")

        platform = client_data["platform"].lower()

        # Fetch platform-specific audience data
        audience_data = list(
            audience_collection.find(
                {"client_id": client_id, "platform": platform},
                {"_id": 0}
            )
            .sort("fetched_at", -1)  # newest first
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
    """Get detailed client status and progress (UUID-safe)."""
    try:
        client_data = clients_collection.find_one({"client_id": client_id})
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
        "POST /register": "Register a new client with platform, profession, location, and search terms (returns client_id as UUID)",
        "POST /fetch-audience/{client_id}": "Run Apify and store platform-specific audience data (client_id is UUID)",
        "POST /generate-messages/{client_id}": "Generate personalized outreach messages (client_id is UUID)",
        "GET /audience/{client_id}": "View stored audience data with platform stats (client_id is UUID)",
        "GET /client/{client_id}/status": "Get detailed client status and progress (client_id is UUID)",
        "GET /task-status/{task_id}": "Check background task status (task_id is UUID)"
    
        },
        "status": "✅ Ready to process multi-platform outreach requests with improved reliability"
    }