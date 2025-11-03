from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from datetime import datetime
from uuid import uuid4
from bson import ObjectId
import asyncio
from concurrent.futures import ThreadPoolExecutor
import re, time, os, traceback
from utils.serialization_utils import convert_datetime_to_str, serialize_crew_output

# Internal imports
from db_config import clients_collection, audience_collection
from utils.apify_utils import run_apify, filter_profiles_by_location
from utils.serialization_utils import serialize_crew_output, convert_datetime_to_str
from utils.task_utils import tasks
from pipeline_utils import kickoff_message_generation, auto_send_gmail
from instagram_dm_sender import send_instagram_dm

router = APIRouter()

# -----------------------------
# 1️⃣ MODELS
# -----------------------------
class ClientRegistration(BaseModel):
    name: str
    role: str
    email: str
    platform: str
    search_terms_with_location: list[str]
    preferred_profession: str
    preferred_location: str


# -----------------------------
# 2️⃣ REGISTER CLIENT
# -----------------------------
@router.post("/register")
def register_client(data: ClientRegistration):
    platform = data.platform.lower()
    if platform not in ["instagram", "linkedin", "facebook"]:
        raise HTTPException(status_code=400, detail="Invalid platform")

    client_id = str(uuid4())
    client_info = data.dict()
    client_info.update({
        "client_id": client_id,
        "platform": platform,
        "status": "registered",
        "created_at": datetime.utcnow()
    })

    clients_collection.insert_one(client_info)
    return {"message": f"✅ Client registered for {platform.upper()} successfully.", "client_id": client_id}


# -----------------------------
# 3️⃣ BACKGROUND FETCH TASK
# -----------------------------
def run_fetch_audience_task(task_id: str, client_id: str):
    try:
        tasks[task_id]["status"] = "running"
        client_data = clients_collection.find_one({"client_id": client_id})
        if not client_data:
            raise Exception("Client not found")

        results = run_apify(
            client_data["platform"],
            client_data["search_terms_with_location"],
            client_data.get("preferred_profession"),
            client_data.get("preferred_location"),
        )

        stored_count = 0
        for i, r in enumerate(results):
            if not r:
                continue
            r.update({
                "client_id": client_id,
                "platform": client_data["platform"],
                "fetched_at": datetime.utcnow(),
                "location_relevance_score": r.get("location_relevance_score", 0)
            })
            r["unique_key"] = (
                r.get("profileUrl") or r.get("publicIdentifier") or
                f"{client_data['platform']}_{i}_{datetime.utcnow().timestamp()}"
            )
            if not audience_collection.find_one(
                {"client_id": client_id, "unique_key": r["unique_key"]}
            ):
                audience_collection.insert_one(r)
                stored_count += 1

        clients_collection.update_one(
            {"client_id": client_id},
            {"$set": {
                "status": "data_fetched" if stored_count else "data_fetch_attempted",
                "data_fetched_at": datetime.utcnow(),
                "last_fetch_count": stored_count
            }}
        )

        tasks[task_id]["status"] = "completed"
        tasks[task_id]["result"] = {"stored_count": stored_count}

    except Exception as e:
        tasks[task_id]["status"] = f"failed: {str(e)}"
    finally:
        print(f"🧹 Cleaning up task {task_id}")
        time.sleep(2)
        del tasks[task_id]


@router.post("/fetch-audience/{client_id}")
async def fetch_audience(client_id: str, background_tasks: BackgroundTasks):
    task_id = str(uuid4())
    tasks[task_id] = {"status": "pending", "client_id": client_id, "start_time": datetime.utcnow()}
    background_tasks.add_task(run_fetch_audience_task, task_id, client_id)
    return {"task_id": task_id, "message": "🎯 Audience fetch started in background."}


# -----------------------------
# 4️⃣ GENERATE MESSAGE (ASYNC)
# -----------------------------
async def generate_message_task_async(client_id: str, task_id: str):
    try:
        # 🕒 Track start time (converted for JSON safety)
        start_time = datetime.utcnow()
        tasks[task_id] = {
            "status": "running",
            "client_id": client_id,
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S")
        }

        # 🧠 Fetch client data
        client_data = clients_collection.find_one({"client_id": client_id}, {"_id": 0})
        if not client_data:
            tasks[task_id]["status"] = "failed: client not found"
            return

        client_data = convert_datetime_to_str(client_data)
        print(f"🎯 Generating message for {client_id} - {client_data['platform']}")

        # ⚙️ Run message generation in background thread
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as pool:
            generated_message = await loop.run_in_executor(pool, kickoff_message_generation, client_data)

        # 🧩 Serialize and store safely in MongoDB
        serialized_message = serialize_crew_output(generated_message)
        clients_collection.update_one(
            {"client_id": client_id},
            {"$set": {
                "generated_messages": serialized_message,
                "messages_generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "current_status": "message_generated"
            }}
        )

        # ✅ Update task status
        tasks[task_id]["status"] = "completed"

    except Exception as e:
        tasks[task_id]["status"] = f"failed: {str(e)}"
        print(f"❌ Error in message generation for {client_id}: {e}")



        # ✅ Auto-send DM
        if client_data.get("platform") == "instagram":
            best_match = audience_collection.find_one(
                {"client_id": client_id, "platform": "instagram"},
                sort=[("location_relevance_score", -1)]
            )
            if best_match and "username" in best_match:
                dm_result = send_instagram_dm(best_match["username"], serialized_message["raw_message"], client_id)
                clients_collection.update_one(
                    {"client_id": client_id},
                    {"$set": {
                        "dm_sent_to": best_match["username"],
                        "dm_sent_status": dm_result.get("success"),
                        "dm_sent_at": datetime.utcnow(),
                        "current_status": "dm_sent" if dm_result.get("success") else "dm_failed"
                    }}
                )

        # ✅ Auto-send Gmail
        email_doc = audience_collection.find_one({"client_id": client_id, "email": {"$exists": True, "$ne": None}})
        if email_doc:
            email_result = auto_send_gmail(client_id)
            clients_collection.update_one(
                {"client_id": client_id},
                {"$set": {
                    "email_sent_to": email_result.get("email"),
                    "email_sent_status": email_result.get("success"),
                    "email_sent_at": datetime.utcnow(),
                    "current_status": "email_sent" if email_result.get("success") else "email_failed"
                }}
            )

        tasks[task_id]["status"] = "completed"
        tasks[task_id]["result"] = serialized_message

    except Exception as e:
        tasks[task_id]["status"] = f"failed: {str(e)}"
        clients_collection.update_one(
            {"client_id": client_id},
            {"$set": {"current_status": "message_generation_failed", "error": str(e)}}
        )


@router.post("/generate-messages/{client_id}")
async def generate_messages(client_id: str, background_tasks: BackgroundTasks):
    client_data = clients_collection.find_one({"client_id": client_id})
    if not client_data:
        raise HTTPException(status_code=404, detail="Client not found")

    task_id = str(uuid4())
    tasks[task_id] = {"status": "pending", "client_id": client_id, "start_time": datetime.utcnow()}
    background_tasks.add_task(generate_message_task_async, client_id, task_id)

    return {"task_id": task_id, "message": "🧠 Message generation started in background."}


# -----------------------------
# 5️⃣ STATUS & DATA ENDPOINTS
# -----------------------------
@router.get("/task-status/{task_id}")
def get_task_status(task_id: str):
    task = tasks.get(task_id)
    if not task:
        return {"task_id": task_id, "status": "not found or completed"}
    return {"task_id": task_id, "status": task["status"], "details": task}

# -----------------------------
# Endpoint to fetch generated message
# -----------------------------
@router.get("/client/{client_id}/message")
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



@router.get("/client/{client_id}/status")
def get_client_status(client_id: str):
    data = clients_collection.find_one({"client_id": client_id})
    if not data:
        raise HTTPException(status_code=404, detail="Client not found")

    audience_count = audience_collection.count_documents({"client_id": client_id})
    return {
        "client_id": client_id,
        "platform": data["platform"],
        "status": data.get("status"),
        "audience_count": audience_count,
        "last_fetch_count": data.get("last_fetch_count", 0),
        "messages_generated_at": data.get("messages_generated_at"),
    }


@router.get("/audience/{client_id}")
def get_audience(client_id: str):
    data = list(audience_collection.find({"client_id": client_id}, {"_id": 0}).sort("fetched_at", -1).limit(10))
    return {"client_id": client_id, "sample_audience": data, "total": len(data)}
