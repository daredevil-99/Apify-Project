#pipeline_utils.py - FIXED VERSION
import os
import random
import json
import re
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from crewai import Agent, Task, Crew, Process, LLM
from crewai.tools import BaseTool
from typing import List, Dict, Any
from gmail_utils import send_email
from datetime import datetime
from fetch_tool import FetchFromMongoTool
from dotenv import load_dotenv
from apify_client import ApifyClient
from db_config import audience_collection, clients_collection  # ✅ Remove apify_client from here

load_dotenv()

# ✅ CREATE APIFY CLIENT HERE INSTEAD
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
apify_client = ApifyClient(APIFY_API_TOKEN)

def auto_send_gmail(client_id: str):
    """Automatically send generated outreach message via Gmail"""
    # 1️⃣ Get client-generated message
    client = clients_collection.find_one({"client_id": client_id})
    if not client or not client.get("generated_messages"):
        print(f"⚠️ No generated message found for {client_id}")
        return

    message_text = client["generated_messages"].get("final_output", "")
    if not message_text:
        print(f"⚠️ Empty generated message for {client_id}")
        return

    # 2️⃣ Find all prospects (email check added inside loop)
    prospects = audience_collection.find({"client_id": client_id})

    for prospect in prospects:
        to_email = prospect.get("email")

        # ✅ Safe email validation check
        if not to_email or "@" not in to_email:
            print(f"⚠️ Skipping {prospect.get('pageName', 'Unknown')} — No valid email available.")
            audience_collection.update_one(
                {"_id": prospect["_id"]},
                {"$set": {
                    "email_sent": False,
                    "email_error": "missing_or_invalid_email",
                    "email_sent_at": datetime.utcnow()
                }}
            )
            continue

        # 🧩 Build email details
        subject = f"Let's Connect, {prospect.get('pageName', 'there')}!"
        print(f"📧 Sending Gmail to {to_email}")

        success = send_email(to_email, subject, message_text)

        # 3️⃣ Log result
        audience_collection.update_one(
            {"_id": prospect["_id"]},
            {"$set": {
                "email_sent": success,
                "email_sent_at": datetime.utcnow()
            }}
        )

    print("✅ All Gmail messages processed.")


# Instantiate tool
fetch_from_mongo_tool = FetchFromMongoTool()

# -------------------------------
# LLM Setup
# -------------------------------
llm = LLM(
    model="gpt-4o-mini",
    temperature=0.7,
    max_tokens=800
)

# -------------------------------
# Enhanced Agents with Strict Platform Control
# -------------------------------

platform_router = Agent(
    role="Client Data Analyzer",
    goal="Extract and structure client requirements for targeted audience discovery on the EXACT specified platform.",
    backstory=(
        "You are a precision-focused analyst who ensures platform consistency. You extract client requirements "
        "and validate that all data processing stays within the specified platform boundaries."
    ),
    allow_delegation=False,
    verbose=True,
    llm=llm
)

audience_retriever = Agent(
    role="Platform-Specific Audience Retriever",
    goal="Find and retrieve ONLY valid prospect profiles from the specified platform with actual content.",
    backstory=(
        "You are a data quality specialist who retrieves profiles exclusively from the requested platform. "
        "You validate data quality and reject empty or invalid profiles to ensure message generation "
        "uses only authentic, platform-appropriate content."
    ),
    tools=[fetch_from_mongo_tool],
    allow_delegation=False,
    verbose=True,
    llm=llm
)

message_generator = Agent(
    role="Single-Platform Message Specialist",
    goal="Create ONE personalized message for the EXACT platform specified, using ONLY the provided profile data.",
    backstory=(
        "You are a focused outreach specialist who creates messages for ONE specific platform at a time. "
        "You NEVER mix platforms or create generic examples. You use ONLY the actual profile data provided "
        "and create authentic, personalized messages that match the platform's communication style. "
        "If the data is insufficient, you acknowledge this limitation rather than creating fake examples."
        "You can analyse their comments, hastags, events, images and videos. And based on your analyse create your message."
        
    ),
    allow_delegation=False,
    verbose=True,
    llm=llm
)

# -----------------------------
# Helper Function to Convert DateTime Objects
# -----------------------------
def deep_convert(obj):
    """Recursively convert datetime objects to ISO format strings"""
    if isinstance(obj, dict):
        return {k: deep_convert(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [deep_convert(i) for i in obj]
    elif hasattr(obj, "isoformat"):
        # handle datetime, date, etc.
        return obj.isoformat()
    else:
        return obj

# -----------------------------
# Fixed Crew Function with Strict Platform Control
# -----------------------------

def kickoff_message_generation(client_data: dict):
    """
    Run the crew to generate personalized outreach messages for the SPECIFIC platform only.
    """
    # 🩹 Convert datetime objects BEFORE any JSON operations
    safe_client_data = deep_convert(client_data)

    print("\n-----------------------------")
    print("🎯 Starting message generation for LINKEDIN ONLY")
    print(f"Client Data: {json.dumps(safe_client_data, indent=2)}\n\n")

    platform = safe_client_data.get("platform", "").lower()
    client_name = safe_client_data.get("name", "Unknown Client")

    print(f"🎯 Starting message generation for {platform.upper()} ONLY - Client: {client_name}")

    # Task 1: Analyze client requirements with platform validation
    task1 = Task(
        description=(
            f"Analyze the client registration data and extract targeting requirements for {platform.upper()} ONLY:\n"
            f"Client Data: {json.dumps(safe_client_data, indent=2)}\n\n"
            f"CRITICAL: The platform is {platform.upper()}. Do NOT process any other platforms.\n\n"
            f"Your job is to:\n"
            f"1. Extract the client_id from the _id field\n"
            f"2. Confirm the platform is '{platform}'\n"
            f"3. Extract search terms from search_terms_with_location\n"
            f"4. Include profession and location context\n\n"
            f"Return ONLY a JSON object with:\n"
            f"- client_id: string\n"
            f"- platform: '{platform}' (EXACT MATCH)\n"
            f"- search_terms: array of strings\n"
            f"- preferred_profession: string\n"
            f"- preferred_location: string\n"
        ),
        agent=platform_router,
        expected_output=f"Clean JSON object with client_id, platform (must be '{platform}'), search_terms, preferred_profession, and preferred_location"
    )

    # Task 2: Find platform-specific prospect with validation
    task2 = Task(
        description=(
            f"Find a valid {platform.upper()} prospect using the client requirements from Task 1.\n\n"
            f"CRITICAL REQUIREMENTS:\n"
            f"1. Use ONLY platform='{platform}' (no other platforms)\n"
            f"2. Validate that the returned profile has actual content\n"
            f"3. If no valid data is found, return the error information\n\n"
            f"Steps:\n"
            f"1. Use fetch_from_mongo tool with exact parameters from Task 1\n"
            f"2. Verify the profile has 'has_valid_content': true\n"
            f"3. If data is empty/invalid, return the error details\n\n"
            f"Call format: fetch_from_mongo(client_id='id', platform='{platform}', search_terms=['terms'])\n"
        ),
        agent=audience_retriever,
        context=[task1],
        expected_output=(
            f"Either: (1) A valid {platform} profile with has_valid_content=true and actual bio/content data, "
            f"OR (2) An error object explaining why no valid {platform} data was found"
        )
    )

    # Task 3: Generate SINGLE platform-specific message
    task3 = Task(
        description=(
            f"Create ONE personalized message for {platform.upper()} ONLY using the profile from Task 2.\n\n"
            f"CRITICAL RULES:\n"
            f"1. Create message for {platform.upper()} ONLY - no other platforms\n"
            f"2. Use ONLY the actual profile data provided\n"
            f"3. If profile data is invalid/empty, acknowledge this - don't create fake examples\n"
            f"4. Reference actual bio, content, and engagement from the profile for collaborating with their profession\n\n"
            f"{platform.upper()}-SPECIFIC Guidelines:\n"
            + (
                "- Casual, friendly tone with emojis\n"
                "- Reference bio/caption, hashtags and comments\n"
                "- Keep it short (2-3 sentences)\n" 
                if platform == "instagram" else
                "- Professional but warm tone\n"
                "- Reference headline, industry, experience\n"
                "- 3-4 sentences with depth\n"
                if platform == "linkedin" else
                "- Friendly, conversational tone\n"
                "- Reference categories, about_me text, or page info\n"
                "- Mention likes/followers if relevant\n"
                "- 2–4 sentences\n"
                if platform == "facebook" else
                "- Platform-appropriate tone\n"
            ) +
            f"\nIf the profile data is insufficient or empty, respond with:\n"
            f"'Unable to generate authentic {platform.upper()} message - insufficient profile data found.'\n\n"
            f"SUCCESS FORMAT (only if valid data exists):\n"
            f"Platform: {platform.upper()}\n"
            f"Target: [actual username from profile]\n"
            f"Message: [personalized message using actual bio/content]\n"
            f"Reasoning: [explain how you used the specific profile data]\n"
        ),
        agent=message_generator,
        context=[task2],
        expected_output=(
            f"Either: (1) A properly formatted {platform.upper()} message using actual profile data, "
            f"OR (2) An acknowledgment that insufficient data was found"
        )
    )


    # Create and run crew
    crew = Crew(
        agents=[platform_router, audience_retriever, message_generator],
        tasks=[task1, task2, task3],
        process=Process.sequential,
        verbose=True,
        max_iter=2,  # Reduced iterations for faster execution
    )

    try:
        result = crew.kickoff(inputs={"client_data": safe_client_data})
        print(f"✅ {platform.upper()}-specific message generation completed!")
        return result
    except Exception as e:
        print(f"❌ Error in crew execution: {e}")
        return {
            "error": str(e), 
            "final_output": f"Failed to generate {platform} message - check data quality",
            "platform": platform
        }