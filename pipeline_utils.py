import os
import random
import json
import re
from datetime import datetime
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from crewai import Agent, Task, Crew, Process, LLM
from crewai.tools import BaseTool
from typing import List, Dict, Any
from dotenv import load_dotenv
from apify_client import ApifyClient

from gmail_utils import send_email
from fetch_tool import FetchFromMongoTool
from db_config import audience_collection, clients_collection

load_dotenv()

APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
apify_client = ApifyClient(APIFY_API_TOKEN)


def auto_send_gmail(client_id: str):
    client = clients_collection.find_one({"client_id": client_id})
    if not client or not client.get("generated_messages"):
        print(f"⚠️ No generated message found for {client_id}")
        return

    message_text = client["generated_messages"].get("final_output", "")
    if not message_text:
        print(f"⚠️ Empty generated message for {client_id}")
        return

    prospects = audience_collection.find({"client_id": client_id})

    for prospect in prospects:
        to_email = prospect.get("email")

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

        subject = f"Let's Connect, {prospect.get('pageName', 'there')}!"
        print(f"📧 Sending Gmail to {to_email}")

        success = send_email(to_email, subject, message_text)

        audience_collection.update_one(
            {"_id": prospect["_id"]},
            {"$set": {
                "email_sent": success,
                "email_sent_at": datetime.utcnow()
            }}
        )

    print("✅ All Gmail messages processed.")


fetch_from_mongo_tool = FetchFromMongoTool()
llm = LLM(model="gpt-4o-mini", temperature=0.7, max_tokens=800)

platform_router = Agent(
    role="Client Data Analyzer",
    goal="Extract and structure client requirements for targeted audience discovery.",
    backstory="You validate platform consistency and extract accurate targeting data.",
    allow_delegation=False,
    verbose=True,
    llm=llm
)

audience_retriever = Agent(
    role="Platform-Specific Audience Retriever",
    goal="Find and retrieve valid prospect profiles from the specified platform with real content.",
    backstory="You ensure quality prospect data for message generation.",
    tools=[fetch_from_mongo_tool],
    allow_delegation=False,
    verbose=True,
    llm=llm
)

message_generator = Agent(
    role="Message Creator",
    goal="Create one authentic, platform-specific outreach message.",
    backstory="You generate natural, data-driven outreach messages without mixing platforms.",
    allow_delegation=False,
    verbose=True,
    llm=llm
)


def deep_convert(obj):
    if isinstance(obj, dict):
        return {k: deep_convert(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [deep_convert(i) for i in obj]
    elif hasattr(obj, "isoformat"):
        return obj.isoformat()
    else:
        return obj


def kickoff_message_generation(client_data: dict):
    safe_client_data = deep_convert(client_data)
    platform = safe_client_data.get("platform", "").lower()
    client_id = str(safe_client_data.get("client_id", safe_client_data.get("_id", "")))

    print(f"\n🎯 Starting message generation for {platform.upper()} — Client ID: {client_id}")

    task1 = Task(
        description=(
            f"Analyze the client registration data and extract targeting requirements for {platform.upper()} ONLY:\n"
            f"Client Data: {json.dumps(safe_client_data, indent=2)}\n\n"
            f"CRITICAL: The platform is {platform.upper()}. Do NOT process any other platforms.\n"
            f"IMPORTANT:\n"
            f"- Use exactly this client_id: {client_id}\n"
            f"- Do NOT alter or invent a new client_id.\n"
            f"- This ID is used to retrieve prospects from MongoDB.\n\n"
            f"Return ONLY a JSON object with:\n"
            f"- client_id: string\n"
            f"- platform: '{platform}' (EXACT MATCH)\n"
            f"- search_terms: array of strings\n"
            f"- preferred_profession: string\n"
            f"- preferred_location: string\n"
        ),
        agent=platform_router,
        expected_output=(
            f"Clean JSON with client_id, platform='{platform}', search_terms, preferred_profession, and preferred_location"
        )
    )

    task2 = Task(
        description=(
            f"Find a valid {platform.upper()} prospect using the client requirements from Task 1.\n\n"
            f"Ensure the profile has actual content and belongs to platform='{platform}'.\n"
            f"Use fetch_from_mongo(client_id='id', platform='{platform}', search_terms=['terms']).\n"
        ),
        agent=audience_retriever,
        context=[task1],
        expected_output=(
            f"Either: (1) A valid {platform} profile with has_valid_content=true, "
            f"OR (2) An error object explaining why no valid {platform} data was found."
        )
    )

    tone_instructions = {
        "instagram": "- Casual, friendly tone (do NOT include an explanation/reasoning section)\n- Reference bio/caption and hashtags\n- 2–3 short sentences",
        "linkedin": "- Professional but warm tone (do NOT include an explanation/reasoning section)\n- Reference headline and experience\n- 3–4 thoughtful sentences",
        "facebook": "- Friendly, conversational tone (do NOT include an explanation/reasoning section)\n- Reference about_me, followers, or categories\n- 2–4 sentences",
    }

    tone = tone_instructions.get(platform, "- Platform-appropriate tone (do NOT include an explanation/reasoning section)")

    task3 = Task(
        description=(
            f"Create ONE personalized message for {platform.upper()} using profile data from Task 2.\n\n"
            f"Follow these rules:\n"
            f"1. Use real profile data only.\n"
            f"2. Do not fabricate missing information.\n"
            f"3. DO NOT include any 'Reasoning', 'Explanation', or 'Rationale' section in the output. Output must contain only the final message text.\n\n"
            f"Message style:\n{tone}\n\n"
            f"SUCCESS FORMAT:\n"
            f"Don't use Emojis:\n"
            f"Platform: {platform.upper()}\n"
            f"Target: [actual username]\n"
            f"Message: [personalized message]\n"
        ),
        agent=message_generator,
        context=[task2],
        expected_output=(
            f"Either: (1) A properly formatted {platform.upper()} message using actual data (exactly in the SUCCESS FORMAT above), "
            f"OR (2) A clear acknowledgment if data was insufficient (the acknowledgment must NOT include reasoning)."
        )
    )

    crew = Crew(
        agents=[platform_router, audience_retriever, message_generator],
        tasks=[task1, task2, task3],
        process=Process.sequential,
        verbose=True,
        max_iter=2
    )

    def clean_agent_output(text: str) -> str:
        """
        Remove any 'Reasoning:' (or similar) block from agent output.
        Keeps only the portion up to the 'Reasoning:' label if present,
        and trims whitespace.
        """
        if not text:
            return text
        # Remove "Reasoning:" and everything after (case-insensitive)
        cleaned = re.split(r"\nReasoning\s*:\s*|(\nReasoning:)|(\nReasoning )", text, flags=re.IGNORECASE)[0]
        # Some agents may append "Reasoning:" inline; also remove any trailing "Reasoning" variants
        cleaned = re.split(r"\nReason(s|ing)?\b.*", cleaned, flags=re.IGNORECASE)[0]
        return cleaned.strip()

    try:
        result = crew.kickoff(inputs={"client_data": safe_client_data})
        print(f"✅ {platform.upper()} message generation completed!")

        # ✅ Get raw result properly
        if hasattr(result, 'raw'):
            final_output = result.raw
        elif isinstance(result, dict):
            final_output = result.get("final_output", str(result))
        else:
            final_output = str(result)
            
        cleaned_output = clean_agent_output(final_output)

        # ✅ FIXED: Extract username properly (handles dots, underscores, etc.)
        username_match = re.search(r"Target:\s*@?([\w\.\_]+)", cleaned_output or "", re.IGNORECASE)
        username = username_match.group(1).strip() if username_match else None

        # ✅ Validate username
        if not username or len(username) < 2 or username in ['p', 'reel', 'tv', 'stories']:
            print(f"⚠️ Invalid username detected: '{username}'")
            return {
                "error": "Invalid username in agent response",
                "final_output": cleaned_output,
                "platform": platform
            }

        print(f"🔍 Extracted username from agent: '{username}'")

        if username and final_output and "Unable to generate" not in final_output:
            # ✅ Get prospects document
            prospects_doc = audience_collection.find_one({
                "client_id": client_id,
                "platform": platform,
                "type": "prospects"
            })
            
            if not prospects_doc:
                print(f"❌ No prospects document found for client {client_id}")
                return {
                    "error": "No prospects found in database",
                    "final_output": final_output
                }
            
            # ✅ Find prospect in array with flexible username matching
            prospects_array = prospects_doc.get("prospects", [])
            prospect = None
            prospect_index = None
            
            print(f"🔍 Searching for '{username}' in {len(prospects_array)} prospects...")
            
            for idx, p in enumerate(prospects_array):
                # ✅ Check multiple username fields and normalize
                p_username = (
                    p.get("username") or 
                    p.get("ownerUsername") or 
                    p.get("pageName", "")
                ).strip().lower()
                
                # Remove @ if present in stored username
                p_username = p_username.lstrip('@')
                username_normalized = username.strip().lower().lstrip('@')
                
                if p_username == username_normalized:
                    prospect = p
                    prospect_index = idx
                    print(f"✅ Found exact match at index {prospect_index}: @{p_username}")
                    break
            
            # ✅ Fallback: partial match if exact match fails
            if not prospect:
                print(f"⚠️ No exact match found. Trying partial match...")
                for idx, p in enumerate(prospects_array):
                    p_username = (
                        p.get("username") or 
                        p.get("ownerUsername") or 
                        p.get("pageName", "")
                    ).strip().lower().lstrip('@')
                    
                    if username.lower() in p_username or p_username in username.lower():
                        prospect = p
                        prospect_index = idx
                        print(f"✅ Found partial match at index {prospect_index}: @{p_username}")
                        break
            
            if prospect:
                actual_username = prospect.get("username") or prospect.get("ownerUsername")
                print(f"✅ Matched prospect: @{actual_username}")
                
                # ✅ Extract just the message text (remove Platform/Target lines)
                message_lines = []
                for line in cleaned_output.split('\n'):
                    if not line.strip().startswith(('Platform:', 'Target:')):
                        message_lines.append(line.strip())
                message_only = '\n'.join(message_lines).strip()
                
                # If message starts with "Message:", remove that too
                if message_only.startswith('Message:'):
                    message_only = message_only.split('Message:', 1)[1].strip()
                
                # ✅ Update the specific prospect in the array
                audience_collection.update_one(
                    {
                        "client_id": client_id,
                        "platform": platform,
                        "type": "prospects"
                    },
                    {
                        "$set": {
                            f"prospects.{prospect_index}.generated_message": message_only,
                            f"prospects.{prospect_index}.status": "ready",
                            f"prospects.{prospect_index}.message_generated_at": datetime.utcnow().isoformat()
                        }
                    }
                )
                print(f"✅ Message saved for @{actual_username}")
                
                # ✅ Update client record with crew output
                clients_collection.update_one(
                    {"client_id": client_id},
                    {"$set": {
                        "target_prospect_username": actual_username,
                        "message_generated_at": datetime.utcnow(),
                        "message_status": "generated_not_sent",
                        "generated_messages": {
                            "full_output": cleaned_output,
                            "message_only": message_only,
                            "username": actual_username,
                            "platform": platform,
                            "generated_at": datetime.utcnow().isoformat()
                        }
                    }}
                )
                print(f"✅ Client updated with target: @{actual_username}")
                
                return {
                    "success": True,
                    "username": actual_username,
                    "message": message_only,
                    "platform": platform
                }
                
            else:
                print(f"⚠️ Warning: Prospect '{username}' not found in prospects array")
                print(f"📊 Available prospects ({len(prospects_array)}):")
                for idx, p in enumerate(prospects_array[:10]):
                    p_user = p.get("username") or p.get("ownerUsername") or p.get("pageName")
                    p_owner = p.get("ownerFullName", "")
                    print(f"   [{idx}] {p_user} ({p_owner})")
                
                return {
                    "error": f"Prospect '{username}' not found in database",
                    "final_output": cleaned_output,
                    "searched_for": username,
                    "available_prospects": [
                        {
                            "username": p.get("username") or p.get("ownerUsername") or p.get("pageName"),
                            "full_name": p.get("ownerFullName") or p.get("full_name", "")
                        }
                        for p in prospects_array[:10]
                    ],
                    "platform": platform
                }

        return {"final_output": cleaned_output, "username": username, "platform": platform}
        
    except Exception as e:
        print(f"❌ Error during message generation: {e}")
        import traceback
        traceback.print_exc()
        return {
            "error": str(e),
            "final_output": f"Failed to generate {platform} message",
            "platform": platform
        }