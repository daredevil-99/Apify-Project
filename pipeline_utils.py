# pipeline_utils.py - FIXED VERSION WITH ENRICHMENT SUPPORT

import os
import random
import json
import re
from datetime import datetime
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from crewai import Agent, Task, Crew, Process, LLM
from crewai.tools import BaseTool
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from apify_client import ApifyClient
from langchain_core.prompts import ChatPromptTemplate

from gmail_utils import send_email
from fetch_tool import FetchFromMongoTool
from db_config import audience_collection, clients_collection

load_dotenv()

APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
apify_client = ApifyClient(APIFY_API_TOKEN)

# LinkedIn prompt template
linkedin_prompt_template = ChatPromptTemplate.from_messages([
    ("system", """
You are an expert in creating HUMAN-LIKE LinkedIn outreach messages.
Your job is to write short, warm, highly personalized messages based on the recipient's LinkedIn profile.

TOP PRIORITY:
Use profile themes, industry topics, hashtags, skills, or engagement patterns that align with the sender's campaign message.

FALLBACK RULE:
If no relevant themes are found, personalize using:
- Their headline, job role, experience, About section, overall professional identity and interests

CORE LINKEDIN ANALYSIS LOGIC:
1. Analyze the LinkedIn profile summary deeply (profession, industry, expertise, career focus)
2. Select ONLY themes/skills/topics that make the campaign message more relevant

STRICT RULES:
- NO specific posts/timestamps/events. NO emojis. NO generic phrases. NO salesy tone.
- Warm, human, professional, conversational. 4-6 sentences max.

OUTPUT RULES:
- Only output the final message text. No bullet points, no lists, no headings, no emojis.
    """),
    ("user", """
Use the data below to create one highly personalized LinkedIn DM:

=== LINKEDIN PROFILE DATA ===
{profile_summary}

=== MY CAMPAIGN MESSAGE ===
{campaign_message}

TASK:
1. Identify their industry, expertise, communication style, professional identity
2. Select ONLY profile themes/skills/hashtags relevant to campaign
3. Start naturally, blend campaign smoothly, end with soft call-to-action
4. No emojis, no formatting, no mention of specific posts

Generate the final message now:
    """)
])


def auto_send_gmail(client_id: str):
    """Auto-send Gmail to prospects"""
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
                {"$set": {"email_sent": False, "email_error": "missing_or_invalid_email", "email_sent_at": datetime.utcnow()}}
            )
            continue
        subject = f"Let's Connect, {prospect.get('pageName', 'there')}!"
        print(f"📧 Sending Gmail to {to_email}")
        success = send_email(to_email, subject, message_text)
        audience_collection.update_one(
            {"_id": prospect["_id"]}, 
            {"$set": {"email_sent": success, "email_sent_at": datetime.utcnow()}}
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
    """Convert MongoDB objects to JSON-serializable format"""
    if isinstance(obj, dict):
        return {k: deep_convert(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [deep_convert(i) for i in obj]
    elif hasattr(obj, "isoformat"):
        return obj.isoformat()
    else:
        return obj


def format_linkedin_profile_summary(prospect: dict) -> str:
    """
    Format LinkedIn profile data into readable summary for message generation.
    ✅ Enhanced to include enriched data (posts, engagement, experience)
    """
    summary_parts = []
    
    # Name
    first_name = prospect.get("firstName", "") or prospect.get("first_name", "")
    last_name = prospect.get("lastName", "") or prospect.get("last_name", "")
    if first_name or last_name:
        summary_parts.append(f"Name: {first_name} {last_name}".strip())
    
    # Headline
    headline = prospect.get("headline", "") or prospect.get("summary", "")
    if headline:
        summary_parts.append(f"Headline: {headline}")
    
    # About section
    about = prospect.get("about", "") or prospect.get("description", "")
    if about:
        summary_parts.append(f"About: {about}")
    
    # Current experience
    experience = prospect.get("experience", []) or prospect.get("current_positions", [])
    if experience and len(experience) > 0:
        current = experience[0]
        title = current.get("title", "") or current.get("position", "")
        company = current.get("companyName", "") or current.get("company", "")
        if title or company:
            summary_parts.append(f"Current Role: {title} at {company}".strip())
    
    # Skills
    skills = prospect.get("skills", [])
    if skills:
        skills_str = ", ".join([
            s.get("name", s) if isinstance(s, dict) else str(s) 
            for s in skills[:10]
        ])
        summary_parts.append(f"Skills: {skills_str}")
    
    # Location
    location = prospect.get("location", "")
    if location:
        if isinstance(location, dict):
            location = location.get("linkedinText", "") or location.get("name", "")
        summary_parts.append(f"Location: {location}")
    
    # ✅ NEW: Include recent posts if available (from enrichment)
    posts = prospect.get("posts", [])
    if posts and len(posts) > 0:
        summary_parts.append("\nRecent Posts & Engagement:")
        for idx, post in enumerate(posts[:3]):  # Top 3 posts
            post_text = post.get("text", "")[:200]  # First 200 chars
            likes = post.get("likesCount", 0)
            comments = post.get("commentsCount", 0)
            if post_text:
                summary_parts.append(
                    f"  Post {idx+1}: {post_text}... "
                    f"(👍 {likes} likes, 💬 {comments} comments)"
                )
    
    # ✅ NEW: Include engagement metrics
    engagement = prospect.get("engagement", {})
    if engagement:
        total_reactions = engagement.get("totalReactions", 0)
        total_comments = engagement.get("totalComments", 0)
        if total_reactions or total_comments:
            summary_parts.append(
                f"\nEngagement: {total_reactions} total reactions, "
                f"{total_comments} total comments"
            )
    
    return "\n".join(summary_parts) if summary_parts else "Limited profile information available"


def kickoff_message_generation(
    client_data: dict,
    campaign_message: str = None,
    target_prospect: dict = None,
    target_prospect_index: int = None
):
    """
    🎯 Enhanced message generation supporting BOTH random and user-selected prospects.
    ✅ Now properly handles enriched LinkedIn profiles from apimaestro actor
    
    TWO MODES:
    1. RANDOM MODE: target_prospect=None → Uses fetch tool to get random prospect
    2. SELECTED MODE: target_prospect provided → Uses specific user-selected prospect
    
    Args:
        client_data: Client info from DB
        campaign_message: Custom campaign message (optional)
        target_prospect: Specific prospect dict (for user-selection mode)
        target_prospect_index: Index in prospects array (for DB update)
    """
    safe_client_data = deep_convert(client_data)
    platform = safe_client_data.get("platform", "").lower()
    client_id = str(safe_client_data.get("client_id", safe_client_data.get("_id", "")))

    # Default campaign message
    if not campaign_message:
        campaign_message = safe_client_data.get("campaign_message", 
            "I'd love to connect and explore potential collaboration opportunities.")

    print(f"\n🎯 Starting message generation for {platform.upper()} — Client ID: {client_id}")
    print(f"📝 Campaign Message: {campaign_message[:100]}...")
    
    selected_mode = target_prospect is not None
    print(f"{'🎯 USER-SELECTED' if selected_mode else '🎲 RANDOM'} prospect mode")
    
    # ✅ ENHANCED ENRICHMENT CHECK
    if selected_mode and target_prospect:
        # Verify enrichment is complete for LinkedIn
        if platform == "linkedin":
            is_enriched = target_prospect.get("enriched", False)
            has_posts = bool(target_prospect.get("posts"))
            
            print(f"📊 Enriched profile: {is_enriched}, Has posts: {has_posts}")
            
            # ⚠️ WARNING if not enriched (but still proceed)
            if not is_enriched:
                print("⚠️ WARNING: Profile not enriched yet!")
                print("💡 TIP: Use /pipeline/enrich-and-generate endpoint to enrich first")
                print("⏭️  Proceeding with basic profile data...")
            else:
                print("✅ Using enriched profile data with posts and engagement metrics")
                
                # Log enrichment details
                posts = target_prospect.get("posts", [])
                engagement = target_prospect.get("engagement", {})
                skills = target_prospect.get("skills", [])
                
                if posts:
                    print(f"   📝 Posts available: {len(posts)}")
                if engagement:
                    print(f"   💬 Engagement data: {engagement.get('totalReactions', 0)} reactions, "
                          f"{engagement.get('totalComments', 0)} comments")
                if skills:
                    print(f"   🎯 Skills: {len(skills)} skills identified")
        else:
            # For Instagram/Facebook, enrichment might not be necessary
            print(f"📊 Platform: {platform} (enrichment not required)")

    platform_requirements = {
        "instagram": {
            "identifier": "username (e.g., @johndoe)", 
            "content_fields": "caption, hashtags, bio", 
            "example": "username: creative_designer..."
        },
        "linkedin": {
            "identifier": "full name (e.g., John Doe)", 
            "content_fields": "firstName, lastName, headline, summary, experience, skills, posts", 
            "example": "firstName: John..."
        },
        "facebook": {
            "identifier": "page name", 
            "content_fields": "pageName, about, categories", 
            "example": "pageName: 'Beauty Studio'..."
        }
    }
    req = platform_requirements.get(platform, platform_requirements["instagram"])

    # TASK 1: Platform router
    task1 = Task(
        description=(
            f"Extract targeting data from client registration for {platform.upper()}.\n\n"
            f"CRITICAL: Use EXACTLY this client_id: {client_id}\n"
            f"DO NOT create or modify the client_id.\n\n"
            f"Client Data:\n{json.dumps(safe_client_data, indent=2)}\n\n"
            f"Return ONLY a JSON object with:\n"
            f"- client_id: '{client_id}' (EXACT COPY)\n"
            f"- platform: '{platform}'\n"
            f"- search_terms: array from data\n"
            f"- preferred_profession: string from data\n"
            f"- preferred_location: string from data\n"
        ),
        agent=platform_router,
        expected_output=f"JSON with client_id='{client_id}', platform='{platform}', and targeting data"
    )

    # TASK 2: Audience retriever (differs by mode)
    if selected_mode and target_prospect:
        # ✅ ENHANCED: Check enrichment status before proceeding
        if platform == "linkedin":
            if not target_prospect.get("enriched"):
                # Profile not enriched - use basic formatting
                print("⚠️ Using basic profile data (not enriched)")
                profile_summary = format_linkedin_profile_summary(target_prospect)
                enrichment_status = "basic_profile_only"
            else:
                # Profile is enriched - use enhanced formatting
                print("✅ Using enriched profile data")
                profile_summary = format_linkedin_profile_summary(target_prospect)
                enrichment_status = "fully_enriched"
            
            prospect_json = json.dumps({
                "profile_summary": profile_summary,
                "enriched": target_prospect.get("enriched", False),
                "enrichment_status": enrichment_status,
                "has_posts": bool(target_prospect.get("posts")),
                "post_count": len(target_prospect.get("posts", [])),
                "has_engagement_data": bool(target_prospect.get("engagement")),
                "full_name": target_prospect.get("full_name") or target_prospect.get("fullName", ""),
                "headline": target_prospect.get("headline", ""),
                "profile_url": target_prospect.get("profile_url", "")
            }, indent=2)
        else:
            prospect_json = json.dumps(target_prospect, indent=2)
        
        task2 = Task(
            description=f"USER-SELECTED PROSPECT: Validate this {platform} prospect has data for message generation:\n{prospect_json}\n\nReturn full prospect JSON with validation confirmation.",
            agent=audience_retriever,
            context=[task1],
            expected_output="Validated prospect data as JSON with profile_summary for LinkedIn"
        )
    else:
        task2 = Task(
            description=f"Use fetch_from_mongo tool to find valid {platform} prospect. Return FULL profile JSON.",
            agent=audience_retriever,
            context=[task1],
            expected_output=f"Valid {platform} profile JSON with has_valid_content=true"
        )

    # TASK 3: Message generator (LinkedIn special handling)
    if platform == "linkedin":
        # ✅ ENHANCED: Adjust prompt based on enrichment status
        enrichment_note = ""
        
        if selected_mode and target_prospect:
            if target_prospect.get("enriched"):
                # Profile is enriched - use rich context
                enrichment_note = """
✅ ENRICHED PROFILE DETECTED:
This profile has been ENRICHED with recent posts and engagement data.
Use this rich information to create a HIGHLY personalized message that references:
- Their recent content themes (without mentioning specific post text)
- Topics they're passionate about (inferred from posts)
- Their engagement patterns and activity level
- Specific interests shown in their professional content

Make the message feel like you've genuinely researched their profile.
"""
            else:
                # Profile not enriched - use basic approach
                enrichment_note = """
⚠️ BASIC PROFILE DATA:
This profile has NOT been enriched yet. Use available information:
- Headline, job title, and current role
- Professional summary/about section
- Listed skills and experience
- Location and industry

Focus on their professional identity and how it aligns with the campaign message.
"""
        
        task3 = Task(
            description=f"""Create a highly personalized LinkedIn message using the prospect profile from Task 2.

=== CAMPAIGN MESSAGE (MUST INCORPORATE THIS) ===
{campaign_message}

{enrichment_note}

=== LINKEDIN MESSAGE GENERATION RULES ===
1. Analyze the prospect's LinkedIn profile identity (profession, industry, expertise, career focus, communication style)
2. If posts are available, reference themes from their recent content (DO NOT mention specific post text)
3. Select ONLY profile themes, skills, or topics that align with the campaign message
4. If no relevant themes are found, personalize using their headline, job role, experience, About section
5. Create a 4-6 sentence message that:
   - Starts naturally (reference their expertise/role/interests)
   - Smoothly incorporates the campaign message
   - Ends with a soft, genuine call-to-action
6. STRICT RULES:
   - NO emojis, NO generic phrases, NO salesy tone, NO mention of specific posts/timestamps
   - Warm, human, professional, conversational tone
   - Must feel like a real person reaching out, not a template

=== CRITICAL OUTPUT FORMAT (YOU MUST FOLLOW THIS EXACTLY) ===
You MUST format your response EXACTLY like this:

Platform: LINKEDIN
Target: [Prospect's Full Name from the profile data]
Message: [Your personalized message text here]

DO NOT output only the message. You MUST include all three lines above.

Generate the LinkedIn message now following the exact format.""",
            agent=message_generator,
            context=[task2],
            expected_output="Formatted LinkedIn message with Platform, Target, and Message lines"
        )
    else:
        # Instagram/Facebook handling (unchanged)
        tone = {
            "instagram": {
                "tone": "Casual, friendly",
                "ref": "bio/hashtags",
                "len": "2-3 sentences",
                "fmt": "Target: @username"
            },
            "facebook": {
                "tone": "Friendly",
                "ref": "page content",
                "len": "2-4 sentences",
                "fmt": "Target: PageName"
            }
        }
        tc = tone.get(platform, tone["instagram"])
        task3 = Task(
            description=f"""Create {platform} message using profile from Task 2.

=== CAMPAIGN MESSAGE ===
{campaign_message}

RULES:
- Tone: {tc['tone']}
- Reference: {tc['ref']}
- Length: {tc['len']}
- Incorporate the campaign message naturally

Format: 'Platform: {platform.upper()}\\n{tc['fmt']}\\nMessage: [text]'""",
            agent=message_generator,
            context=[task2],
            expected_output=f"{platform.upper()} formatted message"
        )

    # Create and run crew
    crew = Crew(
        agents=[platform_router, audience_retriever, message_generator],
        tasks=[task1, task2, task3],
        process=Process.sequential,
        verbose=True,
        max_iter=2
    )

    def clean_agent_output(text: str) -> str:
        """Clean up agent reasoning from output"""
        if not text: 
            return text
        cleaned = re.split(r"\nReasoning\s*:\s*|(\nReasoning:)|(\nReasoning )", text, flags=re.IGNORECASE)[0]
        cleaned = re.split(r"\nReason(s|ing)?\b.*", cleaned, flags=re.IGNORECASE)[0]
        return cleaned.strip()

    try:
        result = crew.kickoff(inputs={
            "client_data": safe_client_data,
            "campaign_message": campaign_message
        })
        print(f"✅ {platform.upper()} message generation completed!")

        # Extract output
        if hasattr(result, 'raw'):
            final_output = result.raw
        elif isinstance(result, dict):
            final_output = result.get("final_output", str(result))
        else:
            final_output = str(result)
        
        cleaned_output = clean_agent_output(final_output)
        print(f"\n📄 Cleaned Output:\n{cleaned_output}\n")

        # Extract identifier from output
        username, is_url = None, False
        
        if platform == "linkedin":
            # Try URL first
            url_match = re.search(r"(https?://[^\s]+linkedin[^\s]+)", cleaned_output, re.IGNORECASE)
            if url_match:
                username, is_url = url_match.group(1).strip().rstrip('/'), True
            else:
                # Try Target: line
                name_match = re.search(r"Target:\s*(.+?)(?:\n|$)", cleaned_output)
                if name_match:
                    username = name_match.group(1).strip()
            
            # FALLBACK: Extract from prospect data
            if not username and selected_mode and target_prospect:
                first = target_prospect.get("firstName", "") or target_prospect.get("first_name", "")
                last = target_prospect.get("lastName", "") or target_prospect.get("last_name", "")
                username = (target_prospect.get("full_name") or 
                           target_prospect.get("fullName") or 
                           f"{first} {last}".strip())
                print(f"⚠️ No 'Target:' line found. Using prospect name: {username}")
        else:
            # Instagram/Facebook
            username_match = re.search(r"Target:\s*@?([\w\.\_\-\/]+)", cleaned_output or "", re.IGNORECASE)
            if username_match:
                username = username_match.group(1).strip()
                is_url = '/' in username and username.lower().startswith('http')
            
            # FALLBACK
            if not username and selected_mode and target_prospect:
                username = (target_prospect.get("username") or 
                           target_prospect.get("ownerUsername") or 
                           target_prospect.get("pageName") or 
                           target_prospect.get("fullName"))
                print(f"⚠️ No 'Target:' line found. Using username: {username}")

        if not username or len(username) < 2:
            return {
                "error": "Invalid identifier", 
                "final_output": cleaned_output, 
                "platform": platform
            }
        
        if "Unable to generate" in final_output or "error" in cleaned_output.lower():
            return {
                "error": "Agent failed", 
                "final_output": cleaned_output, 
                "platform": platform
            }

        # DATABASE LOOKUP & UPDATE
        if selected_mode and target_prospect_index is not None:
            # User-selected mode: use provided prospect
            prospect, prospect_index = target_prospect, target_prospect_index
            
            if platform == "linkedin":
                first = prospect.get("firstName", "") or prospect.get("first_name", "")
                last = prospect.get("lastName", "") or prospect.get("last_name", "")
                actual_username = (prospect.get("full_name") or 
                                  prospect.get("fullName") or 
                                  f"{first} {last}".strip())
                profile_url = (prospect.get("profile_url") or 
                              prospect.get("profileUrl") or 
                              prospect.get("linkedinUrl") or 
                              prospect.get("url"))
            else:
                actual_username = (prospect.get("username") or 
                                  prospect.get("ownerUsername") or 
                                  prospect.get("pageName") or 
                                  prospect.get("fullName"))
                profile_url = prospect.get("profile_url") or prospect.get("url")
            
            print(f"✅ Using PRE-SELECTED prospect: {actual_username} at index {prospect_index}")
        else:
            # Random mode: find prospect in DB
            prospects_doc = audience_collection.find_one({
                "client_id": client_id, 
                "platform": platform, 
                "type": "prospects"
            })
            
            if not prospects_doc:
                return {
                    "error": "No prospects in DB", 
                    "final_output": final_output
                }
            
            prospects_array = prospects_doc.get("prospects", [])
            prospect, prospect_index = None, None
            
            def _normalize_url(u):
                return str(u).strip().lower().rstrip('/') if u else None
            
            # Platform-specific matching
            if platform == "linkedin":
                if is_url:
                    target_url = _normalize_url(username)
                    for idx, p in enumerate(prospects_array):
                        p_url = (p.get("profile_url") or 
                                p.get("profileUrl") or 
                                p.get("linkedinUrl") or 
                                p.get("url"))
                        if p_url and _normalize_url(p_url) == target_url:
                            prospect, prospect_index = p, idx
                            break
                
                if not prospect:
                    # Name matching
                    target_tokens = set(re.sub(r'[^a-zA-Z0-9\s]', '', username.lower()).split())
                    for idx, p in enumerate(prospects_array):
                        p_full = (p.get("fullName") or 
                                 p.get("full_name") or 
                                 p.get("firstName", "") + " " + p.get("lastName", "")).strip().lower()
                        p_tokens = set(re.sub(r'[^a-zA-Z0-9\s]', '', p_full).split())
                        if len(target_tokens & p_tokens) >= 2:
                            prospect, prospect_index = p, idx
                            break
            
            elif platform == "instagram":
                username_normalized = username.strip().lower().lstrip('@')
                for idx, p in enumerate(prospects_array):
                    p_username = (p.get("username") or 
                                 p.get("ownerUsername") or 
                                 p.get("pageName") or "").strip().lower().lstrip('@')
                    if p_username and (p_username == username_normalized or username_normalized in p_username):
                        prospect, prospect_index = p, idx
                        break
            
            elif platform == "facebook":
                username_normalized = username.strip().lower()
                for idx, p in enumerate(prospects_array):
                    p_page = (p.get("pageName") or 
                             p.get("name") or 
                             p.get("username") or "").strip().lower()
                    if p_page and (p_page == username_normalized):
                        prospect, prospect_index = p, idx
                        break
            
            if not prospect:
                return {
                    "error": f"Prospect '{username}' not found in DB", 
                    "final_output": cleaned_output, 
                    "platform": platform
                }
            
            # Resolve actual username & URL
            if platform == "linkedin":
                first = prospect.get("firstName", "") or prospect.get("first_name", "")
                last = prospect.get("lastName", "") or prospect.get("last_name", "")
                actual_username = (prospect.get("full_name") or 
                                  prospect.get("fullName") or 
                                  f"{first} {last}".strip())
                profile_url = (prospect.get("profile_url") or 
                              prospect.get("profileUrl") or 
                              prospect.get("linkedinUrl") or 
                              prospect.get("url"))
            else:
                actual_username = (prospect.get("username") or 
                                  prospect.get("ownerUsername") or 
                                  prospect.get("pageName") or 
                                  prospect.get("fullName"))
                profile_url = prospect.get("profile_url") or prospect.get("url")

        # Extract message text
        message_lines = [
            line.strip() 
            for line in cleaned_output.split('\n') 
            if line.strip() and not line.strip().startswith(('Platform:', 'Target:'))
        ]
        message_only = '\n'.join(message_lines).strip()
        
        if message_only.startswith('Message:'):
            message_only = message_only.split('Message:', 1)[1].strip()
        
        if not message_only or len(message_only) < 10:
            return {
                "error": "Message too short", 
                "final_output": cleaned_output, 
                "platform": platform
            }

        # ✅ UPDATE DATABASE WITH ENRICHMENT STATUS
        update_data = {
            f"prospects.{prospect_index}.generated_message": message_only,
            f"prospects.{prospect_index}.status": "ready_to_send",
            f"prospects.{prospect_index}.message_generated_at": datetime.utcnow().isoformat(),
            f"prospects.{prospect_index}.profile_url": profile_url,
            f"prospects.{prospect_index}.campaign_message": campaign_message
        }
        
        # Add enrichment metadata if available
        if selected_mode and target_prospect and platform == "linkedin":
            update_data[f"prospects.{prospect_index}.message_used_enriched_data"] = target_prospect.get("enriched", False)
        
        audience_collection.update_one(
            {"client_id": client_id, "platform": platform, "type": "prospects"},
            {"$set": update_data}
        )
        
        clients_collection.update_one(
            {"client_id": client_id},
            {"$set": {
                "target_prospect_username": actual_username,
                "message_generated_at": datetime.utcnow(),
                "message_status": "generated_not_sent",
                "campaign_message": campaign_message,
                "generated_messages": {
                    "full_output": cleaned_output,
                    "message_only": message_only,
                    "username": actual_username,
                    "platform": platform,
                    "generated_at": datetime.utcnow().isoformat(),
                    "campaign_message": campaign_message,
                    "used_enriched_data": target_prospect.get("enriched", False) if selected_mode and target_prospect else False
                }
            }}
        )
        
        print(f"✅ Message saved: {actual_username}")
        print(f"📝 Campaign message used: {campaign_message[:50]}...")
        
        # ✅ ENHANCED RETURN WITH ENRICHMENT STATUS
        return {
            "success": True,
            "username": actual_username,
            "message": message_only,
            "platform": platform,
            "campaign_message": campaign_message,
            "enrichment_used": target_prospect.get("enriched", False) if selected_mode and target_prospect else False,
            "post_count": len(target_prospect.get("posts", [])) if selected_mode and target_prospect else 0
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return {
            "error": str(e), 
            "final_output": f"Failed to generate {platform} message", 
            "platform": platform
        }