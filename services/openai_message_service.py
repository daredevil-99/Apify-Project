# services/openai_message_service.py
"""
Complete LinkedIn Pipeline: Scraping, Enrichment, and AI Message Generation
FIXED: Now uses full profile URL path (works like Apify UI)
"""

import os
import re
from typing import Dict, Optional, List
from openai import OpenAI
from dotenv import load_dotenv
from apify_client import ApifyClient

load_dotenv()

# Initialize clients
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")

if not OPENAI_API_KEY:
    raise ValueError("Missing OPENAI_API_KEY in .env")
if not APIFY_API_TOKEN:
    raise ValueError("Missing APIFY_API_TOKEN in .env")

openai_client = OpenAI(api_key=OPENAI_API_KEY)
apify_client = ApifyClient(APIFY_API_TOKEN)


# ============================================================
# 🔍 LINKEDIN SCRAPING FUNCTIONS
# ============================================================

# def scrape_linkedin(search_terms: List[str], profession: str = None, location: str = None) -> List[Dict]:
#     """
#     Scrape LinkedIn profiles via Apify (harvestapi/linkedin-profile-search)
#     Returns basic profile information for selection.
#     """
#     try:
#         actor_id = "harvestapi/linkedin-profile-search"
        
#         # Build smart search query
#         company_keywords = ['solutions', 'pvt', 'ltd', 'inc', 'llc', 'corp', 'limited']
#         query_parts = []
        
#         # 1. Prioritize profession
#         if profession:
#             prof_clean = profession.strip()
#             if prof_clean.lower() not in ['full', 'string']:
#                 query_parts.append(prof_clean)
#                 print(f"✅ Using profession: {prof_clean}")
        
#         # 2. Add relevant search terms (skip company names)
#         if search_terms and isinstance(search_terms, list):
#             for term in search_terms:
#                 term_str = str(term).strip().lower()
                
#                 if len(term_str) < 3:
#                     continue
                    
#                 if any(keyword in term_str for keyword in company_keywords):
#                     print(f"⏭️  Skipping company name: {term_str}")
#                     continue
                
#                 if term_str not in query_parts:
#                     query_parts.append(term_str)
#                     print(f"✅ Added search term: {term_str}")
        
#         # 3. Build final query
#         if query_parts:
#             search_query = " ".join(query_parts[:3])
#         else:
#             if location and location.lower() == "india":
#                 search_query = "Software Engineer"
#             else:
#                 search_query = "Developer"
#             print(f"⚠️  No valid search terms, using fallback: {search_query}")
        
#         search_query = search_query.strip()
        
#         print(f"\n💼 FINAL LinkedIn search query: '{search_query}'")
#         print(f"📍 Location filter: {location if location else 'None'}\n")

#         # Build payload
#         payload = {
#             "searchQuery": search_query,
#             "profileScraperMode": "Short",
#             "startPage": 1,
#             "maxItems": 25,
#             "proxyConfigurationOptions": {"useApifyProxy": True}
#         }
        
#         if location:
#             payload["locations"] = [location]

#         print(f"🚀 Running LinkedIn actor...")
#         print(f"📦 Payload: {payload}\n")

#         run = apify_client.actor(actor_id).call(run_input=payload)

#         if not run or "defaultDatasetId" not in run:
#             print("⚠️ LinkedIn actor returned no dataset")
#             return []

#         dataset_id = run["defaultDatasetId"]
#         items = list(apify_client.dataset(dataset_id).iterate_items())

#         print(f"\n✅ Retrieved {len(items)} LinkedIn profiles")
        
#         if items:
#             print(f"🔍 Sample profile keys: {list(items[0].keys())}")
#             first_profile = items[0]
#             print(f"\n👤 Sample profile:")
#             print(f"   Name: {first_profile.get('firstName', '')} {first_profile.get('lastName', '')}")
#             print(f"   Headline: {first_profile.get('headline', 'N/A')}")
#             print(f"   Location: {first_profile.get('location', 'N/A')}")
#         else:
#             print(f"\n⚠️  No profiles found for query: '{search_query}'")
#             print(f"💡 TIP: Try broader search terms like 'Developer', 'Engineer', 'Designer'")

#         return items

#     except Exception as e:
#         print(f"❌ LinkedIn scraping error: {e}")
#         import traceback
#         traceback.print_exc()
#         return []


def enrich_linkedin_profiles(profile_urls: List[str]) -> Dict[str, List[Dict]]:
    """
    Enrich LinkedIn profiles by fetching POSTS data using apimaestro/linkedin-profile-posts actor.
    
    IMPORTANT: This function scrapes profiles ONE AT A TIME to properly associate posts with profiles.
    The actor accepts full LinkedIn profile URLs (e.g., linkedin.com/in/username or just username).
    
    Args:
        profile_urls: List of LinkedIn profile URLs to enrich
        
    Returns:
        Dictionary mapping profile_url -> list of post objects
    """
    try:
        if not profile_urls:
            print("⚠️ No profile URLs provided")
            return {}
        
        actor_id = "LQQIXN9Othf8f7R5n"  # apimaestro/linkedin-profile-posts
        
        print(f"\n🔍 Enriching {len(profile_urls)} LinkedIn profiles...")
        print(f"📋 URLs: {profile_urls[:3]}{'...' if len(profile_urls) > 3 else ''}")
        
        # Results dictionary: profile_url -> [posts]
        all_results = {}
        
        # Process each profile individually
        for idx, profile_url in enumerate(profile_urls):
            print(f"\n{'='*60}")
            print(f"📊 Processing profile {idx+1}/{len(profile_urls)}")
            print(f"{'='*60}")
            
            # Clean the URL - remove https:// and www. if present
            # This matches what works in Apify UI
            clean_url = profile_url.replace("https://", "").replace("http://", "").replace("www.", "")
            
            print(f"✅ Using profile URL: {clean_url}")
            
            # Build payload - use the full URL path as username parameter
            # Actor accepts: "linkedin.com/in/username" or just "username"
            payload = {
                "username": clean_url,
                "page_number": 1,
                "limit": 10,  # Get up to 10 posts
                "total_posts": 10  # Auto-pagination for 10 posts
            }
            
            print(f"🚀 Running actor...")
            print(f"📦 Payload: {payload}")
            
            try:
                run = apify_client.actor(actor_id).call(run_input=payload)
                
                if not run or "defaultDatasetId" not in run:
                    print(f"⚠️ Actor returned no dataset for {clean_url}")
                    all_results[profile_url] = []
                    continue
                
                dataset_id = run["defaultDatasetId"]
                posts = list(apify_client.dataset(dataset_id).iterate_items())
                
                print(f"✅ Retrieved {len(posts)} posts")
                
                # Store posts with normalized URL as key
                norm_url = profile_url.lower().rstrip("/")
                all_results[norm_url] = posts
                
                # Show sample if posts found
                if posts:
                    sample = posts[0]
                    print(f"📝 Sample post:")
                    print(f"   Text: {sample.get('text', 'N/A')[:80]}...")
                    stats = sample.get('stats', {})
                    print(f"   Likes: {stats.get('numLikes', 0)}")
                else:
                    print(f"⚠️ No posts found - profile may be private or have no posts")
                
            except Exception as e:
                print(f"❌ Error processing {clean_url}: {e}")
                import traceback
                traceback.print_exc()
                all_results[profile_url] = []
                continue
        
        print(f"\n{'='*60}")
        print(f"✅ ENRICHMENT COMPLETE")
        print(f"{'='*60}")
        print(f"Total profiles processed: {len(profile_urls)}")
        print(f"Profiles with posts: {sum(1 for posts in all_results.values() if posts)}")
        print(f"{'='*60}\n")
        
        return all_results
        
    except Exception as e:
        print(f"❌ LinkedIn profile enrichment error: {e}")
        import traceback
        traceback.print_exc()
        return {}


# ============================================================
# 🔍 DATABASE INSPECTION FUNCTIONS
# ============================================================

def inspect_prospect_data(prospect_data: Dict, show_full_posts: bool = False) -> None:
    """
    Inspect and display what data is saved for a prospect in the database.
    Useful for debugging whether posts were actually scraped and saved.
    
    Args:
        prospect_data: Prospect dictionary from MongoDB
        show_full_posts: Whether to show full post content (default: False)
    """
    print(f"\n{'='*70}")
    print(f"🔍 PROSPECT DATABASE INSPECTION")
    print(f"{'='*70}")
    
    # Basic Info
    full_name = prospect_data.get("full_name") or prospect_data.get("fullName", "Unknown")
    headline = prospect_data.get("headline", "N/A")
    location = prospect_data.get("location", "N/A")
    profile_url = prospect_data.get("profile_url") or prospect_data.get("profileUrl", "N/A")
    
    print(f"\n📋 BASIC INFO:")
    print(f"   Name: {full_name}")
    print(f"   Headline: {headline}")
    print(f"   Location: {location}")
    print(f"   Profile URL: {profile_url}")
    
    # Enrichment Status
    is_enriched = prospect_data.get("enriched", False)
    print(f"\n✨ ENRICHMENT STATUS:")
    print(f"   Enriched: {is_enriched}")
    
    if not is_enriched:
        print(f"   ⚠️  Profile NOT enriched yet! Posts won't be available.")
        print(f"   💡 Run /enrich-posts endpoint to enrich this profile.")
        return
    
    # POSTS - THE CRITICAL SECTION! ⭐
    posts = prospect_data.get("posts", [])
    print(f"\n📱 POSTS DATA: ⭐⭐⭐")
    if posts and isinstance(posts, list) and len(posts) > 0:
        print(f"   ✅ Posts found: {len(posts)} posts")
        print(f"\n   POST SUMMARY:")
        
        for idx, post in enumerate(posts[:5], 1):  # Show first 5
            text = post.get("text", "")
            likes = post.get("likesCount", 0)
            comments = post.get("commentsCount", 0)
            hashtags = post.get("hashtags", [])
            posted_at = post.get("postedAt", "N/A")
            
            print(f"\n   --- Post #{idx} ---")
            if show_full_posts:
                print(f"   Text: {text}")
            else:
                print(f"   Text Preview: {text[:100]}...")
            print(f"   Likes: {likes} | Comments: {comments}")
            print(f"   Hashtags: {hashtags}")
            print(f"   Posted: {posted_at}")
    else:
        print(f"   ❌ NO POSTS FOUND IN DATABASE!")
        print(f"\n   🔍 DEBUGGING TIPS:")
        print(f"   1. Check if profile has public posts on LinkedIn")
        print(f"   2. Profile might be set to private")
        print(f"   3. Actor might have failed to scrape posts")
    
    # Generated Message
    generated_message = prospect_data.get("generated_message")
    print(f"\n💬 GENERATED MESSAGE:")
    if generated_message:
        print(f"   ✅ Message generated")
        print(f"   Preview: {generated_message[:100]}...")
    else:
        print(f"   ⚠️  No message generated yet")
    
    print(f"\n{'='*70}\n")


def inspect_apify_response(enriched_data: Dict[str, List[Dict]]) -> None:
    """
    Inspect raw Apify enrichment response to see exactly what was returned.
    Call this RIGHT AFTER enrich_linkedin_profiles() to debug.
    
    Args:
        enriched_data: Dict returned from enrich_linkedin_profiles()
    """
    if not enriched_data:
        print("❌ No enriched data to inspect!")
        return
    
    print(f"\n{'='*70}")
    print(f"🔬 RAW APIFY RESPONSE INSPECTION")
    print(f"{'='*70}")
    
    print(f"\n📦 Profiles processed: {len(enriched_data)}")
    
    for profile_url, posts in enriched_data.items():
        print(f"\n🔗 Profile: {profile_url}")
        print(f"   Posts count: {len(posts)}")
        
        if posts:
            sample = posts[0]
            print(f"   Post keys: {list(sample.keys())}")
            print(f"   Sample text: {sample.get('text', 'N/A')[:80]}...")
    
    print(f"\n{'='*70}\n")


LINKEDIN_SYSTEM_PROMPT = """
You write short, warm, human-sounding LinkedIn outreach messages that feel like someone genuinely looked at the person’s profile.

Your job: Create a single message under 300 characters that highlights one specific detail from the person’s work, product, or interests. Make it feel personal and conversational, never templated.

Rules:

Focus on one sharp, profile-specific insight.

Sound like a real human noticing something interesting.

No buzzwords, no formal tone, no selling, no flattery.

No structure or steps in the output.

Keep it light, friendly, and genuinely curious.

If relevance fits naturally, mention the sender’s background briefly.

End with a soft, no-pressure invite to connect.

Do not use any emojis or the "—" symbol in the message.

Output only the final message.

"""

LINKEDIN_USER_PROMPT = """
Using the profile info below, write a single, human, friendly LinkedIn message under 300 characters.

Profile:
{profile_summary}

My context:
{campaign_message}

Now write the message, keeping it personal, specific, authentic, and under 300 characters.
"""

def extract_profile_summary(prospect_data: Dict) -> str:
    """
    Extract COMPLETE LinkedIn profile with RICH CONTEXT for AI personalization.
    
    Goal: Give AI enough information to write deeply personalized messages
    that show genuine understanding of the recipient's work.
    """
    summary_parts = []
    
    # ============================================================
    # 1️⃣ BASIC INFO
    # ============================================================
    full_name = prospect_data.get("full_name") or prospect_data.get("fullName", "")
    headline = prospect_data.get("headline", "")
    location = prospect_data.get("location", "")
    
    summary_parts.append("=== PROFILE BASICS ===")
    if full_name:
        summary_parts.append(f"Name: {full_name}")
    if headline:
        summary_parts.append(f"Current Role/Headline: {headline}")
    if location:
        summary_parts.append(f"Location: {location}")
    
    # ============================================================
    # 2️⃣ ABOUT SECTION (Professional Identity) ⭐⭐⭐
    # ============================================================
    about = prospect_data.get("about") or prospect_data.get("summary", "")
    if about and len(about.strip()) > 20:
        summary_parts.append("\n=== PROFESSIONAL SUMMARY (About Section) ===")
        summary_parts.append(f"{about[:800]}")  # Increased to 800 chars for more context
        summary_parts.append("^ This reveals their professional identity, mission, and what makes them unique")
    
    # ============================================================
    # 3️⃣ WORK EXPERIENCE ⭐⭐⭐ CRITICAL
    # ============================================================
    experience = prospect_data.get("experience") or prospect_data.get("positions", [])
    
    if experience and isinstance(experience, list) and len(experience) > 0:
        summary_parts.append("\n=== WORK EXPERIENCE ===")
        
        # Current role gets special attention
        current_role = experience[0] if len(experience) > 0 else None
        if current_role and isinstance(current_role, dict):
            summary_parts.append("\n🎯 CURRENT ROLE (Most Important for Personalization):")
            
            title = current_role.get("title") or current_role.get("jobTitle", "")
            company = current_role.get("companyName") or current_role.get("company", "")
            date_range = current_role.get("dateRange") or current_role.get("timePeriod", "")
            description = current_role.get("description", "")
            
            if title and company:
                summary_parts.append(f"  Position: {title}")
                summary_parts.append(f"  Company: {company}")
                if date_range:
                    summary_parts.append(f"  Duration: {date_range}")
                if description and len(description) > 30:
                    summary_parts.append(f"  What they do: {description[:400]}")
                    summary_parts.append("  ^ Use this to understand their unique work/mission")
        
        # Previous roles (context)
        if len(experience) > 1:
            summary_parts.append("\nPrevious Roles (for career context):")
            for exp in experience[1:3]:  # Next 2 roles
                if isinstance(exp, dict):
                    title = exp.get("title") or exp.get("jobTitle", "")
                    company = exp.get("companyName") or exp.get("company", "")
                    if title and company:
                        summary_parts.append(f"  • {title} at {company}")
    else:
        summary_parts.append("\n⚠️ No work experience data available")
    
    # ============================================================
    # 4️⃣ RECENT POSTS (Communication Style & Interests) ⭐⭐
    # ============================================================
    posts = prospect_data.get("posts", [])
    
    if posts and isinstance(posts, list) and len(posts) > 0:
        summary_parts.append("\n=== RECENT LINKEDIN ACTIVITY ===")
        summary_parts.append("What topics/themes do they post about? Use this to understand their interests:")
        
        for i, post in enumerate(posts[:5]):  # Show top 5 posts
            text = post.get("text", "")
            if text and len(text.strip()) > 20:
                summary_parts.append(f"\nPost {i+1}:")
                summary_parts.append(f"{text[:250]}")  # Increased to 250 chars
                
                # Engagement context
                likes = post.get("likesCount") or post.get("numLikes", 0)
                comments = post.get("commentsCount") or post.get("numComments", 0)
                hashtags = post.get("hashtags", [])
                
                if hashtags:
                    summary_parts.append(f"Topics/Tags: {', '.join(hashtags[:5])}")
                if likes > 50 or comments > 10:
                    summary_parts.append(f"High engagement: {likes} likes, {comments} comments")
        
        summary_parts.append("\n^ Look for recurring themes, passions, or professional interests")
    else:
        summary_parts.append("\n⚠️ No recent posts available")
    
    # ============================================================
    # 5️⃣ EDUCATION
    # ============================================================
    education = prospect_data.get("education") or prospect_data.get("schools", [])
    
    if education and isinstance(education, list) and len(education) > 0:
        summary_parts.append("\n=== EDUCATION ===")
        
        for edu in education[:2]:  # Top 2
            if isinstance(edu, dict):
                school = edu.get("schoolName") or edu.get("school", "")
                degree = edu.get("degree") or edu.get("degreeName", "")
                field = edu.get("fieldOfStudy") or edu.get("field", "")
                
                if school:
                    parts = [school]
                    if degree:
                        parts.append(degree)
                    if field:
                        parts.append(f"in {field}")
                    summary_parts.append(f"  • {' - '.join(parts)}")
    
    # ============================================================
    # 6️⃣ SKILLS (Technical Context)
    # ============================================================
    skills = prospect_data.get("skills", [])
    
    if skills and isinstance(skills, list) and len(skills) > 0:
        skill_names = []
        for skill in skills[:15]:  # Increased to 15 skills
            if isinstance(skill, dict):
                skill_names.append(skill.get("name", ""))
            elif isinstance(skill, str):
                skill_names.append(skill)
        
        if skill_names:
            summary_parts.append(f"\n=== KEY SKILLS ===")
            summary_parts.append(f"{', '.join(filter(None, skill_names))}")
    
    # ============================================================
    # 7️⃣ AI INSTRUCTIONS (How to Use This Data)
    # ============================================================
    summary_parts.append("\n" + "="*60)
    summary_parts.append("AI PERSONALIZATION GUIDE:")
    summary_parts.append("="*60)
    summary_parts.append("1. PRIORITY: Focus on their CURRENT ROLE and what makes it unique")
    summary_parts.append("2. CONTEXT: Use About section to understand their professional mission")
    summary_parts.append("3. INTERESTS: Look at post themes to find shared interests")
    summary_parts.append("4. SPECIFICITY: Mention actual company names, products, or focus areas")
    summary_parts.append("5. AUTHENTICITY: Make it feel like genuine interest, not template")
    
    # ============================================================
    # BUILD FINAL SUMMARY
    # ============================================================
    result = "\n".join(summary_parts)
    
    # Debug statistics
    print(f"\n📊 Profile Summary Stats:")
    print(f"   - Total length: {len(result)} characters")
    print(f"   - Has about: {bool(about and len(about) > 20)}")
    print(f"   - Work experience: {len(experience) if experience else 0} roles")
    print(f"   - Current role description: {len(current_role.get('description', '')) if experience and len(experience) > 0 and isinstance(experience[0], dict) else 0} chars")
    print(f"   - Education: {len(education) if education else 0} entries")
    print(f"   - Skills: {len(skills) if skills else 0} total")
    print(f"   - Posts: {len(posts) if posts else 0} posts")
    print(f"   - Post content available: {sum(1 for p in (posts or []) if p.get('text', '').strip())}")
    
    return result

def generate_linkedin_message(
    prospect_data: Dict,
    campaign_message: str,
    model: str = "gpt-4o-mini",
    debug: bool = False
) -> Dict:
    """
    Generate a personalized LinkedIn message using OpenAI.
    
    Args:
        prospect_data: Enriched LinkedIn profile data (must include posts!)
        campaign_message: User's campaign objective/pitch
        model: OpenAI model to use (default: gpt-4o-mini)
        debug: Show detailed profile summary being sent to AI
        
    Returns:
        Dict with generated message and metadata
    """
    try:
        # Extract profile summary
        profile_summary = extract_profile_summary(prospect_data)
        
        if debug:
            print(f"\n{'='*70}")
            print(f"🔍 PROFILE SUMMARY SENT TO AI:")
            print(f"{'='*70}")
            print(profile_summary)
            print(f"{'='*70}\n")
        
        if not profile_summary or len(profile_summary) < 50:
            return {
                "status": "error",
                "error": "Insufficient profile data for personalization",
                "message": None
            }
        
        # Format user prompt
        user_prompt = LINKEDIN_USER_PROMPT.format(
            profile_summary=profile_summary,
            campaign_message=campaign_message
        )
        
        # Call OpenAI API
        print(f"🤖 Generating message using {model}...")
        
        response = openai_client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": LINKEDIN_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
            max_tokens=300,
            top_p=0.9
        )
        
        generated_message = response.choices[0].message.content.strip()
        
        # Validate output
        if not generated_message:
            return {
                "status": "error",
                "error": "OpenAI returned empty response",
                "message": None
            }
        
        # Clean up any unwanted formatting
        generated_message = generated_message.replace("**", "").replace("*", "")
        
        return {
            "status": "success",
            "message": generated_message,
            "model_used": model,
            "tokens_used": response.usage.total_tokens,
            "profile_name": prospect_data.get("full_name") or prospect_data.get("fullName", "Unknown"),
            "had_posts": bool(prospect_data.get("posts"))
        }
        
    except Exception as e:
        print(f"❌ OpenAI message generation error: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            "status": "error",
            "error": str(e),
            "message": None
        }


def generate_batch_messages(
    prospects: List[Dict],
    campaign_message: str,
    model: str = "gpt-4o-mini"
) -> List[Dict]:
    """
    Generate messages for multiple prospects in batch.
    
    Args:
        prospects: List of enriched LinkedIn profile data
        campaign_message: User's campaign objective
        model: OpenAI model to use
        
    Returns:
        List of results with generated messages
    """
    results = []
    
    for idx, prospect in enumerate(prospects):
        print(f"\n📝 Generating message {idx + 1}/{len(prospects)}...")
        
        result = generate_linkedin_message(
            prospect_data=prospect,
            campaign_message=campaign_message,
            model=model
        )
        
        result["prospect_index"] = idx
        result["prospect_name"] = prospect.get("full_name") or prospect.get("fullName", "Unknown")
        result["profile_url"] = prospect.get("profile_url") or prospect.get("profileUrl", "")
        
        results.append(result)
    
    success_count = sum(1 for r in results if r.get("status") == "success")
    print(f"\n✅ Generated {success_count}/{len(prospects)} messages successfully")
    
    return results