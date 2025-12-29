# services/facebook_message_service.py
"""
Complete Facebook Pipeline: Scraping, Enrichment, and AI Message Generation
Updated to handle correct Facebook data structure from Apify actors
"""

import os
from typing import Dict, Optional, List
from datetime import datetime
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
# 🔧 PROSPECT ENRICHMENT FUNCTIONS
# ============================================================

def enrich_prospect_with_posts(
    prospect: Dict, 
    posts: List[Dict], 
    enrichment_status: Dict = None
) -> Dict:
    """
    HIGH-LEVEL: Add posts to prospect and calculate engagement.
    
    FIXED: Simplified - posts is already a flat list from router
    
    Args:
        prospect: Base prospect data
        posts: Flat list of posts (already extracted by router)
        enrichment_status: Status info from scraper
    """
    # Default enrichment status
    if not enrichment_status:
        enrichment_status = {"status": "unknown", "error": None}
    
    status = enrichment_status.get("status", "unknown")
    error = enrichment_status.get("error")
    
    # Mark prospect as enriched (even if 0 posts)
    prospect["enriched"] = True
    prospect["enriched_at"] = datetime.utcnow().isoformat()
    prospect["enrichment_status"] = status
    
    if error:
        prospect["enrichment_error"] = error
    
    # ✅ SIMPLIFIED: posts is already a flat list - no extraction needed
    prospect["posts"] = posts
    prospect["posts_count"] = len(posts)
    
    # Log the status
    name = prospect.get('name', 'Unknown')
    if status == "blocked":
        print(f"⚠️  {name}: Enrichment blocked by Facebook (0 posts)")
    elif status == "no_posts":
        print(f"⚠️  {name}: No public posts found (0 posts)")
    elif status == "success":
        print(f"✅ {name}: Enriched with {len(posts)} posts")
    else:
        print(f"⚠️  {name}: Enrichment status unknown ({len(posts)} posts)")
    
    # Calculate engagement (only if posts exist)
    if posts and len(posts) > 0:
        total_likes = sum(p.get("likes", 0) for p in posts)
        total_comments = sum(p.get("comments", 0) for p in posts)
        total_shares = sum(p.get("shares", 0) for p in posts)
        
        prospect["engagement"] = {
            "total_likes": total_likes,
            "total_comments": total_comments,
            "total_shares": total_shares,
            "avg_likes_per_post": total_likes / len(posts),
            "total_engagement": total_likes + total_comments + total_shares
        }
        
        print(f"   📊 Engagement: {total_likes} likes, {total_comments} comments, {total_shares} shares")
    
    return prospect




def get_prospect_summary(prospect: Dict) -> str:
    """Generate a human-readable summary of a prospect."""
    lines = [
        f"📄 {prospect.get('name', 'Unknown')}",
        f"🔗 {prospect.get('profile_url', 'N/A')}",
        f"📁 Category: {prospect.get('category', 'N/A')}",
    ]
    
    if prospect.get("email"):
        lines.append(f"📧 Email: {prospect['email']}")
    
    if prospect.get("website"):
        lines.append(f"🌐 Website: {prospect['website']}")
    
    if prospect.get("followers"):
        lines.append(f"👥 Followers: {prospect['followers']}")
    
    if prospect.get("description"):
        desc = prospect["description"][:100]
        lines.append(f"📝 {desc}...")
    
    return "\n".join(lines)


# ============================================================
# 🔍 DATABASE INSPECTION FUNCTIONS
# ============================================================

def inspect_prospect_data(prospect_data: Dict, show_full_posts: bool = False) -> None:
    """
    Inspect and display what data is saved for a Facebook prospect.
    
    Args:
        prospect_data: Prospect dictionary from MongoDB
        show_full_posts: Whether to show full post content
    """
    print(f"\n{'='*70}")
    print(f"🔍 FACEBOOK PROSPECT DATABASE INSPECTION")
    print(f"{'='*70}")
    
    # Use get_prospect_summary for basic info
    print(f"\n{get_prospect_summary(prospect_data)}")
    
    # Page Details
    description = prospect_data.get("description", "")
    about = prospect_data.get("about", "")
    followers = prospect_data.get("followers", 0)
    likes = prospect_data.get("likes", 0)
    
    print(f"\n📊 PAGE DETAILS:")
    print(f"   Followers: {followers}")
    print(f"   Likes: {likes}")
    if description:
        print(f"   Description: {description[:100]}...")
    if about:
        print(f"   About: {about[:100]}...")
    
    # Contact Info
    email = prospect_data.get("email", "")
    phone = prospect_data.get("phone", "")
    website = prospect_data.get("website", "")
    
    if email or phone or website:
        print(f"\n📧 CONTACT INFO:")
        if email:
            print(f"   Email: {email}")
        if phone:
            print(f"   Phone: {phone}")
        if website:
            print(f"   Website: {website}")
    
    # Enrichment Status
    is_enriched = prospect_data.get("enriched", False)
    posts_count = prospect_data.get("posts_count", 0)
    print(f"\n✨ ENRICHMENT STATUS:")
    print(f"   Enriched: {is_enriched}")
    print(f"   Posts Count: {posts_count}")
    
    if not is_enriched:
        print(f"   ⚠️ Profile NOT enriched yet! Posts won't be available.")
        print(f"   💡 Run /facebook/enrich-posts endpoint to enrich this profile.")
        return
    
    # Engagement metrics
    if prospect_data.get("engagement"):
        engagement = prospect_data["engagement"]
        print(f"\n📈 ENGAGEMENT METRICS:")
        print(f"   Total Likes: {engagement.get('total_likes', 0)}")
        print(f"   Total Comments: {engagement.get('total_comments', 0)}")
        print(f"   Total Shares: {engagement.get('total_shares', 0)}")
        print(f"   Avg Likes/Post: {engagement.get('avg_likes_per_post', 0):.1f}")
        print(f"   Total Engagement: {engagement.get('total_engagement', 0)}")
    
    # POSTS - CRITICAL SECTION ⭐
    posts = prospect_data.get("posts", [])
    print(f"\n📱 POSTS DATA: ⭐⭐⭐")
    if posts and isinstance(posts, list) and len(posts) > 0:
        print(f"   ✅ Posts found: {len(posts)} posts")
        print(f"\n   POST SUMMARY:")
        
        for idx, post in enumerate(posts[:5], 1):
            text = post.get("text", "")
            likes = post.get("likes", 0)
            comments = post.get("comments", 0)
            shares = post.get("shares", 0)
            post_url = post.get("url", "")
            
            print(f"\n   --- Post #{idx} ---")
            if show_full_posts:
                print(f"   Text: {text}")
            else:
                print(f"   Text Preview: {text[:100]}...")
            print(f"   Engagement: {likes} likes | {comments} comments | {shares} shares")
            if post_url:
                print(f"   URL: {post_url}")
    else:
        print(f"   ❌ NO POSTS FOUND IN DATABASE!")
        print(f"\n   🔍 DEBUGGING TIPS:")
        print(f"   1. Check if page has public posts on Facebook")
        print(f"   2. Page might be set to private")
        print(f"   3. Actor might have failed to scrape posts")
    
    # Generated Message
    generated_message = prospect_data.get("generated_message")
    print(f"\n💬 GENERATED MESSAGE:")
    if generated_message:
        print(f"   ✅ Message generated")
        print(f"   Preview: {generated_message[:100]}...")
    else:
        print(f"   ⚠️ No message generated yet")
    
    print(f"\n{'='*70}\n")


# ============================================================
# 🤖 AI MESSAGE GENERATION
# ============================================================

FACEBOOK_SYSTEM_PROMPT = """
You are an expert at writing PERSONALIZED Facebook messages that feel genuine and conversational.

YOUR MISSION: Make the recipient think "This person actually looked at my page and gets what I do."

PERSONALIZATION HIERARCHY:
1. **PAGE PURPOSE + VALUE PROPOSITION** (Best)
   - What problem does their business/page solve?
   - What makes their approach unique?
   - Any recent milestones or initiatives?

2. **SHARED INTERESTS/VALUES** (Great)
   - Common business interests
   - Overlapping target audiences
   - Similar missions or goals

3. **RECENT CONTENT THEMES** (Good)
   - Topics they frequently post about
   - Community engagement style
   - Professional achievements or updates

4. **BASIC PAGE INFO** (Okay - last resort)
   - Page category and focus
   - General industry connection

PERSONALIZATION TECHNIQUES:
✅ "I came across..." or "I noticed..." (shows you found them organically)
✅ Reference SPECIFIC details (business name, service, mission)
✅ Find COMMON GROUND (shared interests, values, audience)
✅ Show GENUINE INTEREST in their work
✅ Be CONVERSATIONAL and friendly, not corporate

❌ AVOID:
- Generic praise ("Love your page...")
- Overly salesy language
- Asking for favors immediately
- Mentioning specific post dates
- Excessive emojis

MESSAGE STRUCTURE:
1. **Personalized Opening** (1-2 sentences)
   - Reference something SPECIFIC about their page/business
   - Show you understand their value proposition

2. **Natural Bridge** (1-2 sentences)
   - Connect their work to your background
   - Establish common ground authentically

3. **Value Proposition** (1-2 sentences)
   - Briefly mention your skills/focus
   - Frame as potential collaboration, not ask

4. **Friendly Call-to-Action** (1 sentence)
   - Suggest connecting or discussing further
   - Keep it casual and low-pressure

TONE: Friendly, professional, genuinely curious, conversational
LENGTH: 4-6 sentences maximum

OUTPUT: Only the final message text. No formatting, bullets, or excessive emojis.
"""

FACEBOOK_USER_PROMPT = """
Create a PERSONALIZED Facebook message using the data below.

=== RECIPIENT'S FACEBOOK PAGE/PROFILE ===
{profile_summary}

=== MY CAMPAIGN MESSAGE ===
{campaign_message}

=== YOUR TASK ===

Step 1: ANALYZE THE PAGE DEEPLY
- What is their business/page about?
- What value do they provide?
- What topics do they post about?
- What's their mission or focus area?
- Any notable achievements or initiatives?

Step 2: FIND GENUINE CONNECTION POINTS
- How does their work relate to the campaign message?
- What shared interests or values exist?
- Where could there be mutual benefit?

Step 3: CRAFT SPECIFIC OPENING
Instead of: "Love your page..."
Try: "I came across [business name] and was impressed by [specific thing]..." or
     "Your focus on [specific service/mission] really resonated with me..." or
     "I noticed you're working on [specific initiative]..."

Step 4: BUILD THE MESSAGE
Opening: Show understanding of their business/mission
Bridge: Connect their work to your background naturally
Value: Mention your offer, but frame as mutual opportunity
Close: Friendly, casual invitation to connect

CRITICAL RULES:
- Use SPECIFIC details (business name, service, mission, post topics)
- Make it feel PERSONAL, not templated
- Reference their content themes (not individual posts)
- Show GENUINE interest in what they do
- Keep it conversational and warm

Now write the message:
"""


def extract_profile_summary(prospect_data: Dict) -> str:
    """
    Extract Facebook profile/page data for AI personalization.
    Handles the correct Facebook data structure from extraction.
    """
    summary_parts = []
    
    # ============================================================
    # 1️⃣ BASIC INFO
    # ============================================================
    name = prospect_data.get("name", "")
    username = prospect_data.get("username", "")
    category = prospect_data.get("category", "")
    categories = prospect_data.get("categories", [])
    profile_url = prospect_data.get("profile_url", "")
    
    summary_parts.append("=== PAGE BASICS ===")
    if name:
        summary_parts.append(f"Name: {name}")
    if username:
        summary_parts.append(f"Page Username: {username}")
    if category:
        summary_parts.append(f"Primary Category: {category}")
    if categories:
        summary_parts.append(f"All Categories: {', '.join(categories)}")
    if profile_url:
        summary_parts.append(f"URL: {profile_url}")
    
    # ============================================================
    # 2️⃣ DESCRIPTION / ABOUT SECTION ⭐⭐⭐
    # ============================================================
    description = prospect_data.get("description", "")
    about = prospect_data.get("about", "")
    
    if description and len(description.strip()) > 20:
        summary_parts.append("\n=== PAGE DESCRIPTION ===")
        summary_parts.append(f"{description[:800]}")
        summary_parts.append("^ This reveals their mission and what they do")
    
    if about and len(about.strip()) > 20:
        summary_parts.append("\n=== DETAILED ABOUT SECTION ===")
        summary_parts.append(f"{about[:800]}")
        summary_parts.append("^ Additional business details and value proposition")
    
    # ============================================================
    # 3️⃣ PAGE METRICS
    # ============================================================
    likes = prospect_data.get("likes", 0)
    followers = prospect_data.get("followers", 0)
    rating = prospect_data.get("rating")
    rating_count = prospect_data.get("rating_count", 0)
    
    if likes or followers or rating:
        summary_parts.append("\n=== PAGE ENGAGEMENT ===")
        if likes:
            summary_parts.append(f"Likes: {likes}")
        if followers:
            summary_parts.append(f"Followers: {followers}")
        if rating:
            summary_parts.append(f"Rating: {rating} stars ({rating_count} reviews)")
    
    # ============================================================
    # 4️⃣ CONTACT INFO
    # ============================================================
    phone = prospect_data.get("phone", "")
    email = prospect_data.get("email", "")
    website = prospect_data.get("website", "")
    location = prospect_data.get("location", "")
    
    if any([phone, email, website, location]):
        summary_parts.append("\n=== CONTACT INFORMATION ===")
        if website:
            summary_parts.append(f"Website: {website}")
        if location:
            summary_parts.append(f"Location: {location}")
        if email:
            summary_parts.append(f"Email: {email}")
    
    # ============================================================
    # 5️⃣ RECENT POSTS ⭐⭐⭐
    # ============================================================
    posts = prospect_data.get("posts", [])
    
    if posts and isinstance(posts, list) and len(posts) > 0:
        summary_parts.append("\n=== RECENT FACEBOOK POSTS ===")
        summary_parts.append("What topics/themes does this page post about? Use this to understand their focus:")
        
        for i, post in enumerate(posts[:5], 1):
            text = post.get("text", "")
            if text and len(text.strip()) > 20:
                summary_parts.append(f"\nPost {i}:")
                summary_parts.append(f"{text[:250]}")
                
                # Engagement context
                likes = post.get("likes", 0)
                comments = post.get("comments", 0)
                shares = post.get("shares", 0)
                
                engagement_parts = []
                if likes:
                    engagement_parts.append(f"{likes} likes")
                if comments:
                    engagement_parts.append(f"{comments} comments")
                if shares:
                    engagement_parts.append(f"{shares} shares")
                
                if engagement_parts:
                    summary_parts.append(f"Engagement: {', '.join(engagement_parts)}")
        
        summary_parts.append("\n^ Look for recurring themes, business focus, or community engagement style")
    else:
        summary_parts.append("\n⚠️ No recent posts available")
    
    # ============================================================
    # 6️⃣ AI INSTRUCTIONS
    # ============================================================
    summary_parts.append("\n" + "="*60)
    summary_parts.append("AI PERSONALIZATION GUIDE:")
    summary_parts.append("="*60)
    summary_parts.append("1. PRIORITY: Focus on their business mission and unique value")
    summary_parts.append("2. CONTEXT: Use description/about section to understand what they offer")
    summary_parts.append("3. INTERESTS: Look at post themes to find shared topics")
    summary_parts.append("4. SPECIFICITY: Mention actual business name and services")
    summary_parts.append("5. AUTHENTICITY: Make it feel like genuine interest, not spam")
    
    # ============================================================
    # BUILD FINAL SUMMARY
    # ============================================================
    result = "\n".join(summary_parts)
    
    # Debug statistics
    print(f"\n📊 Facebook Profile Summary Stats:")
    print(f"   - Total length: {len(result)} characters")
    print(f"   - Has description: {bool(description and len(description) > 20)}")
    print(f"   - Has about: {bool(about and len(about) > 20)}")
    print(f"   - Engagement metrics: Likes={likes}, Followers={followers}")
    print(f"   - Posts: {len(posts) if posts else 0} posts")
    print(f"   - Post content available: {sum(1 for p in (posts or []) if p.get('text', '').strip())}")
    
    return result


def generate_facebook_message(prospect_data: Dict, campaign_message: str, **kwargs) -> Dict:
    """
    HIGH-LEVEL: Generate AI message using OpenAI.
    
    RESPONSIBILITY: AI message generation only
    """
    # Extract profile summary
    profile_summary = extract_profile_summary(prospect_data)
    
    # Call OpenAI
    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": FACEBOOK_SYSTEM_PROMPT},
            {"role": "user", "content": FACEBOOK_USER_PROMPT.format(
                profile_summary=profile_summary,
                campaign_message=campaign_message
            )}
        ],
        temperature=0.7,
        max_tokens=300
    )
    
    generated_message = response.choices[0].message.content.strip()
    
    return {
        "status": "success",
        "message": generated_message,
        "tokens_used": response.usage.total_tokens
    }


def generate_batch_messages(
    prospects: List[Dict],
    campaign_message: str,
    model: str = "gpt-4o-mini"
) -> List[Dict]:
    """
    Generate messages for multiple Facebook prospects in batch.
    
    Args:
        prospects: List of enriched Facebook page/profile data
        campaign_message: User's campaign objective
        model: OpenAI model to use
        
    Returns:
        List of results with generated messages
    """
    results = []
    
    for idx, prospect in enumerate(prospects):
        print(f"\n📝 Generating Facebook message {idx + 1}/{len(prospects)}...")
        
        # Show prospect summary before generation
        print(f"\n{get_prospect_summary(prospect)}")
        
        result = generate_facebook_message(
            prospect_data=prospect,
            campaign_message=campaign_message,
            model=model
        )
        
        result["prospect_index"] = idx
        result["page_name"] = prospect.get("name", "Unknown")
        result["profile_url"] = prospect.get("profile_url", "")
        
        results.append(result)
    
    success_count = sum(1 for r in results if r.get("status") == "success")
    print(f"\n✅ Generated {success_count}/{len(prospects)} Facebook messages successfully")
    
    return results