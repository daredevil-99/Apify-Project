# services/scraping_service.py
from typing import List, Dict
from utils.apify_utils import scrape_instagram, scrape_linkedin, scrape_facebook
from services.db_service import save_scraped_data, get_scraped_posts, save_prospects
from datetime import datetime


def scrape_and_store(
    client_id: str,
    platform: str,
    search_terms: List[str],
    profession: str = None,
    location: str = None
):
    """
    Universal scraper that routes to the correct platform scraper
    and stores results in MongoDB.
    
    ✅ FIX: Properly handle all three parameters for LinkedIn
    """
    print(f"\n{'='*60}")
    print(f"🔍 Starting {platform.upper()} scraping for {client_id}")
    print(f"{'='*60}")
    print(f"📋 Search terms: {search_terms}")
    print(f"💼 Profession: {profession}")
    print(f"📍 Location: {location}")
    print()

    scraped_data_items = []

    try:
        if platform == "instagram":
            scraped_data_items = scrape_instagram(search_terms, profession, location)
            
        elif platform == "linkedin":
            # ✅ FIX: Pass all three parameters correctly
            scraped_data_items = scrape_linkedin(search_terms, profession, location)
            
        elif platform == "facebook":
            scraped_data_items = scrape_facebook(search_terms, profession, location)
            
        else:
            print(f"❌ Unsupported platform: {platform}")
            return

        if not scraped_data_items:
            print(f"⚠️  No data scraped from {platform}")
            return

        print(f"✅ Scraped {len(scraped_data_items)} items from {platform}")

        # Save to database
        save_scraped_data(client_id, platform, scraped_data_items)
        print(f"💾 Saved scraped data to database")

    except Exception as e:
        print(f"❌ Scraping error for {platform}: {e}")
        import traceback
        traceback.print_exc()


def extract_and_store_prospects(client_id: str, platform: str) -> int:
    """
    Extract prospects from scraped data and store them separately.
    Returns the count of prospects extracted.
    """
    print(f"\n{'='*60}")
    print(f"🎯 Extracting prospects for {client_id} ({platform})")
    print(f"{'='*60}\n")

    # Get scraped posts
    scraped_posts = get_scraped_posts(client_id, platform)
    
    if not scraped_posts:
        print(f"⚠️  No scraped data found for {client_id}")
        return 0

    print(f"📊 Found {len(scraped_posts)} scraped posts")

    prospects = []
    
    for post in scraped_posts:
        prospect = extract_prospect_info(post, platform)
        if prospect:
            prospects.append(prospect)

    if prospects:
        save_prospects(client_id, platform, prospects)
        print(f"✅ Extracted and saved {len(prospects)} prospects")
    else:
        print("⚠️  No valid prospects extracted")

    return len(prospects)


def extract_prospect_info(post: Dict, platform: str) -> Dict:
    """
    Extract relevant prospect information from a scraped post.
    Returns a normalized prospect dict.
    """
    prospect = {
        "status": "new",
        "platform": platform,
        "extracted_at": datetime.utcnow().isoformat()
    }

    # In scraping_service.py, line 64-75
    if platform == "instagram":
        # ✅ Ensure ownerUsername is properly stored
        owner_username = post.get("ownerUsername") or post.get("username")
        
        prospect.update({
            "username": owner_username,  # ✅ Store correct username
            "ownerUsername": owner_username,  # ✅ Keep this field too
            "profile_url": f"https://instagram.com/{owner_username}" if owner_username else None,
            "full_name": post.get("ownerFullName"),
            "ownerFullName": post.get("ownerFullName"),  # ✅ Store this too
            "follower_count": post.get("ownerFollowersCount"),
            "post_url": post.get("url"),  # This is the POST URL (keep separate)
            "caption": post.get("caption"),
            "hashtags": post.get("hashtags", []),
            "likes": post.get("likesCount"),
            "comments": post.get("commentsCount"),
            "location_relevance_score": post.get("location_relevance_score", 0),
            "ownerId": post.get("ownerId")  # ✅ Store owner ID
        })

    elif platform == "linkedin":
        # ✅ FIX: Handle LinkedIn profile data structure properly
        # Extract location (can be dict or string)
        location = post.get("location")
        if isinstance(location, dict):
            location = location.get("linkedinText", "")
        
        # Extract current position info
        current_positions = post.get("currentPositions", [])
        company = ""
        position = ""
        if current_positions and len(current_positions) > 0:
            first_position = current_positions[0]
            company = first_position.get("companyName", "")
            position = first_position.get("title", "")
        
        # Build full name
        first_name = post.get("firstName", "")
        last_name = post.get("lastName", "")
        full_name = f"{first_name} {last_name}".strip()
        
        # LinkedIn URL or ID for username
        linkedin_url = post.get("linkedinUrl", "")
        username = linkedin_url.split("/in/")[-1].rstrip("/") if "/in/" in linkedin_url else post.get("id", "")
        
        prospect.update({
            "username": username,
            "profile_url": linkedin_url,
            "full_name": full_name,
            "first_name": first_name,
            "last_name": last_name,
            "headline": post.get("summary", ""),
            "location": location,
            "company": company,
            "position": position,
            "profile_picture": post.get("pictureUrl"),
            "premium": post.get("premium", False),
            "open_profile": post.get("openProfile", False),
            "current_positions": current_positions
        })

    if platform == "facebook":
        # Extract core identifiers
        page_id = post.get("pageId") or post.get("facebookId")
        page_name = post.get("pageName", "")
        page_url = post.get("pageUrl") or post.get("facebookUrl", "")
        
        # Page details
        title = post.get("title", "")
        categories = post.get("categories", [])
        category = categories[0] if categories else "Business"
        
        # Description/About
        info_array = post.get("info", [])
        description = " ".join(info_array) if info_array else ""
        
        about_me = post.get("about_me", {})
        about_text = ""
        if isinstance(about_me, dict):
            about_text = about_me.get("text", "")
        
        # Contact info
        email = post.get("email", "")
        phone = post.get("phone", "")
        website = post.get("website", "")
        address = post.get("address", "")
        
        # Engagement metrics
        likes = post.get("likes", 0)
        followers = post.get("followers", 0)
        rating_overall = post.get("ratingOverall")
        rating_count = post.get("ratingCount", 0)
        
        prospect.update({
            # Core identifiers
            "username": page_name,
            "page_id": page_id,
            "name": title or page_name,
            "profile_url": page_url,
            
            # Page details
            "type": "page",
            "category": category,
            "categories": categories,
            "description": description,
            "about": about_text,
            
            # Contact info
            "email": email,
            "phone": phone,
            "website": website,
            "location": address,
            
            # Metrics
            "likes": likes,
            "followers": followers,
            "rating": rating_overall,
            "rating_count": rating_count,
            
            # Status
            "enriched": False,
            "source": "facebook_search_scraper",
            "scraped_at": datetime.utcnow().isoformat(),
            
            # Additional
            "creation_date": post.get("creation_date"),
            "ad_status": post.get("ad_status"),
            "price_range": post.get("priceRange"),
            "messenger": post.get("messenger"),
        })

    # Remove empty values
    prospect = {k: v for k, v in prospect.items() if v not in [None, "", [], {}]}

    # Validate
    if prospect.get("username") and prospect.get("profile_url"):
        return prospect
    
    return None
