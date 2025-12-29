# services/facebook_prospect_extraction.py
"""
Facebook-specific prospect extraction logic
Handles the unique data structure from apify/facebook-search-scraper
"""

from typing import List, Dict
from datetime import datetime


def extract_facebook_prospects(scraped_data: List[Dict], client_data: Dict) -> List[Dict]:
    """
    Extract prospects from Facebook search results.
    
    Facebook Search Scraper returns PAGES/BUSINESSES with structure:
    {
        "pageName": "Jordanmcinnisdesigns",
        "pageUrl": "https://www.facebook.com/Jordanmcinnisdesigns/",
        "title": "Jordan Mcinnis Designs | Montreal QC",
        "email": "mcinnisj.design@gmail.com",
        "phone": "+1 514-716-0922",
        "website": "jordanmcinnis.com",
        "categories": ["Page", "Graphic Designer"],
        "likes": 88,
        "followers": 86,
        "info": ["Description text..."],
        "address": "4972 belleville, Montreal, QC...",
        "pageId": "100068579062225",
        "about_me": {"text": "..."}
    }
    
    Args:
        scraped_data: Raw data from facebook-search-scraper
        client_data: Client information for context
        
    Returns:
        List of standardized prospect dictionaries
    """
    prospects = []
    seen_page_ids = set()
    
    print(f"\n{'='*60}")
    print(f"🔍 FACEBOOK PROSPECT EXTRACTION")
    print(f"{'='*60}")
    print(f"Input: {len(scraped_data)} scraped items")
    
    for idx, item in enumerate(scraped_data):
        try:
            # ============================================================
            # EXTRACT CORE IDENTIFIERS
            # ============================================================
            page_id = item.get("pageId") or item.get("facebookId")
            page_name = item.get("pageName", "")
            page_url = item.get("pageUrl") or item.get("facebookUrl", "")
            
            # Skip if missing critical data
            if not page_id or not page_url:
                print(f"⚠️  Item {idx+1}: Missing pageId or pageUrl, skipping")
                continue
            
            # Skip duplicates
            if page_id in seen_page_ids:
                print(f"⚠️  Item {idx+1}: Duplicate page_id {page_id}, skipping")
                continue
            
            seen_page_ids.add(page_id)
            
            # ============================================================
            # EXTRACT PAGE DETAILS
            # ============================================================
            
            # Page title (usually includes business name + location)
            title = item.get("title", "")
            
            # Categories (e.g., ["Page", "Graphic Designer"])
            categories = item.get("categories", [])
            category = categories[0] if categories else "Business"
            
            # Info/description (array of text snippets)
            info_array = item.get("info", [])
            description = " ".join(info_array) if info_array else ""
            
            # About section (detailed business info)
            about_me = item.get("about_me", {})
            about_text = ""
            if isinstance(about_me, dict):
                about_text = about_me.get("text", "")
            
            # ============================================================
            # CONTACT INFORMATION
            # ============================================================
            email = item.get("email", "")
            phone = item.get("phone", "")
            website = item.get("website", "")
            address = item.get("address", "")
            
            # ============================================================
            # ENGAGEMENT METRICS
            # ============================================================
            likes = item.get("likes", 0)
            followers = item.get("followers", 0)
            rating_overall = item.get("ratingOverall")
            rating_count = item.get("ratingCount", 0)
            
            # ============================================================
            # BUILD PROSPECT OBJECT
            # ============================================================
            prospect = {
                # Core identifiers
                "username": page_name,  # Using pageName as username equivalent
                "page_id": page_id,
                "name": title or page_name,  # Full title or fallback to pageName
                "profile_url": page_url,
                "platform": "facebook",
                
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
                
                # Status tracking
                "status": "new",
                "enriched": False,
                "source": "facebook_search_scraper",
                "scraped_at": datetime.utcnow().isoformat(),
                
                # Additional data (optional)
                "creation_date": item.get("creation_date"),
                "ad_status": item.get("ad_status"),
                "price_range": item.get("priceRange"),
                "messenger": item.get("messenger"),
            }
            
            # Remove None/empty values
            prospect = {k: v for k, v in prospect.items() if v not in [None, "", [], {}]}
            
            prospects.append(prospect)
            
            # Log successful extraction
            print(f"✅ Extracted: {prospect['name']}")
            print(f"   - Page ID: {page_id}")
            print(f"   - Category: {category}")
            if email:
                print(f"   - Email: {email}")
            if website:
                print(f"   - Website: {website}")
            
        except Exception as e:
            print(f"❌ Error extracting item {idx+1}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print(f"\n{'='*60}")
    print(f"✅ EXTRACTION COMPLETE")
    print(f"{'='*60}")
    print(f"Total prospects extracted: {len(prospects)}")
    print(f"Success rate: {len(prospects)}/{len(scraped_data)} ({len(prospects)/len(scraped_data)*100:.1f}%)")
    
    # Show sample if available
    if prospects:
        sample = prospects[0]
        print(f"\n📋 SAMPLE PROSPECT:")
        print(f"   Name: {sample.get('name')}")
        print(f"   Category: {sample.get('category')}")
        print(f"   Email: {sample.get('email', 'N/A')}")
        print(f"   Website: {sample.get('website', 'N/A')}")
    
    print(f"{'='*60}\n")
    
    return prospects


def validate_facebook_prospect(prospect: Dict) -> bool:
    """
    Validate that a prospect has minimum required data.
    
    Args:
        prospect: Prospect dictionary
        
    Returns:
        True if valid, False otherwise
    """
    required_fields = ["username", "profile_url", "page_id"]
    
    for field in required_fields:
        if not prospect.get(field):
            print(f"⚠️  Invalid prospect: Missing {field}")
            return False
    
    return True


def enrich_prospect_with_posts(prospect: Dict, posts_data: List[Dict]) -> Dict:
    """
    Enrich a prospect with posts from facebook-posts-scraper.
    
    Args:
        prospect: Base prospect data
        posts_data: Posts from apify/facebook-posts-scraper
        
    Returns:
        Enriched prospect with posts
    """
    if not posts_data:
        print(f"⚠️  No posts data to enrich {prospect.get('name')}")
        return prospect
    
    # Extract posts (usually nested in first result)
    posts = []
    
    for item in posts_data:
        # Check if this is a page-level result with nested posts
        if "posts" in item and isinstance(item["posts"], list):
            posts.extend(item["posts"])
        # Or if it's already a post object
        elif item.get("postId"):
            posts.append(item)
    
    # Add posts to prospect
    prospect["posts"] = posts
    prospect["enriched"] = True
    prospect["enriched_at"] = datetime.utcnow().isoformat()
    prospect["posts_count"] = len(posts)
    
    # Calculate engagement metrics
    if posts:
        total_likes = sum(p.get("likes", 0) for p in posts)
        total_comments = sum(p.get("comments", 0) for p in posts)
        total_shares = sum(p.get("shares", 0) for p in posts)
        
        prospect["engagement"] = {
            "total_likes": total_likes,
            "total_comments": total_comments,
            "total_shares": total_shares,
            "avg_likes_per_post": total_likes / len(posts) if posts else 0,
            "total_engagement": total_likes + total_comments + total_shares
        }
    
    print(f"✅ Enriched {prospect.get('name')} with {len(posts)} posts")
    
    return prospect


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def filter_prospects_by_criteria(
    prospects: List[Dict],
    min_followers: int = None,
    has_email: bool = None,
    categories: List[str] = None,
    location_keywords: List[str] = None
) -> List[Dict]:
    """
    Filter prospects based on criteria.
    
    Args:
        prospects: List of prospects
        min_followers: Minimum follower count
        has_email: Whether email is required
        categories: List of required categories
        location_keywords: Keywords to match in location
        
    Returns:
        Filtered list of prospects
    """
    filtered = prospects
    
    if min_followers is not None:
        filtered = [p for p in filtered if p.get("followers", 0) >= min_followers]
        print(f"🔍 Filtered by min_followers ({min_followers}): {len(filtered)} remaining")
    
    if has_email:
        filtered = [p for p in filtered if p.get("email")]
        print(f"🔍 Filtered by has_email: {len(filtered)} remaining")
    
    if categories:
        filtered = [
            p for p in filtered
            if any(cat.lower() in str(p.get("categories", [])).lower() for cat in categories)
        ]
        print(f"🔍 Filtered by categories {categories}: {len(filtered)} remaining")
    
    if location_keywords:
        filtered = [
            p for p in filtered
            if any(kw.lower() in str(p.get("location", "")).lower() for kw in location_keywords)
        ]
        print(f"🔍 Filtered by location {location_keywords}: {len(filtered)} remaining")
    
    return filtered


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