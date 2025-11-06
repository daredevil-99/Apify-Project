# services/scraping_service.py
from typing import Dict, List
from datetime import datetime
import re
from utils.apify_utils import scrape_instagram, scrape_linkedin, scrape_facebook
from services.db_service import save_scraped_profiles, get_audience_collection


def scrape_and_store(client_id: str, platform: str, search_terms: List[str], 
                     profession: str = None, location: str = None) -> Dict:
    """
    Internal function: Scrape platform and store in MongoDB
    This is called by pipeline_service, NOT exposed as endpoint
    """
    print(f"\n{'='*60}")
    print(f"🔍 Starting {platform.upper()} scraping for {client_id}")
    print(f"{'='*60}\n")

    # Route to correct scraper
    if platform == "instagram":
        results = scrape_instagram(search_terms, profession, location)
    elif platform == "linkedin":
        results = scrape_linkedin(search_terms, profession, location)
    elif platform == "facebook":
        results = scrape_facebook(search_terms, profession, location)
    else:
        return {"status": "error", "message": f"Unsupported platform: {platform}"}

    # Save to MongoDB
    saved_count = save_scraped_profiles(client_id, platform, results)

    if saved_count > 0:
        return {
            "status": "success",
            "profiles_count": saved_count,
            "message": f"Scraped and saved {saved_count} {platform} profiles"
        }
    else:
        return {
            "status": "no_data",
            "profiles_count": 0,
            "message": f"No {platform} profiles found for given criteria"
        }


# ========================================
# PROSPECT EXTRACTION METHODS - ADD THESE
# ========================================

def extract_mentions_from_text(text: str) -> List[str]:
    """Extract @mentions from text"""
    if not text:
        return []
    mentions = re.findall(r'@(\w+)', text)
    return mentions


def extract_prospects_from_instagram_posts(posts_data: List[Dict]) -> List[Dict]:
    """
    Extract unique prospect profiles from Instagram posts data
    """
    prospects = {}
    
    print("\n🔍 Extracting prospects from Instagram posts...")
    
    for post in posts_data:
        # Method 1: Extract from tagged users
        if 'childPosts' in post:
            for child_post in post.get('childPosts', []):
                if 'taggedUsers' in child_post:
                    for user in child_post.get('taggedUsers', []):
                        username = user.get('username')
                        if username and username not in prospects:
                            prospects[username] = {
                                'platform': 'instagram',
                                'username': username,
                                'full_name': user.get('full_name', ''),
                                'profile_url': f"https://instagram.com/{username}",
                                'profile_pic_url': user.get('profile_pic_url', ''),
                                'is_verified': user.get('is_verified', False),
                                'is_private': user.get('is_private', False),
                                'bio': '',
                                'email': '',
                                'phone': '',
                                'website': '',
                                'followers': 0,
                                'following': 0,
                                'posts_count': 0,
                                'engagement_data': {
                                    'tagged_in_post': post.get('unique_key', ''),
                                    'post_caption': post.get('caption', '')[:200],
                                    'discovered_date': datetime.now().isoformat()
                                },
                                'status': 'new',
                                'last_contacted': None,
                                'created_at': datetime.now().isoformat()
                            }
                            print(f"  ✓ Found prospect: @{username}")
        
        # Method 2: Extract from mentions in caption
        caption = post.get('caption', '')
        mentions = extract_mentions_from_text(caption)
        for mention in mentions:
            if mention not in prospects:
                prospects[mention] = {
                    'platform': 'instagram',
                    'username': mention,
                    'full_name': '',
                    'profile_url': f"https://instagram.com/{mention}",
                    'profile_pic_url': '',
                    'is_verified': False,
                    'is_private': False,
                    'bio': '',
                    'email': '',
                    'phone': '',
                    'website': '',
                    'followers': 0,
                    'following': 0,
                    'posts_count': 0,
                    'engagement_data': {
                        'mentioned_in_post': post.get('unique_key', ''),
                        'post_caption': caption[:200],
                        'discovered_date': datetime.now().isoformat()
                    },
                    'status': 'new',
                    'last_contacted': None,
                    'created_at': datetime.now().isoformat()
                }
                print(f"  ✓ Found mention: @{mention}")
    
    prospect_list = list(prospects.values())
    print(f"\n📊 Total unique prospects found: {len(prospect_list)}")
    return prospect_list


def extract_and_store_prospects(client_id: str, platform: str) -> Dict:
    """
    Main function: Extract prospects from scraped data and store in audience collection
    This runs in background task
    """
    try:
        print(f"\n{'='*60}")
        print(f"🚀 Starting prospect extraction for client: {client_id}")
        print(f"   Platform: {platform.upper()}")
        print(f"{'='*60}\n")
        
        # Get audience collection
        audience_collection = get_audience_collection()
        
        # Fetch scraped posts for this client (exclude prospects document)
        posts = list(
            audience_collection.find(
                {
                    "client_id": client_id, 
                    "platform": platform,
                    "type": {"$ne": "prospects"}  # Exclude existing prospects doc
                },
                {"_id": 0}
            )
        )
        
        if not posts:
            print("❌ No scraped posts found. Run scraping first.")
            return {
                "status": "error",
                "message": "No scraped data found. Run scraping first."
            }
        
        print(f"📄 Found {len(posts)} posts to analyze")
        
        # Extract prospects based on platform
        if platform.lower() == "instagram":
            prospects = extract_prospects_from_instagram_posts(posts)
        elif platform.lower() == "linkedin":
            # TODO: Implement LinkedIn extraction
            print("⚠️  LinkedIn prospect extraction not yet implemented")
            prospects = []
        elif platform.lower() == "facebook":
            # TODO: Implement Facebook extraction
            print("⚠️  Facebook prospect extraction not yet implemented")
            prospects = []
        else:
            return {
                "status": "error",
                "message": f"Platform {platform} not yet supported for prospect extraction"
            }
        
        if not prospects:
            print("⚠️  No prospects found in the posts")
            return {
                "status": "warning",
                "message": "No prospects found in the scraped data",
                "prospects_count": 0
            }
        
        # Store prospects in a separate document
        prospects_doc = {
            "client_id": client_id,
            "platform": platform,
            "type": "prospects",  # Special marker to identify prospects document
            "prospects": prospects,
            "prospects_count": len(prospects),
            "last_updated": datetime.now().isoformat(),
            "extraction_date": datetime.now().isoformat()
        }
        
        # Update or insert prospects document
        result = audience_collection.update_one(
            {
                "client_id": client_id, 
                "platform": platform,
                "type": "prospects"
            },
            {"$set": prospects_doc},
            upsert=True
        )
        
        print(f"\n✅ Successfully stored {len(prospects)} prospects in database")
        print(f"{'='*60}\n")
        
        return {
            "status": "success",
            "prospects_count": len(prospects),
            "message": f"Extracted and stored {len(prospects)} prospects"
        }
        
    except Exception as e:
        print(f"\n❌ Error in prospect extraction: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "message": f"Error extracting prospects: {str(e)}"
        }