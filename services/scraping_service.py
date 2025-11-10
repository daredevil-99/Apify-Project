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
# HELPER FUNCTIONS
# ========================================

def extract_mentions_from_text(text: str) -> List[str]:
    """Extract @mentions from text"""
    if not text:
        return []
    mentions = re.findall(r'@(\w+)', text)
    return mentions


def extract_email_from_text(text: str) -> str:
    """Extract email from text"""
    if not text:
        return ""
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    emails = re.findall(email_pattern, text)
    return emails[0] if emails else ""


def extract_phone_from_text(text: str) -> str:
    """Extract phone number from text"""
    if not text:
        return ""
    phone_pattern = r'[\+]?[(]?[0-9]{1,4}[)]?[-\s\.]?[(]?[0-9]{1,4}[)]?[-\s\.]?[0-9]{1,4}[-\s\.]?[0-9]{1,9}'
    phones = re.findall(phone_pattern, text)
    return phones[0] if phones else ""


# ========================================
# INSTAGRAM PROSPECT EXTRACTION
# ========================================

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


# ========================================
# LINKEDIN PROSPECT EXTRACTION
# ========================================

def extract_prospects_from_linkedin_profiles(profiles_data: List[Dict]) -> List[Dict]:
    """
    Extract prospect profiles from LinkedIn search results
    LinkedIn data is already profile-based, so we just need to format it
    """
    prospects = []
    
    print("\n🔍 Extracting prospects from LinkedIn profiles...")
    
    for profile in profiles_data:
        username = profile.get('username', '')
        if not username:
            continue
        
        # Extract contact info from about/summary
        about_text = profile.get('about', '')
        email = extract_email_from_text(about_text)
        phone = extract_phone_from_text(about_text)
        
        # Build prospect profile
        prospect = {
            'platform': 'linkedin',
            'username': username,
            'full_name': profile.get('full_name', ''),
            'profile_url': profile.get('profile_url', ''),
            'profile_pic_url': profile.get('profile_pic_url', ''),
            'headline': profile.get('headline', ''),
            'location': profile.get('location', ''),
            'bio': about_text,
            'email': email,
            'phone': phone,
            'website': '',
            
            # LinkedIn specific
            'current_company': profile.get('current_company', ''),
            'current_position': profile.get('current_position', ''),
            'connections_count': profile.get('connections_count', 0),
            'followers_count': profile.get('followers_count', 0),
            'is_premium': profile.get('is_premium', False),
            'is_open_to_work': profile.get('is_open_to_work', False),
            
            # Professional background
            'experience': profile.get('experience', []),
            'education': profile.get('education', []),
            'skills': profile.get('skills', []),
            
            # Engagement data
            'engagement_data': {
                'posts_count': profile.get('posts_count', 0),
                'recent_posts': profile.get('posts', [])[:3],  # Keep last 3 posts
                'search_query': profile.get('search_query', ''),
                'discovered_date': datetime.now().isoformat()
            },
            
            'status': 'new',
            'last_contacted': None,
            'created_at': datetime.now().isoformat()
        }
        prospects.append(prospect)
        print(f"  ✓ Found prospect: {prospect['full_name']} ({username})")
    
    print(f"\n📊 Total unique prospects found: {len(prospects)}")
    return prospects


# ========================================
# FACEBOOK PROSPECT EXTRACTION
# ========================================

def extract_prospects_from_facebook_posts(posts_data: List[Dict]) -> List[Dict]:
    """
    Extract prospect profiles from Facebook posts
    Focus on post authors and engaged commenters
    """
    prospects = {}
    
    print("\n🔍 Extracting prospects from Facebook posts...")
    
    for post in posts_data:
        # Method 1: Extract post author as prospect
        author_url = post.get('author_url', '')
        if author_url and author_url not in prospects:
            author_name = post.get('author_name', '')
            
            # Try to extract username from URL
            username = author_url.split('/')[-1] if author_url else ''
            
            prospects[author_url] = {
                'platform': 'facebook',
                'username': username,
                'full_name': author_name,
                'profile_url': author_url,
                'profile_pic_url': post.get('author_profile_pic', ''),
                'bio': '',
                'email': '',
                'phone': '',
                'website': '',
                
                # Engagement metrics from their post
                'engagement_data': {
                    'post_url': post.get('post_url', ''),
                    'post_text': post.get('text', '')[:200],
                    'likes': post.get('likes', 0),
                    'comments': post.get('comments_count', 0),
                    'shares': post.get('shares', 0),
                    'discovered_date': datetime.now().isoformat()
                },
                
                'status': 'new',
                'last_contacted': None,
                'created_at': datetime.now().isoformat()
            }
            print(f"  ✓ Found author prospect: {author_name}")
        
        # Method 2: Extract engaged commenters (top commenters)
        comment_details = post.get('comment_details', [])
        for comment in comment_details[:10]:  # Top 10 commenters
            commenter_url = comment.get('authorUrl', '')
            commenter_name = comment.get('authorName', '')
            
            if commenter_url and commenter_url not in prospects:
                username = commenter_url.split('/')[-1] if commenter_url else ''
                
                prospects[commenter_url] = {
                    'platform': 'facebook',
                    'username': username,
                    'full_name': commenter_name,
                    'profile_url': commenter_url,
                    'profile_pic_url': '',
                    'bio': '',
                    'email': '',
                    'phone': '',
                    'website': '',
                    
                    # Engagement context
                    'engagement_data': {
                        'commented_on_post': post.get('post_url', ''),
                        'comment_text': comment.get('text', '')[:200],
                        'comment_likes': comment.get('likes', 0),
                        'discovered_date': datetime.now().isoformat()
                    },
                    
                    'status': 'new',
                    'last_contacted': None,
                    'created_at': datetime.now().isoformat()
                }
                print(f"  ✓ Found commenter prospect: {commenter_name}")
    
    prospect_list = list(prospects.values())
    print(f"\n📊 Total unique prospects found: {len(prospect_list)}")
    return prospect_list


# ========================================
# MAIN EXTRACTION FUNCTION
# ========================================

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
        
        print(f"📄 Found {len(posts)} items to analyze")
        
        # Extract prospects based on platform
        if platform.lower() == "instagram":
            prospects = extract_prospects_from_instagram_posts(posts)
        elif platform.lower() == "linkedin":
            prospects = extract_prospects_from_linkedin_profiles(posts)
        elif platform.lower() == "facebook":
            prospects = extract_prospects_from_facebook_posts(posts)
        else:
            return {
                "status": "error",
                "message": f"Platform {platform} not supported for prospect extraction"
            }
        
        if not prospects:
            print("⚠️  No prospects found in the data")
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