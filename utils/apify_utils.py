# utils/apify_utils.py
import os
import re
from typing import List, Dict
from apify_client import ApifyClient
from dotenv import load_dotenv
import time


load_dotenv()

APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
if not APIFY_API_TOKEN:
    raise ValueError("Missing APIFY_API_TOKEN in .env")

apify_client = ApifyClient(APIFY_API_TOKEN)


def clean_hashtag(tag: str) -> str:
    """Clean hashtag for Instagram"""
    cleaned = re.sub(r'[^a-zA-Z0-9_]', '', tag.replace(' ', ''))
    return cleaned.lower()


def get_popular_hashtags(profession: str, location: str = None) -> List[str]:
    """
    Get popular Instagram hashtags for a profession.
    Returns hashtags that are actually used on Instagram (verified manually).
    """
    profession_lower = profession.lower() if profession else ""
    
    # Popular hashtag mapping (these are real hashtags with good engagement)
    hashtag_map = {
        'makeupartist': ['makeup', 'mua', 'makeupartist', 'makeuplover', 'bridalmakeup'],
        'makeup': ['makeup', 'mua', 'makeupartist', 'makeuplover', 'bridalmakeup'],
        'photographer': ['photography', 'photographer', 'photo', 'photoshoot', 'portraits'],
        'photography': ['photography', 'photographer', 'photo', 'photoshoot', 'portraits'],
        'weddingphotographer': ['weddingphotography', 'weddingphotographer', 'bride', 'wedding'],
        'weddingphotography': ['weddingphotography', 'weddingphotographer', 'bride', 'wedding'],
        'baker': ['baking', 'baker', 'cakes', 'homebaker', 'dessert'],
        'chef': ['chef', 'food', 'cooking', 'foodie', 'instafood'],
        'designer': ['design', 'designer', 'graphicdesign', 'creative'],
        'fitness': ['fitness', 'gym', 'workout', 'fitnessmotivation', 'trainer'],
    }
    
    # Find matching hashtags
    hashtags = []
    for key, tags in hashtag_map.items():
        if key in profession_lower:
            hashtags = tags.copy()
            break
    
    # If no match, use the profession itself
    if not hashtags:
        hashtags = [profession_lower]
    
    # Add location-specific versions if location provided
    if location:
        loc_clean = clean_hashtag(location)
        location_hashtags = []
        
        # Add location to each hashtag
        for tag in hashtags[:3]:  # Only add location to top 3 hashtags
            location_hashtags.append(f"{tag}{loc_clean}")
        
        # Also add standalone location
        location_hashtags.append(loc_clean)
        
        # Combine: location-specific first, then general
        hashtags = location_hashtags + hashtags
    
    return hashtags[:8]  # Limit to 8 hashtags


def scrape_instagram(search_terms: List[str], profession: str = None, location: str = None) -> List[Dict]:
    """
    Simple Instagram scraper - fetch profiles for given hashtags.
    
    Args:
        search_terms: List of hashtags to search
                     - If empty/generic, will auto-generate from profession
        profession: Profession to search (used for hashtag suggestions)
        location: Location to add to hashtags
    
    Returns:
        List of Instagram profiles from posts matching the hashtags
    """
    try:
        # Clean and prepare hashtags from search terms
        hashtags = []
        for term in search_terms:
            cleaned = clean_hashtag(term)
            if cleaned and len(cleaned) >= 2:
                hashtags.append(cleaned)
        
        # If no valid hashtags or only generic ones, use profession-based suggestions
        if not hashtags or (len(hashtags) <= 2 and profession):
            print(f"⚠️ Using profession-based hashtag suggestions...")
            suggested = get_popular_hashtags(profession, location)
            print(f"💡 Suggested hashtags: {suggested}")
            
            # Merge with existing, prefer suggested ones
            hashtags = suggested + [h for h in hashtags if h not in suggested]
        
        # Add location-based hashtags if location is provided and not already added
        elif location:
            loc_clean = clean_hashtag(location)
            
            # Add standalone location hashtag
            if loc_clean not in hashtags:
                hashtags.append(loc_clean)
            
            # Add profession+location combo if profession exists
            if profession:
                prof_clean = clean_hashtag(profession)
                combined = f"{prof_clean}{loc_clean}"
                if combined not in hashtags:
                    hashtags.insert(0, combined)  # Add at beginning (highest priority)
        
        # Remove duplicates while preserving order
        seen = set()
        hashtags = [x for x in hashtags if not (x in seen or seen.add(x))]
        
        # Limit to 8 hashtags max
        hashtags = hashtags[:8]
        
        if not hashtags:
            print("⚠️ No valid hashtags provided")
            return []
        
        print(f"\n📸 Instagram Search:")
        print(f"   Hashtags: {hashtags}")
        if location:
            print(f"   Location Focus: {location}")
        if profession:
            print(f"   Profession: {profession}")
        
        # Call Apify - exact same as UI
        actor_id = "apify/instagram-hashtag-scraper"
        payload = {
            "hashtags": hashtags,
            "resultsType": "posts",
            "resultsLimit": 20,
            "addParentData": False
        }
        
        print(f"🔍 Calling Apify Actor with payload: {payload}")
        
        run = apify_client.actor(actor_id).call(run_input=payload)
        
        if not run or "defaultDatasetId" not in run:
            print("⚠️ Instagram actor returned no dataset")
            return []
        
        dataset_id = run["defaultDatasetId"]
        items = list(apify_client.dataset(dataset_id).iterate_items())
        
        print(f"✅ Retrieved {len(items)} posts from Apify")
        print(f"💾 Dataset: https://console.apify.com/storage/datasets/{dataset_id}")
        
        if not items:
            print("⚠️ No posts found for these hashtags!")
            print("💡 Suggestions:")
            print("   - Try more popular hashtags (check Instagram to see post counts)")
            print("   - Use broader terms like 'makeup' instead of 'makeupartist'")
            print("   - Check Apify dataset URL above to see what was returned")
            return []
        
        # Show sample of what we got
        print(f"\n📋 Sample posts retrieved:")
        for i, item in enumerate(items[:3], 1):
            username = item.get('ownerUsername', 'unknown')
            caption = item.get('caption', '')[:60]
            hashtags_sample = item.get('hashtags', [])[:3]
            print(f"   {i}. @{username}")
            print(f"      Caption: {caption}...")
            print(f"      Hashtags: {hashtags_sample}")
        
        # Extract unique profiles from posts
        profiles_map = {}
        
        for post in items:
            username = post.get('ownerUsername')
            if not username:
                continue
            
            # If we already have this profile, just increment post count
            if username in profiles_map:
                profiles_map[username]['post_count'] += 1
                profiles_map[username]['total_likes'] += post.get('likesCount', 0)
            else:
                # Create new profile entry
                profiles_map[username] = {
                    "profile_url": f"https://instagram.com/{username}",
                    "username": username,
                    "full_name": post.get('ownerFullName', ''),
                    "profile_pic_url": post.get('ownerProfilePicUrl', ''),
                    "verified": post.get('ownerIsVerified', False),
                    "bio": "",  # Empty until profile enrichment
                    "followers": 0,
                    "following": 0,
                    "posts": 0,
                    "private": False,
                    
                    # Metadata for sorting
                    "post_count": 1,
                    "total_likes": post.get('likesCount', 0),
                }
        
        profiles = list(profiles_map.values())
        
        print(f"✅ Found {len(profiles)} unique profiles")
        
        # Sort by engagement (profiles with more matching posts first)
        profiles.sort(key=lambda x: (
            x.get('post_count', 0) * 1000 +  # More posts = more relevant
            x.get('total_likes', 0)           # More likes = higher quality
        ), reverse=True)
        
        # Return top 20 profiles
        result_profiles = profiles[:20]
        
        print(f"\n🏆 Returning {len(result_profiles)} profiles")
        if result_profiles:
            print("\nTop 5 Profiles:")
            for i, profile in enumerate(result_profiles[:5], 1):
                verified = "✓" if profile.get('verified') else " "
                print(f"   {i}. {verified} @{profile['username']}")
                print(f"      Found in {profile['post_count']} posts, {profile['total_likes']} total likes")
        
        return result_profiles
    
    except Exception as e:
        print(f"❌ Instagram scraping error: {e}")
        import traceback
        traceback.print_exc()
        return []



def enrich_instagram_profiles(profile_urls: List[str]) -> Dict[str, Dict]:
    """
    Enrich Instagram profiles by fetching full profile data + recent posts
    using Apify Instagram Profile Scraper.
    
    Args:
        profile_urls: List of Instagram profile URLs to enrich
        
    Returns:
        Dict mapping normalized URLs to enriched profile data:
        {
            "instagram.com/username": {
                "username": "username",
                "fullName": "Full Name",
                "biography": "Bio text...",
                "followersCount": 1000,
                "followsCount": 500,
                "postsCount": 100,
                "verified": False,
                "private": False,
                "latestPosts": [...],
                "engagement_rate": 3.5,
                "avg_likes": 50,
                "avg_comments": 10,
                "status": "success|no_posts|error",
                "error": None
            }
        }
    """
    try:
        if not profile_urls:
            print("⚠️ No profile URLs provided")
            return {}
        
        actor_id = "apify/instagram-profile-scraper"
        all_results = {}
        
        # Extract usernames from URLs
        usernames = []
        url_to_username = {}
        
        for url in profile_urls:
            # Extract username from URL
            if '/' in url:
                username = url.split('/')[-1].strip('/').replace('@', '')
            else:
                username = url.strip().replace('@', '')
            
            if username:
                usernames.append(username)
                norm_url = f"instagram.com/{username}".lower()
                url_to_username[norm_url] = username
        
        if not usernames:
            print("⚠️ No valid usernames extracted from URLs")
            return {}
        
        print(f"\n📸 Enriching {len(usernames)} Instagram profiles...")
        print(f"👥 Usernames: {usernames[:5]}{'...' if len(usernames) > 5 else ''}")
        
        # Build payload for Instagram Profile Scraper
        payload = {
            "usernames": usernames,
            "resultsLimit": len(usernames),
            "addParentData": True,  # Include additional data
            "extendOutputFunction": "",
            "extendScraperFunction": ""
        }
        
        print(f"🚀 Running Instagram Profile Scraper...")
        
        run = apify_client.actor(actor_id).call(run_input=payload)
        
        if not run or "defaultDatasetId" not in run:
            print("⚠️ Instagram actor returned no dataset")
            # Return error status for all profiles
            for norm_url in url_to_username.keys():
                all_results[norm_url] = {
                    "status": "error",
                    "error": "No dataset returned from Apify",
                    "posts": []
                }
            return all_results
        
        dataset_id = run["defaultDatasetId"]
        items = list(apify_client.dataset(dataset_id).iterate_items())
        
        print(f"✅ Retrieved {len(items)} Instagram profiles")
        
        # Process each profile
        for profile in items:
            username = profile.get('username', '')
            norm_url = f"instagram.com/{username}".lower()
            
            # Extract comprehensive profile data
            latest_posts = profile.get('latestPosts', [])
            posts_count = len(latest_posts)
            
            # Calculate engagement metrics
            total_likes = 0
            total_comments = 0
            engagement_rate = 0
            avg_likes = 0
            avg_comments = 0
            
            if latest_posts:
                for post in latest_posts:
                    total_likes += post.get('likesCount', 0)
                    total_comments += post.get('commentsCount', 0)
                
                if posts_count > 0:
                    avg_likes = total_likes // posts_count
                    avg_comments = total_comments // posts_count
                    
                    # Calculate engagement rate
                    followers = profile.get('followersCount', 0)
                    if followers > 0:
                        total_engagement = total_likes + total_comments
                        engagement_rate = round(
                            (total_engagement / (posts_count * followers)) * 100,
                            2
                        )
            
            # Determine status
            if profile.get('private', False):
                status = "private"
                error = "Profile is private"
            elif posts_count == 0:
                status = "no_posts"
                error = "No recent posts found"
            else:
                status = "success"
                error = None
            
            # Build enriched profile data
            enriched_data = {
                # Basic Info
                "username": username,
                "fullName": profile.get('fullName', ''),
                "biography": profile.get('biography', ''),
                "externalUrl": profile.get('externalUrl', ''),
                "externalUrlShimmed": profile.get('externalUrlShimmed', ''),
                
                # Counts
                "followersCount": profile.get('followersCount', 0),
                "followsCount": profile.get('followsCount', 0),
                "postsCount": profile.get('postsCount', 0),
                
                # Account Type
                "verified": profile.get('verified', False),
                "private": profile.get('private', False),
                "businessCategoryName": profile.get('businessCategoryName', ''),
                "categoryName": profile.get('categoryName', ''),
                
                # Profile Picture
                "profilePicUrl": profile.get('profilePicUrl', ''),
                "profilePicUrlHD": profile.get('profilePicUrlHD', ''),
                
                # Posts Data
                "latestPosts": latest_posts[:12],  # Limit to 12 posts
                
                # Engagement Metrics
                "engagementRate": engagement_rate,
                "avgLikes": avg_likes,
                "avgComments": avg_comments,
                
                # Status
                "status": status,
                "error": error
            }
            
            all_results[norm_url] = enriched_data
            
            print(f"✅ @{username}: {posts_count} posts, {engagement_rate}% engagement")
        
        # Add error entries for profiles that weren't returned
        for norm_url, username in url_to_username.items():
            if norm_url not in all_results:
                print(f"⚠️ No data returned for @{username}")
                all_results[norm_url] = {
                    "username": username,
                    "status": "error",
                    "error": "Profile not found or failed to scrape",
                    "latestPosts": []
                }
        
        success_count = len([r for r in all_results.values() if r.get('status') == 'success'])
        print(f"\n📊 Enrichment Summary: {success_count}/{len(all_results)} successful")
        
        return all_results
        
    except Exception as e:
        print(f"❌ Instagram enrichment fatal error: {e}")
        import traceback
        traceback.print_exc()
        return {}


def scrape_linkedin(search_terms: List[str], profession: str = None, location: str = None) -> List[Dict]:
    """
    Scrape LinkedIn profiles via Apify (harvestapi/linkedin-profile-search)
    
    ✅ FIX: Smart query building for LinkedIn people search
    """
    try:
        actor_id = "harvestapi/linkedin-profile-search"
        
        # 🔧 BUILD SMART SEARCH QUERY
        # LinkedIn searches for PEOPLE with job titles, not companies
        
        # Filter out company names and non-relevant terms
        company_keywords = ['solutions', 'pvt', 'ltd', 'inc', 'llc', 'corp', 'limited']
        
        query_parts = []
        
        # 1️⃣ Prioritize profession (this is what LinkedIn searches for)
        if profession:
            # Clean profession
            prof_clean = profession.strip()
            if prof_clean.lower() not in ['full', 'string']:  # Avoid garbage values
                query_parts.append(prof_clean)
                print(f"✅ Using profession: {prof_clean}")
        
        # 2️⃣ Add relevant search terms (skip company names)
        if search_terms and isinstance(search_terms, list):
            for term in search_terms:
                term_str = str(term).strip().lower()
                
                # Skip if it's a company name or too short
                if len(term_str) < 3:
                    continue
                    
                # Skip if contains company keywords
                if any(keyword in term_str for keyword in company_keywords):
                    print(f"⏭️  Skipping company name: {term_str}")
                    continue
                
                # Add if it looks like a job title/skill
                if term_str not in query_parts:
                    query_parts.append(term_str)
                    print(f"✅ Added search term: {term_str}")
        
        # 3️⃣ Build final query
        if query_parts:
            # Use only the first 2-3 most relevant terms
            search_query = " ".join(query_parts[:3])
        else:
            # Fallback: use generic role-based searches
            if location and location.lower() == "india":
                search_query = "Software Engineer"  # Popular role in India
            else:
                search_query = "Developer"
            print(f"⚠️  No valid search terms, using fallback: {search_query}")
        
        search_query = search_query.strip()
        
        print(f"\n💼 FINAL LinkedIn search query: '{search_query}'")
        print(f"📍 Location filter: {location if location else 'None'}\n")

        # 🚀 BUILD PAYLOAD
        payload = {
            "searchQuery": search_query,
            "profileScraperMode": "Short",  # Valid options: "Short", "Full", "Full + email search"
            "startPage": 1,
            "maxItems": 10,  # Free tier limit
            "proxyConfigurationOptions": {"useApifyProxy": True}
        }
        
        # Add location filter if provided
        if location:
            payload["locations"] = [location]

        print(f"🚀 Running LinkedIn actor...")
        print(f"📦 Payload: {payload}\n")

        run = apify_client.actor(actor_id).call(run_input=payload)

        if not run or "defaultDatasetId" not in run:
            print("⚠️ LinkedIn actor returned no dataset")
            return []

        dataset_id = run["defaultDatasetId"]
        items = list(apify_client.dataset(dataset_id).iterate_items())

        print(f"\n✅ Retrieved {len(items)} LinkedIn profiles")
        
        if items:
            print(f"🔍 Sample profile keys: {list(items[0].keys())}")
            # Show first profile for debugging
            first_profile = items[0]
            print(f"\n👤 Sample profile:")
            print(f"   Name: {first_profile.get('firstName', '')} {first_profile.get('lastName', '')}")
            print(f"   Headline: {first_profile.get('headline', 'N/A')}")
            print(f"   Location: {first_profile.get('location', 'N/A')}")
        else:
            print(f"\n⚠️  No profiles found for query: '{search_query}'")
            print(f"💡 TIP: Try broader search terms like 'Developer', 'Engineer', 'Designer'")

        return items

    except Exception as e:
        print(f"❌ LinkedIn scraping error: {e}")
        import traceback
        traceback.print_exc()
        return []
    
def enrich_linkedin_profiles(profile_urls: List[str]) -> List[Dict]:
    """
    Enrich LinkedIn profiles by fetching full profile + posts data
    using apimaestro/linkedin-profile-posts actor.
    
    Args:
        profile_urls: List of LinkedIn profile URLs to enrich
        
    Returns:
        List of enriched profile data with posts, engagement, etc.
    """
    try:
        if not profile_urls:
            print("⚠️ No profile URLs provided")
            return []
        
        actor_id = "apimaestro/linkedin-profile-posts"
        
        print(f"\n🔍 Enriching {len(profile_urls)} LinkedIn profiles...")
        print(f"📋 URLs: {profile_urls[:3]}{'...' if len(profile_urls) > 3 else ''}")
        
        # Build payload for apimaestro actor
        payload = {
            "profileUrls": profile_urls,  # List of profile URLs
            "maxPosts": 10,  # Get recent posts for context
            "proxyConfigurationOptions": {"useApifyProxy": True}
        }
        
        print(f"🚀 Running LinkedIn profile enrichment actor...")
        
        run = apify_client.actor(actor_id).call(run_input=payload)
        
        if not run or "defaultDatasetId" not in run:
            print("⚠️ LinkedIn enrichment actor returned no dataset")
            return []
        
        dataset_id = run["defaultDatasetId"]
        enriched_profiles = list(apify_client.dataset(dataset_id).iterate_items())
        
        print(f"✅ Retrieved {len(enriched_profiles)} enriched LinkedIn profiles")
        
        if enriched_profiles:
            print(f"\n🔍 Enriched profile data includes:")
            sample = enriched_profiles[0]
            print(f"   - Basic info: {bool(sample.get('firstName'))}")
            print(f"   - Posts: {len(sample.get('posts', []))}")
            print(f"   - Engagement: {bool(sample.get('engagement'))}")
            print(f"   - Experience: {bool(sample.get('experience'))}")
            print(f"   - Skills: {bool(sample.get('skills'))}")
        
        return enriched_profiles
        
    except Exception as e:
        print(f"❌ LinkedIn profile enrichment error: {e}")
        import traceback
        traceback.print_exc()
        return []


def scrape_facebook(search_terms: List[str], profession: str = None, location: str = None) -> List[Dict]:
    """
    Scrape Facebook pages via Apify (apify/facebook-search-scraper)
    
    Returns business pages with structure:
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
        "pageId": "100068579062225"
    }
    
    Args:
        search_terms: List of search keywords
        profession: Professional category filter
        location: Geographic location filter
        
    Returns:
        List of Facebook page data
    """
    try:
        actor_id = "apify/facebook-search-scraper"
        
        # Build search categories
        categories = []
        
        # Add profession if provided
        if profession and profession.strip():
            prof_clean = profession.strip()
            if prof_clean.lower() not in ['full', 'string']:
                categories.append(prof_clean)
                print(f"✅ Using profession: {prof_clean}")
        
        # Add search terms
        if search_terms and isinstance(search_terms, list):
            for term in search_terms:
                term_str = str(term).strip()
                if len(term_str) >= 3 and term_str not in categories:
                    categories.append(term_str)
                    print(f"✅ Added search term: {term_str}")
        
        # Fallback if no categories
        if not categories:
            categories = ["Business", "Technology"]
            print(f"⚠️ No valid search terms, using fallback: {categories}")
        
        print(f"\n📘 FINAL Facebook search categories: {categories}")
        print(f"📍 Location filter: {location if location else 'None'}\n")
        
        # Build payload
        payload = {
            "categories": categories,
            "resultsLimit": 25,
            "maxRequestRetries": 5,
            "proxy": {"apifyProxyGroups": ["RESIDENTIAL"]}
        }
        
        if location:
            payload["locations"] = [location]
        
        print(f"🚀 Running Facebook search scraper...")
        print(f"📦 Payload: {payload}\n")
        
        run = apify_client.actor(actor_id).call(run_input=payload)
        
        if not run or "defaultDatasetId" not in run:
            print("⚠️ Facebook actor returned no dataset")
            return []
        
        dataset_id = run["defaultDatasetId"]
        items = list(apify_client.dataset(dataset_id).iterate_items())
        
        print(f"\n✅ Retrieved {len(items)} Facebook pages")
        
        if items:
            print(f"🔍 Sample result keys: {list(items[0].keys())}")
            first_item = items[0]
            print(f"\n📘 Sample page:")
            print(f"   Title: {first_item.get('title', 'N/A')}")
            print(f"   Page Name: {first_item.get('pageName', 'N/A')}")
            print(f"   Categories: {first_item.get('categories', [])}")
            print(f"   Email: {first_item.get('email', 'N/A')}")
        else:
            print(f"\n⚠️ No results found for categories: {categories}")
            print(f"💡 TIP: Try broader terms like 'Business', 'Technology', 'Marketing'")
        
        return items
        
    except Exception as e:
        print(f"❌ Facebook scraping error: {e}")
        import traceback
        traceback.print_exc()
        return []
    
def enrich_facebook_profiles(profile_urls: List[str]) -> Dict[str, Dict]:
    """
    LOW-LEVEL: Call Apify facebook-posts-scraper actor.
    Returns posts data with status tracking.
    
    FIXED: Filters out invalid posts and handles blocking better
    
    Returns:
        {
            "url": {
                "posts": [...],
                "status": "success|blocked|no_posts|error",
                "error": "error message if any"
            }
        }
    """
    try:
        if not profile_urls:
            return {}
        
        actor_id = "apify/facebook-posts-scraper"
        all_results = {}
        
        for idx, profile_url in enumerate(profile_urls):
            # Add delay between profiles to avoid rate limits
            if idx > 0:
                time.sleep(3)  # Increased delay
            
            norm_url = profile_url.lower().rstrip("/")
            
            payload = {
                "startUrls": [{"url": profile_url}],
                "resultsLimit": 3,  # ⚠️ REDUCED from 5 - less aggressive
                "maxRequestRetries": 2,
                "proxy": {
                    "useApifyProxy": True,
                    "apifyProxyGroups": ["RESIDENTIAL"]  # Use residential IPs
                }
            }
            
            print(f"🔄 Scraping {profile_url}...")
            
            try:
                run = apify_client.actor(actor_id).call(run_input=payload)
                
                if not run or "defaultDatasetId" not in run:
                    print(f"❌ No dataset returned for {profile_url}")
                    all_results[norm_url] = {
                        "posts": [],
                        "status": "error",
                        "error": "No dataset returned from Apify"
                    }
                    continue
                
                # Get all items
                items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
                
                # ✅ CRITICAL FIX: Filter out invalid/garbage posts
                valid_posts = []
                for item in items:
                    post_url = item.get("url", "")
                    post_text = item.get("text", "")
                    
                    # Skip if:
                    # 1. URL is a GraphQL API endpoint (garbage data)
                    # 2. No URL at all
                    # 3. No text content
                    if (post_url and 
                        not "/api/graphql" in post_url and
                        not post_url.endswith("/graphql/") and
                        (post_text or item.get("media"))):  # Has text or media
                        valid_posts.append(item)
                
                # Get run details to check for blocking
                run_status = run.get("status", "")
                exit_message = run.get("exitMessage", "")
                
                # Determine enrichment status
                if "BLOCKED" in str(exit_message).upper() or run_status == "FAILED":
                    status = "blocked"
                    error = "Facebook blocked the scraping request (anti-bot protection)"
                    print(f"🚫 BLOCKED: {profile_url}")
                elif not valid_posts and items:
                    # Had items but all were invalid
                    status = "blocked"
                    error = "All posts filtered as invalid (likely blocked by Facebook)"
                    print(f"🚫 BLOCKED (filtered): {profile_url}")
                elif not valid_posts:
                    status = "no_posts"
                    error = "No public posts found or posts are restricted to logged-in users"
                    print(f"⚠️  NO POSTS: {profile_url}")
                else:
                    status = "success"
                    error = None
                    print(f"✅ SUCCESS: Found {len(valid_posts)} valid posts from {profile_url}")
                
                all_results[norm_url] = {
                    "posts": valid_posts,
                    "status": status,
                    "error": error
                }
                
            except Exception as e:
                error_msg = str(e)
                
                # Check if it's a blocking error
                if "BLOCKED" in error_msg.upper() or "blocked" in error_msg.lower():
                    status = "blocked"
                    error = "Facebook blocked the scraping request"
                else:
                    status = "error"
                    error = error_msg
                
                print(f"❌ ERROR: {profile_url} - {error}")
                
                all_results[norm_url] = {
                    "posts": [],
                    "status": status,
                    "error": error
                }
        
        return all_results
        
    except Exception as e:
        print(f"❌ Apify enrichment fatal error: {e}")
        import traceback
        traceback.print_exc()
        return {}
