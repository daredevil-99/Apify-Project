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


def calculate_location_relevance(profile: Dict, preferred_location: str) -> int:
    """Calculate location relevance score (0-10)"""
    if not preferred_location:
        return 5

    location_lower = preferred_location.lower()
    score = 0

    # Check caption (most likely to have location info)
    caption = profile.get('caption', '').lower()
    if location_lower in caption:
        score += 5
    
    # Check hashtags (very important for Instagram)
    hashtags = profile.get('hashtags', [])
    for tag in hashtags:
        if location_lower in tag.lower():
            score += 3
            break
    
    # Check alt text
    alt = profile.get('alt', '').lower()
    if location_lower in alt:
        score += 2
    
    # Check location tag if available
    location_name = profile.get('locationName', '').lower()
    if location_lower in location_name:
        score += 5
    
    # Check owner username (sometimes has location)
    owner_username = profile.get('ownerUsername', '').lower()
    if location_lower in owner_username:
        score += 3

    # If score is 0 but profile uses location-based hashtag, give minimum score
    if score == 0:
        for tag in hashtags:
            if any(loc_word in tag.lower() for loc_word in ['chennai', 'india', 'tamil']):
                score += 1
                break

    return min(score, 10)


def filter_profiles_by_location(profiles: List[Dict], preferred_location: str, min_score: int = 2) -> List[Dict]:
    """Filter and sort profiles by location relevance"""
    if not preferred_location:
        return profiles

    scored_profiles = []
    for profile in profiles:
        score = calculate_location_relevance(profile, preferred_location)
        profile['location_relevance_score'] = score
        if score >= min_score:
            scored_profiles.append(profile)

    scored_profiles.sort(key=lambda x: x.get('location_relevance_score', 0), reverse=True)
    return scored_profiles

def calculate_profile_relevance(profile: Dict, profession: str, location: str, search_terms: List[str]) -> Dict:
    """
    Calculate comprehensive relevance score for an Instagram profile.
    
    Returns:
        {
            "total_score": int (0-100),
            "profession_score": int (0-40),
            "location_score": int (0-30),
            "keyword_score": int (0-30),
            "is_relevant": bool
        }
    """
    scores = {
        "profession_score": 0,
        "location_score": 0,
        "keyword_score": 0,
        "total_score": 0,
        "is_relevant": False
    }
    
    # Get searchable text from profile
    username = profile.get('username', '').lower()
    full_name = profile.get('full_name', '').lower()
    bio = profile.get('bio', '').lower()
    
    # For post-based profiles, also check sample post
    sample_post = profile.get('sample_post', {})
    caption = sample_post.get('caption', '').lower() if sample_post else ''
    hashtags = sample_post.get('hashtags', []) if sample_post else []
    hashtags_text = ' '.join([str(h).lower() for h in hashtags])
    
    # Combine all searchable text
    searchable_text = f"{username} {full_name} {bio} {caption} {hashtags_text}"
    
    # ==========================================
    # 1️⃣ PROFESSION RELEVANCE (Max: 40 points)
    # ==========================================
    if profession:
        prof_lower = profession.lower()
        prof_keywords = [prof_lower]
        
        # Add related terms for common professions
        profession_variants = {
            'baker': ['baking', 'bakery', 'bakes', 'pastry', 'cake', 'bread'],
            'designer': ['design', 'designs', 'designing', 'creative'],
            'developer': ['dev', 'coding', 'programmer', 'software', 'engineer'],
            'photographer': ['photo', 'photography', 'photos', 'camera'],
            'chef': ['cooking', 'cook', 'culinary', 'food', 'kitchen'],
            'artist': ['art', 'painting', 'drawing', 'illustration'],
            'fitness': ['gym', 'workout', 'training', 'coach', 'trainer'],
        }
        
        # Get variants for this profession
        for key, variants in profession_variants.items():
            if key in prof_lower:
                prof_keywords.extend(variants)
                break
        
        # Score based on profession mentions
        profession_mentions = 0
        for keyword in prof_keywords:
            if keyword in username:
                scores['profession_score'] += 15  # Username is strong signal
                profession_mentions += 1
            if keyword in bio:
                scores['profession_score'] += 10  # Bio is good signal
                profession_mentions += 1
            if keyword in full_name:
                scores['profession_score'] += 8
                profession_mentions += 1
            if keyword in caption:
                scores['profession_score'] += 3
                profession_mentions += 1
            if keyword in hashtags_text:
                scores['profession_score'] += 2
                profession_mentions += 1
        
        # Cap profession score at 40
        scores['profession_score'] = min(scores['profession_score'], 40)
    
    # ==========================================
    # 2️⃣ LOCATION RELEVANCE (Max: 30 points)
    # ==========================================
    if location:
        loc_lower = location.lower()
        location_keywords = [loc_lower]
        
        # Add related location terms
        location_variants = {
            'chennai': ['chennai', 'madras', 'tamilnadu', 'tamil nadu', 'tn'],
            'mumbai': ['mumbai', 'bombay', 'maharashtra'],
            'delhi': ['delhi', 'newdelhi', 'new delhi', 'ncr'],
            'bangalore': ['bangalore', 'bengaluru', 'blr', 'karnataka'],
        }
        
        for key, variants in location_variants.items():
            if key in loc_lower:
                location_keywords.extend(variants)
                break
        
        # Score based on location mentions
        for keyword in location_keywords:
            if keyword in username:
                scores['location_score'] += 12
            if keyword in bio:
                scores['location_score'] += 8
            if keyword in caption:
                scores['location_score'] += 4
            if keyword in hashtags_text:
                scores['location_score'] += 3
        
        # Cap location score at 30
        scores['location_score'] = min(scores['location_score'], 30)
    
    # ==========================================
    # 3️⃣ SEARCH TERM RELEVANCE (Max: 30 points)
    # ==========================================
    if search_terms:
        for term in search_terms:
            term_lower = str(term).lower()
            if len(term_lower) < 3:
                continue
            
            if term_lower in username:
                scores['keyword_score'] += 10
            if term_lower in bio:
                scores['keyword_score'] += 6
            if term_lower in full_name:
                scores['keyword_score'] += 5
            if term_lower in caption:
                scores['keyword_score'] += 2
            if term_lower in hashtags_text:
                scores['keyword_score'] += 1
        
        # Cap keyword score at 30
        scores['keyword_score'] = min(scores['keyword_score'], 30)
    
    # ==========================================
    # 4️⃣ SPAM/IRRELEVANT DETECTION (Penalties)
    # ==========================================
    spam_indicators = [
        'vc_', '_vc', 'girl', 'girls', 'aunty', 'dating', 'call', 'whatsapp',
        'tamil_', '_tamil', 'hot', 'sexy', 'adult', 'xxx', 'porn'
    ]
    
    spam_penalty = 0
    for indicator in spam_indicators:
        if indicator in username:
            spam_penalty += 20  # Heavy penalty for spam in username
        if indicator in bio:
            spam_penalty += 10
    
    # ==========================================
    # FINAL SCORE CALCULATION
    # ==========================================
    scores['total_score'] = (
        scores['profession_score'] + 
        scores['location_score'] + 
        scores['keyword_score'] - 
        spam_penalty
    )
    
    # Ensure score doesn't go negative
    scores['total_score'] = max(scores['total_score'], 0)
    
    # Determine if profile is relevant
    # Minimum thresholds:
    # - Total score >= 25 (out of 100)
    # - At least some profession match (>= 10) OR some location match (>= 10)
    scores['is_relevant'] = (
        scores['total_score'] >= 25 and
        (scores['profession_score'] >= 10 or scores['location_score'] >= 10)
    )
    
    return scores


def scrape_instagram(search_terms: List[str], profession: str = None, location: str = None) -> List[Dict]:
    """
    Scrape Instagram profiles via Apify with SMART RELEVANCE FILTERING.
    
    Now properly filters out irrelevant profiles based on:
    - Profession relevance
    - Location relevance  
    - Search term matches
    - Spam detection
    """
    try:
        # Build hashtags with location
        hashtags = [clean_hashtag(t) for t in search_terms if clean_hashtag(t)]

        if profession:
            hashtags.append(clean_hashtag(profession))

        if location:
            location_clean = clean_hashtag(location)
            hashtags.extend([
                location_clean,
                f"{location_clean}{clean_hashtag(profession)}" if profession else None,
            ])

        hashtags = list({h for h in hashtags if h})[:10]

        print(f"\n📸 Instagram Search Parameters:")
        print(f"   Hashtags: {hashtags}")
        print(f"   Profession: {profession}")
        print(f"   Location: {location}")

        actor_id = "apify/instagram-hashtag-scraper"
        payload = {
            "hashtags": hashtags,
            "resultsLimit": 100,  # Get more to filter from
            "addParentData": False
        }

        run = apify_client.actor(actor_id).call(run_input=payload)
        
        if not run or "defaultDatasetId" not in run:
            print("⚠️ Instagram actor returned no dataset")
            return []

        items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
        print(f"\n✅ Retrieved {len(items)} Instagram posts")
        
        # Extract unique profiles from posts
        profiles_map = {}
        
        for post in items:
            username = post.get('ownerUsername')
            
            if not username:
                continue
            
            # Skip if we already have this profile
            if username in profiles_map:
                continue
            
            # Build profile object from post data
            profile = {
                "profile_url": f"https://instagram.com/{username}",
                "username": username,
                "full_name": post.get('ownerFullName', ''),
                "profile_pic_url": post.get('ownerProfilePicUrl', ''),
                
                # Will be filled by enrichment
                "followers": 0,
                "following": 0,
                "posts": 0,
                "bio": "",
                "verified": False,
                "private": False,
                
                # Keep sample post for relevance scoring
                "sample_post": {
                    "caption": post.get('caption', ''),
                    "likes": post.get('likesCount', 0),
                    "comments": post.get('commentsCount', 0),
                    "hashtags": post.get('hashtags', []),
                    "post_url": post.get('url', '')
                }
            }
            
            profiles_map[username] = profile
        
        profiles = list(profiles_map.values())
        print(f"✅ Extracted {len(profiles)} unique profiles from posts")
        
        # ⭐ APPLY SMART RELEVANCE FILTERING
        print(f"\n🎯 Applying relevance filtering...")
        print(f"   Profession filter: {profession}")
        print(f"   Location filter: {location}")
        print(f"   Search terms: {search_terms}")
        
        scored_profiles = []
        for profile in profiles:
            scores = calculate_profile_relevance(
                profile=profile,
                profession=profession,
                location=location,
                search_terms=search_terms
            )
            
            profile['relevance_scores'] = scores
            profile['total_relevance_score'] = scores['total_score']
            profile['is_relevant'] = scores['is_relevant']
            
            # Only keep relevant profiles
            if scores['is_relevant']:
                scored_profiles.append(profile)
        
        print(f"\n📊 Filtering Results:")
        print(f"   Total profiles found: {len(profiles)}")
        print(f"   Relevant profiles: {len(scored_profiles)}")
        print(f"   Filtered out: {len(profiles) - len(scored_profiles)}")
        
        # Sort by relevance score (highest first)
        scored_profiles.sort(key=lambda x: x.get('total_relevance_score', 0), reverse=True)
        
        # Show top profiles with scores
        if scored_profiles:
            print(f"\n🏆 Top 5 Most Relevant Profiles:")
            for i, profile in enumerate(scored_profiles[:5], 1):
                scores = profile['relevance_scores']
                print(f"\n   {i}. @{profile['username']}")
                print(f"      Total Score: {scores['total_score']}/100")
                print(f"      - Profession: {scores['profession_score']}/40")
                print(f"      - Location: {scores['location_score']}/30")
                print(f"      - Keywords: {scores['keyword_score']}/30")
        else:
            print(f"\n⚠️  No relevant profiles found!")
            print(f"💡 Try broader search terms or different hashtags")
        
        # Return top 20 relevant profiles
        result_profiles = scored_profiles[:20]
        
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
            "maxItems": 25,  # Free tier limit
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
