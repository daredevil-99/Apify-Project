# utils/apify_utils.py
import os
import re
from typing import List, Dict
from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv()

APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
if not APIFY_API_TOKEN:
    raise ValueError("Missing APIFY_API_TOKEN in .env")

apify_client = ApifyClient(APIFY_API_TOKEN)


def clean_hashtag(tag: str) -> str:
    """Clean hashtag for Instagram"""
    cleaned = re.sub(r'[^a-zA-Z0-9_]', '', tag.replace(' ', ''))
    return cleaned.lower()


# utils/apify_utils.py

def calculate_location_relevance(profile: Dict, preferred_location: str) -> int:
    """Calculate location relevance score (0-10)"""
    if not preferred_location:
        return 5

    location_lower = preferred_location.lower()
    score = 0

    # ✅ UPDATED: Check more fields for Instagram hashtag results
    
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

    # ✅ If score is 0 but profile uses location-based hashtag, give minimum score
    if score == 0:
        for tag in hashtags:
            # Check if any hashtag contains location-related words
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


# utils/apify_utils.py

def scrape_instagram(search_terms: List[str], profession: str = None, location: str = None) -> List[Dict]:
    """Scrape Instagram profiles via Apify"""
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

        print(f"📸 Instagram hashtags: {hashtags}")

        actor_id = "apify/instagram-hashtag-scraper"
        payload = {
            "hashtags": hashtags,
            "resultsLimit": 50,
            "addParentData": False
        }

        run = apify_client.actor(actor_id).call(run_input=payload)
        
        if not run or "defaultDatasetId" not in run:
            print("⚠️ Instagram actor returned no dataset")
            return []

        items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
        print(f"✅ Retrieved {len(items)} Instagram results")

        # ✅ DEBUG: Print first profile structure
        if items:
            print(f"🔍 Sample profile keys: {list(items[0].keys())}")
            print(f"🔍 Sample profile: {items[0]}")
        # ✅ FIX: Apply location filtering with LOWER threshold (or skip if no location)
        if location:
            items = filter_profiles_by_location(items, location, min_score=0)  # ✅ Changed from 2 to 0
            print(f"🎯 Filtered to {len(items)} location-relevant profiles")
            
            # ✅ DEBUG: Show location scores
            if items:
                top_5_scores = [(i.get('username', 'unknown'), i.get('location_relevance_score', 0)) for i in items[:5]]
                print(f"📊 Top 5 profiles with scores: {top_5_scores}")
        else:
            # If no location preference, give all profiles a neutral score
            for item in items:
                item['location_relevance_score'] = 5

        return items[:20]

    except Exception as e:
        print(f"❌ Instagram scraping error: {e}")
        return []

def scrape_linkedin(search_terms: List[str], profession: str = None, location: str = None) -> List[Dict]:
    """Scrape LinkedIn profiles via Apify"""
    try:
        actor_id = "harvestapi/linkedin-profile-search"
        payload = {
            "searchQuery": " OR ".join(search_terms[:5]),
            "profileScraperMode": "Full",
            "startPage": 1,
            "maxItems": 0,
            "locations": [location] if location else []
        }

        print(f"💼 LinkedIn search: {payload['searchQuery']}")

        run = apify_client.actor(actor_id).call(run_input=payload)
        
        if not run or "defaultDatasetId" not in run:
            print("⚠️ LinkedIn actor returned no dataset")
            return []

        items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
        print(f"✅ Retrieved {len(items)} LinkedIn results")

        return items

    except Exception as e:
        print(f"❌ LinkedIn scraping error: {e}")
        return []


def scrape_facebook(search_terms: List[str], profession: str = None, location: str = None) -> List[Dict]:
    """Scrape Facebook profiles via Apify"""
    try:
        actor_id = "apify/facebook-search-scraper"
        payload = {
            "categories": search_terms,
            "locations": [location] if location else [],
            "resultsLimit": 20,
            "maxRequestRetries": 5,
            "proxy": {"apifyProxyGroups": ["RESIDENTIAL"]}
        }

        print(f"📘 Facebook search in {location}")

        run = apify_client.actor(actor_id).call(run_input=payload)
        
        if not run or "defaultDatasetId" not in run:
            print("⚠️ Facebook actor returned no dataset")
            return []

        items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
        print(f"✅ Retrieved {len(items)} Facebook results")

        return items

    except Exception as e:
        print(f"❌ Facebook scraping error: {e}")
        return []