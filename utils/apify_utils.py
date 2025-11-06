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


def calculate_location_relevance(profile: Dict, preferred_location: str) -> int:
    """Calculate location relevance score (0-10)"""
    if not preferred_location:
        return 5

    location_lower = preferred_location.lower()
    score = 0

    # Check bio
    bio = profile.get('bio', '').lower()
    if location_lower in bio:
        score += 5

    # Check username
    username = profile.get('username', '').lower()
    if location_lower in username:
        score += 3

    # Check hashtags
    hashtags = profile.get('hashtags', [])
    for tag in hashtags:
        if location_lower in tag.lower():
            score += 2
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

        # Apply location filtering
        if location:
            items = filter_profiles_by_location(items, location, min_score=2)
            print(f"🎯 Filtered to {len(items)} location-relevant profiles")

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