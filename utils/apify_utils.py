# utils/apify_utils.py
import re
from datetime import datetime
from db_config import apify_client, audience_collection, clients_collection


def clean_hashtag(tag: str):
    return re.sub(r'[^a-zA-Z0-9_]', '', tag.replace(' ', '')).lower()

def calculate_location_relevance(profile, preferred_location):
    if not preferred_location:
        return 5
    location_lower = preferred_location.lower()
    score = 0
    bio = profile.get('bio', '').lower()
    if location_lower in bio:
        score += 5
    username = profile.get('username', '').lower()
    if location_lower in username:
        score += 3
    for tag in profile.get('hashtags', []):
        if location_lower in tag.lower():
            score += 2
            break
    for post in profile.get('recent_posts', [])[:3]:
        if location_lower in post.get('caption', '').lower():
            score += 1
            break
    return min(score, 10)

def filter_profiles_by_location(profiles, preferred_location, min_score=3):
    if not preferred_location:
        return profiles
    scored = []
    for profile in profiles:
        score = calculate_location_relevance(profile, preferred_location)
        profile['location_relevance_score'] = score
        if score >= min_score:
            scored.append(profile)
    scored.sort(key=lambda x: x.get('location_relevance_score', 0), reverse=True)
    return scored

def run_apify(platform, search_terms, profession=None, preferred_location=None):
    """Unified Apify runner for multiple platforms."""
    from db_config import apify_client
    try:
        if platform == "facebook":
            actor_id = "apify/facebook-search-scraper"
            payload = {"categories": search_terms, "locations": [preferred_location] if preferred_location else []}
        elif platform == "instagram":
            hashtags = [clean_hashtag(t) for t in search_terms if clean_hashtag(t)]
            if profession: hashtags.append(clean_hashtag(profession))
            if preferred_location:
                location_clean = clean_hashtag(preferred_location)
                hashtags += [location_clean, f"{location_clean}{clean_hashtag(profession)}"]
            hashtags = list({t for t in hashtags if t})[:10]
            actor_id = "apify/instagram-hashtag-scraper"
            payload = {"hashtags": hashtags, "resultsLimit": 50}
        elif platform == "linkedin":
            actor_id = "harvestapi/linkedin-profile-search"
            payload = {"searchQuery": " OR ".join(search_terms[:5]), "locations": [preferred_location] if preferred_location else []}
        else:
            return []

        actor_client = apify_client.actor(actor_id)
        run = actor_client.call(run_input=payload)
        if not run or "defaultDatasetId" not in run:
            return []
        dataset_client = apify_client.dataset(run["defaultDatasetId"])
        items = list(dataset_client.iterate_items())
        if platform == "instagram" and preferred_location:
            items = filter_profiles_by_location(items, preferred_location)
        return items
    except Exception as e:
        print(f"❌ Error running {platform} Apify actor: {e}")
        return []
