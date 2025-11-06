# services/scraping_service.py
from typing import Dict, List
from utils.apify_utils import scrape_instagram, scrape_linkedin, scrape_facebook
from services.db_service import save_scraped_profiles


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