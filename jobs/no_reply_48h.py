# jobs/no_reply_48h.py

import logging
from datetime import datetime, timedelta
from db_config import audience_collection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


def mark_no_reply_after_48h(platform: str = None):
    """
    Mark prospects as no_reply_48h if:
    - message was sent
    - no reply received
    - 48 hours have passed
    
    Args:
        platform: "instagram", "linkedin", or None (both platforms)
    """
    
    cutoff_time = datetime.utcnow() - timedelta(hours=48)
    
    # Build query
    query = {
        "sent_at": {"$lte": cutoff_time},
        "$or": [
            {"replied": {"$exists": False}},
            {"replied": False}
        ],
        "status": {"$nin": ["replied", "no_reply_48h"]}
    }
    
    # Add platform filter if specified
    if platform:
        query["platform"] = platform
    
    # Update matching documents
    result = audience_collection.update_many(
        query,
        {
            "$set": {
                "status": "no_reply_48h",
                "no_reply_marked_at": datetime.utcnow(),
                "last_event": "no_reply_48h",
                "last_webhook_event_at": datetime.utcnow()
            }
        }
    )
    
    platform_label = platform.upper() if platform else "ALL PLATFORMS"
    logger.info(
        f"⏱️ No-reply-48h job completed for {platform_label} | updated={result.modified_count}"
    )
    
    return result.modified_count


def mark_no_reply_all_platforms_48h():
    """Run for both Instagram and LinkedIn"""
    return mark_no_reply_after_48h(platform=None)


def mark_no_reply_instagram_48h():
    """Run for Instagram only"""
    return mark_no_reply_after_48h(platform="instagram")


def mark_no_reply_linkedin_48h():
    """Run for LinkedIn only"""
    return mark_no_reply_after_48h(platform="linkedin")