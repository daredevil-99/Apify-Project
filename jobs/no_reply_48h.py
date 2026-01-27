# jobs/no_reply_48h.py

import logging
from datetime import datetime, timedelta
from db_config import audience_collection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


def mark_no_reply_after_48h():
    """
    Mark prospects as no_reply_48h if:
    - message was sent
    - no reply received
    - 48 hours have passed
    """

    cutoff_time = datetime.utcnow() - timedelta(hours=48)

    result = audience_collection.update_many(
        {
            "sent_at": {"$lte": cutoff_time},
            "$or": [
                {"replied": {"$exists": False}},
                {"replied": False}
            ],
            "status": {"$ne": "replied"}
        },
        {
            "$set": {
                "status": "no_reply_48h",
                "no_reply_marked_at": datetime.utcnow(),
                "last_event": "no_reply_48h",
                "last_webhook_event_at": datetime.utcnow()
            }
        }
    )

    logger.info(
        f"⏱️ No-reply-48h job completed | updated={result.modified_count}"
    )
