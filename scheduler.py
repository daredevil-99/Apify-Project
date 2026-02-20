# scheduler.py

from apscheduler.schedulers.background import BackgroundScheduler
from jobs.no_reply_48h import mark_no_reply_all_platforms_48h  # ← Changed import
import logging

logger = logging.getLogger(__name__)


def start_scheduler():
    """Start background job scheduler"""
    scheduler = BackgroundScheduler()
    
    # Run every 6 hours - checks both Instagram AND LinkedIn
    scheduler.add_job(
        mark_no_reply_all_platforms_48h,  # ← Changed function name
        'interval',
        hours=6,
        id='no_reply_48h_all_platforms',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info("✅ Scheduler started - Running no_reply_48h job every 6 hours for Instagram + LinkedIn")
    
    return scheduler