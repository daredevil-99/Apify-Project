# scheduler.py

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from jobs.no_reply_48h import mark_no_reply_after_48h

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

scheduler = BackgroundScheduler()


def start_scheduler():
    """
    Starts APScheduler and registers background jobs
    """

    if scheduler.running:
        logger.warning("⚠️ Scheduler already running")
        return

    scheduler.add_job(
        mark_no_reply_after_48h,
        trigger=IntervalTrigger(hours=1),
        id="no_reply_48h_job",
        name="Mark no-reply after 48h",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=3600
    )

    scheduler.start()
    logger.info("✅ APScheduler started | no_reply_48h job registered")
