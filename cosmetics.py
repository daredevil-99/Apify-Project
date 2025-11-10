from fastapi import FastAPI
from routers.routes import router
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Initialize app
app = FastAPI(
    title="Multi-Platform Outreach Engine",
    description="Automated outreach workflow for Instagram, LinkedIn, and Facebook",
    version="2.0"
)

# Include all pipeline routes
app.include_router(router)

# Background scheduler (if used for async jobs or cron tasks)
scheduler = BackgroundScheduler()
scheduler.start()


@app.on_event("shutdown")
def shutdown_scheduler():
    scheduler.shutdown()


@app.get("/")
def root():
    """
    Root endpoint showing system overview and available APIs
    """
    return {
        "message": "🚀 Multi-Platform Personalized Outreach Engine - Active",
        "supported_platforms": ["Instagram", "LinkedIn", "Facebook"],
        "architecture": "FastAPI + BackgroundTasks + Apify + MongoDB",
        "workflow": {
            "1️⃣ POST /pipeline/register": "Register a new client",
            "2️⃣ POST /pipeline/scrape/{client_id}": "Scrape platform data for that client",
            "3️⃣ POST /pipeline/extract-prospects/{client_id}": "Extract prospects from scraped data",
            "4️⃣ POST /pipeline/generate-messages/{client_id}": "Generate personalized outreach messages",
            "5️⃣ POST /pipeline/send-dms/{client_id}": "Send messages automatically (DM/Email)",
            "6️⃣ GET /pipeline/prospects/{client_id}": "View all prospects and statuses",
        },
        "status": "✅ Server is up and running"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
