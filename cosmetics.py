# main.py
from fastapi import FastAPI
from routers.routes import router
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI(
    title="Multi-Platform Outreach Engine",
    description="Hybrid stateless API with agentic orchestration",
    version="2.0"
)

# Include routers
app.include_router(router)

# Background scheduler
scheduler = BackgroundScheduler()
scheduler.start()

@app.on_event("shutdown")
def shutdown_scheduler():
    scheduler.shutdown()

@app.get("/")
def root():
    return {
        "message": "🚀 Multi-Platform Personalized Outreach Engine - Hybrid Model",
        "supported_platforms": ["Instagram", "LinkedIn", "Facebook"],
        "architecture": "Stateless API + Agentic Controller + Internal State Management",
        "endpoints": {
            "POST /pipeline/register": "Register client (returns UUID)",
            "POST /pipeline/generate/{client_id}": "Scrape → Generate → Send (auto)",
            "GET /pipeline/client/{client_id}": "Get client status",
            "GET /pipeline/audience/{client_id}": "View audience data",
        },
        "status": "✅ Ready"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)