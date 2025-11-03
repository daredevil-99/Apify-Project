from fastapi import FastAPI
from routers import routes
from apscheduler.schedulers.background import BackgroundScheduler

app = FastAPI(title="Cosmetics Outreach Automation")

# Include all routes
app.include_router(routes.router)

# Optional: Start background scheduler if used for periodic tasks
scheduler = BackgroundScheduler()
scheduler.start()

@app.on_event("startup")
def startup_event():
    print("🚀 FastAPI app started successfully")

@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()
    print("🛑 FastAPI app shut down gracefully")

# For local run
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("cosmetics:app", host="127.0.0.1", port=8009, reload=True)
