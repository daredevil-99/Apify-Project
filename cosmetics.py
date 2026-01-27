# cosmetics.py
from fastapi import FastAPI
from dotenv import load_dotenv
import os
import logging

# ------------------ INTERNAL ROUTERS ------------------
from routers.routes import router as pipeline_router       # All /pipeline endpoints
from routers.admin import router as admin_router           # ✅ NEW: Admin endpoints
from webhooks.unipile import router as unipile_router      # Unipile webhook

# ------------------ SCHEDULER ------------------
from scheduler import start_scheduler

# ------------------ WEBHOOK MANAGER ------------------
from services.webhook_manager import get_ngrok_url, register_unipile_webhooks, verify_webhooks

# ------------------ ACCOUNT MANAGEMENT ------------------
from db_config import list_all_unipile_accounts, clients_collection

# ------------------ LOGGING ------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ------------------ ENVIRONMENT ------------------
load_dotenv()

# ------------------ APP INIT ------------------
app = FastAPI(
    title="Multi-Platform Outreach Engine",
    description="Automated outreach workflow for Instagram, LinkedIn, and Facebook",
    version="2.0"
)

# ------------------ ROUTER INCLUSION ------------------
app.include_router(pipeline_router)     # ✅ All /pipeline endpoints
app.include_router(admin_router)        # ✅ NEW: Admin endpoints at /admin
app.include_router(unipile_router)      # ✅ Unipile webhook at /pipeline/webhook


# ------------------ HELPER: CHECK FOR DUPLICATE ACCOUNTS ------------------
async def check_duplicate_accounts():
    """
    Check if there are duplicate Unipile accounts that could cause webhook issues.
    Returns a report of duplicates found.
    """
    try:
        # Get all Unipile accounts
        all_accounts = await list_all_unipile_accounts()
        
        if not all_accounts:
            logger.warning("⚠️ No Unipile accounts found or error occurred")
            return {"has_duplicates": False}
        
        # Separate by type
        instagram_accounts = []
        linkedin_accounts = []
        
        for acc in all_accounts:
            if not isinstance(acc, dict):
                logger.warning(f"⚠️ Skipping non-dict account: {acc}")
                continue
            
            acc_type = acc.get('type', acc.get('provider', 'UNKNOWN'))
            
            if acc_type == 'INSTAGRAM':
                instagram_accounts.append(acc)
            elif acc_type == 'LINKEDIN':
                linkedin_accounts.append(acc)
        
        report = {
            "has_duplicates": False,
            "instagram": {
                "total": len(instagram_accounts),
                "accounts": instagram_accounts,
                "is_duplicate": len(instagram_accounts) > 1
            },
            "linkedin": {
                "total": len(linkedin_accounts),
                "accounts": linkedin_accounts,
                "is_duplicate": len(linkedin_accounts) > 1
            }
        }
        
        if len(instagram_accounts) > 1 or len(linkedin_accounts) > 1:
            report["has_duplicates"] = True
        
        return report
        
    except Exception as e:
        logger.error(f"❌ Error checking duplicates: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {"has_duplicates": False, "error": str(e)}

async def check_client_account_mapping():
    """
    Check if any clients have account_ids that don't exist in Unipile anymore
    """
    try:
        # Get all valid Unipile account IDs
        all_accounts = await list_all_unipile_accounts()
        
        if not all_accounts:
            logger.warning("⚠️ Could not retrieve Unipile accounts")
            return []
        
        # Extract account IDs safely
        valid_account_ids = set()
        for acc in all_accounts:
            if isinstance(acc, dict):
                acc_id = acc.get('id')
                if acc_id:
                    valid_account_ids.add(acc_id)
        
        logger.info(f"📊 Valid Unipile account IDs: {valid_account_ids}")
        
        # Check all clients
        orphaned_clients = []
        
        clients = clients_collection.find()
        
        for client in clients:
            # ✅ FIX: Safely get client_id
            client_id = client.get("client_id")
            
            if not client_id:
                logger.warning(f"⚠️ Found client without client_id: {client.get('_id')}")
                continue
            
            # Check Instagram
            instagram_data = client.get("instagram", {})
            if isinstance(instagram_data, dict):
                instagram_account_id = instagram_data.get("account_id")
                
                if instagram_account_id and instagram_account_id not in valid_account_ids:
                    orphaned_clients.append({
                        "client_id": client_id,
                        "platform": "instagram",
                        "orphaned_account_id": instagram_account_id
                    })
                    logger.warning(f"⚠️ Client {client_id} has orphaned Instagram account: {instagram_account_id}")
            
            # Check LinkedIn
            linkedin_data = client.get("linkedin", {})
            if isinstance(linkedin_data, dict):
                linkedin_account_id = linkedin_data.get("account_id")
                
                if linkedin_account_id and linkedin_account_id not in valid_account_ids:
                    orphaned_clients.append({
                        "client_id": client_id,
                        "platform": "linkedin",
                        "orphaned_account_id": linkedin_account_id
                    })
                    logger.warning(f"⚠️ Client {client_id} has orphaned LinkedIn account: {linkedin_account_id}")
        
        return orphaned_clients
        
    except Exception as e:
        logger.error(f"❌ Error checking client mappings: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []


# ------------------ STARTUP: SCHEDULER + WEBHOOKS + HEALTH CHECK ------------------
@app.on_event("startup")
async def startup_event():
    """
    Auto-configure webhooks and start scheduler on server startup
    """
    logger.info("\n" + "🚀"*35)
    logger.info("MULTI-PLATFORM OUTREACH ENGINE - STARTING")
    logger.info("🚀"*35 + "\n")
    
    # ✅ Step 1: Detect ngrok URL (or use .env fallback)
    current_url = get_ngrok_url()
    
    if not current_url:
        logger.warning("⚠️ WARNING: Could not detect ngrok URL!")
        logger.warning("   Using APP_BASE_URL from .env")
        current_url = os.getenv("APP_BASE_URL", "http://localhost:8000")
    
    # Update environment variable dynamically
    os.environ["APP_BASE_URL"] = current_url
    logger.info(f"🌐 Base URL set to: {current_url}")
    
    # ✅ Step 2: Register Unipile webhooks automatically
    try:
        logger.info("\n🔔 Registering Unipile webhooks...")
        result = register_unipile_webhooks(current_url)
        
        if result["registered"]:
            logger.info(f"✅ Successfully registered: {', '.join(result['registered'])}")
        
        if result["failed"]:
            logger.warning(f"⚠️ Failed to register: {', '.join(result['failed'])}")
        
        # Verify registration
        logger.info("\n📋 Verifying webhook registration...")
        verify_webhooks()
        
    except Exception as e:
        logger.error(f"❌ Webhook registration failed: {str(e)}")
        logger.warning("   Server will continue, but webhooks may not work!")
    
    # ✅ Step 3: Check for duplicate accounts (NEW!)
    logger.info("\n🔍 Checking for duplicate Unipile accounts...")
    try:
        duplicate_report = await check_duplicate_accounts()
        
        if duplicate_report.get("has_duplicates"):
            logger.warning("\n" + "⚠️"*35)
            logger.warning("⚠️ DUPLICATE ACCOUNTS DETECTED!")
            logger.warning("⚠️"*35)
            
            if duplicate_report["instagram"]["is_duplicate"]:
                logger.warning(f"\n📱 INSTAGRAM: Found {duplicate_report['instagram']['total']} accounts")
                for acc in duplicate_report["instagram"]["accounts"]:
                    logger.warning(f"   • {acc['id']} - {acc.get('username', 'N/A')} ({acc.get('status', 'unknown')})")
                
                logger.warning("\n💡 FIX: Run cleanup script or use admin API:")
                logger.warning("   python cleanup_unipile.py")
                logger.warning("   OR")
                logger.warning("   POST /admin/client/{client_id}/cleanup-duplicates?platform=instagram")
            
            if duplicate_report["linkedin"]["is_duplicate"]:
                logger.warning(f"\n💼 LINKEDIN: Found {duplicate_report['linkedin']['total']} accounts")
                for acc in duplicate_report["linkedin"]["accounts"]:
                    logger.warning(f"   • {acc['id']} - {acc.get('username', 'N/A')} ({acc.get('status', 'unknown')})")
                
                logger.warning("\n💡 FIX: Run cleanup script or use admin API:")
                logger.warning("   python cleanup_unipile.py")
                logger.warning("   OR")
                logger.warning("   POST /admin/client/{client_id}/cleanup-duplicates?platform=linkedin")
            
            logger.warning("\n⚠️ IMPACT: Duplicate accounts cause:")
            logger.warning("   • Multiple webhook events for same message")
            logger.warning("   • Tracking confusion (sent/read/reply status)")
            logger.warning("   • Database inconsistencies")
            logger.warning("⚠️"*35 + "\n")
        else:
            logger.info("✅ No duplicate accounts detected")
        
        # Check for orphaned client mappings
        logger.info("\n🔍 Checking client account mappings...")
        orphaned = await check_client_account_mapping()
        
        if orphaned:
            logger.warning("\n⚠️ ORPHANED CLIENT ACCOUNTS DETECTED!")
            logger.warning("   These clients have account_ids that don't exist in Unipile:")
            for item in orphaned:
                logger.warning(f"   • Client: {item['client_id']}")
                logger.warning(f"     Platform: {item['platform']}")
                logger.warning(f"     Orphaned Account ID: {item['orphaned_account_id']}")
            
            logger.warning("\n💡 FIX: These will be auto-marked as orphaned")
            logger.warning("   Clients need to reconnect their accounts")
        else:
            logger.info("✅ All client accounts are valid")
            
    except Exception as e:
        logger.error(f"❌ Error during account health check: {e}")
    
    # ✅ Step 4: Start APScheduler jobs
    logger.info("\n⏰ Starting APScheduler...")
    start_scheduler()
    
    logger.info("\n" + "="*70)
    logger.info("✅ APPLICATION STARTUP COMPLETE")
    logger.info("="*70 + "\n")

@app.on_event("shutdown")
def shutdown_event():
    logger.info("🛑 Shutting down application...")

# ------------------ ROOT ENDPOINT ------------------
@app.get("/")
def root():
    """
    Root endpoint showing system overview and available APIs
    """
    return {
        "message": "🚀 Multi-Platform Personalized Outreach Engine - Active",
        "supported_platforms": ["Instagram", "LinkedIn", "Facebook"],
        "architecture": "FastAPI + Webhooks + APScheduler + MongoDB",
        "base_url": os.getenv("APP_BASE_URL", "Not configured"),
        "workflow": {
            "1️⃣ POST /pipeline/register": "Register a new client",
            "2️⃣ POST /pipeline/connect-instagram/{client_id}": "Connect Instagram account",
            "3️⃣ POST /pipeline/scrape/{client_id}": "Scrape platform data",
            "4️⃣ POST /pipeline/extract-prospects/{client_id}": "Extract prospects",
            "5️⃣ POST /pipeline/generate-messages/{client_id}": "Generate personalized messages",
            "6️⃣ POST /pipeline/instagram/send-messages": "Send Instagram DMs",
            "7️⃣ GET /pipeline/instagram/prospects/{client_id}": "View prospects & tracking",
        },
        "admin_endpoints": {
            "GET /admin/client/{client_id}/accounts": "View connected accounts",
            "GET /admin/client/{client_id}/duplicates": "Check for duplicate accounts",
            "POST /admin/client/{client_id}/cleanup-duplicates": "Auto-cleanup duplicates",
            "GET /admin/client/{client_id}/tracking-health": "Check tracking health",
            "GET /admin/client/{client_id}/prospects/stats": "View prospect statistics"
        },
        "webhooks": {
            "POST /pipeline/webhook/account-status": "Account status events",
            "POST /pipeline/webhook/messaging": "Message events (sent/read/reply)",
            "POST /pipeline/webhook/users": "User relationship events"
        },
        "webhook_management": {
            "GET /webhooks/status": "Check current webhook registration",
            "POST /webhooks/re-register": "Manually re-register webhooks"
        },
        "health_checks": {
            "GET /health/accounts": "Check for duplicate/orphaned accounts",
            "GET /health/webhooks": "Verify webhook configuration"
        },
        "status": "✅ Server is up and running"
    }

# ------------------ HEALTH CHECK ENDPOINTS ------------------
@app.get("/health/accounts")
async def health_check_accounts():
    """
    Check for duplicate or orphaned Unipile accounts
    """
    try:
        duplicate_report = await check_duplicate_accounts()
        orphaned = await check_client_account_mapping()
        
        health_status = "healthy"
        issues = []
        
        if duplicate_report.get("has_duplicates"):
            health_status = "unhealthy"
            if duplicate_report["instagram"]["is_duplicate"]:
                issues.append(f"Instagram: {duplicate_report['instagram']['total']} accounts (should be 1)")
            if duplicate_report["linkedin"]["is_duplicate"]:
                issues.append(f"LinkedIn: {duplicate_report['linkedin']['total']} accounts (should be 1)")
        
        if orphaned:
            health_status = "unhealthy"
            issues.append(f"{len(orphaned)} clients have orphaned account IDs")
        
        return {
            "status": health_status,
            "timestamp": os.popen('date -u').read().strip(),
            "duplicate_accounts": duplicate_report,
            "orphaned_clients": orphaned,
            "issues": issues,
            "recommendations": [
                "Run: python cleanup_unipile.py" if duplicate_report.get("has_duplicates") else None,
                f"POST /admin/client/{{client_id}}/cleanup-duplicates" if duplicate_report.get("has_duplicates") else None,
                "Reconnect accounts for orphaned clients" if orphaned else None
            ]
        }
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")
        return {
            "status": "error",
            "message": str(e)
        }

@app.get("/health/webhooks")
async def health_check_webhooks():
    """
    Verify webhook configuration
    """
    try:
        webhooks = verify_webhooks()
        
        expected_webhooks = ["account_status", "messaging", "users"]
        registered_sources = [wh.get("source") for wh in webhooks]
        
        missing = [name for name in expected_webhooks if name not in registered_sources]
        
        health_status = "healthy" if not missing else "unhealthy"
        
        return {
            "status": health_status,
            "base_url": os.getenv("APP_BASE_URL"),
            "webhooks_registered": len(webhooks),
            "webhooks_expected": len(expected_webhooks),
            "missing_webhooks": missing,
            "webhooks": [
                {
                    "name": wh.get("source"),
                    "url": wh.get("request_url"),
                    "events": wh.get("events", [])
                }
                for wh in webhooks
            ],
            "recommendations": [
                "POST /webhooks/re-register" if missing else "All webhooks are registered correctly"
            ]
        }
    except Exception as e:
        logger.error(f"❌ Webhook health check failed: {e}")
        return {
            "status": "error",
            "message": str(e)
        }

# ------------------ WEBHOOK STATUS ENDPOINT ------------------
@app.get("/webhooks/status")
async def check_webhook_status():
    """
    Check currently registered webhooks with Unipile.
    """
    try:
        webhooks = verify_webhooks()
        
        return {
            "status": "success",
            "app_base_url": os.getenv("APP_BASE_URL"),
            "webhooks_count": len(webhooks),
            "webhooks": [
                {
                    "id": wh.get("id"),
                    "source": wh.get("source"),
                    "url": wh.get("request_url"),
                    "enabled": wh.get("enabled", True),
                    "events": wh.get("events", []),
                    "created_at": wh.get("created_at")
                }
                for wh in webhooks
            ]
        }
    except Exception as e:
        logger.error(f"❌ Failed to check webhook status: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

# ------------------ WEBHOOK RE-REGISTRATION ENDPOINT ------------------
@app.post("/webhooks/re-register")
async def reregister_webhooks():
    """
    Manually re-register webhooks with Unipile.
    """
    try:
        # Detect current URL
        current_url = get_ngrok_url()
        
        if not current_url:
            current_url = os.getenv("APP_BASE_URL")
            if not current_url:
                return {
                    "status": "error",
                    "message": "Could not detect ngrok URL and APP_BASE_URL is not set"
                }
        
        # Update environment
        os.environ["APP_BASE_URL"] = current_url
        
        # Re-register webhooks
        logger.info(f"🔄 Re-registering webhooks with URL: {current_url}")
        result = register_unipile_webhooks(current_url)
        
        # Verify
        webhooks = verify_webhooks()
        
        return {
            "status": "success",
            "base_url": current_url,
            "registered": result["registered"],
            "failed": result["failed"],
            "total_webhooks": len(webhooks)
        }
        
    except Exception as e:
        logger.error(f"❌ Webhook re-registration failed: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

# ------------------ UVICORN RUN ------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,
        log_level="info"
    )