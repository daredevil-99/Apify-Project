# routers/admin.py
"""
Admin endpoints for account management and debugging
Users only need to know their client_id
"""

from fastapi import APIRouter, HTTPException
from db_config import (
    list_all_unipile_accounts,
    delete_unipile_account,
    audit_client_accounts,
    clients_collection,
    audience_collection
)
import logging

router = APIRouter(prefix="/admin", tags=["Admin"])
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# USER-FRIENDLY ENDPOINTS - ONLY NEED CLIENT_ID
# ═══════════════════════════════════════════════════════════════════════

@router.get("/client/{client_id}/accounts")
async def get_client_accounts(client_id: str):
    """
    Get all connected accounts for a client
    GET /admin/client/{client_id}/accounts
    
    Returns:
    {
        "client_id": "fef5abad-68fa-4abc-a0b4-f8face2d8814",
        "instagram": {
            "connected": true,
            "account_id": "ZBQS7d6bQNuvBJyShrynXw",
            "username": "@madd.ykai99",
            "status": "active",
            "is_active": true
        },
        "linkedin": {
            "connected": false
        }
    }
    """
    try:
        client = clients_collection.find_one({"client_id": client_id})
        
        if not client:
            raise HTTPException(status_code=404, detail="Client not found")
        
        result = {
            "client_id": client_id,
            "instagram": {"connected": False},
            "linkedin": {"connected": False}
        }
        
        # Instagram
        if "instagram" in client and client["instagram"].get("account_id"):
            insta = client["instagram"]
            result["instagram"] = {
                "connected": True,
                "account_id": insta.get("account_id"),
                "username": insta.get("username"),
                "status": insta.get("status", "unknown"),
                "is_active": insta.get("is_active", False),
                "last_status_check": insta.get("last_status_check")
            }
        
        # LinkedIn
        if "linkedin" in client and client["linkedin"].get("account_id"):
            linkedin = client["linkedin"]
            result["linkedin"] = {
                "connected": True,
                "account_id": linkedin.get("account_id"),
                "username": linkedin.get("username"),
                "status": linkedin.get("status", "unknown"),
                "is_active": linkedin.get("is_active", False),
                "last_status_check": linkedin.get("last_status_check")
            }
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting client accounts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/client/{client_id}/duplicates")
async def check_client_duplicates(client_id: str):
    """
    Check if this client has duplicate accounts in Unipile
    GET /admin/client/{client_id}/duplicates
    
    Returns:
    {
        "client_id": "...",
        "has_duplicates": true,
        "instagram": {
            "registered_account": "ZBQS7d6bQNuvBJyShrynXw",
            "duplicate_accounts": ["eefdgYTTQjOP7ny8omZGBA", "bXS-CfPgTZahb4oD41ZqeA"],
            "total_duplicates": 2
        }
    }
    """
    try:
        # Get client's registered accounts
        client = clients_collection.find_one({"client_id": client_id})
        
        if not client:
            raise HTTPException(status_code=404, detail="Client not found")
        
        registered_instagram = client.get("instagram", {}).get("account_id")
        registered_linkedin = client.get("linkedin", {}).get("account_id")
        
        # Get all Unipile accounts
        all_accounts = await list_all_unipile_accounts()
        
        instagram_accounts = [acc["id"] for acc in all_accounts if acc.get("type") == "INSTAGRAM"]
        linkedin_accounts = [acc["id"] for acc in all_accounts if acc.get("type") == "LINKEDIN"]
        
        result = {
            "client_id": client_id,
            "has_duplicates": False,
            "instagram": {"registered_account": registered_instagram, "duplicate_accounts": [], "total_duplicates": 0},
            "linkedin": {"registered_account": registered_linkedin, "duplicate_accounts": [], "total_duplicates": 0}
        }
        
        # Check Instagram duplicates
        if registered_instagram and len(instagram_accounts) > 1:
            duplicates = [acc_id for acc_id in instagram_accounts if acc_id != registered_instagram]
            result["instagram"]["duplicate_accounts"] = duplicates
            result["instagram"]["total_duplicates"] = len(duplicates)
            result["has_duplicates"] = True
        
        # Check LinkedIn duplicates
        if registered_linkedin and len(linkedin_accounts) > 1:
            duplicates = [acc_id for acc_id in linkedin_accounts if acc_id != registered_linkedin]
            result["linkedin"]["duplicate_accounts"] = duplicates
            result["linkedin"]["total_duplicates"] = len(duplicates)
            result["has_duplicates"] = True
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking duplicates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/client/{client_id}/cleanup-duplicates")
async def cleanup_client_duplicates(client_id: str, platform: str = "instagram"):
    """
    Automatically cleanup duplicate accounts for a client
    POST /admin/client/{client_id}/cleanup-duplicates?platform=instagram
    
    This will:
    1. Keep the registered account from database
    2. Delete all other accounts of same type from Unipile
    """
    try:
        if platform not in ["instagram", "linkedin"]:
            raise HTTPException(status_code=400, detail="Platform must be 'instagram' or 'linkedin'")
        
        # Get client's registered account
        client = clients_collection.find_one({"client_id": client_id})
        
        if not client:
            raise HTTPException(status_code=404, detail="Client not found")
        
        registered_account_id = client.get(platform, {}).get("account_id")
        
        if not registered_account_id:
            raise HTTPException(status_code=400, detail=f"No {platform} account registered for this client")
        
        # Get all Unipile accounts
        all_accounts = await list_all_unipile_accounts()
        
        platform_type = "INSTAGRAM" if platform == "instagram" else "LINKEDIN"
        platform_accounts = [acc["id"] for acc in all_accounts if acc.get("type") == platform_type]
        
        # Find duplicates
        duplicate_accounts = [acc_id for acc_id in platform_accounts if acc_id != registered_account_id]
        
        if not duplicate_accounts:
            return {
                "success": True,
                "message": f"No duplicate {platform} accounts found",
                "kept": registered_account_id,
                "deleted": 0
            }
        
        # Delete duplicates
        deleted_count = 0
        failed_count = 0
        deleted_ids = []
        failed_ids = []
        
        for acc_id in duplicate_accounts:
            logger.info(f"Deleting duplicate {platform} account: {acc_id}")
            success = await delete_unipile_account(acc_id)
            
            if success:
                deleted_count += 1
                deleted_ids.append(acc_id)
            else:
                failed_count += 1
                failed_ids.append(acc_id)
        
        return {
            "success": True,
            "message": f"Cleanup completed for {platform}",
            "kept": registered_account_id,
            "deleted": deleted_count,
            "failed": failed_count,
            "deleted_account_ids": deleted_ids,
            "failed_account_ids": failed_ids
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/client/{client_id}/prospects/stats")
async def get_client_prospect_stats(client_id: str, platform: str = "instagram"):
    """
    Get prospect statistics for a client
    GET /admin/client/{client_id}/prospects/stats?platform=instagram
    
    Returns detailed tracking stats:
    - Total prospects
    - Sent/Read/Replied counts
    - Read rate & Reply rate
    - No reply after 48h
    """
    try:
        if platform not in ["instagram", "linkedin"]:
            raise HTTPException(status_code=400, detail="Platform must be 'instagram' or 'linkedin'")
        
        # Verify client exists
        client = clients_collection.find_one({"client_id": client_id})
        if not client:
            raise HTTPException(status_code=404, detail="Client not found")
        
        # Get prospects
        audience_doc = audience_collection.find_one({
            "client_id": client_id,
            "platform": platform,
            "type": "prospects"
        })
        
        if not audience_doc:
            return {
                "success": True,
                "client_id": client_id,
                "platform": platform,
                "stats": {
                    "total": 0,
                    "sent": 0,
                    "read": 0,
                    "replied": 0,
                    "no_reply_48h": 0,
                    "failed": 0,
                    "read_rate": 0,
                    "reply_rate": 0
                },
                "prospects": []
            }
        
        prospects = audience_doc.get("prospects", [])
        
        # Calculate stats
        total = len(prospects)
        sent = sum(1 for p in prospects if p.get("status") in ["sent", "read", "replied", "confirmed_sent"])
        read = sum(1 for p in prospects if p.get("message_read") is True)
        replied = sum(1 for p in prospects if p.get("replied") is True)
        no_reply_48h = sum(1 for p in prospects if p.get("status") == "no_reply_48h")
        failed = sum(1 for p in prospects if p.get("message_failed") is True)
        
        stats = {
            "total": total,
            "sent": sent,
            "read": read,
            "replied": replied,
            "no_reply_48h": no_reply_48h,
            "failed": failed,
            "read_rate": round((read / sent * 100), 2) if sent > 0 else 0,
            "reply_rate": round((replied / sent * 100), 2) if sent > 0 else 0
        }
        
        # Return detailed prospect info
        prospect_details = []
        for p in prospects:
            prospect_details.append({
                "username": p.get("username"),
                "status": p.get("status", "unknown"),
                "sent_at": p.get("sent_at"),
                "read_at": p.get("read_at"),
                "reply_at": p.get("reply_at"),
                "message_read": p.get("message_read", False),
                "replied": p.get("replied", False),
                "last_message": p.get("last_message", "")[:100]  # First 100 chars
            })
        
        return {
            "success": True,
            "client_id": client_id,
            "platform": platform,
            "stats": stats,
            "prospects": prospect_details
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting prospect stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/client/{client_id}/tracking-health")
async def check_tracking_health(client_id: str):
    """
    Check if tracking is working correctly for this client
    GET /admin/client/{client_id}/tracking-health
    
    Checks:
    - Are accounts connected?
    - Are webhooks being received?
    - Are there duplicate accounts causing issues?
    """
    try:
        client = clients_collection.find_one({"client_id": client_id})
        
        if not client:
            raise HTTPException(status_code=404, detail="Client not found")
        
        health = {
            "client_id": client_id,
            "overall_health": "healthy",
            "issues": [],
            "warnings": [],
            "instagram": {"status": "not_connected"},
            "linkedin": {"status": "not_connected"}
        }
        
        # Check Instagram
        if "instagram" in client and client["instagram"].get("account_id"):
            insta = client["instagram"]
            insta_health = {
                "status": "connected",
                "account_id": insta.get("account_id"),
                "is_active": insta.get("is_active", False),
                "last_webhook": insta.get("last_status_check"),
                "webhook_confirmed": insta.get("webhook_confirmed", False)
            }
            
            if not insta.get("is_active"):
                health["issues"].append("Instagram account is not active")
                health["overall_health"] = "unhealthy"
            
            if not insta.get("webhook_confirmed"):
                health["warnings"].append("Instagram webhooks not yet confirmed")
            
            health["instagram"] = insta_health
        
        # Check LinkedIn
        if "linkedin" in client and client["linkedin"].get("account_id"):
            linkedin = client["linkedin"]
            linkedin_health = {
                "status": "connected",
                "account_id": linkedin.get("account_id"),
                "is_active": linkedin.get("is_active", False),
                "last_webhook": linkedin.get("last_status_check"),
                "webhook_confirmed": linkedin.get("webhook_confirmed", False)
            }
            
            if not linkedin.get("is_active"):
                health["issues"].append("LinkedIn account is not active")
                health["overall_health"] = "unhealthy"
            
            if not linkedin.get("webhook_confirmed"):
                health["warnings"].append("LinkedIn webhooks not yet confirmed")
            
            health["linkedin"] = linkedin_health
        
        # Check for duplicates
        all_accounts = await list_all_unipile_accounts()
        instagram_accounts = [acc["id"] for acc in all_accounts if acc.get("type") == "INSTAGRAM"]
        linkedin_accounts = [acc["id"] for acc in all_accounts if acc.get("type") == "LINKEDIN"]
        
        if len(instagram_accounts) > 1 and client.get("instagram", {}).get("account_id"):
            health["issues"].append(f"Multiple Instagram accounts detected ({len(instagram_accounts)} total)")
            health["overall_health"] = "unhealthy"
        
        if len(linkedin_accounts) > 1 and client.get("linkedin", {}).get("account_id"):
            health["issues"].append(f"Multiple LinkedIn accounts detected ({len(linkedin_accounts)} total)")
            health["overall_health"] = "unhealthy"
        
        if len(health["issues"]) == 0 and len(health["warnings"]) == 0:
            health["overall_health"] = "healthy"
        elif len(health["issues"]) == 0:
            health["overall_health"] = "warning"
        
        return health
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking tracking health: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════════════
# SYSTEM-WIDE ADMIN ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════

@router.get("/system/unipile/accounts")
async def list_all_accounts():
    """
    List ALL Unipile accounts (system-wide)
    GET /admin/system/unipile/accounts
    """
    try:
        accounts = await list_all_unipile_accounts()
        return {
            "success": True,
            "count": len(accounts),
            "accounts": accounts
        }
    except Exception as e:
        logger.error(f"Error listing accounts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/system/database/audit")
async def audit_all_clients():
    """
    Audit ALL clients' accounts against Unipile
    POST /admin/system/database/audit
    """
    try:
        await audit_client_accounts()
        return {
            "success": True,
            "message": "System-wide audit completed successfully"
        }
    except Exception as e:
        logger.error(f"Error during audit: {e}")
        raise HTTPException(status_code=500, detail=str(e))