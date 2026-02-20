"""
LinkedIn Tracking System - Complete Implementation
===================================================
This extends your existing Instagram tracking to work with LinkedIn

Key Features:
1. Track sent invitations
2. Track accepted connections (via webhook)
3. Track message replies
4. Mark no-reply after 48h
5. Admin endpoints for stats
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from db_config import audience_collection, clients_collection

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# LINKEDIN PROSPECT TRACKING
# ═══════════════════════════════════════════════════════════════════════

def track_linkedin_invitation_sent(
    client_id: str,
    provider_id: str,
    recipient_name: str,
    recipient_url: str,
    message: str,
    invitation_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Track when a LinkedIn invitation is sent
    
    Args:
        client_id: Client identifier
        provider_id: LinkedIn provider_id (ACwAAA...)
        recipient_name: Name of recipient
        recipient_url: LinkedIn profile URL
        message: Message sent (or empty if no message)
        invitation_id: Invitation ID from Unipile response
    
    Returns:
        dict with tracking result
    """
    try:
        # Get or create audience document
        audience_doc = audience_collection.find_one({
            "client_id": client_id,
            "platform": "linkedin",
            "type": "prospects"
        })
        
        if not audience_doc:
            # Create new document
            audience_collection.insert_one({
                "client_id": client_id,
                "platform": "linkedin",
                "type": "prospects",
                "prospects": [],
                "created_at": datetime.utcnow(),
                "last_updated": datetime.utcnow()
            })
            audience_doc = audience_collection.find_one({
                "client_id": client_id,
                "platform": "linkedin",
                "type": "prospects"
            })
        
        prospects = audience_doc.get("prospects", [])
        
        # Check if prospect already exists
        existing_idx = None
        for idx, p in enumerate(prospects):
            if p.get("provider_id") == provider_id:
                existing_idx = idx
                break
        
        # Create prospect entry
        prospect_data = {
            "provider_id": provider_id,
            "name": recipient_name,
            "profile_url": recipient_url,
            "status": "invitation_sent",
            "message": message,
            "invitation_id": invitation_id,
            "sent_at": datetime.utcnow(),
            "connection_accepted": False,
            "message_read": False,
            "replied": False,
            "last_event": "invitation_sent",
            "last_event_at": datetime.utcnow()
        }
        
        if existing_idx is not None:
            # Update existing prospect
            prospects[existing_idx] = {**prospects[existing_idx], **prospect_data}
        else:
            # Add new prospect
            prospects.append(prospect_data)
        
        # Update document
        audience_collection.update_one(
            {"_id": audience_doc["_id"]},
            {
                "$set": {
                    "prospects": prospects,
                    "last_updated": datetime.utcnow()
                }
            }
        )
        
        logger.info(f"✅ Tracked LinkedIn invitation sent: {recipient_name} ({provider_id})")
        
        return {
            "status": "success",
            "message": "Invitation tracked",
            "provider_id": provider_id
        }
        
    except Exception as e:
        logger.error(f"Error tracking LinkedIn invitation: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }


def track_linkedin_connection_accepted(
    client_id: str,
    provider_id: str,
    accepted_at: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Track when a LinkedIn connection is accepted
    Called by webhook handler
    
    Args:
        client_id: Client identifier
        provider_id: LinkedIn provider_id
        accepted_at: When connection was accepted
    
    Returns:
        dict with tracking result
    """
    try:
        audience_doc = audience_collection.find_one({
            "client_id": client_id,
            "platform": "linkedin",
            "type": "prospects"
        })
        
        if not audience_doc:
            logger.warning(f"No LinkedIn prospects found for client {client_id}")
            return {
                "status": "error",
                "error": "No prospects document found"
            }
        
        prospects = audience_doc.get("prospects", [])
        
        # Find the prospect
        found = False
        for prospect in prospects:
            if prospect.get("provider_id") == provider_id:
                prospect["connection_accepted"] = True
                prospect["accepted_at"] = accepted_at or datetime.utcnow()
                prospect["status"] = "connected"
                prospect["last_event"] = "connection_accepted"
                prospect["last_event_at"] = datetime.utcnow()
                found = True
                break
        
        if not found:
            logger.warning(f"Prospect not found for provider_id: {provider_id}")
            # Add them anyway (might be an old connection)
            prospects.append({
                "provider_id": provider_id,
                "name": "Unknown",
                "status": "connected",
                "connection_accepted": True,
                "accepted_at": accepted_at or datetime.utcnow(),
                "last_event": "connection_accepted",
                "last_event_at": datetime.utcnow()
            })
        
        # Update document
        audience_collection.update_one(
            {"_id": audience_doc["_id"]},
            {
                "$set": {
                    "prospects": prospects,
                    "last_updated": datetime.utcnow()
                }
            }
        )
        
        logger.info(f"✅ Tracked LinkedIn connection accepted: {provider_id}")
        
        return {
            "status": "success",
            "message": "Connection acceptance tracked",
            "provider_id": provider_id
        }
        
    except Exception as e:
        logger.error(f"Error tracking connection acceptance: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }


def track_linkedin_message_reply(
    client_id: str,
    provider_id: str,
    message_text: str,
    replied_at: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Track when someone replies to your LinkedIn message
    Called by webhook handler
    
    Args:
        client_id: Client identifier
        provider_id: LinkedIn provider_id
        message_text: Reply text
        replied_at: When reply was received
    
    Returns:
        dict with tracking result
    """
    try:
        audience_doc = audience_collection.find_one({
            "client_id": client_id,
            "platform": "linkedin",
            "type": "prospects"
        })
        
        if not audience_doc:
            logger.warning(f"No LinkedIn prospects found for client {client_id}")
            return {
                "status": "error",
                "error": "No prospects document found"
            }
        
        prospects = audience_doc.get("prospects", [])
        
        # Find the prospect
        found = False
        for prospect in prospects:
            if prospect.get("provider_id") == provider_id:
                prospect["replied"] = True
                prospect["reply_at"] = replied_at or datetime.utcnow()
                prospect["last_message"] = message_text
                prospect["status"] = "replied"
                prospect["last_event"] = "replied"
                prospect["last_event_at"] = datetime.utcnow()
                found = True
                break
        
        if not found:
            logger.warning(f"Prospect not found for provider_id: {provider_id}")
            # Add them anyway
            prospects.append({
                "provider_id": provider_id,
                "name": "Unknown",
                "status": "replied",
                "replied": True,
                "reply_at": replied_at or datetime.utcnow(),
                "last_message": message_text,
                "last_event": "replied",
                "last_event_at": datetime.utcnow()
            })
        
        # Update document
        audience_collection.update_one(
            {"_id": audience_doc["_id"]},
            {
                "$set": {
                    "prospects": prospects,
                    "last_updated": datetime.utcnow()
                }
            }
        )
        
        logger.info(f"✅ Tracked LinkedIn reply: {provider_id}")
        
        return {
            "status": "success",
            "message": "Reply tracked",
            "provider_id": provider_id
        }
        
    except Exception as e:
        logger.error(f"Error tracking reply: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }


def track_linkedin_message_read(
    client_id: str,
    provider_id: str,
    read_at: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Track when LinkedIn message is read
    Called by webhook handler
    
    Args:
        client_id: Client identifier
        provider_id: LinkedIn provider_id
        read_at: When message was read
    
    Returns:
        dict with tracking result
    """
    try:
        audience_doc = audience_collection.find_one({
            "client_id": client_id,
            "platform": "linkedin",
            "type": "prospects"
        })
        
        if not audience_doc:
            return {"status": "error", "error": "No prospects document found"}
        
        prospects = audience_doc.get("prospects", [])
        
        # Find the prospect
        for prospect in prospects:
            if prospect.get("provider_id") == provider_id:
                prospect["message_read"] = True
                prospect["read_at"] = read_at or datetime.utcnow()
                
                # Only update status if not already replied
                if prospect.get("status") != "replied":
                    prospect["status"] = "read"
                
                prospect["last_event"] = "message_read"
                prospect["last_event_at"] = datetime.utcnow()
                break
        
        # Update document
        audience_collection.update_one(
            {"_id": audience_doc["_id"]},
            {
                "$set": {
                    "prospects": prospects,
                    "last_updated": datetime.utcnow()
                }
            }
        )
        
        logger.info(f"✅ Tracked LinkedIn message read: {provider_id}")
        
        return {"status": "success", "message": "Read status tracked"}
        
    except Exception as e:
        logger.error(f"Error tracking message read: {str(e)}", exc_info=True)
        return {"status": "error", "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════
# GET TRACKING STATS
# ═══════════════════════════════════════════════════════════════════════

def get_linkedin_tracking_stats(client_id: str) -> Dict[str, Any]:
    """
    Get LinkedIn tracking statistics for a client
    
    Args:
        client_id: Client identifier
    
    Returns:
        dict with stats
    """
    try:
        audience_doc = audience_collection.find_one({
            "client_id": client_id,
            "platform": "linkedin",
            "type": "prospects"
        })
        
        if not audience_doc:
            return {
                "status": "success",
                "client_id": client_id,
                "platform": "linkedin",
                "stats": {
                    "total": 0,
                    "invitation_sent": 0,
                    "connected": 0,
                    "read": 0,
                    "replied": 0,
                    "no_reply_48h": 0,
                    "connection_rate": 0,
                    "read_rate": 0,
                    "reply_rate": 0
                },
                "prospects": []
            }
        
        prospects = audience_doc.get("prospects", [])
        
        # Calculate stats
        total = len(prospects)
        invitation_sent = sum(1 for p in prospects if p.get("status") in [
            "invitation_sent", "connected", "read", "replied"
        ])
        connected = sum(1 for p in prospects if p.get("connection_accepted") is True)
        read = sum(1 for p in prospects if p.get("message_read") is True)
        replied = sum(1 for p in prospects if p.get("replied") is True)
        no_reply_48h = sum(1 for p in prospects if p.get("status") == "no_reply_48h")
        
        stats = {
            "total": total,
            "invitation_sent": invitation_sent,
            "connected": connected,
            "read": read,
            "replied": replied,
            "no_reply_48h": no_reply_48h,
            "connection_rate": round((connected / invitation_sent * 100), 2) if invitation_sent > 0 else 0,
            "read_rate": round((read / connected * 100), 2) if connected > 0 else 0,
            "reply_rate": round((replied / connected * 100), 2) if connected > 0 else 0
        }
        
        # Format prospect details
        prospect_details = []
        for p in prospects:
            prospect_details.append({
                "provider_id": p.get("provider_id"),
                "name": p.get("name"),
                "profile_url": p.get("profile_url"),
                "status": p.get("status", "unknown"),
                "sent_at": p.get("sent_at"),
                "accepted_at": p.get("accepted_at"),
                "read_at": p.get("read_at"),
                "reply_at": p.get("reply_at"),
                "connection_accepted": p.get("connection_accepted", False),
                "message_read": p.get("message_read", False),
                "replied": p.get("replied", False),
                "last_message": p.get("last_message", "")[:100]
            })
        
        return {
            "status": "success",
            "client_id": client_id,
            "platform": "linkedin",
            "stats": stats,
            "prospects": prospect_details
        }
        
    except Exception as e:
        logger.error(f"Error getting LinkedIn stats: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }


# ═══════════════════════════════════════════════════════════════════════
# NO-REPLY AFTER 48H JOB
# ═══════════════════════════════════════════════════════════════════════

def mark_linkedin_no_reply_48h() -> int:
    """
    Mark LinkedIn prospects as no_reply_48h if:
    - Connection was accepted
    - Message was sent (or invitation with message)
    - No reply received
    - 48 hours have passed
    
    Returns:
        Number of prospects updated
    """
    try:
        cutoff_time = datetime.utcnow() - timedelta(hours=48)
        
        # Find all LinkedIn prospect documents
        audience_docs = audience_collection.find({
            "platform": "linkedin",
            "type": "prospects"
        })
        
        total_updated = 0
        
        for doc in audience_docs:
            prospects = doc.get("prospects", [])
            updated = False
            
            for prospect in prospects:
                # Check if prospect qualifies for no_reply_48h
                sent_at = prospect.get("sent_at") or prospect.get("accepted_at")
                
                if (sent_at and 
                    sent_at <= cutoff_time and
                    not prospect.get("replied", False) and
                    prospect.get("status") not in ["replied", "no_reply_48h"]):
                    
                    prospect["status"] = "no_reply_48h"
                    prospect["no_reply_marked_at"] = datetime.utcnow()
                    prospect["last_event"] = "no_reply_48h"
                    prospect["last_event_at"] = datetime.utcnow()
                    updated = True
                    total_updated += 1
            
            if updated:
                audience_collection.update_one(
                    {"_id": doc["_id"]},
                    {
                        "$set": {
                            "prospects": prospects,
                            "last_updated": datetime.utcnow()
                        }
                    }
                )
        
        logger.info(f"⏱️ LinkedIn no-reply-48h job completed | updated={total_updated}")
        
        return total_updated
        
    except Exception as e:
        logger.error(f"Error in LinkedIn no-reply job: {str(e)}", exc_info=True)
        return 0