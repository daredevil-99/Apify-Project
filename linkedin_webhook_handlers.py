"""
Webhook Handlers for LinkedIn Tracking
=======================================
Add these to your webhooks file to enable LinkedIn tracking

These handlers work with the webhooks you already have registered:
1. users webhook (new_relation event) - tracks connection acceptances
2. messaging webhook (message_received event) - tracks replies and reads
"""

import logging
from datetime import datetime
from fastapi import Request
from linkedin_tracking import (
    track_linkedin_connection_accepted,
    track_linkedin_message_reply,
    track_linkedin_message_read
)
from db_config import clients_collection

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# LINKEDIN CONNECTION ACCEPTED WEBHOOK
# ═══════════════════════════════════════════════════════════════════════

async def handle_linkedin_new_relation(webhook_data: dict):
    """
    Handle 'new_relation' webhook - someone accepted your connection
    
    Webhook data format:
    {
        "object": "users",
        "type": "new_relation",
        "account_id": "8hXfUqMmRaO8oDauqzuNWw",
        "provider_id": "ACwAAAQDZYwBoBfno4RF2EP_dcqu_KUHTREzNIw",
        "created_at": "2026-02-04T08:13:14Z"
    }
    """
    try:
        account_id = webhook_data.get("account_id")
        provider_id = webhook_data.get("provider_id")  # The person who accepted
        created_at_str = webhook_data.get("created_at")
        
        if not account_id or not provider_id:
            logger.warning("Missing account_id or provider_id in new_relation webhook")
            return
        
        # Find client by account_id
        client = clients_collection.find_one({
            "linkedin.account_id": account_id
        })
        
        if not client:
            logger.warning(f"No client found for LinkedIn account {account_id}")
            return
        
        client_id = client.get("client_id")
        
        # Parse created_at
        created_at = None
        if created_at_str:
            try:
                created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            except:
                created_at = datetime.utcnow()
        
        # Track the connection acceptance
        result = track_linkedin_connection_accepted(
            client_id=client_id,
            provider_id=provider_id,
            accepted_at=created_at
        )
        
        if result["status"] == "success":
            logger.info(f"✅ Tracked LinkedIn connection accepted for {client_id}: {provider_id}")
        else:
            logger.error(f"❌ Failed to track connection: {result.get('error')}")
        
    except Exception as e:
        logger.error(f"Error handling new_relation webhook: {str(e)}", exc_info=True)


# ═══════════════════════════════════════════════════════════════════════
# LINKEDIN MESSAGE REPLY WEBHOOK
# ═══════════════════════════════════════════════════════════════════════

async def handle_linkedin_message_received(webhook_data: dict):
    """
    Handle 'message_received' webhook - someone replied to your message
    
    Webhook data format:
    {
        "object": "messaging",
        "type": "message_received",
        "account_id": "8hXfUqMmRaO8oDauqzuNWw",
        "chat_id": "urn:li:messagingThread:...",
        "message": {
            "id": "urn:li:message:...",
            "text": "Thanks for reaching out!",
            "from": "ACwAAAQDZYwBoBfno4RF2EP_dcqu_KUHTREzNIw",
            "created_at": "2026-02-04T09:00:00Z"
        }
    }
    """
    try:
        account_id = webhook_data.get("account_id")
        message_data = webhook_data.get("message", {})
        
        sender_provider_id = message_data.get("from")
        message_text = message_data.get("text", "")
        created_at_str = message_data.get("created_at")
        
        if not account_id or not sender_provider_id:
            logger.warning("Missing account_id or sender in message_received webhook")
            return
        
        # Find client by account_id
        client = clients_collection.find_one({
            "linkedin.account_id": account_id
        })
        
        if not client:
            logger.warning(f"No client found for LinkedIn account {account_id}")
            return
        
        client_id = client.get("client_id")
        
        # Parse created_at
        replied_at = None
        if created_at_str:
            try:
                replied_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            except:
                replied_at = datetime.utcnow()
        
        # Track the reply
        result = track_linkedin_message_reply(
            client_id=client_id,
            provider_id=sender_provider_id,
            message_text=message_text,
            replied_at=replied_at
        )
        
        if result["status"] == "success":
            logger.info(f"✅ Tracked LinkedIn reply from {sender_provider_id}")
        else:
            logger.error(f"❌ Failed to track reply: {result.get('error')}")
        
    except Exception as e:
        logger.error(f"Error handling message_received webhook: {str(e)}", exc_info=True)


# ═══════════════════════════════════════════════════════════════════════
# LINKEDIN MESSAGE READ WEBHOOK
# ═══════════════════════════════════════════════════════════════════════

async def handle_linkedin_message_read(webhook_data: dict):
    """
    Handle 'message_read' webhook - your message was read
    
    Webhook data format:
    {
        "object": "messaging",
        "type": "message_read",
        "account_id": "8hXfUqMmRaO8oDauqzuNWw",
        "chat_id": "urn:li:messagingThread:...",
        "read_by": "ACwAAAQDZYwBoBfno4RF2EP_dcqu_KUHTREzNIw",
        "read_at": "2026-02-04T08:45:00Z"
    }
    """
    try:
        account_id = webhook_data.get("account_id")
        read_by_provider_id = webhook_data.get("read_by")
        read_at_str = webhook_data.get("read_at")
        
        if not account_id or not read_by_provider_id:
            logger.warning("Missing account_id or read_by in message_read webhook")
            return
        
        # Find client by account_id
        client = clients_collection.find_one({
            "linkedin.account_id": account_id
        })
        
        if not client:
            logger.warning(f"No client found for LinkedIn account {account_id}")
            return
        
        client_id = client.get("client_id")
        
        # Parse read_at
        read_at = None
        if read_at_str:
            try:
                read_at = datetime.fromisoformat(read_at_str.replace('Z', '+00:00'))
            except:
                read_at = datetime.utcnow()
        
        # Track the read status
        result = track_linkedin_message_read(
            client_id=client_id,
            provider_id=read_by_provider_id,
            read_at=read_at
        )
        
        if result["status"] == "success":
            logger.info(f"✅ Tracked LinkedIn message read by {read_by_provider_id}")
        else:
            logger.error(f"❌ Failed to track read: {result.get('error')}")
        
    except Exception as e:
        logger.error(f"Error handling message_read webhook: {str(e)}", exc_info=True)


# ═══════════════════════════════════════════════════════════════════════
# MAIN WEBHOOK ROUTER (ADD TO YOUR EXISTING WEBHOOK FILE)
# ═══════════════════════════════════════════════════════════════════════

async def handle_users_webhook(request: Request):
    """
    Main handler for users webhook
    Routes to appropriate LinkedIn handler
    """
    try:
        data = await request.json()
        
        webhook_type = data.get("type")
        provider = data.get("provider", "").upper()
        
        # Only handle LinkedIn events
        if provider != "LINKEDIN":
            return
        
        if webhook_type == "new_relation":
            await handle_linkedin_new_relation(data)
        else:
            logger.debug(f"Unhandled users webhook type: {webhook_type}")
    
    except Exception as e:
        logger.error(f"Error in users webhook: {str(e)}", exc_info=True)


async def handle_messaging_webhook(request: Request):
    """
    Main handler for messaging webhook
    Routes to appropriate LinkedIn handler
    """
    try:
        data = await request.json()
        
        webhook_type = data.get("type")
        provider = data.get("provider", "").upper()
        
        # Only handle LinkedIn events
        if provider != "LINKEDIN":
            return
        
        if webhook_type == "message_received":
            await handle_linkedin_message_received(data)
        elif webhook_type == "message_read":
            await handle_linkedin_message_read(data)
        else:
            logger.debug(f"Unhandled messaging webhook type: {webhook_type}")
    
    except Exception as e:
        logger.error(f"Error in messaging webhook: {str(e)}", exc_info=True)


# ═══════════════════════════════════════════════════════════════════════
# INTEGRATION EXAMPLE FOR YOUR EXISTING WEBHOOK FILE
# ═══════════════════════════════════════════════════════════════════════

"""
Add this to your existing webhooks.py file:

from linkedin_webhook_handlers import (
    handle_linkedin_new_relation,
    handle_linkedin_message_received,
    handle_linkedin_message_read
)

# In your users webhook endpoint:
@router.post("/pipeline/webhook/users")
async def users_webhook(request: Request):
    try:
        data = await request.json()
        
        # Existing Instagram handling
        if data.get("provider") == "INSTAGRAM":
            # ... your existing Instagram code ...
            pass
        
        # NEW: LinkedIn handling
        elif data.get("provider") == "LINKEDIN":
            if data.get("type") == "new_relation":
                await handle_linkedin_new_relation(data)
        
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return {"status": "error"}


# In your messaging webhook endpoint:
@router.post("/pipeline/webhook/messaging")
async def messaging_webhook(request: Request):
    try:
        data = await request.json()
        
        # Existing Instagram handling
        if data.get("provider") == "INSTAGRAM":
            # ... your existing Instagram code ...
            pass
        
        # NEW: LinkedIn handling
        elif data.get("provider") == "LINKEDIN":
            if data.get("type") == "message_received":
                await handle_linkedin_message_received(data)
            elif data.get("type") == "message_read":
                await handle_linkedin_message_read(data)
        
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return {"status": "error"}
"""