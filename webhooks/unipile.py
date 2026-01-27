# webhooks/unipile.py

from fastapi import APIRouter, Request
import json
import logging
from datetime import datetime
from db_config import clients_collection, audience_collection

router = APIRouter(prefix="/pipeline/webhook", tags=["Webhooks"])
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# HELPER FUNCTION: Find client by account_id
# ═══════════════════════════════════════════════════════════════════════

def find_client_by_account_id(account_id: str):
    """
    Find client in database by Unipile account_id
    Returns: (client_doc, platform) or (None, None)
    """
    client = clients_collection.find_one({
        "$or": [
            {"instagram.account_id": account_id},
            {"linkedin.account_id": account_id}
        ]
    })
    
    if not client:
        return None, None
    
    # Determine which platform
    platform = None
    if "instagram" in client and client["instagram"].get("account_id") == account_id:
        platform = "instagram"
    elif "linkedin" in client and client["linkedin"].get("account_id") == account_id:
        platform = "linkedin"
    
    return client, platform


@router.post("/account-status")
async def unipile_account_status_webhook(request: Request):
    """
    Handles account connection status changes.
    Events: CREATION_SUCCESS, CREATION_FAIL, DELETED, RECONNECTED, ERROR, etc.
    """
    try:
        body = await request.body()
        
        if not body:
            logger.info("🔔 Empty webhook received (likely verification ping)")
            return {"status": "ok"}
        
        payload = json.loads(body)
        logger.info(f"🔔 Account Status Webhook: {json.dumps(payload, indent=2)}")
        
        # Extract from nested AccountStatus structure
        account_status = payload.get("AccountStatus", {})
        account_id = account_status.get("account_id")
        event = account_status.get("message")  # "CREATION_SUCCESS", "DELETED", etc.
        account_type = account_status.get("account_type")  # "INSTAGRAM" or "LINKEDIN"
        
        if not account_id or not event or not account_type:
            logger.warning(f"⚠️ Missing required fields in webhook: {payload}")
            return {"status": "ok", "message": "Missing required fields"}
        
        logger.info(f"📊 Account ID: {account_id}")
        logger.info(f"📊 Event: {event}")
        logger.info(f"📊 Type: {account_type}")
        
        # ✅ NEW: Verify account exists in our database
        client, platform = find_client_by_account_id(account_id)
        
        if not client:
            logger.warning(f"⚠️ Ignoring webhook from unknown account_id: {account_id}")
            logger.warning(f"   This might be a duplicate/orphaned account")
            return {"status": "ignored", "reason": "unknown_account"}
        
        client_id = client["client_id"]
        logger.info(f"✅ Account belongs to client: {client_id}")
        
        # Determine platform field name
        platform_field = "instagram" if account_type == "INSTAGRAM" else "linkedin"
        
        # ═══════════════════════════════════════════════════════════════════
        # HANDLE DIFFERENT EVENTS
        # ═══════════════════════════════════════════════════════════════════
        
        if event == "CREATION_SUCCESS":
            logger.info(f"✅ Account connected successfully: {account_type}")
            
            result = clients_collection.update_one(
                {f"{platform_field}.account_id": account_id},
                {
                    "$set": {
                        f"{platform_field}.status": "active",
                        f"{platform_field}.is_active": True,
                        f"{platform_field}.last_webhook_event": event,
                        f"{platform_field}.webhook_confirmed": True,
                        f"{platform_field}.last_status_check": datetime.utcnow().isoformat()
                    }
                }
            )
            
            if result.modified_count > 0:
                logger.info(f"✅ Updated {platform_field} account {account_id} to active status")
            else:
                logger.info(f"   ℹ️  Account details will be stored by success endpoint")
        
        elif event == "DELETED":
            logger.info(f"❌ Account deleted: {account_type}")
            
            clients_collection.update_one(
                {f"{platform_field}.account_id": account_id},
                {
                    "$set": {
                        f"{platform_field}.status": "deleted",
                        f"{platform_field}.is_active": False,
                        f"{platform_field}.deleted_at": datetime.utcnow().isoformat(),
                        f"{platform_field}.last_webhook_event": event,
                        f"{platform_field}.last_status_check": datetime.utcnow().isoformat()
                    }
                }
            )
            logger.info(f"✅ Marked {platform_field} account {account_id} as deleted")
        
        elif event == "RECONNECTED":
            logger.info(f"🔄 Account reconnected: {account_type}")
            
            clients_collection.update_one(
                {f"{platform_field}.account_id": account_id},
                {
                    "$set": {
                        f"{platform_field}.status": "active",
                        f"{platform_field}.is_active": True,
                        f"{platform_field}.reconnected_at": datetime.utcnow().isoformat(),
                        f"{platform_field}.last_webhook_event": event,
                        f"{platform_field}.last_status_check": datetime.utcnow().isoformat()
                    }
                }
            )
            logger.info(f"✅ Marked {platform_field} account {account_id} as reconnected")
        
        elif event == "ERROR":
            logger.error(f"❌ Account error: {account_type}")
            
            clients_collection.update_one(
                {f"{platform_field}.account_id": account_id},
                {
                    "$set": {
                        f"{platform_field}.status": "error",
                        f"{platform_field}.error_at": datetime.utcnow().isoformat(),
                        f"{platform_field}.last_webhook_event": event,
                        f"{platform_field}.last_status_check": datetime.utcnow().isoformat()
                    }
                }
            )
            logger.info(f"✅ Marked {platform_field} account {account_id} with error status")
        
        elif event == "CREDENTIALS":
            logger.warning(f"⚠️ Account needs re-authentication: {account_type}")
            
            clients_collection.update_one(
                {f"{platform_field}.account_id": account_id},
                {
                    "$set": {
                        f"{platform_field}.status": "credentials_required",
                        f"{platform_field}.is_active": False,
                        f"{platform_field}.credentials_error_at": datetime.utcnow().isoformat(),
                        f"{platform_field}.last_webhook_event": event,
                        f"{platform_field}.last_status_check": datetime.utcnow().isoformat()
                    }
                }
            )
            logger.info(f"✅ Marked {platform_field} account {account_id} as needing credentials")
        
        elif event == "CREATION_FAIL":
            logger.error(f"❌ Account creation failed: {account_type}")
            
            clients_collection.update_one(
                {f"{platform_field}.account_id": account_id},
                {
                    "$set": {
                        f"{platform_field}.status": "creation_failed",
                        f"{platform_field}.is_active": False,
                        f"{platform_field}.failed_at": datetime.utcnow().isoformat(),
                        f"{platform_field}.last_webhook_event": event,
                        f"{platform_field}.last_status_check": datetime.utcnow().isoformat()
                    }
                }
            )
            logger.info(f"✅ Marked {platform_field} account {account_id} as creation failed")
        
        else:
            logger.warning(f"⚠️ Unknown event type: {event}")
            
            clients_collection.update_one(
                {f"{platform_field}.account_id": account_id},
                {
                    "$set": {
                        f"{platform_field}.last_webhook_event": event,
                        f"{platform_field}.last_status_check": datetime.utcnow().isoformat()
                    }
                }
            )
        
        return {"status": "ok", "processed": True}
        
    except json.JSONDecodeError:
        logger.error("❌ Invalid JSON in webhook payload")
        return {"status": "error", "message": "Invalid JSON"}
    except Exception as e:
        logger.error(f"❌ Account status webhook error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


@router.post("/messaging")
async def unipile_messaging_webhook(request: Request):
    """
    Handles all messaging events:
    - message_received: New message/reply received OR sent message confirmation
    - message_read: Message was read
    - message_delivered: Message was delivered
    - message_reaction: Someone reacted to a message
    """
    try:
        body = await request.body()
        
        if not body:
            logger.info("🔔 Empty webhook received (likely verification ping)")
            return {"status": "ok"}
        
        payload = json.loads(body)
        logger.info(f"💬 Messaging Webhook: {json.dumps(payload, indent=2)}")
        
        # Extract event data
        event = payload.get("event")
        account_id = payload.get("account_id")
        message_id = payload.get("message_id")
        chat_id = payload.get("chat_id")
        timestamp = payload.get("timestamp")
        
        # Get account_info to determine if message is from connected user
        account_info = payload.get("account_info", {})
        account_user_id = account_info.get("user_id")
        
        # Get message text
        message_text = payload.get("message", "")
        if isinstance(message_text, dict):
            message_text = message_text.get("text", "")
        
        # Get sender info
        sender = payload.get("sender", {})
        if isinstance(sender, dict):
            sender_provider_id = sender.get("attendee_provider_id")
            sender_name = sender.get("attendee_name", "Unknown")
            sender_username = ""
            if sender.get("attendee_specifics"):
                sender_username = sender["attendee_specifics"].get("public_identifier", "")
        else:
            sender_provider_id = None
            sender_name = "Unknown"
            sender_username = ""
        
        # Determine if this is a sent message (from connected account)
        is_sender = payload.get("is_sender", False)  # ✅ Use the is_sender flag from payload
        
        # Get recipient info (attendees)
        attendees = payload.get("attendees", [])
        recipient_provider_id = None
        recipient_username = None
        
        if attendees and len(attendees) > 0:
            first_attendee = attendees[0]
            if isinstance(first_attendee, dict):
                recipient_provider_id = first_attendee.get("attendee_provider_id")
                if first_attendee.get("attendee_specifics"):
                    recipient_username = first_attendee["attendee_specifics"].get("public_identifier")
        
        logger.info(f"📊 Event: {event}")
        logger.info(f"   Is Sender: {is_sender}")
        logger.info(f"   Sender: {sender_name} (@{sender_username})")
        logger.info(f"   Recipient: @{recipient_username}")
        logger.info(f"   Message: {message_text[:100]}...")
        
        # ✅ NEW: Find client by account_id with verification
        client, platform = find_client_by_account_id(account_id)
        
        if not client:
            logger.warning(f"⚠️ Ignoring webhook from unknown account_id: {account_id}")
            logger.warning(f"   This might be a duplicate/orphaned account")
            return {"status": "ignored", "reason": "unknown_account"}
        
        client_id = client["client_id"]
        
        logger.info(f"   Client: {client_id} | Platform: {platform}")
        
        # ═══════════════════════════════════════════════════════════════════
        # HANDLE DIFFERENT MESSAGE EVENTS
        # ═══════════════════════════════════════════════════════════════════
        
        # 💬 REPLY RECEIVED FROM PROSPECT
        if event == "message_received" and not is_sender:
            logger.info(f"💬 REPLY RECEIVED from @{sender_username}")
            
            update_result = audience_collection.update_one(
                {
                    "client_id": client_id,
                    "platform": platform,
                    "type": "prospects",
                    "prospects.username": sender_username
                },
                {
                    "$set": {
                        "prospects.$.replied": True,
                        "prospects.$.reply_at": timestamp,
                        "prospects.$.last_message": message_text[:500],
                        "prospects.$.last_reply_event": event,
                        "prospects.$.status": "replied"  # ✅ Update status to replied
                    }
                }
            )
            
            if update_result.modified_count > 0:
                logger.info(f"✅ Updated reply status for @{sender_username}")
            else:
                logger.warning(f"⚠️ Could not find prospect @{sender_username} in database")
        
        # 👀 MESSAGE READ BY RECIPIENT
        elif event == "message_read":
            logger.info(f"👀 MESSAGE READ by @{recipient_username}")
            
            if recipient_username:
                update_result = audience_collection.update_one(
                    {
                        "client_id": client_id,
                        "platform": platform,
                        "type": "prospects",
                        "prospects.username": recipient_username
                    },
                    {
                        "$set": {
                            "prospects.$.message_read": True,
                            "prospects.$.read_at": timestamp,
                            "prospects.$.status": "read"  # ✅ Update status to read
                        }
                    }
                )
                
                if update_result.modified_count > 0:
                    logger.info(f"✅ Updated read status for @{recipient_username}")
        
        # ✅ MESSAGE DELIVERED
        elif event == "message_delivered":
            logger.info(f"✅ MESSAGE DELIVERED to @{recipient_username}")
            
            if recipient_username:
                update_result = audience_collection.update_one(
                    {
                        "client_id": client_id,
                        "platform": platform,
                        "type": "prospects",
                        "prospects.username": recipient_username
                    },
                    {
                        "$set": {
                            "prospects.$.message_delivered": True,
                            "prospects.$.delivered_at": timestamp
                        }
                    }
                )
                
                if update_result.modified_count > 0:
                    logger.info(f"✅ Updated delivery status for @{recipient_username}")
        
        # 📤 SENT MESSAGE CONFIRMATION (is_sender=True)
        elif event == "message_received" and is_sender:
            logger.info(f"📤 SENT MESSAGE CONFIRMATION")
            
            # Confirm pending messages
            if recipient_username:
                update_result = audience_collection.update_one(
                    {
                        "client_id": client_id,
                        "platform": platform,
                        "type": "prospects",
                        "prospects.username": recipient_username,
                        "prospects.status": "pending_confirmation"
                    },
                    {
                        "$set": {
                            "prospects.$.status": "sent",
                            "prospects.$.confirmed_at": timestamp,
                            "prospects.$.message_id": message_id,
                            "prospects.$.webhook_confirmed": True
                        },
                        "$unset": {
                            "prospects.$.warning": ""
                        }
                    }
                )
                
                if update_result.modified_count > 0:
                    logger.info(f"✅✅ CONFIRMED pending message for @{recipient_username}")
            
            # Check for error messages from Instagram
            if "can't receive your message" in message_text.lower() or "don't allow new message requests" in message_text.lower():
                logger.warning(f"⚠️ MESSAGE REJECTED by Instagram: {message_text}")
                
                if recipient_username:
                    update_result = audience_collection.update_one(
                        {
                            "client_id": client_id,
                            "platform": platform,
                            "type": "prospects",
                            "prospects.username": recipient_username
                        },
                        {
                            "$set": {
                                "prospects.$.message_failed": True,
                                "prospects.$.failure_reason": "Message requests not allowed",
                                "prospects.$.failed_at": timestamp,
                                "prospects.$.status": "failed"
                            }
                        }
                    )
                    
                    if update_result.modified_count > 0:
                        logger.info(f"✅ Updated failure status for @{recipient_username}")
        
        return {"status": "ok", "processed": True}
        
    except json.JSONDecodeError:
        logger.error("❌ Invalid JSON in webhook payload")
        return {"status": "error", "message": "Invalid JSON"}
    except Exception as e:
        logger.error(f"❌ Messaging webhook error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


@router.post("/users")
async def unipile_users_webhook(request: Request):
    """
    Handles user relationship events:
    - new_relation: New connection/follower on LinkedIn/Instagram
    """
    try:
        body = await request.body()
        
        if not body:
            logger.info("🔔 Empty webhook received (likely verification ping)")
            return {"status": "ok"}
        
        payload = json.loads(body)
        logger.info(f"👥 Users Webhook: {json.dumps(payload, indent=2)}")
        
        event = payload.get("event")
        account_id = payload.get("account_id")
        
        # ✅ NEW: Verify account exists in our database
        client, platform = find_client_by_account_id(account_id)
        
        if not client:
            logger.warning(f"⚠️ Ignoring webhook from unknown account_id: {account_id}")
            return {"status": "ignored", "reason": "unknown_account"}
        
        if event == "new_relation":
            user_public_identifier = payload.get("user_public_identifier")
            user_full_name = payload.get("user_full_name")
            
            logger.info(f"🤝 New connection: {user_full_name} (@{user_public_identifier})")
        
        return {"status": "ok"}
        
    except Exception as e:
        logger.error(f"❌ Users webhook error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}