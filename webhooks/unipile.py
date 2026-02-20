# webhooks/unipile.py - FIXED VERSION

from fastapi import APIRouter, Request
import json
import logging
from datetime import datetime
from db_config import clients_collection, audience_collection
import os

router = APIRouter(prefix="/pipeline/webhook", tags=["Webhooks"])
logger = logging.getLogger(__name__)


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


def find_pending_connection(account_type: str):
    """
    Find the most recent pending connection for the given platform.
    This is needed because Unipile overwrites the 'name' field with the profile name.
    
    Returns: client_id or None
    """
    from db_config import db
    pending_connections_collection = db["pending_connections"]
    
    platform_name = account_type.lower()  # "LINKEDIN" -> "linkedin"
    
    # Find most recent pending connection for this platform
    pending = pending_connections_collection.find_one(
        {
            "platform": platform_name,
            "status": "pending"
        },
        sort=[("initiated_at", -1)]  # Most recent first
    )
    
    if pending:
        logger.info(f"✅ Found pending connection: {pending['client_id']}")
        logger.info(f"   Initiated at: {pending.get('initiated_at')}")
        return pending
    
    logger.warning(f"⚠️ No pending {platform_name} connection found in database")
    return None


def extract_client_id_from_account(account_details: dict, account_type: str) -> str:
    """
    Extract client_id from account details.
    
    Unipile overwrites the 'name' field with the LinkedIn/Instagram profile name,
    so we need to:
    1. Check 'name' field first (might work for some cases)
    2. Look up pending connection in database
    """
    # Try to get from 'name' field first
    name = account_details.get("name", "")
    
    # Check if name looks like a UUID (client_id format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
    if name and len(name) == 36 and name.count('-') == 4:
        logger.info(f"✅ Found client_id in 'name' field: {name}")
        return name
    
    # Name doesn't look like UUID - it's been overwritten by profile name
    logger.warning(f"⚠️ 'name' field contains profile name, not client_id: {name}")
    logger.warning(f"⚠️ Unipile overwrote the 'name' field with profile name")
    logger.error("❌ Cannot extract client_id - Unipile overwrote the name field")
    logger.error("💡 Workaround: Looking up pending connection in database...")
    
    # Look up in pending_connections table
    pending = find_pending_connection(account_type)
    
    if pending:
        return pending["client_id"]
    
    logger.error("❌ No pending connection found in database")
    return None


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
        
        # ═══════════════════════════════════════════════════════════════════
        # SPECIAL HANDLING FOR CREATION_SUCCESS - Account doesn't exist yet!
        # ═══════════════════════════════════════════════════════════════════
        if event == "CREATION_SUCCESS":
            logger.info(f"🆕 New account created: {account_type}")
            
            # Fetch account details from Unipile
            try:
                import httpx
                
                UNIPILE_API_TOKEN = os.getenv("UNIPILE_API_TOKEN")
                UNIPILE_DSN = os.getenv("UNIPILE_DSN")

                # ✅ Fix: Ensure DSN has https:// protocol
                if UNIPILE_DSN and not UNIPILE_DSN.startswith("http"):
                    UNIPILE_DSN = f"https://{UNIPILE_DSN}"
                
                headers = {
                    "X-API-KEY": UNIPILE_API_TOKEN,
                    "accept": "application/json"
                }
                
                async with httpx.AsyncClient() as http_client:
                    response = await http_client.get(
                        f"{UNIPILE_DSN}/api/v1/accounts/{account_id}",
                        headers=headers,
                        timeout=10
                    )
                    
                    if response.status_code != 200:
                        logger.error(f"❌ Failed to fetch account details: {response.text}")
                        return {"status": "error", "message": "Could not fetch account details"}
                    
                    account_details = response.json()
                    logger.info(f"📋 Account Details: {json.dumps(account_details, indent=2)}")
                    
                    # ✅ CRITICAL FIX: Extract client_id using pending connection lookup
                    client_id = extract_client_id_from_account(account_details, account_type)
                    
                    if not client_id:
                        logger.error("❌ Cannot extract client_id from account details")
                        logger.error(f"   Account ID: {account_id}")
                        logger.error(f"   Account Name: {account_details.get('name')}")
                        logger.error("💡 Make sure /connect-linkedin or /connect-instagram stores pending connection BEFORE OAuth")
                        
                        return {
                            "status": "error",
                            "message": "Cannot determine which client this account belongs to",
                            "hint": "Pending connection not found - ensure it's stored before OAuth redirect",
                            "account_id": account_id,
                            "account_name": account_details.get("name")
                        }
                    
                    # Get username from account details
                    account_username = "Unknown"
                    account_name = account_details.get("name", "Unknown")
                    
                    connection_params = account_details.get("connection_params", {})
                    if connection_params and "im" in connection_params:
                        im_data = connection_params["im"]
                        account_username = im_data.get("publicIdentifier", account_details.get("name", "Unknown"))
                    
                    logger.info(f"🔑 Client ID: {client_id}")
                    logger.info(f"👤 Account username: {account_username}")
                    
                    # Verify client exists in database
                    client = clients_collection.find_one({"client_id": client_id})
                    if not client:
                        logger.error(f"❌ Client {client_id} not found in database")
                        return {"status": "error", "message": "Client not found"}
                    
                    # Determine platform field name
                    platform_field = "instagram" if account_type == "INSTAGRAM" else "linkedin"
                    
                    # ✅ CRITICAL: Store account in MongoDB with all required fields
                    result = clients_collection.update_one(
                        {"client_id": client_id},
                        {
                            "$set": {
                                f"{platform_field}": {
                                    "account_id": account_id,
                                    "provider": account_type,
                                    "is_active": True,
                                    "connected_at": datetime.utcnow().isoformat(),
                                    "account_name": account_name,
                                    "username": account_username,
                                    "oauth_completed": True,
                                    "status": "active",
                                    "webhook_confirmed": True,
                                    "last_webhook_event": event,
                                    "last_status_check": datetime.utcnow().isoformat()
                                },
                                "updated_at": datetime.utcnow().isoformat()
                            }
                        }
                    )
                    
                    if result.modified_count > 0:
                        logger.info(f"✅✅✅ {platform_field.upper()} account saved: {client_id} → {account_id} (@{account_username})")
                    else:
                        logger.warning(f"⚠️ No changes made to client {client_id}")
                    
                    # ✅ Mark pending connection as completed
                    from db_config import db
                    pending_connections_collection = db["pending_connections"]
                    
                    pending_connections_collection.update_one(
                        {
                            "client_id": client_id,
                            "platform": platform_field,
                            "status": "pending"
                        },
                        {
                            "$set": {
                                "status": "completed",
                                "completed_at": datetime.utcnow().isoformat(),
                                "account_id": account_id,
                                "account_name": account_name,
                                "username": account_username
                            }
                        }
                    )
                    logger.info(f"✅ Marked pending connection as completed")
                    
                    return {
                        "status": "success",
                        "message": "Account created and linked successfully",
                        "client_id": client_id,
                        "account_id": account_id,
                        "account_name": account_username,
                        "platform": platform_field
                    }
                    
            except Exception as e:
                logger.error(f"❌ Error processing CREATION_SUCCESS: {str(e)}")
                import traceback
                traceback.print_exc()
                return {"status": "error", "message": str(e)}
        
        # ═══════════════════════════════════════════════════════════════════
        # FOR OTHER EVENTS: Verify account exists in our database
        # ═══════════════════════════════════════════════════════════════════
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
        
        if event == "DELETED":
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
    Handles all messaging events for Instagram & LinkedIn:
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
        
        # Get message text
        message_text = payload.get("message", "")
        if isinstance(message_text, dict):
            message_text = message_text.get("text", "")
        
        # Determine if this is a sent message (from connected account)
        is_sender = payload.get("is_sender", False)
        
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
        
        # Find client by account_id with verification
        client, platform = find_client_by_account_id(account_id)
        
        if not client:
            logger.warning(f"⚠️ Ignoring webhook from unknown account_id: {account_id}")
            return {"status": "ignored", "reason": "unknown_account"}
        
        client_id = client["client_id"]
        logger.info(f"   Client: {client_id} | Platform: {platform}")
        
        # ═══════════════════════════════════════════════════════════════════
        # HANDLE DIFFERENT MESSAGE EVENTS
        # ═══════════════════════════════════════════════════════════════════
        
        # 💬 REPLY RECEIVED FROM PROSPECT
        if event == "message_received" and not is_sender:
            logger.info(f"💬 REPLY RECEIVED from @{sender_username}")
            
            # Update the specific prospect in the prospects array
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
                        "prospects.$.status": "replied",
                        "prospects.$.chat_id": chat_id,  # Store chat_id
                        "prospects.$.message_id": message_id,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            if update_result.modified_count > 0:
                logger.info(f"✅ Updated reply status for @{sender_username}")
            else:
                logger.warning(f"⚠️ Could not find prospect @{sender_username} in database")
        
        # 👀 MESSAGE READ BY RECIPIENT
        elif event == "message_read":
            logger.info(f"👀 MESSAGE READ event received")
            
            # ✅ FIX: Find who read the message (exclude yourself)
            read_by_username = None
            
            # Loop through all attendees
            for attendee in attendees:
                if isinstance(attendee, dict):
                    attendee_id = attendee.get("attendee_provider_id")
                    attendee_specs = attendee.get("attendee_specifics", {})
                    attendee_username = attendee_specs.get("public_identifier", "")
                    
                    # Skip if this is YOUR connected account
                    if attendee_id != account_id and attendee_username:
                        read_by_username = attendee_username
                        logger.info(f"   Found reader: @{read_by_username}")
                        break
            
            if not read_by_username:
                logger.error(f"❌ Could not find who read the message")
                return {"status": "error"}
            
            logger.info(f"👀 MESSAGE READ by @{read_by_username}")
            
            # Update database
            update_result = audience_collection.update_one(
                {
                    "client_id": client_id,
                    "platform": platform,
                    "type": "prospects",
                    "prospects.username": read_by_username
                },
                {
                    "$set": {
                        "prospects.$.message_read": True,
                        "prospects.$.read_at": timestamp,
                        "prospects.$.status": "read",
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            if update_result.modified_count > 0:
                logger.info(f"✅✅✅ Updated read status for @{read_by_username}")
            else:
                logger.warning(f"⚠️ Could not find prospect @{read_by_username}")
        
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
                            "prospects.$.delivered_at": timestamp,
                            "prospects.$.status": "delivered",
                            "updated_at": datetime.utcnow()
                        }
                    }
                )
                
                if update_result.modified_count > 0:
                    logger.info(f"✅ Updated delivery status for @{recipient_username}")
        
        # 📤 SENT MESSAGE CONFIRMATION (is_sender=True)
        elif event == "message_received" and is_sender:
            logger.info(f"📤 SENT MESSAGE CONFIRMATION to @{recipient_username}")
            
            if recipient_username:
                # Confirm pending messages
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
                            "prospects.$.chat_id": chat_id,
                            "prospects.$.webhook_confirmed": True,
                            "updated_at": datetime.utcnow()
                        },
                        "$unset": {
                            "prospects.$.warning": ""
                        }
                    }
                )
                
                if update_result.modified_count > 0:
                    logger.info(f"✅✅ CONFIRMED pending message for @{recipient_username}")
                else:
                    # Try updating any sent message (fallback)
                    update_result = audience_collection.update_one(
                        {
                            "client_id": client_id,
                            "platform": platform,
                            "type": "prospects",
                            "prospects.username": recipient_username,
                            "prospects.status": {"$in": ["sent", "queued"]}
                        },
                        {
                            "$set": {
                                "prospects.$.status": "confirmed_sent",
                                "prospects.$.confirmed_at": timestamp,
                                "prospects.$.message_id": message_id,
                                "prospects.$.chat_id": chat_id,
                                "prospects.$.webhook_confirmed": True,
                                "updated_at": datetime.utcnow()
                            }
                        }
                    )
                    
                    if update_result.modified_count > 0:
                        logger.info(f"✅ Confirmed existing sent message for @{recipient_username}")
        
        # 💙 MESSAGE REACTION
        elif event == "message_reaction":
            reaction = payload.get("reaction", {})
            reaction_type = reaction.get("type", "unknown") if isinstance(reaction, dict) else "unknown"
            
            logger.info(f"💙 MESSAGE REACTION: {reaction_type} from @{sender_username}")
            
            if sender_username:
                update_result = audience_collection.update_one(
                    {
                        "client_id": client_id,
                        "platform": platform,
                        "type": "prospects",
                        "prospects.username": sender_username
                    },
                    {
                        "$set": {
                            "prospects.$.has_reaction": True,
                            "prospects.$.reaction_type": reaction_type,
                            "prospects.$.reaction_at": timestamp,
                            "updated_at": datetime.utcnow()
                        }
                    }
                )
                
                if update_result.modified_count > 0:
                    logger.info(f"✅ Updated reaction for @{sender_username}")
        
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
        
        # ✅ Verify account exists in our database
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