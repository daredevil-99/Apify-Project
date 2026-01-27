# services/message_tracking.py

import logging
from datetime import datetime
from db_config import audience_collection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


def handle_unipile_event(payload: dict):
    """
    Entry point for all Unipile webhook events
    """
    event = payload.get("event")
    data = payload.get("data", {})

    logger.info(f"📩 Unipile event received: {event} | data_keys={list(data.keys())}")

    try:
        if event == "message_received":
            handle_message_received(data)

        elif event == "message_sent":
            handle_message_sent(data)

        elif event == "message_read":
            handle_message_read(data)

        elif event == "message_delivered":
            handle_message_delivered(data)

        elif event == "message_deleted":
            handle_message_deleted(data)

        else:
            logger.warning(f"⚠️ Unhandled Unipile event: {event}")
    
    except Exception as e:
        logger.error(f"❌ Error processing event {event}: {str(e)}")
        raise


def handle_message_received(data: dict):
    """
    When a prospect replies to our message
    """
    conversation_id = data.get("conversation_id")
    message_text = data.get("text", "")
    platform = data.get("platform")
    in_reply_to = data.get("in_reply_to")

    logger.info(f"📬 Message received | conv_id={conversation_id} | in_reply_to={in_reply_to}")

    if not conversation_id and not in_reply_to:
        logger.error("❌ message_received without conversation_id or in_reply_to")
        return

    # Build query to match either conversation_id or the message_id we sent
    query = {"$or": []}
    
    if conversation_id:
        query["$or"].append({"conversation_id": conversation_id})
    
    if in_reply_to:
        query["$or"].append({"message_id": in_reply_to})

    result = audience_collection.update_one(
        query,
        {
            "$set": {
                "replied": True,
                "reply_text": message_text,
                "replied_at": datetime.utcnow(),
                "status": "replied",
                "last_event": "message_received",
                "last_webhook_event_at": datetime.utcnow(),
                "platform": platform
            }
        }
    )

    if result.matched_count > 0:
        logger.info(f"✅ Reply tracked | matched={result.matched_count} | modified={result.modified_count}")
    else:
        logger.warning(f"⚠️ No matching prospect found for reply | conv_id={conversation_id} | in_reply_to={in_reply_to}")


def handle_message_sent(data: dict):
    """
    Confirms our outbound message was sent successfully
    """
    message_id = data.get("message_id")

    if not message_id:
        logger.error("❌ message_sent without message_id")
        return

    result = audience_collection.update_one(
        {"message_id": message_id},
        {
            "$set": {
                "sent_confirmed": True,
                "sent_confirmed_at": datetime.utcnow(),
                "status": "sent",  # ✅ Update status to sent
                "last_event": "message_sent",
                "last_webhook_event_at": datetime.utcnow()
            }
        }
    )

    if result.matched_count > 0:
        logger.info(f"📤 Message sent confirmed | message_id={message_id}")
    else:
        logger.warning(f"⚠️ No prospect found for sent message | message_id={message_id}")


def handle_message_read(data: dict):
    """
    Prospect has read our message
    """
    message_id = data.get("message_id")

    if not message_id:
        logger.warning("⚠️ message_read without message_id")
        return

    result = audience_collection.update_one(
        {"message_id": message_id},
        {
            "$set": {
                "read": True,
                "read_at": datetime.utcnow(),
                "last_event": "message_read",
                "last_webhook_event_at": datetime.utcnow()
            }
        }
    )

    if result.matched_count > 0:
        logger.info(f"👀 Message read | message_id={message_id}")


def handle_message_delivered(data: dict):
    """
    Message successfully delivered to prospect
    """
    message_id = data.get("message_id")

    if not message_id:
        logger.warning("⚠️ message_delivered without message_id")
        return

    result = audience_collection.update_one(
        {"message_id": message_id},
        {
            "$set": {
                "delivered": True,
                "delivered_at": datetime.utcnow(),
                "status": "delivered",  # ✅ Update status
                "last_event": "message_delivered",
                "last_webhook_event_at": datetime.utcnow()
            }
        }
    )

    if result.matched_count > 0:
        logger.info(f"📦 Message delivered | message_id={message_id}")


def handle_message_deleted(data: dict):
    """
    Message was deleted (by us or prospect)
    """
    message_id = data.get("message_id")

    if not message_id:
        logger.warning("⚠️ message_deleted without message_id")
        return

    result = audience_collection.update_one(
        {"message_id": message_id},
        {
            "$set": {
                "deleted": True,
                "deleted_at": datetime.utcnow(),
                "status": "deleted",
                "last_event": "message_deleted",
                "last_webhook_event_at": datetime.utcnow()
            }
        }
    )

    if result.matched_count > 0:
        logger.warning(f"🗑️ Message deleted | message_id={message_id}")