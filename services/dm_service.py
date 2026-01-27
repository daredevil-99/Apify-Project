from datetime import datetime
from pipeline_utils import auto_send_gmail
from services.db_service import get_client_data, update_client_status
from db_config import audience_collection, clients_collection
import re
import time
import logging

logger = logging.getLogger(__name__)


def send_dm_message(client_id: str):
    """
    Send DM messages to prospects across different platforms.
    Supports: Instagram, LinkedIn, Gmail
    """
    print(f"\n{'=' * 60}")
    print(f"📤 DM Sending started for client: {client_id}")
    print(f"{'=' * 60}\n")

    client_data = get_client_data(client_id)
    if not client_data:
        return {"error": f"Client {client_id} not found"}

    platform = client_data.get("platform", "").lower()
    print(f"📱 Platform: {platform}")

    # Fetch prospects document
    prospects_doc = audience_collection.find_one({
        "client_id": client_id,
        "platform": platform,
        "type": "prospects"
    })

    if not prospects_doc:
        return {
            "status": "error",
            "error": "No prospects found. Run /scrape and /extract-prospects first.",
        }

    prospects_array = prospects_doc.get("prospects", [])
    
    # Filter prospects ready to send
    ready_prospects = [
        (idx, p) for idx, p in enumerate(prospects_array)
        if p.get("status") == "ready_to_send"
        and p.get("generated_message")
        and not p.get("message_sent")
    ]

    if not ready_prospects:
        return {
            "status": "no_prospects_ready",
            "message": "No prospects ready to send.",
        }

    print(f"📊 Found {len(ready_prospects)} prospects ready to send")

    results = {
        "total": len(ready_prospects),
        "sent": 0,
        "failed": 0,
        "details": []
    }

    for prospect_index, prospect in ready_prospects:
        username = (
            prospect.get("username")
            or prospect.get("ownerUsername")
            or prospect.get("pageName", "unknown")
        )
        message_content = prospect.get("generated_message")

        print(f"\n🎯 Sending to @{username}")
        logger.info(f"Sending DM to @{username} on {platform}")

        try:
            send_success = False
            message_id = None
            provider_messaging_id = None
            error_msg = None

            if platform == "gmail":
                result = auto_send_gmail(client_id)
                send_success = True
                logger.info(f"✅ Gmail sent to {username}")

            elif platform == "instagram":
                # ✅ CORRECT IMPORT - from insta_dm_sender.py
                from insta_dm_sender import send_instagram_dm_via_unipile
                
                # Get Instagram account_id from client
                instagram_data = client_data.get("instagram", {})
                account_id = instagram_data.get("account_id")
                
                if not account_id:
                    error_msg = "Instagram account not connected. Please connect Instagram first."
                    logger.error(f"❌ {error_msg}")
                    send_success = False
                else:
                    # Send Instagram DM
                    ig_result = send_instagram_dm_via_unipile(
                        username=username,
                        message=message_content,
                        account_id=account_id
                    )
                    
                    send_success = ig_result.get("status") == "success"
                    
                    if send_success:
                        message_id = ig_result.get("message_id")
                        provider_messaging_id = ig_result.get("provider_messaging_id")
                        logger.info(f"✅ Instagram DM sent to @{username}: message_id={message_id}")
                    else:
                        error_msg = ig_result.get("error", "Unknown error")
                        logger.error(f"❌ Instagram DM failed for @{username}: {error_msg}")

            elif platform == "linkedin":
                from linkedin_dm_sender import send_linkedin_dm
                
                # Get LinkedIn account_id from client
                linkedin_data = client_data.get("linkedin", {})
                account_id = linkedin_data.get("account_id")
                
                if not account_id:
                    error_msg = "LinkedIn account not connected. Please connect LinkedIn first."
                    logger.error(f"❌ {error_msg}")
                    send_success = False
                else:
                    recipient_url = prospect.get("profile_url") or prospect.get("linkedinUrl")
                    
                    if not recipient_url:
                        error_msg = "No LinkedIn profile URL found for prospect"
                        logger.error(f"❌ {error_msg}")
                        send_success = False
                    else:
                        li_result = send_linkedin_dm(
                            recipient_url=recipient_url,
                            message=message_content,
                            account_id=account_id
                        )
                        
                        send_success = li_result.get("status") == "success"
                        
                        if send_success:
                            message_id = li_result.get("message_id")
                            logger.info(f"✅ LinkedIn DM sent to {username}: message_id={message_id}")
                        else:
                            error_msg = li_result.get("error", "Unknown error")
                            logger.error(f"❌ LinkedIn DM failed for {username}: {error_msg}")

            # Update prospect in database
            update_fields = {
                f"prospects.{prospect_index}.message_sent": send_success,
                f"prospects.{prospect_index}.last_sent_at": datetime.utcnow().isoformat(),
                f"prospects.{prospect_index}.status": "sent" if send_success else "failed"
            }
            
            # Add message_id if available
            if message_id:
                update_fields[f"prospects.{prospect_index}.message_id"] = message_id
            
            # Add provider_messaging_id if available (Instagram)
            if provider_messaging_id:
                update_fields[f"prospects.{prospect_index}.provider_messaging_id"] = provider_messaging_id
            
            # Add error message if failed
            if not send_success and error_msg:
                update_fields[f"prospects.{prospect_index}.send_error"] = error_msg
            
            audience_collection.update_one(
                {
                    "client_id": client_id,
                    "platform": platform,
                    "type": "prospects"
                },
                {"$set": update_fields}
            )

            if send_success:
                results["sent"] += 1
                print(f"✅ Sent to @{username}")
            else:
                results["failed"] += 1
                print(f"❌ Failed to send to @{username}: {error_msg}")

            results["details"].append({
                "username": username,
                "status": "sent" if send_success else "failed",
                "error": error_msg if not send_success else None,
                "message_id": message_id if send_success else None
            })

            # Rate limiting - wait between messages
            time.sleep(3)

        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Exception sending DM to @{username}: {error_msg}", exc_info=True)

            audience_collection.update_one(
                {
                    "client_id": client_id,
                    "platform": platform,
                    "type": "prospects"
                },
                {"$set": {
                    f"prospects.{prospect_index}.status": "failed",
                    f"prospects.{prospect_index}.message_sent": False,
                    f"prospects.{prospect_index}.last_sent_at": datetime.utcnow().isoformat(),
                    f"prospects.{prospect_index}.send_error": error_msg
                }}
            )

            results["failed"] += 1
            results["details"].append({
                "username": username,
                "status": "failed",
                "error": error_msg
            })
            
            print(f"❌ Exception for @{username}: {error_msg}")

    # Update client document with campaign results
    clients_collection.update_one(
        {"client_id": client_id},
        {
            "$set": {
                "last_dm_campaign": results,
                "last_dm_sent_at": datetime.utcnow().isoformat(),
                "total_messages_sent": results["sent"],
            }
        },
    )

    print(f"\n{'=' * 60}")
    print(f"📊 DM Campaign Complete")
    print(f"✅ Sent: {results['sent']}")
    print(f"❌ Failed: {results['failed']}")
    print(f"📈 Total: {results['total']}")
    print(f"{'=' * 60}\n")

    return {
        "status": "success",
        "platform": platform,
        "results": results
    }