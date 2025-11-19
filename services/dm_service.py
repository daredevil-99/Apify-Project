from datetime import datetime
from pipeline_utils import auto_send_gmail
from instagram_dm_sender import send_instagram_dm
from services.db_service import get_client_data, update_client_status
from db_config import audience_collection, clients_collection
import re
import time


def send_dm_message(client_id: str):
    print(f"\n{'=' * 60}")
    print(f"📤 DM Sending started for client: {client_id}")
    print(f"{'=' * 60}\n")

    client_data = get_client_data(client_id)
    if not client_data:
        return {"error": f"Client {client_id} not found"}

    platform = client_data.get("platform", "").lower()
    print(f"📱 Platform: {platform}")

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

        try:
            send_success = False

            if platform == "gmail":
                result = auto_send_gmail(client_id)
                send_success = True

            elif platform == "instagram":
                ig_result = send_instagram_dm(username, message_content, client_id)
                send_success = ig_result and ig_result.get("status") == "success"

            elif platform == "linkedin":
                from linkedin_dm_sender import send_linkedin_dm
                recipient_url = prospect.get("profile_url") or prospect.get("linkedinUrl")
                li_result = send_linkedin_dm(recipient_url, message_content, client_id)
                send_success = li_result.get("status") == "success"

            audience_collection.update_one(
                {
                    "client_id": client_id,
                    "platform": platform,
                    "type": "prospects"
                },
                {"$set": {
                    f"prospects.{prospect_index}.message_sent": send_success,
                    f"prospects.{prospect_index}.last_sent_at": datetime.utcnow().isoformat(),
                    f"prospects.{prospect_index}.status": "sent" if send_success else "failed"
                }}
            )

            if send_success:
                results["sent"] += 1
            else:
                results["failed"] += 1

            results["details"].append({
                "username": username,
                "status": "sent" if send_success else "failed"
            })

            time.sleep(3)

        except Exception as e:
            error_msg = str(e)

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

    clients_collection.update_one(
        {"client_id": client_id},
        {
            "$set": {
                "last_dm_campaign": results,
                "last_dm_sent_at": datetime.utcnow(),
                "total_messages_sent": results["sent"],
            }
        },
    )

    return {
        "status": "success",
        "platform": platform,
        "results": results
    }
