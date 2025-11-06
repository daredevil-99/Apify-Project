# services/gmail_service.py
from gmail_utils import send_email
from db_config import audience_collection, clients_collection
from datetime import datetime

def send_gmail_to_audience(client_id: str):
    """Send generated Gmail message to all prospects of a client."""
    client = clients_collection.find_one({"client_id": client_id})
    if not client or not client.get("generated_messages"):
        print(f"⚠️ No generated message found for {client_id}")
        return {"error": "No generated message found"}

    message_text = client["generated_messages"].get("final_output", "")
    if not message_text:
        print(f"⚠️ Empty message for {client_id}")
        return {"error": "Empty message text"}

    prospects = audience_collection.find({"client_id": client_id})
    sent_count = 0
    failed = []

    for prospect in prospects:
        email = prospect.get("email")
        if not email or "@" not in email:
            audience_collection.update_one(
                {"_id": prospect["_id"]},
                {"$set": {
                    "email_sent": False,
                    "email_error": "invalid_email",
                    "email_sent_at": datetime.utcnow()
                }}
            )
            failed.append(prospect.get("pageName", "unknown"))
            continue

        subject = f"Let's Connect, {prospect.get('pageName', 'there')}!"
        print(f"📧 Sending Gmail to {email}")

        success = send_email(email, subject, message_text)
        audience_collection.update_one(
            {"_id": prospect["_id"]},
            {"$set": {
                "email_sent": success,
                "email_sent_at": datetime.utcnow()
            }}
        )

        if success:
            sent_count += 1
        else:
            failed.append(email)

    print(f"✅ Gmail sending completed. Sent: {sent_count}, Failed: {len(failed)}")
    return {"sent": sent_count, "failed": failed}
