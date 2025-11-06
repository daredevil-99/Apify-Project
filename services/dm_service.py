# services/dm_service.py
from dm_sender import DMCampaignManager
from db_config import clients_collection

def send_dm_message(client_id: str):
    """
    Send DMs automatically for a client based on generated message.
    Uses the DMCampaignManager class from dm_sender.py.
    """
    dm_manager = DMCampaignManager(use_apify=True)
    result = dm_manager.send_bulk_dms(client_id)

    # Optional: update client status
    clients_collection.update_one(
        {"client_id": client_id},
        {"$set": {
            "dm_campaign_result": result,
            "last_dm_campaign_at": result.get("campaign_completed_at")
        }}
    )

    return result
