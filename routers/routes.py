# ============================================================
# UPDATED ROUTES.PY - WITH AUTO DM SENDING AFTER MESSAGE GENERATION
# ============================================================

from fastapi import (
    APIRouter, BackgroundTasks, HTTPException, Body, 
    UploadFile, File, Query, Request
)
from pydantic import BaseModel, field_validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
import json
import re
import os
import logging
import time
# ------------------- INTERNAL SERVICES ----------------------
from services.db_service import (
    register_client, 
    get_client_data, 
    save_client_cookies,
    update_prospect_with_enrichment,
)
from services.scraping_service import scrape_and_store, extract_and_store_prospects
from services.dm_service import send_dm_message
from services.openai_message_service import (
    enrich_linkedin_profiles,
    generate_linkedin_message,
    generate_batch_messages,
    inspect_prospect_data,
    inspect_apify_response
)
from services.facebook_message_service import (
    get_prospect_summary,
    extract_profile_summary,
    generate_facebook_message,
    generate_batch_messages as generate_facebook_batch_messages,
    inspect_prospect_data as inspect_facebook_prospect_data
)

from linkedin_dm_sender import send_linkedin_dm  # ⭐ Import DM sender

from db_config import audience_collection, clients_collection
from fastapi.responses import RedirectResponse, HTMLResponse
from fb_dm_sender import (
    initiate_facebook_connection,
    verify_facebook_connection,
    send_facebook_message_to_prospect,
    extract_facebook_id_from_url
)

from insta_dm_sender import (
    send_instagram_dm_via_unipile,
    extract_username_from_url
)
import requests

# ------------------- CONFIGURATION ----------------------
UNIPILE_API_TOKEN = os.getenv("UNIPILE_API_TOKEN")
UNIPILE_DSN = os.getenv("UNIPILE_DSN", "https://api.unipile.com:13410")

# Ensure UNIPILE_DSN has https:// prefix
if UNIPILE_DSN and not UNIPILE_DSN.startswith(('http://', 'https://')):
    UNIPILE_DSN = f"https://{UNIPILE_DSN}"
UNIPILE_CONNECT_URL = f"{UNIPILE_DSN}/api/v1/hosted/accounts/link"
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://localhost:8000")

# ------------------- LOGGING ----------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# ROUTER
# ============================================================

router = APIRouter(prefix="/pipeline", tags=["Pipeline"])


# ============================================================
# MODELS
# ============================================================

class ClientRegistration(BaseModel):
    name: str
    role: str
    email: str
    platform: str
    search_terms_with_location: List[str]
    preferred_profession: str
    preferred_location: str


class CookieUpload(BaseModel):
    platform: str
    cookies: Union[List[Dict[str, Any]], Dict[str, Any], str]

    @field_validator("platform")
    def valid_platform(cls, v):
        allowed = ["instagram", "linkedin", "facebook"]
        v = v.lower().strip()
        if v not in allowed:
            raise ValueError("Platform must be: instagram, linkedin, or facebook")
        return v

    @field_validator("cookies")
    def parse_cookie_string(cls, v):
        if isinstance(v, list):
            if not v:
                raise ValueError("Cookie list cannot be empty")
            return v

        if isinstance(v, dict):
            return [v]

        if isinstance(v, str):
            v = v.strip()
            if not v:
                raise ValueError("Empty cookie string")
            try:
                clean = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', v)
                clean = re.sub(r',(\s*[}\]])', r'\1', clean)
                parsed = json.loads(clean)
                return [parsed] if isinstance(parsed, dict) else parsed
            except:
                raise ValueError("Invalid JSON cookie string")

        raise ValueError("Cookies must be list/dict/string")


class EnrichPostsRequest(BaseModel):
    """Request to scrape posts for specific profiles"""
    client_id: str
    profile_urls: List[str]
    
    @field_validator("profile_urls")
    def validate_urls(cls, v):
        if not v:
            raise ValueError("At least one profile URL required")
        for url in v:
            if not url.startswith("http"):
                raise ValueError(f"Invalid LinkedIn URL: {url}")
        return [url.strip() for url in v]


class GenerateMessagesRequest(BaseModel):
    """Request to ONLY generate messages (no sending)"""
    client_id: str
    profile_urls: List[str]
    campaign_message: str
    debug: bool = False
    
    @field_validator("profile_urls")
    def validate_urls(cls, v):
        if not v:
            raise ValueError("At least one profile URL required")
        return [url.strip() for url in v]


class SendMessagesRequest(BaseModel):
    """Request to send pre-generated messages"""
    client_id: str
    profile_urls: Optional[List[str]] = None  # If None, send to all ready prospects
    send_all_ready: bool = False  # Send to all prospects with generated messages
    send_delay_seconds: int = 5
    dry_run: bool = False  # Preview what would be sent without actually sending
    
    @field_validator("profile_urls")
    def validate_urls(cls, v):
        if v is not None:
            return [url.strip() for url in v]
        return v
    
    @field_validator("send_delay_seconds")
    def validate_delay(cls, v):
        if v < 0 or v > 60:
            raise ValueError("Delay must be between 0-60 seconds")
        return v

class FacebookScrapeRequest(BaseModel):
    client_id: str

class FacebookExtractProspectsRequest(BaseModel):
    """Request to extract Facebook prospects from scraped data"""
    client_id: str


class FacebookEnrichRequest(BaseModel):
    """Request to enrich Facebook profiles with posts"""
    client_id: str
    profile_urls: List[str]
    
    @field_validator("profile_urls")
    def validate_urls(cls, v):
        if not v:
            raise ValueError("At least one profile URL required")
        for url in v:
            if "facebook.com" not in url:
                raise ValueError(f"Invalid Facebook URL: {url}")
        return [url.strip() for url in v]


class FacebookGenerateMessagesRequest(BaseModel):
    """Request to generate Facebook messages"""
    client_id: str
    profile_urls: List[str]
    campaign_message: str
    debug: bool = False
    
    @field_validator("profile_urls")
    def validate_urls(cls, v):
        if not v:
            raise ValueError("At least one profile URL required")
        return [url.strip() for url in v]


# ============================================================
# EXISTING ENDPOINTS (UNCHANGED)
# ============================================================

@router.post("/register")
async def register_client_endpoint(client_data: ClientRegistration):
    data = client_data.dict()
    data["search_terms"] = data.pop("search_terms_with_location")
    data["profession"] = data.pop("preferred_profession")
    data["location"] = data.pop("preferred_location")
    client_id = register_client(data)
    return {
        "message": "Client registered successfully",
        "client_id": client_id,
        "next": "Upload cookies → Scrape → Extract → Enrich"
    }


@router.post("/cookies-file/{client_id}")
async def upload_cookie_file(client_id: str, platform: str, file: UploadFile = File(...)):
    if not file.filename.endswith(".json"):
        raise HTTPException(400, "Only .json files allowed")
    try:
        content = json.loads(await file.read())
        cookies_list = content if isinstance(content, list) else [content]
        save_client_cookies(client_id, platform.lower(), cookies_list, expires_in_days=30)
        return {
            "message": f"Cookies uploaded for {platform}",
            "client_id": client_id,
            "cookie_count": len(cookies_list)
        }
    except:
        raise HTTPException(400, "Invalid cookie JSON file")


@router.post("/scrape/{client_id}")
async def scrape_endpoint(client_id: str, background: BackgroundTasks):
    client = get_client_data(client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    background.add_task(
        scrape_and_store,
        client_id,
        client["platform"],
        client.get("search_terms"),
        client.get("profession"),
        client.get("location"),
    )
    return {"message": "Scraping started", "client_id": client_id}


@router.post("/extract-prospects/{client_id}")
async def extract_prospects_endpoint(client_id: str):
    client = get_client_data(client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    count = extract_and_store_prospects(client_id, client["platform"])
    return {
        "message": f"Extracted {count} prospects",
        "next": "Use GET /prospects/{client_id} to select profiles for enrichment"
    }


@router.get("/prospects/{client_id}")
async def get_prospects(client_id: str):
    """Get all prospects for a client"""
    client = get_client_data(client_id)
    if not client:
        raise HTTPException(404, "Client not found")

    doc = audience_collection.find_one({
        "client_id": client_id,
        "platform": client["platform"],
        "type": "prospects"
    })

    if not doc:
        return {"count": 0, "prospects": []}

    formatted = []
    for idx, p in enumerate(doc["prospects"]):
        if client["platform"] == "linkedin":
            formatted.append({
                "index": idx,
                "name": p.get("full_name") or p.get("fullName"),
                "headline": p.get("headline"),
                "location": p.get("location"),
                "profile_url": p.get("profile_url") or p.get("profileUrl"),
                "enriched": p.get("enriched", False),
                "has_posts": bool(p.get("posts")),
                "post_count": len(p.get("posts", [])),
                "has_message": bool(p.get("generated_message")),
                "message_sent": p.get("message_sent", False),  # ⭐ Added
            })
        else:
            formatted.append({
                "index": idx,
                "username": p.get("username"),
                "full_name": p.get("ownerFullName"),
                "profile_url": p.get("profile_url"),
                "bio": p.get("bio", "")[:100],
                "has_message": bool(p.get("generated_message")),
                "message_sent": p.get("message_sent", False),  # ⭐ Added
            })

    return {
        "count": len(formatted),
        "prospects": formatted,
        "next_step": "Use POST /enrich-posts to scrape LinkedIn posts"
    }


@router.post("/enrich-posts")
async def enrich_posts_endpoint(request: EnrichPostsRequest):
    """Scrape LinkedIn posts for selected profiles"""
    client = get_client_data(request.client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    
    if client["platform"] != "linkedin":
        raise HTTPException(400, "Only LinkedIn is supported")

    doc = audience_collection.find_one({
        "client_id": request.client_id,
        "platform": "linkedin",
        "type": "prospects"
    })
    if not doc:
        raise HTTPException(404, "No prospects found. Run /extract-prospects first")

    prospects = doc["prospects"]
    
    to_enrich = []
    indices_map = {}
    
    for url in request.profile_urls:
        norm_url = url.lower().rstrip("/")
        found = False
        
        for idx, p in enumerate(prospects):
            p_url = (p.get("profile_url") or p.get("profileUrl", "")).lower().rstrip("/")
            if p_url == norm_url:
                to_enrich.append({
                    "url": url,
                    "index": idx,
                    "already_enriched": p.get("enriched", False),
                    "name": p.get("full_name") or p.get("fullName", "Unknown")
                })
                indices_map[url] = idx
                found = True
                break
        
        if not found:
            print(f"⚠️ Profile URL not found in prospects: {url}")

    if not to_enrich:
        raise HTTPException(404, "No matching prospects found in database")

    print(f"\n{'='*70}")
    print(f"🔍 ENRICHING POSTS FOR {len(to_enrich)} PROFILES")
    print(f"{'='*70}")
    
    urls_to_scrape = [item["url"] for item in to_enrich]
    enriched_data = enrich_linkedin_profiles(urls_to_scrape)
    
    if not enriched_data:
        raise HTTPException(500, "Post scraping failed. Check Apify logs.")

    results = []
    
    for item in to_enrich:
        url = item["url"]
        idx = item["index"]
        name = item["name"]
        
        norm_url = url.lower().rstrip("/")
        
        posts = None
        for enriched_url, post_list in enriched_data.items():
            if enriched_url.lower().rstrip("/") == norm_url:
                posts = post_list
                break
        
        if not posts or len(posts) == 0:
            print(f"⚠️ No posts found for {name} ({url})")
            results.append({
                "profile_url": url,
                "name": name,
                "posts_scraped": 0,
                "enrichment_status": "no_posts_found",
                "error": "Profile may be private or have no posts"
            })
            continue
        
        print(f"\n📊 Profile {len(results)+1}: {name}")
        print(f"   Posts scraped: {len(posts)}")
        
        existing_prospect = prospects[idx]

        enriched_profile = {
            **existing_prospect,
            "posts": posts,
            "enriched": True,
            "enriched_at": datetime.utcnow().isoformat()
        }
        
        if posts and len(posts) > 0:
            first_post = posts[0]
            enriched_profile["headline"] = first_post.get("authorHeadline", "")
            enriched_profile["location"] = first_post.get("authorLocation", "")
        
        update_prospect_with_enrichment(
            request.client_id, 
            "linkedin", 
            idx, 
            enriched_profile
        )
        
        results.append({
            "profile_url": url,
            "name": name,
            "posts_scraped": len(posts),
            "enrichment_status": "success"
        })

    print(f"\n✅ Successfully enriched {len(results)} profiles")
    print(f"{'='*70}\n")

    return {
        "status": "success",
        "enriched_count": len([r for r in results if r["enrichment_status"] == "success"]),
        "results": results
    }

@router.get("/connect-linkedin/{client_id}")
async def initiate_linkedin_connection(client_id: str, redirect: bool = Query(True)):
    """
    Step 1: Generate Unipile Hosted Auth Link
    
    By default (redirect=true): Returns 302 redirect to Unipile OAuth page
    With redirect=false: Returns JSON with the auth URL
    
    Examples:
    - GET /connect-linkedin/{client_id}  → Auto-redirects to Unipile
    - GET /connect-linkedin/{client_id}?redirect=false  → Returns JSON with URL
    """
    try:
        # Verify client exists
        client = get_client_data(client_id)
        if not client:
            raise HTTPException(404, "Client not found")
        
        import requests
        from datetime import datetime, timedelta
        
        # Generate hosted auth link via Unipile API
        headers = {
            "X-API-KEY": UNIPILE_API_TOKEN,
            "accept": "application/json",
            "content-type": "application/json"
        }
        
        # Link expires in 1 hour - MUST use .000Z format (3 decimal places)
        expires_on = (datetime.utcnow() + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
        # Webhook URL where Unipile will notify us after successful connection
        notify_url = f"{APP_BASE_URL}/pipeline/linkedin-webhook"
        
        # ✅ CORRECT payload with all required fields
        payload = {
            "type": "create",  # REQUIRED: "create" or "reconnect"
            "api_url": UNIPILE_DSN,  # REQUIRED: Your Unipile server URL
            "providers": ["LINKEDIN"],  # REQUIRED
            "expiresOn": expires_on,  # REQUIRED: ISO 8601 with .000Z format
            "notify_url": notify_url,
            "name": client_id,
            "success_redirect_url": f"{APP_BASE_URL}/pipeline/connection-success?client_id={client_id}",
            "failure_redirect_url": f"{APP_BASE_URL}/pipeline/connection-failed?client_id={client_id}"
        }
        
        logger.info(f"Generating hosted auth link for client {client_id}")
        logger.info(f"Payload: {payload}")
        
        response = requests.post(
            f"{UNIPILE_DSN}/api/v1/hosted/accounts/link",
            json=payload,
            headers=headers,
            timeout=15
        )
        
        # Log full response for debugging
        logger.info(f"Unipile response status: {response.status_code}")
        logger.info(f"Unipile response body: {response.text}")
        
        if response.status_code not in [200, 201]:
            try:
                error_data = response.json()
                error_msg = error_data.get("message", error_data.get("error", "Failed to generate auth link"))
                logger.error(f"Unipile error details: {error_data}")
            except:
                error_msg = response.text or "Failed to generate auth link"
            
            logger.error(f"Unipile error: {response.status_code} - {error_msg}")
            raise HTTPException(500, f"Unipile API error: {error_msg}")
        
        result = response.json()
        hosted_url = result.get("url")
        
        if not hosted_url:
            logger.error(f"No URL in response: {result}")
            raise HTTPException(500, "No URL returned from Unipile")
        
        logger.info(f"✅ Generated hosted auth URL: {hosted_url}")
        
        # Return JSON or redirect based on query parameter
        if redirect:
            # Browser-friendly: Auto-redirect to Unipile
            return RedirectResponse(url=hosted_url, status_code=302)
        else:
            # API-friendly: Return JSON with URL
            return {
                "status": "success",
                "client_id": client_id,
                "oauth_url": hosted_url,
                "expires_at": expires_on,
                "instructions": [
                    "1. Open the 'oauth_url' in a browser",
                    "2. Click 'Connect with LinkedIn'",
                    "3. Log in with LinkedIn credentials",
                    "4. After success, webhook will be called (needs ngrok for localhost)",
                    "5. Check connection status with GET /pipeline/linkedin-status/{client_id}"
                ],
                "webhook_url": notify_url,
                "webhook_warning": "⚠️ Webhook won't work with localhost. Use ngrok for testing." if "localhost" in notify_url else None
            }
        
    except HTTPException:
        raise
    except requests.RequestException as e:
        logger.error(f"Network error: {str(e)}")
        raise HTTPException(500, f"Failed to connect to Unipile: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(500, f"Failed to initiate LinkedIn connection: {str(e)}")
    

@router.post("/linkedin-webhook")
async def linkedin_webhook(request: Request):
    """
    Webhook that receives account_id from Unipile after OAuth.
    Automatically saves to database.
    """
    try:
        body = await request.body()
        
        if not body:
            return HTMLResponse(
                content="<h1>Webhook Endpoint</h1><p>This endpoint receives POST requests from Unipile OAuth.</p>",
                status_code=200
            )
        
        payload = json.loads(body)
        logger.info(f"Webhook received: {payload}")
        
        status = payload.get("status")
        account_id = payload.get("account_id")
        client_id = payload.get("name")
        
        if not account_id or not client_id:
            return {"status": "error", "message": "Missing required fields"}
        
        if status not in ["CREATION_SUCCESS", "RECONNECTED"]:
            return {"status": "error", "message": f"Unexpected status: {status}"}
        
        # Verify account
        import requests
        headers = {"X-API-KEY": UNIPILE_API_TOKEN, "accept": "application/json"}
        response = requests.get(f"{UNIPILE_DSN}/api/v1/accounts/{account_id}", headers=headers, timeout=10)
        
        if response.status_code != 200:
            return {"status": "error", "message": "Account verification failed"}
        
        account_data = response.json()
        
        # Save to database
        clients_collection.update_one(
            {"client_id": client_id},
            {
                "$set": {
                    "linkedin": {
                        "account_id": account_id,
                        "provider": "LINKEDIN",
                        "is_active": True,
                        "connected_at": datetime.utcnow().isoformat(),
                        "account_name": account_data.get("name"),
                        "oauth_completed": True
                    }
                }
            }
        )
        
        logger.info(f"✅ LinkedIn connected: {client_id} → {account_id}")
        
        return {
            "status": "success",
            "client_id": client_id,
            "account_id": account_id
        }
        
    except Exception as e:
        logger.error(f"Webhook error: {str(e)}")
        return {"status": "error", "message": str(e)}

# Add these endpoints to your routes.py after the webhook endpoint

@router.get("/connection-success", response_class=HTMLResponse)
async def connection_success_page(client_id: str = Query(...)):
    """
    Success page after LinkedIn OAuth completion.
    Displays account details fetched from database.
    """
    try:
        # Fetch account details from database
        client = get_client_data(client_id)
        
        if not client:
            return HTMLResponse(
                content=f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Client Not Found</title>
                    <meta charset="utf-8">
                    <style>
                        body {{
                            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                            display: flex;
                            justify-content: center;
                            align-items: center;
                            height: 100vh;
                            margin: 0;
                            background: #f3f4f6;
                        }}
                        .container {{
                            background: white;
                            padding: 40px;
                            border-radius: 12px;
                            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                            text-align: center;
                            max-width: 500px;
                        }}
                        .error {{ font-size: 48px; margin-bottom: 20px; }}
                        h1 {{ color: #1f2937; margin: 0 0 10px 0; }}
                        p {{ color: #6b7280; line-height: 1.6; }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <div class="error">⚠️</div>
                        <h1>Client Not Found</h1>
                        <p>Client ID: <code>{client_id}</code></p>
                        <p>The client may not be registered in the system.</p>
                    </div>
                </body>
                </html>
                """,
                status_code=404
            )
        
        linkedin_data = client.get("linkedin", {})
        account_id = linkedin_data.get("account_id", "Not yet connected")
        account_name = linkedin_data.get("account_name", "Loading...")
        connected_at = linkedin_data.get("connected_at", "Unknown")
        is_connected = linkedin_data.get("oauth_completed", False)
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>LinkedIn Connected Successfully</title>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }}
                
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    min-height: 100vh;
                    margin: 0;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    padding: 20px;
                }}
                
                .container {{
                    background: white;
                    padding: 50px;
                    border-radius: 16px;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                    text-align: center;
                    max-width: 650px;
                    width: 100%;
                    animation: slideIn 0.5s ease-out;
                }}
                
                @keyframes slideIn {{
                    from {{
                        opacity: 0;
                        transform: translateY(-20px);
                    }}
                    to {{
                        opacity: 1;
                        transform: translateY(0);
                    }}
                }}
                
                .success-icon {{
                    font-size: 80px;
                    margin-bottom: 20px;
                    animation: bounce 0.6s ease-in-out;
                }}
                
                @keyframes bounce {{
                    0%, 100% {{ transform: scale(1); }}
                    50% {{ transform: scale(1.1); }}
                }}
                
                h1 {{
                    color: #1f2937;
                    font-size: 32px;
                    margin-bottom: 10px;
                    font-weight: 700;
                }}
                
                .subtitle {{
                    color: #6b7280;
                    font-size: 16px;
                    margin-bottom: 30px;
                }}
                
                .info-box {{
                    background: #f9fafb;
                    border: 2px solid #e5e7eb;
                    padding: 20px;
                    border-radius: 10px;
                    margin: 25px 0;
                    text-align: left;
                }}
                
                .info-row {{
                    display: flex;
                    justify-content: space-between;
                    padding: 10px 0;
                    border-bottom: 1px solid #e5e7eb;
                }}
                
                .info-row:last-child {{
                    border-bottom: none;
                }}
                
                .info-label {{
                    color: #6b7280;
                    font-weight: 600;
                    font-size: 14px;
                }}
                
                .info-value {{
                    color: #1f2937;
                    font-family: 'Courier New', monospace;
                    font-size: 14px;
                    word-break: break-all;
                }}
                
                .status-badge {{
                    display: inline-block;
                    background: #10b981;
                    color: white;
                    padding: 4px 12px;
                    border-radius: 20px;
                    font-size: 12px;
                    font-weight: 600;
                }}
                
                .next-steps {{
                    background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%);
                    border-left: 4px solid #3b82f6;
                    padding: 25px;
                    margin-top: 30px;
                    text-align: left;
                    border-radius: 8px;
                }}
                
                .next-steps h3 {{
                    margin: 0 0 15px 0;
                    color: #1e40af;
                    font-size: 18px;
                }}
                
                .next-steps ol {{
                    margin: 15px 0;
                    padding-left: 25px;
                    color: #374151;
                    line-height: 1.8;
                }}
                
                .next-steps li {{
                    margin-bottom: 8px;
                }}
                
                .api-endpoint {{
                    background: #1f2937;
                    color: #10b981;
                    padding: 12px;
                    border-radius: 6px;
                    font-family: 'Courier New', monospace;
                    font-size: 13px;
                    margin: 15px 0;
                    overflow-x: auto;
                }}
                
                .endpoint-label {{
                    color: #9ca3af;
                    font-size: 12px;
                    margin-bottom: 5px;
                    font-weight: 600;
                }}
                
                .close-note {{
                    margin-top: 30px;
                    font-size: 14px;
                    color: #9ca3af;
                }}
                
                .status-check-btn {{
                    display: inline-block;
                    background: #3b82f6;
                    color: white;
                    padding: 12px 24px;
                    border-radius: 8px;
                    text-decoration: none;
                    margin-top: 20px;
                    font-weight: 600;
                    transition: background 0.3s;
                }}
                
                .status-check-btn:hover {{
                    background: #2563eb;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="success-icon">✅</div>
                <h1>LinkedIn Connected Successfully!</h1>
                <p class="subtitle">Your LinkedIn account has been linked and is ready to send messages.</p>
                
                <div class="info-box">
                    <div class="info-row">
                        <span class="info-label">Status</span>
                        <span class="status-badge">{"✓ Connected" if is_connected else "⏳ Pending"}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Client ID</span>
                        <span class="info-value">{client_id}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Account ID</span>
                        <span class="info-value">{account_id}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Account Name</span>
                        <span class="info-value">{account_name}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Connected At</span>
                        <span class="info-value">{connected_at[:19] if connected_at != "Unknown" else connected_at}</span>
                    </div>
                </div>
                
                <div class="next-steps">
                    <h3>🚀 Next Steps</h3>
                    <ol>
                        <li><strong>Scrape prospects:</strong> Find potential leads on LinkedIn</li>
                        <li><strong>Extract profiles:</strong> Get detailed profile information</li>
                        <li><strong>Enrich with posts:</strong> Analyze their recent activity</li>
                        <li><strong>Generate messages:</strong> AI creates personalized DMs</li>
                        <li><strong>Send automatically:</strong> Deliver messages via LinkedIn</li>
                    </ol>
                    
                    <div class="endpoint-label">📍 Check Connection Status:</div>
                    <div class="api-endpoint">
                        GET /pipeline/linkedin-status/{client_id}
                    </div>
                    
                    <div class="endpoint-label">📍 Start Campaign:</div>
                    <div class="api-endpoint">
                        POST /pipeline/generate-and-send
                    </div>
                </div>
                
                <a href="/pipeline/linkedin-status/{client_id}" class="status-check-btn" target="_blank">
                    Check Connection Status
                </a>
                
                <p class="close-note">
                    You can close this window and return to your application.
                </p>
            </div>
        </body>
        </html>
        """
        
        return HTMLResponse(content=html_content)
        
    except Exception as e:
        logger.error(f"Error rendering success page: {str(e)}")
        return HTMLResponse(
            content=f"""
            <!DOCTYPE html>
            <html>
            <head><title>Error</title></head>
            <body>
                <h1>Error Loading Success Page</h1>
                <p>Client ID: {client_id}</p>
                <p>Error: {str(e)}</p>
            </body>
            </html>
            """,
            status_code=500
        )


@router.get("/connection-failed", response_class=HTMLResponse)
async def connection_failed_page(
    client_id: str = Query(...),
    error: str = Query(None)
):
    """
    Failure page if OAuth fails.
    """
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>LinkedIn Connection Failed</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                display: flex;
                justify-content: center;
                align-items: center;
                min-height: 100vh;
                margin: 0;
                background: linear-gradient(135deg, #f56565 0%, #c53030 100%);
                padding: 20px;
            }}
            
            .container {{
                background: white;
                padding: 50px;
                border-radius: 16px;
                box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                text-align: center;
                max-width: 550px;
                width: 100%;
            }}
            
            .error-icon {{
                font-size: 80px;
                color: #e53e3e;
                margin-bottom: 20px;
            }}
            
            h1 {{
                color: #1f2937;
                font-size: 28px;
                margin-bottom: 10px;
                font-weight: 700;
            }}
            
            .subtitle {{
                color: #6b7280;
                font-size: 16px;
                margin-bottom: 25px;
                line-height: 1.6;
            }}
            
            .error-details {{
                background: #fef2f2;
                border-left: 4px solid #ef4444;
                padding: 15px;
                border-radius: 6px;
                margin: 20px 0;
                text-align: left;
            }}
            
            .error-label {{
                color: #991b1b;
                font-weight: 600;
                font-size: 13px;
                margin-bottom: 5px;
            }}
            
            .error-message {{
                color: #b91c1c;
                font-family: 'Courier New', monospace;
                font-size: 14px;
            }}
            
            .client-id-box {{
                background: #f3f4f6;
                padding: 15px;
                border-radius: 8px;
                margin: 20px 0;
            }}
            
            .client-id-label {{
                color: #6b7280;
                font-size: 13px;
                font-weight: 600;
                margin-bottom: 5px;
            }}
            
            .client-id-value {{
                color: #1f2937;
                font-family: 'Courier New', monospace;
                font-size: 14px;
                word-break: break-all;
            }}
            
            .retry-button {{
                display: inline-block;
                background: #0077b5;
                color: white;
                padding: 14px 28px;
                border-radius: 8px;
                text-decoration: none;
                font-weight: 600;
                margin-top: 25px;
                transition: background 0.3s;
            }}
            
            .retry-button:hover {{
                background: #006399;
            }}
            
            .help-text {{
                margin-top: 30px;
                font-size: 14px;
                color: #9ca3af;
                line-height: 1.6;
            }}
            
            .troubleshooting {{
                background: #f9fafb;
                border: 1px solid #e5e7eb;
                border-radius: 8px;
                padding: 20px;
                margin-top: 25px;
                text-align: left;
            }}
            
            .troubleshooting h3 {{
                color: #374151;
                font-size: 16px;
                margin-bottom: 12px;
            }}
            
            .troubleshooting ul {{
                color: #6b7280;
                font-size: 14px;
                line-height: 1.8;
                padding-left: 25px;
            }}
            
            .troubleshooting li {{
                margin-bottom: 8px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="error-icon">❌</div>
            <h1>LinkedIn Connection Failed</h1>
            <p class="subtitle">We couldn't connect your LinkedIn account. Please try again.</p>
            
            {f'''
            <div class="error-details">
                <div class="error-label">Error Details:</div>
                <div class="error-message">{error}</div>
            </div>
            ''' if error else ''}
            
            <div class="client-id-box">
                <div class="client-id-label">Client ID:</div>
                <div class="client-id-value">{client_id}</div>
            </div>
            
            <a href="/pipeline/connect-linkedin/{client_id}" class="retry-button">
                🔄 Try Again
            </a>
            
            <div class="troubleshooting">
                <h3>💡 Troubleshooting Tips:</h3>
                <ul>
                    <li>Make sure you're using a valid LinkedIn account</li>
                    <li>Check that pop-ups are not blocked in your browser</li>
                    <li>Try using a different browser if the issue persists</li>
                    <li>Ensure your LinkedIn account is not restricted</li>
                </ul>
            </div>
            
            <p class="help-text">
                If the issue persists, please contact support with your Client ID.
            </p>
        </div>
    </body>
    </html>
    """
    
    return HTMLResponse(content=html_content)


@router.get("/linkedin-status/{client_id}")
async def check_linkedin_status(client_id: str):
    """
    Check if LinkedIn account is connected and active.
    Also verifies the account with Unipile API.
    """
    try:
        client = get_client_data(client_id)
        if not client:
            raise HTTPException(404, "Client not found")
        
        linkedin_data = client.get("linkedin", {})
        
        # Not connected at all
        if not linkedin_data or not linkedin_data.get("is_active"):
            return {
                "connected": False,
                "status": "not_connected",
                "message": "LinkedIn not connected",
                "client_id": client_id,
                "connect_url": f"{APP_BASE_URL}/pipeline/connect-linkedin/{client_id}",
                "instructions": [
                    "1. Visit the connect_url to start OAuth flow",
                    "2. Log in with your LinkedIn account",
                    "3. Authorize the connection",
                    "4. Check this endpoint again to verify"
                ]
            }
        
        account_id = linkedin_data.get("account_id")
        
        # Verify with Unipile API
        import requests
        headers = {
            "X-API-KEY": UNIPILE_API_TOKEN,
            "accept": "application/json"
        }
        
        try:
            response = requests.get(
                f"{UNIPILE_DSN}/api/v1/accounts/{account_id}",
                headers=headers,
                timeout=10
            )
            
            account_valid = response.status_code == 200
            
            if account_valid:
                account_info = response.json()
                provider_status = account_info.get("status", "unknown")
            else:
                provider_status = "verification_failed"
                
        except requests.RequestException as e:
            logger.error(f"Unipile verification failed: {str(e)}")
            account_valid = False
            provider_status = "network_error"
        
        return {
            "connected": True,
            "status": "active" if account_valid else "inactive",
            "client_id": client_id,
            "account_id": account_id,
            "account_name": linkedin_data.get("account_name"),
            "provider": linkedin_data.get("provider"),
            "connected_at": linkedin_data.get("connected_at"),
            "oauth_completed": linkedin_data.get("oauth_completed", False),
            "account_valid": account_valid,
            "provider_status": provider_status,
            "message": "✅ LinkedIn connected and active" if account_valid else "⚠️ LinkedIn connected but account may be inactive",
            "ready_to_send": account_valid,
            "next_steps": [
                "1. POST /pipeline/scrape/{client_id} - Scrape prospects",
                "2. POST /pipeline/extract-prospects/{client_id} - Extract profiles",
                "3. POST /pipeline/enrich-posts - Enrich with posts",
                "4. POST /pipeline/generate-and-send - Generate & send messages"
            ] if account_valid else [
                "Connection may be inactive. Try reconnecting:",
                f"GET /pipeline/connect-linkedin/{client_id}"
            ]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Status check error: {str(e)}")
        raise HTTPException(500, f"Failed to check status: {str(e)}")
    

@router.post("/generate-messages")
async def generate_messages_only(request: GenerateMessagesRequest):
    """
    Generate AI-personalized messages for selected prospects.
    Messages are saved to database but NOT sent.
    
    Example:
    {
        "client_id": "abc123",
        "profile_urls": [
            "https://www.linkedin.com/in/person1/",
            "https://www.linkedin.com/in/person2/"
        ],
        "campaign_message": "I help tech leaders optimize cloud costs",
        "debug": false
    }
    """
    client = get_client_data(request.client_id)
    if not client:
        raise HTTPException(404, "Client not found")

    doc = audience_collection.find_one({
        "client_id": request.client_id,
        "platform": "linkedin",
        "type": "prospects"
    })
    if not doc:
        raise HTTPException(404, "No prospects found. Run /extract-prospects first")

    prospects = doc["prospects"]
    
    # Find enriched prospects
    selected_prospects = []
    indices = []
    
    for url in request.profile_urls:
        norm_url = url.lower().rstrip("/")
        found = False
        
        for idx, p in enumerate(prospects):
            p_url = (p.get("profile_url") or p.get("profileUrl", "")).lower().rstrip("/")
            if p_url == norm_url:
                if not p.get("enriched"):
                    raise HTTPException(
                        400, 
                        f"Profile not enriched: {url}. Run POST /enrich-posts first."
                    )
                
                selected_prospects.append(p)
                indices.append(idx)
                found = True
                break
        
        if not found:
            raise HTTPException(404, f"Profile not found: {url}")

    if not selected_prospects:
        raise HTTPException(404, "No enriched prospects found")

    print(f"\n{'='*70}")
    print(f"🤖 GENERATING MESSAGES FOR {len(selected_prospects)} PROFILES")
    print(f"{'='*70}")

    results = []
    for i, prospect in enumerate(selected_prospects):
        idx = indices[i]
        profile_url = request.profile_urls[i]
        name = prospect.get("full_name") or prospect.get("fullName", "Unknown")
        
        print(f"\n📝 Profile {i+1}/{len(selected_prospects)}: {name}")
        
        if request.debug:
            inspect_prospect_data(prospect, show_full_posts=True)
        
        # Generate message using OpenAI
        gen_result = generate_linkedin_message(
            prospect_data=prospect,
            campaign_message=request.campaign_message,
            model="gpt-4o-mini",
            debug=request.debug
        )
        
        if gen_result["status"] != "success":
            print(f"❌ Generation failed: {gen_result.get('error')}")
            results.append({
                "profile_url": profile_url,
                "name": name,
                "status": "failed",
                "error": gen_result.get("error")
            })
            continue
        
        generated_message = gen_result["message"]
        print(f"✅ Generated ({gen_result['tokens_used']} tokens)")
        
        # Save to database
        audience_collection.update_one(
            {
                "client_id": request.client_id,
                "platform": "linkedin",
                "type": "prospects"
            },
            {
                "$set": {
                    f"prospects.{idx}.generated_message": generated_message,
                    f"prospects.{idx}.message_generated_at": datetime.utcnow().isoformat(),
                    f"prospects.{idx}.ai_model_used": gen_result["model_used"],
                    f"prospects.{idx}.tokens_used": gen_result["tokens_used"],
                    f"prospects.{idx}.status": "ready_to_send"
                }
            }
        )
        
        results.append({
            "profile_url": profile_url,
            "name": name,
            "status": "success",
            "message": generated_message,
            "tokens_used": gen_result["tokens_used"]
        })

    success_count = sum(1 for r in results if r["status"] == "success")
    
    print(f"\n{'='*70}")
    print(f"✅ GENERATION COMPLETE: {success_count}/{len(results)} successful")
    print(f"{'='*70}\n")

    return {
        "status": "success",
        "total": len(results),
        "generated": success_count,
        "failed": len(results) - success_count,
        "results": results,
        "next_step": "Use POST /send-messages to send these messages"
    }


@router.post("/send-messages")
async def send_messages(request: SendMessagesRequest):
    """
    Send pre-generated messages to selected prospects.
    
    Options:
    1. Send to specific profiles: provide profile_urls
    2. Send to all ready prospects: set send_all_ready=true
    3. Dry run: set dry_run=true to preview without sending
    
    Examples:
    
    # Send to specific profiles
    {
        "client_id": "abc123",
        "profile_urls": ["https://www.linkedin.com/in/person1/"],
        "send_delay_seconds": 5
    }
    
    # Send to all ready prospects
    {
        "client_id": "abc123",
        "send_all_ready": true,
        "send_delay_seconds": 5
    }
    
    # Dry run (preview only)
    {
        "client_id": "abc123",
        "send_all_ready": true,
        "dry_run": true
    }
    """
    client = get_client_data(request.client_id)
    if not client:
        raise HTTPException(404, "Client not found")

    # Check LinkedIn connection
    linkedin_data = client.get("linkedin", {})
    if not linkedin_data or not linkedin_data.get("is_active"):
        raise HTTPException(
            400, 
            "LinkedIn not connected. Visit /connect-linkedin/{client_id} first"
        )

    doc = audience_collection.find_one({
        "client_id": request.client_id,
        "platform": "linkedin",
        "type": "prospects"
    })
    if not doc:
        raise HTTPException(404, "No prospects found")

    prospects = doc["prospects"]
    
    # Determine which prospects to send to
    to_send = []
    
    if request.send_all_ready:
        # Send to all prospects with generated messages (not yet sent)
        for idx, p in enumerate(prospects):
            if p.get("generated_message") and not p.get("message_sent"):
                to_send.append({
                    "index": idx,
                    "prospect": p,
                    "profile_url": p.get("profile_url") or p.get("profileUrl"),
                    "name": p.get("full_name") or p.get("fullName", "Unknown"),
                    "message": p["generated_message"]
                })
    
    elif request.profile_urls:
        # Send to specific profiles
        for url in request.profile_urls:
            norm_url = url.lower().rstrip("/")
            found = False
            
            for idx, p in enumerate(prospects):
                p_url = (p.get("profile_url") or p.get("profileUrl", "")).lower().rstrip("/")
                if p_url == norm_url:
                    if not p.get("generated_message"):
                        raise HTTPException(
                            400,
                            f"No message generated for {url}. Run /generate-messages first"
                        )
                    
                    if p.get("message_sent"):
                        print(f"⚠️ Message already sent to {url}, skipping")
                        continue
                    
                    to_send.append({
                        "index": idx,
                        "prospect": p,
                        "profile_url": url,
                        "name": p.get("full_name") or p.get("fullName", "Unknown"),
                        "message": p["generated_message"]
                    })
                    found = True
                    break
            
            if not found:
                raise HTTPException(404, f"Profile not found: {url}")
    
    else:
        raise HTTPException(
            400,
            "Must provide either 'profile_urls' or set 'send_all_ready=true'"
        )

    if not to_send:
        return {
            "status": "no_action",
            "message": "No prospects to send to (all may have been sent already)",
            "ready_count": 0
        }

    # Dry run - just preview
    if request.dry_run:
        return {
            "status": "dry_run",
            "message": "Preview only - no messages sent",
            "total_to_send": len(to_send),
            "preview": [
                {
                    "name": item["name"],
                    "profile_url": item["profile_url"],
                    "message_preview": item["message"][:150] + "..."
                }
                for item in to_send
            ],
            "estimated_time": f"{len(to_send) * request.send_delay_seconds} seconds",
            "next_step": "Remove 'dry_run' to actually send messages"
        }

    # Actually send messages
    print(f"\n{'='*70}")
    print(f"📤 SENDING {len(to_send)} MESSAGES")
    print(f"{'='*70}")

    results = []
    for i, item in enumerate(to_send):
        idx = item["index"]
        profile_url = item["profile_url"]
        name = item["name"]
        message = item["message"]
        
        print(f"\n📨 Sending {i+1}/{len(to_send)}: {name}")
        
        try:
            send_result = send_linkedin_dm(
                recipient_url=profile_url,
                message=message,
                client_id=request.client_id
            )
            
            if send_result.get("status") == "success":
                method = send_result.get("method", "unknown")
                
                # Update database
                audience_collection.update_one(
                    {
                        "client_id": request.client_id,
                        "platform": "linkedin",
                        "type": "prospects"
                    },
                    {
                        "$set": {
                            f"prospects.{idx}.message_sent": True,
                            f"prospects.{idx}.sent_at": datetime.utcnow().isoformat(),
                            f"prospects.{idx}.sent_method": method,
                            f"prospects.{idx}.status": "sent"
                        }
                    }
                )
                
                print(f"✅ Sent via {method}")
                
                results.append({
                    "profile_url": profile_url,
                    "name": name,
                    "status": "success",
                    "method": method
                })
            else:
                error = send_result.get("error", "Unknown error")
                print(f"❌ Failed: {error}")
                
                # Mark as failed
                audience_collection.update_one(
                    {
                        "client_id": request.client_id,
                        "platform": "linkedin",
                        "type": "prospects"
                    },
                    {
                        "$set": {
                            f"prospects.{idx}.send_failed": True,
                            f"prospects.{idx}.send_error": error,
                            f"prospects.{idx}.status": "send_failed"
                        }
                    }
                )
                
                results.append({
                    "profile_url": profile_url,
                    "name": name,
                    "status": "failed",
                    "error": error
                })
            
            # Rate limiting delay
            if i < len(to_send) - 1 and request.send_delay_seconds > 0:
                print(f"⏱️ Waiting {request.send_delay_seconds}s...")
                time.sleep(request.send_delay_seconds)
                
        except Exception as e:
            print(f"❌ Exception: {str(e)}")
            results.append({
                "profile_url": profile_url,
                "name": name,
                "status": "exception",
                "error": str(e)
            })

    sent_count = sum(1 for r in results if r["status"] == "success")
    failed_count = len(results) - sent_count
    
    print(f"\n{'='*70}")
    print(f"✅ SENDING COMPLETE: {sent_count} sent, {failed_count} failed")
    print(f"{'='*70}\n")

    return {
        "status": "complete",
        "total": len(results),
        "sent": sent_count,
        "failed": failed_count,
        "results": results
    }

# ============================================================
# OTHER ENDPOINTS (UNCHANGED)
# ============================================================

@router.get("/inspect-prospect/{client_id}/{prospect_index}")
async def inspect_prospect_endpoint(client_id: str, prospect_index: int):
    """Debug endpoint to inspect prospect data"""
    client = get_client_data(client_id)
    if not client:
        raise HTTPException(404, "Client not found")

    doc = audience_collection.find_one({
        "client_id": client_id,
        "platform": "linkedin",
        "type": "prospects"
    })
    
    if not doc:
        raise HTTPException(404, "No prospects found")

    prospects = doc["prospects"]
    
    if prospect_index < 0 or prospect_index >= len(prospects):
        raise HTTPException(400, f"Invalid index. Valid range: 0-{len(prospects)-1}")

    prospect = prospects[prospect_index]
    posts = prospect.get("posts", [])
    
    return {
        "index": prospect_index,
        "name": prospect.get("full_name") or prospect.get("fullName", "Unknown"),
        "profile_url": prospect.get("profile_url") or prospect.get("profileUrl"),
        "enriched": prospect.get("enriched", False),
        "has_about": bool(prospect.get("about")),
        "has_experience": bool(prospect.get("experience")),
        "has_skills": bool(prospect.get("skills")),
        "post_count": len(posts),
        "posts_preview": [
            {
                "text": p.get("text", "")[:100] + "...",
                "likes": p.get("likesCount", 0),
                "comments": p.get("commentsCount", 0),
                "hashtags": p.get("hashtags", [])
            }
            for p in posts[:3]
        ],
        "has_generated_message": bool(prospect.get("generated_message")),
        "message_preview": prospect.get("generated_message", "")[:150] if prospect.get("generated_message") else None,
        "message_sent": prospect.get("message_sent", False),
        "sent_at": prospect.get("sent_at"),
        "send_method": prospect.get("sent_method")
    }


@router.get("/messages/{client_id}")
async def get_messages(client_id: str):
    """View all messages and their statuses"""
    client = get_client_data(client_id)
    if not client:
        raise HTTPException(404, "Client not found")

    doc = audience_collection.find_one({
        "client_id": client_id,
        "platform": client["platform"],
        "type": "prospects"
    })
    if not doc:
        return {"count": 0, "prospects": []}

    ready, sent, pending = [], [], []

    for p in doc["prospects"]:
        entry = {
            "name": p.get("full_name") or p.get("fullName") or p.get("username"),
            "profile_url": p.get("profile_url"),
            "message": p.get("generated_message"),
            "enriched": p.get("enriched", False),
            "sent": p.get("message_sent", False),
            "sent_at": p.get("sent_at"),
            "send_method": p.get("sent_method"),
            "send_error": p.get("send_error")
        }

        if p.get("message_sent"):
            sent.append(entry)
        elif p.get("generated_message"):
            ready.append(entry)
        else:
            pending.append(entry)

    return {
        "summary": {
            "ready_to_send": len(ready),
            "sent": len(sent),
            "pending": len(pending)
        },
        "ready": ready,
        "sent": sent,
        "pending": pending
    }

@router.post("/facebook/scrape")
async def scrape_facebook_pages(request: FacebookScrapeRequest):
    """
    STEP 1: Scrape Facebook pages
    
    FLOW:
    1. Get client data from DB
    2. Call apify_utils.scrape_facebook() → Get RAW data
    3. Save RAW data to DB using db_service
    """
    from utils.apify_utils import scrape_facebook
    from services.db_service import save_scraped_data
    
    client = get_client_data(request.client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    
    # Get search params from client registration
    search_terms = client.get("search_terms", [])
    profession = client.get("profession", "")
    location = client.get("location", "")
    
    # Call Apify (via apify_utils)
    results = scrape_facebook(search_terms, profession, location)
    
    if not results:
        return {"status": "no_results", "message": "No pages found"}
    
    # Save RAW data
    saved_count = save_scraped_data(request.client_id, "facebook", results)
    
    return {
        "status": "success",
        "results_count": len(results),
        "saved_count": saved_count,
        "next_step": "POST /facebook/extract-prospects"
    }


@router.post("/facebook/extract-prospects")
async def extract_facebook_prospects_endpoint(request: FacebookExtractProspectsRequest):
    """
    STEP 2: Extract prospects from scraped data
    
    FLOW:
    1. Get RAW scraped data from DB
    2. Loop through and call scraping_service.extract_prospect_info()
    3. Save formatted prospects to DB
    """
    from services.db_service import get_scraped_posts, save_prospects
    from services.scraping_service import extract_prospect_info
    
    client = get_client_data(request.client_id)
    
    # Get RAW scraped data
    scraped_data = get_scraped_posts(request.client_id, "facebook")
    
    if not scraped_data:
        raise HTTPException(404, "No scraped data. Run /facebook/scrape first")
    
    # Extract prospects
    prospects = []
    for item in scraped_data:
        prospect = extract_prospect_info(item, "facebook")
        if prospect:
            prospects.append(prospect)
    
    # Save prospects
    save_prospects(request.client_id, "facebook", prospects)
    
    return {
        "status": "success",
        "count": len(prospects),
        "next_step": "POST /facebook/enrich-posts"
    }
@router.get("/facebook/prospects/{client_id}")
async def get_facebook_prospects(client_id: str):
    """
    View all Facebook prospects for a client.
    
    Returns:
    - Basic prospect info (name, username, category, profile URL)
    - Contact information (email, website, phone)
    - Engagement metrics (followers, likes, rating)
    - Enrichment status (enriched, post count)
    - Message status (has message, message sent)
    - Current status (new, enriched, ready_to_send, sent)
    
    Example response:
    {
        "count": 10,
        "statistics": {
            "total": 10,
            "enriched": 5,
            "with_messages": 3,
            "sent": 1,
            "ready_to_enrich": 5,
            "ready_to_send": 2
        },
        "prospects": [
            {
                "index": 0,
                "name": "Jordan Mcinnis Designs | Montreal QC",
                "username": "Jordanmcinnisdesigns",
                "category": "Graphic Designer",
                "profile_url": "https://www.facebook.com/Jordanmcinnisdesigns/",
                "email": "mcinnisj.design@gmail.com",
                "website": "jordanmcinnis.com",
                "followers": 86,
                "enriched": true,
                "post_count": 10,
                "has_message": true,
                "message_sent": false,
                "status": "ready_to_send"
            }
        ]
    }
    """
    from services.db_service import get_prospects_from_audience
    
    # Verify client exists
    client = get_client_data(client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    
    # Check if client is registered for Facebook
    if client.get("platform") != "facebook":
        raise HTTPException(400, f"Client is registered for {client.get('platform')}, not Facebook")
    
    # Get prospects from database
    prospects_data = get_prospects_from_audience(client_id, "facebook")
    
    if prospects_data["count"] == 0:
        return {
            "count": 0,
            "prospects": [],
            "statistics": {
                "total": 0,
                "enriched": 0,
                "with_messages": 0,
                "sent": 0,
                "ready_to_enrich": 0,
                "ready_to_send": 0
            },
            "message": "No prospects found",
            "next_steps": [
                "1. POST /pipeline/facebook/scrape - Scrape Facebook pages",
                "2. POST /pipeline/facebook/extract-prospects - Extract prospects from scraped data"
            ]
        }
    
    prospects = prospects_data["prospects"]
    
    # Format prospects with all relevant information
    formatted = []
    for idx, p in enumerate(prospects):
        formatted.append({
            # Index for reference
            "index": idx,
            
            # Basic Info
            "name": p.get("name", "Unknown"),
            "username": p.get("username", ""),
            "page_id": p.get("page_id", ""),
            "category": p.get("category", ""),
            "categories": p.get("categories", []),
            "profile_url": p.get("profile_url", ""),
            
            # Contact Info
            "email": p.get("email", ""),
            "phone": p.get("phone", ""),
            "website": p.get("website", ""),
            "location": p.get("location", ""),
            
            # Page Details
            "description": p.get("description", "")[:150] + "..." if p.get("description") else "",
            "about": p.get("about", "")[:150] + "..." if p.get("about") else "",
            
            # Engagement Metrics
            "followers": p.get("followers", 0),
            "likes": p.get("likes", 0),
            "rating": p.get("rating"),
            "rating_count": p.get("rating_count", 0),
            
            # Enrichment Status
            "enriched": p.get("enriched", False),
            "enriched_at": p.get("enriched_at"),
            "post_count": len(p.get("posts", [])),
            "posts_count": p.get("posts_count", 0),  # Stored count
            
            # Engagement from enrichment
            "engagement": p.get("engagement", {}),
            
            # Message Status
            "has_message": bool(p.get("generated_message")),
            "message_preview": p.get("generated_message", "")[:100] + "..." if p.get("generated_message") else None,
            "message_generated_at": p.get("message_generated_at"),
            "ai_model_used": p.get("ai_model_used"),
            "tokens_used": p.get("tokens_used"),
            
            # Send Status
            "message_sent": p.get("message_sent", False),
            "sent_at": p.get("sent_at"),
            
            # Overall Status
            "status": p.get("status", "new"),
            
            # Source
            "source": p.get("source", ""),
            "scraped_at": p.get("scraped_at")
        })
    
    # Calculate statistics
    enriched_count = sum(1 for p in prospects if p.get("enriched"))
    with_messages = sum(1 for p in prospects if p.get("generated_message"))
    sent_count = sum(1 for p in prospects if p.get("message_sent"))
    
    # Determine next steps based on current state
    next_steps = []
    if enriched_count < len(prospects):
        next_steps.append({
            "action": "enrich_posts",
            "endpoint": "POST /pipeline/facebook/enrich-posts",
            "description": f"Enrich {len(prospects) - enriched_count} prospects with recent posts",
            "ready_count": len(prospects) - enriched_count
        })
    
    if enriched_count > with_messages:
        next_steps.append({
            "action": "generate_messages",
            "endpoint": "POST /pipeline/facebook/generate-messages",
            "description": f"Generate messages for {enriched_count - with_messages} enriched prospects",
            "ready_count": enriched_count - with_messages
        })
    
    if with_messages > sent_count:
        next_steps.append({
            "action": "send_messages",
            "description": f"Send {with_messages - sent_count} generated messages manually via Facebook",
            "note": "Facebook messages must be sent manually (requires Facebook Business API approval for automation)",
            "ready_count": with_messages - sent_count
        })
    
    return {
        "count": len(formatted),
        "statistics": {
            "total": len(prospects),
            "enriched": enriched_count,
            "with_messages": with_messages,
            "sent": sent_count,
            "ready_to_enrich": len(prospects) - enriched_count,
            "ready_to_send": with_messages - sent_count
        },
        "prospects": formatted,
        "next_steps": next_steps if next_steps else [
            {
                "action": "complete",
                "description": "All prospects have been processed",
                "note": "All messages have been sent"
            }
        ]
    }

@router.post("/facebook/enrich-posts")
async def enrich_facebook_posts(request: FacebookEnrichRequest):
    """
    Step 3: Enrich Facebook profiles with recent posts.
    
    FIXED: Better error handling and status tracking
    """
    from utils.apify_utils import enrich_facebook_profiles
    from services.facebook_message_service import enrich_prospect_with_posts
    from services.db_service import get_prospects_from_audience, update_prospect_with_enrichment
    
    client = get_client_data(request.client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    
    prospects_data = get_prospects_from_audience(request.client_id, "facebook")
    
    if prospects_data["count"] == 0:
        raise HTTPException(
            404, 
            "No prospects found. Run /facebook/extract-prospects first"
        )
    
    prospects = prospects_data["prospects"]
    
    # Find prospects to enrich
    indices_map = {}
    for url in request.profile_urls:
        for idx, p in enumerate(prospects):
            if p.get("profile_url", "").lower().rstrip("/") == url.lower().rstrip("/"):
                indices_map[url] = idx
                break
    
    if not indices_map:
        raise HTTPException(404, "No matching prospects found")
    
    print(f"\n{'='*70}")
    print(f"🔍 ENRICHING FACEBOOK POSTS FOR {len(indices_map)} PROFILES")
    print(f"{'='*70}")
    
    # Scrape posts (via apify_utils) - NOW RETURNS STATUS INFO
    enriched_data = enrich_facebook_profiles(request.profile_urls)
    
    if not enriched_data:
        raise HTTPException(500, "Post scraping failed completely")
    
    results = []
    
    for url, idx in indices_map.items():
        norm_url = url.lower().rstrip("/")
        page_data = enriched_data.get(norm_url, {})
        
        posts = page_data.get("posts", [])
        status = page_data.get("status", "unknown")
        error = page_data.get("error")
        
        # Enrich prospect (via facebook_message_service)
        prospect = prospects[idx]
        
        # ✅ FIX: Pass enrichment status to enrich_prospect_with_posts
        enriched_prospect = enrich_prospect_with_posts(
            prospect, 
            posts,
            enrichment_status={"status": status, "error": error}
        )
        
        # Update DB
        update_prospect_with_enrichment(request.client_id, "facebook", idx, enriched_prospect)
        
        # Build result
        result = {
            "profile_url": url,
            "name": prospect.get("name", "Unknown"),
            "posts_scraped": len(posts),
            "enrichment_status": status
        }
        
        if error:
            result["error"] = error
        
        if status == "blocked":
            result["note"] = "Facebook blocked the request. This page may require login or have anti-scraping protection."
        elif status == "no_posts":
            result["note"] = "Page has no public posts or posts are restricted to logged-in users."
        
        results.append(result)
    
    # Calculate statistics
    success_count = sum(1 for r in results if r["enrichment_status"] == "success")
    blocked_count = sum(1 for r in results if r["enrichment_status"] == "blocked")
    no_posts_count = sum(1 for r in results if r["enrichment_status"] == "no_posts")
    
    print(f"\n✅ Enrichment complete: {success_count} success, {blocked_count} blocked, {no_posts_count} no posts")
    print(f"{'='*70}\n")
    
    return {
        "status": "complete",
        "total": len(results),
        "successful": success_count,
        "blocked": blocked_count,
        "no_posts": no_posts_count,
        "results": results,
        "warnings": [
            f"{blocked_count} pages were blocked by Facebook (anti-scraping protection)" if blocked_count > 0 else None,
            f"{no_posts_count} pages have no public posts" if no_posts_count > 0 else None,
            "Consider using Facebook Business API for reliable post access" if blocked_count > 0 or no_posts_count > 0 else None
        ],
        "next_step": "POST /pipeline/facebook/generate-messages (messages can still be generated from page descriptions)" if success_count > 0 or blocked_count > 0 else "No prospects ready for message generation"
    }


@router.post("/facebook/generate-messages")
async def generate_facebook_messages(request: FacebookGenerateMessagesRequest):
    """
    STEP 4: Generate AI messages
    
    FLOW:
    1. Get enriched prospects from DB
    2. Call facebook_message_service.generate_facebook_message()
    3. Save messages to DB
    """
    from services.facebook_message_service import generate_facebook_message
    from services.db_service import get_prospects_from_audience
    
    client = get_client_data(request.client_id)
    
    # Get prospects
    prospects_data = get_prospects_from_audience(request.client_id, "facebook")
    prospects = prospects_data["prospects"]
    
    results = []
    for url in request.profile_urls:
        # Find prospect
        for idx, p in enumerate(prospects):
            if p.get("profile_url", "").lower().rstrip("/") == url.lower().rstrip("/"):
                # Generate message
                gen_result = generate_facebook_message(p, request.campaign_message)
                
                if gen_result["status"] == "success":
                    # Save to DB
                    audience_collection.update_one(
                        {"client_id": request.client_id, "platform": "facebook", "type": "prospects"},
                        {"$set": {
                            f"prospects.{idx}.generated_message": gen_result["message"],
                            f"prospects.{idx}.message_generated_at": datetime.utcnow().isoformat()
                        }}
                    )
                
                results.append({
                    "profile_url": url,
                    "status": gen_result["status"],
                    "message": gen_result.get("message")
                })
                break
    
    return {
        "status": "success",
        "generated": len(results),
        "results": results
    }

@router.get("/facebook/inspect/{client_id}/{prospect_index}")
async def inspect_facebook_prospect(client_id: str, prospect_index: int):
    """
    Debug endpoint to inspect Facebook prospect data.
    
    Shows all available data for a specific prospect including posts,
    enrichment status, and generated messages.
    """
    from services.db_service import get_prospects_from_audience
    
    client = get_client_data(client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    
    prospects_data = get_prospects_from_audience(client_id, "facebook")
    
    if prospects_data["count"] == 0:
        raise HTTPException(404, "No prospects found")
    
    prospects = prospects_data["prospects"]
    
    if prospect_index < 0 or prospect_index >= len(prospects):
        raise HTTPException(400, f"Invalid index. Valid range: 0-{len(prospects)-1}")
    
    prospect = prospects[prospect_index]
    posts = prospect.get("posts", [])
    
    # Print detailed inspection to console
    inspect_facebook_prospect_data(prospect, show_full_posts=True)
    
    return {
        "index": prospect_index,
        "name": prospect.get("name", "Unknown"),
        "username": prospect.get("username", ""),
        "category": prospect.get("category", ""),
        "profile_url": prospect.get("profile_url", ""),
        "enriched": prospect.get("enriched", False),
        "enriched_at": prospect.get("enriched_at"),
        "about": prospect.get("about", "")[:200] + "..." if prospect.get("about") else "",
        "contact_info": {
            "phone": prospect.get("phone", ""),
            "email": prospect.get("email", ""),
            "website": prospect.get("website", ""),
            "location": prospect.get("location", "")
        },
        "engagement": {
            "followers": prospect.get("followers", 0),
            "likes": prospect.get("likes", 0),
            "rating": prospect.get("rating")
        },
        "post_count": len(posts),
        "posts_preview": [
            {
                "text": p.get("text", "")[:150] + "...",
                "likes": p.get("likes", 0),
                "comments": p.get("comments", 0),
                "shares": p.get("shares", 0),
                "url": p.get("url", "")
            }
            for p in posts[:5]
        ],
        "has_generated_message": bool(prospect.get("generated_message")),
        "message_preview": prospect.get("generated_message", "")[:200] if prospect.get("generated_message") else None,
        "message_generated_at": prospect.get("message_generated_at"),
        "ai_model_used": prospect.get("ai_model_used"),
        "tokens_used": prospect.get("tokens_used"),
        "message_sent": prospect.get("message_sent", False),
        "status": prospect.get("status", "new")
    }


@router.get("/facebook/messages/{client_id}")
async def get_facebook_messages(client_id: str):
    """
    View all Facebook messages and their statuses.
    
    Organizes prospects by status: ready to send, sent, and pending.
    """
    from services.db_service import get_prospects_from_audience
    
    client = get_client_data(client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    
    prospects_data = get_prospects_from_audience(client_id, "facebook")
    
    if prospects_data["count"] == 0:
        return {"count": 0, "prospects": []}
    
    prospects = prospects_data["prospects"]
    ready, sent, pending = [], [], []
    
    for p in prospects:
        entry = {
            "name": p.get("name", "Unknown"),
            "username": p.get("username", ""),
            "profile_url": p.get("profile_url", ""),
            "message": p.get("generated_message"),
            "enriched": p.get("enriched", False),
            "post_count": len(p.get("posts", [])),
            "sent": p.get("message_sent", False),
            "sent_at": p.get("sent_at"),
            "status": p.get("status", "pending")
        }
        
        if p.get("message_sent"):
            sent.append(entry)
        elif p.get("generated_message"):
            ready.append(entry)
        else:
            pending.append(entry)
    
    return {
        "summary": {
            "ready_to_send": len(ready),
            "sent": len(sent),
            "pending": len(pending)
        },
        "ready": ready,
        "sent": sent,
        "pending": pending,
        "note": "Facebook messages must be sent manually. Copy the message and paste it in Facebook Messenger."
    }

# ============================================================
# ADD THESE IMPORTS TO THE TOP OF routes.py
# ============================================================

from fb_dm_sender import (
    initiate_facebook_connection,
    verify_facebook_connection,
    send_facebook_message_to_prospect,
    extract_facebook_id_from_url
)


# ============================================================
# ADD THESE ENDPOINTS TO routes.py (FACEBOOK DM SECTION)
# ============================================================

@router.get("/connect-facebook/{client_id}")
async def initiate_facebook_connection_endpoint(
    client_id: str,
    redirect: bool = Query(True)
):
    """
    Step 1: Connect Facebook Account
    
    ⚠️ IMPORTANT: JWT generation requires Ayrshare Business Plan ($499/month).
    For free/standard plans, this endpoint returns manual connection instructions.
    
    Two connection methods:
    
    METHOD 1 (Automatic - requires Business Plan):
    - Generates JWT URL for OAuth
    - User clicks → Logs into Facebook → Auto-connected
    
    METHOD 2 (Manual - FREE):
    - Returns instructions for Ayrshare dashboard
    - User manually connects in dashboard
    - Works the same way, just manual setup
    
    Query Parameters:
    - redirect=true (default): Redirects to Ayrshare OR shows manual page
    - redirect=false: Returns JSON with URL or instructions
    
    Examples:
    - GET /connect-facebook/{client_id}  → Auto-redirects or shows manual instructions
    - GET /connect-facebook/{client_id}?redirect=false  → Returns JSON
    """
    try:
        # Success redirect URL
        success_url = f"{APP_BASE_URL}/pipeline/facebook-connection-success?client_id={client_id}"
        
        # Try to generate JWT URL (will fallback to manual if no Business Plan)
        result = initiate_facebook_connection(client_id, success_url)
        
        # Handle different result types
        if result.get("status") == "error":
            raise HTTPException(500, result.get("error"))
        
        # SUCCESS: JWT URL generated (Business Plan active)
        if result.get("status") == "success":
            jwt_url = result.get("jwt_url")
            
            if redirect:
                return RedirectResponse(url=jwt_url, status_code=302)
            else:
                return {
                    "status": "success",
                    "method": "automatic_oauth",
                    "client_id": client_id,
                    "jwt_url": jwt_url,
                    "redirect_url": success_url,
                    "expires_in": result.get("expires_in"),
                    "instructions": result.get("instructions"),
                    "note": "Open the jwt_url in a browser to connect Facebook account"
                }
        
        # MANUAL: Business Plan required, show manual instructions
        if result.get("status") == "manual_required":
            if redirect:
                # Show manual connection page
                return RedirectResponse(
                    url=f"{APP_BASE_URL}/pipeline/facebook-manual-connection?client_id={client_id}",
                    status_code=302
                )
            else:
                # Return JSON with manual instructions
                return result
        
        # Unknown status
        raise HTTPException(500, "Unexpected response from connection service")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error initiating Facebook connection: {str(e)}")
        raise HTTPException(500, f"Failed to initiate Facebook connection: {str(e)}")


@router.get("/facebook-connection-success", response_class=HTMLResponse)
async def facebook_connection_success_page(client_id: str = Query(...)):
    """
    Success page after Facebook OAuth completion via Ayrshare.
    """
    try:
        client = get_client_data(client_id)
        
        if not client:
            return HTMLResponse(
                content=f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Client Not Found</title>
                    <meta charset="utf-8">
                    <style>
                        body {{
                            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                            display: flex;
                            justify-content: center;
                            align-items: center;
                            height: 100vh;
                            margin: 0;
                            background: #f3f4f6;
                        }}
                        .container {{
                            background: white;
                            padding: 40px;
                            border-radius: 12px;
                            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                            text-align: center;
                            max-width: 500px;
                        }}
                        .error {{ font-size: 48px; margin-bottom: 20px; }}
                        h1 {{ color: #1f2937; margin: 0 0 10px 0; }}
                        p {{ color: #6b7280; line-height: 1.6; }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <div class="error">⚠️</div>
                        <h1>Client Not Found</h1>
                        <p>Client ID: <code>{client_id}</code></p>
                    </div>
                </body>
                </html>
                """,
                status_code=404
            )
        
        fb_data = client.get("facebook_connection", {})
        is_connected = fb_data.get("is_active", False)
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Facebook Connected Successfully</title>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    min-height: 100vh;
                    margin: 0;
                    background: linear-gradient(135deg, #4267B2 0%, #3b5998 100%);
                    padding: 20px;
                }}
                
                .container {{
                    background: white;
                    padding: 50px;
                    border-radius: 16px;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                    text-align: center;
                    max-width: 650px;
                    width: 100%;
                    animation: slideIn 0.5s ease-out;
                }}
                
                @keyframes slideIn {{
                    from {{ opacity: 0; transform: translateY(-20px); }}
                    to {{ opacity: 1; transform: translateY(0); }}
                }}
                
                .success-icon {{
                    font-size: 80px;
                    margin-bottom: 20px;
                    animation: bounce 0.6s ease-in-out;
                }}
                
                @keyframes bounce {{
                    0%, 100% {{ transform: scale(1); }}
                    50% {{ transform: scale(1.1); }}
                }}
                
                h1 {{
                    color: #1f2937;
                    font-size: 32px;
                    margin-bottom: 10px;
                    font-weight: 700;
                }}
                
                .subtitle {{
                    color: #6b7280;
                    font-size: 16px;
                    margin-bottom: 30px;
                }}
                
                .info-box {{
                    background: #f9fafb;
                    border: 2px solid #e5e7eb;
                    padding: 20px;
                    border-radius: 10px;
                    margin: 25px 0;
                    text-align: left;
                }}
                
                .info-row {{
                    display: flex;
                    justify-content: space-between;
                    padding: 10px 0;
                    border-bottom: 1px solid #e5e7eb;
                }}
                
                .info-row:last-child {{ border-bottom: none; }}
                
                .info-label {{
                    color: #6b7280;
                    font-weight: 600;
                    font-size: 14px;
                }}
                
                .info-value {{
                    color: #1f2937;
                    font-family: 'Courier New', monospace;
                    font-size: 14px;
                    word-break: break-all;
                }}
                
                .status-badge {{
                    display: inline-block;
                    background: #10b981;
                    color: white;
                    padding: 4px 12px;
                    border-radius: 20px;
                    font-size: 12px;
                    font-weight: 600;
                }}
                
                .next-steps {{
                    background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%);
                    border-left: 4px solid #3b82f6;
                    padding: 25px;
                    margin-top: 30px;
                    text-align: left;
                    border-radius: 8px;
                }}
                
                .next-steps h3 {{
                    margin: 0 0 15px 0;
                    color: #1e40af;
                    font-size: 18px;
                }}
                
                .next-steps ol {{
                    margin: 15px 0;
                    padding-left: 25px;
                    color: #374151;
                    line-height: 1.8;
                }}
                
                .next-steps li {{ margin-bottom: 8px; }}
                
                .api-endpoint {{
                    background: #1f2937;
                    color: #10b981;
                    padding: 12px;
                    border-radius: 6px;
                    font-family: 'Courier New', monospace;
                    font-size: 13px;
                    margin: 15px 0;
                    overflow-x: auto;
                }}
                
                .endpoint-label {{
                    color: #9ca3af;
                    font-size: 12px;
                    margin-bottom: 5px;
                    font-weight: 600;
                }}
                
                .close-note {{
                    margin-top: 30px;
                    font-size: 14px;
                    color: #9ca3af;
                }}
                
                .status-check-btn {{
                    display: inline-block;
                    background: #4267B2;
                    color: white;
                    padding: 12px 24px;
                    border-radius: 8px;
                    text-decoration: none;
                    margin-top: 20px;
                    font-weight: 600;
                    transition: background 0.3s;
                }}
                
                .status-check-btn:hover {{ background: #365899; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="success-icon">✅</div>
                <h1>Facebook Connected Successfully!</h1>
                <p class="subtitle">Your Facebook account is linked and ready to send messages.</p>
                
                <div class="info-box">
                    <div class="info-row">
                        <span class="info-label">Status</span>
                        <span class="status-badge">{"✓ Connected" if is_connected else "⏳ Pending"}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Client ID</span>
                        <span class="info-value">{client_id}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Profile Name</span>
                        <span class="info-value">{fb_data.get("name", "Loading...")}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Connected At</span>
                        <span class="info-value">{fb_data.get("connected_at", "Just now")}</span>
                    </div>
                </div>
                
                <div class="next-steps">
                    <h3>🚀 Next Steps</h3>
                    <ol>
                        <li><strong>Scrape prospects:</strong> Find Facebook pages/profiles</li>
                        <li><strong>Extract profiles:</strong> Get detailed information</li>
                        <li><strong>Enrich with posts:</strong> Analyze recent activity</li>
                        <li><strong>Generate messages:</strong> AI creates personalized messages</li>
                        <li><strong>Send messages:</strong> Deliver via Facebook Messenger</li>
                    </ol>
                    
                    <div class="endpoint-label">📍 Check Connection Status:</div>
                    <div class="api-endpoint">
                        GET /pipeline/facebook-status/{client_id}
                    </div>
                    
                    <div class="endpoint-label">📍 Send Messages:</div>
                    <div class="api-endpoint">
                        POST /pipeline/facebook/send-messages
                    </div>
                </div>
                
                <a href="/pipeline/facebook-status/{client_id}" class="status-check-btn" target="_blank">
                    Check Connection Status
                </a>
                
                <p class="close-note">
                    You can close this window and return to your application.
                </p>
            </div>
        </body>
        </html>
        """
        
        return HTMLResponse(content=html_content)
        
    except Exception as e:
        logger.error(f"Error rendering success page: {str(e)}")
        raise HTTPException(500, f"Error loading page: {str(e)}")


@router.get("/facebook-manual-connection", response_class=HTMLResponse)
async def facebook_manual_connection_page(client_id: str = Query(...)):
    """
    Manual connection instructions page (for free/standard Ayrshare plans).
    """
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Connect Facebook Account Manually</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                display: flex;
                justify-content: center;
                align-items: center;
                min-height: 100vh;
                margin: 0;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 20px;
            }}
            
            .container {{
                background: white;
                padding: 50px;
                border-radius: 16px;
                box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                max-width: 750px;
                width: 100%;
                animation: slideIn 0.5s ease-out;
            }}
            
            @keyframes slideIn {{
                from {{ opacity: 0; transform: translateY(-20px); }}
                to {{ opacity: 1; transform: translateY(0); }}
            }}
            
            .header {{
                text-align: center;
                margin-bottom: 30px;
            }}
            
            .icon {{
                font-size: 60px;
                margin-bottom: 15px;
            }}
            
            h1 {{
                color: #1f2937;
                font-size: 28px;
                margin-bottom: 10px;
                font-weight: 700;
            }}
            
            .subtitle {{
                color: #6b7280;
                font-size: 16px;
                line-height: 1.6;
            }}
            
            .info-box {{
                background: #eff6ff;
                border-left: 4px solid #3b82f6;
                padding: 20px;
                border-radius: 8px;
                margin: 25px 0;
            }}
            
            .info-box strong {{
                color: #1e40af;
                display: block;
                margin-bottom: 10px;
                font-size: 14px;
            }}
            
            .info-box p {{
                color: #374151;
                font-size: 14px;
                line-height: 1.6;
                margin: 0;
            }}
            
            .steps {{
                background: #f9fafb;
                border: 2px solid #e5e7eb;
                border-radius: 12px;
                padding: 30px;
                margin: 25px 0;
            }}
            
            .steps h2 {{
                color: #1f2937;
                font-size: 20px;
                margin-bottom: 20px;
                display: flex;
                align-items: center;
                gap: 10px;
            }}
            
            .step {{
                display: flex;
                gap: 15px;
                margin-bottom: 20px;
                padding-bottom: 20px;
                border-bottom: 1px solid #e5e7eb;
            }}
            
            .step:last-child {{
                border-bottom: none;
                margin-bottom: 0;
                padding-bottom: 0;
            }}
            
            .step-number {{
                flex-shrink: 0;
                width: 32px;
                height: 32px;
                background: #4267B2;
                color: white;
                border-radius: 50%;
                display: flex;
                align-items: center;
                justify-content: center;
                font-weight: 700;
                font-size: 14px;
            }}
            
            .step-content {{
                flex: 1;
            }}
            
            .step-title {{
                color: #1f2937;
                font-weight: 600;
                font-size: 15px;
                margin-bottom: 5px;
            }}
            
            .step-description {{
                color: #6b7280;
                font-size: 14px;
                line-height: 1.6;
            }}
            
            .button-group {{
                display: flex;
                gap: 15px;
                margin-top: 30px;
                flex-wrap: wrap;
            }}
            
            .btn {{
                display: inline-flex;
                align-items: center;
                gap: 8px;
                padding: 14px 28px;
                border-radius: 8px;
                text-decoration: none;
                font-weight: 600;
                font-size: 15px;
                transition: all 0.3s;
                border: none;
                cursor: pointer;
            }}
            
            .btn-primary {{
                background: #4267B2;
                color: white;
            }}
            
            .btn-primary:hover {{
                background: #365899;
                transform: translateY(-2px);
                box-shadow: 0 4px 12px rgba(66, 103, 178, 0.4);
            }}
            
            .btn-secondary {{
                background: #f3f4f6;
                color: #374151;
                border: 2px solid #e5e7eb;
            }}
            
            .btn-secondary:hover {{
                background: #e5e7eb;
            }}
            
            .note {{
                background: #fef3c7;
                border-left: 4px solid #f59e0b;
                padding: 15px 20px;
                border-radius: 8px;
                margin-top: 25px;
            }}
            
            .note p {{
                color: #78350f;
                font-size: 14px;
                margin: 0;
                line-height: 1.6;
            }}
            
            .client-id {{
                background: #1f2937;
                color: #10b981;
                padding: 12px;
                border-radius: 6px;
                font-family: 'Courier New', monospace;
                font-size: 14px;
                margin: 20px 0;
                text-align: center;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div class="icon">🔗</div>
                <h1>Connect Your Facebook Account</h1>
                <p class="subtitle">
                    Automatic OAuth requires Ayrshare Business Plan ($499/month).<br>
                    Follow these simple steps to connect manually for <strong>FREE</strong>.
                </p>
            </div>
            
            <div class="info-box">
                <strong>💡 Why Manual Connection?</strong>
                <p>
                    JWT-based automatic connection requires Ayrshare's Business Plan. 
                    Manual connection through the dashboard works exactly the same way 
                    and is available on all plans (including free tier).
                </p>
            </div>
            
            <div class="client-id">
                Client ID: {client_id}
            </div>
            
            <div class="steps">
                <h2>📋 Connection Steps</h2>
                
                <div class="step">
                    <div class="step-number">1</div>
                    <div class="step-content">
                        <div class="step-title">Open Ayrshare Dashboard</div>
                        <div class="step-description">
                            Click the button below to open Ayrshare dashboard in a new tab.
                        </div>
                    </div>
                </div>
                
                <div class="step">
                    <div class="step-number">2</div>
                    <div class="step-content">
                        <div class="step-title">Navigate to Social Accounts</div>
                        <div class="step-description">
                            In the sidebar, click on "Social Accounts" section.
                        </div>
                    </div>
                </div>
                
                <div class="step">
                    <div class="step-number">3</div>
                    <div class="step-content">
                        <div class="step-title">Add New Account</div>
                        <div class="step-description">
                            Click the "Add Account" or "+" button to connect a new social account.
                        </div>
                    </div>
                </div>
                
                <div class="step">
                    <div class="step-number">4</div>
                    <div class="step-content">
                        <div class="step-title">Select Facebook</div>
                        <div class="step-description">
                            Choose "Facebook" from the list of available platforms.
                        </div>
                    </div>
                </div>
                
                <div class="step">
                    <div class="step-number">5</div>
                    <div class="step-content">
                        <div class="step-title">Connect & Authorize</div>
                        <div class="step-description">
                            Click "Connect Facebook" → Log in with your Facebook credentials → 
                            Grant permissions (pages_messaging, pages_manage_metadata, pages_read_engagement).
                        </div>
                    </div>
                </div>
                
                <div class="step">
                    <div class="step-number">6</div>
                    <div class="step-content">
                        <div class="step-title">Verify Connection</div>
                        <div class="step-description">
                            Return here and click "Check Connection Status" to verify everything works.
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="button-group">
                <a href="https://app.ayrshare.com" target="_blank" class="btn btn-primary">
                    🚀 Open Ayrshare Dashboard
                </a>
                <a href="/pipeline/facebook-status/{client_id}" class="btn btn-secondary">
                    ✓ Check Connection Status
                </a>
            </div>
            
            <div class="note">
                <p>
                    <strong>⏱️ Takes only 2-3 minutes!</strong><br>
                    Once connected in the dashboard, your API key will work for sending messages 
                    through our pipeline. No additional setup needed.
                </p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return HTMLResponse(content=html_content)
async def facebook_connection_success_page(client_id: str = Query(...)):
    """
    Success page after Facebook OAuth completion via Ayrshare.
    """
    try:
        client = get_client_data(client_id)
        
        if not client:
            return HTMLResponse(
                content=f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Client Not Found</title>
                    <meta charset="utf-8">
                    <style>
                        body {{
                            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                            display: flex;
                            justify-content: center;
                            align-items: center;
                            height: 100vh;
                            margin: 0;
                            background: #f3f4f6;
                        }}
                        .container {{
                            background: white;
                            padding: 40px;
                            border-radius: 12px;
                            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                            text-align: center;
                            max-width: 500px;
                        }}
                        .error {{ font-size: 48px; margin-bottom: 20px; }}
                        h1 {{ color: #1f2937; margin: 0 0 10px 0; }}
                        p {{ color: #6b7280; line-height: 1.6; }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <div class="error">⚠️</div>
                        <h1>Client Not Found</h1>
                        <p>Client ID: <code>{client_id}</code></p>
                    </div>
                </body>
                </html>
                """,
                status_code=404
            )
        
        fb_data = client.get("facebook_connection", {})
        is_connected = fb_data.get("is_active", False)
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Facebook Connected Successfully</title>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    min-height: 100vh;
                    margin: 0;
                    background: linear-gradient(135deg, #4267B2 0%, #3b5998 100%);
                    padding: 20px;
                }}
                
                .container {{
                    background: white;
                    padding: 50px;
                    border-radius: 16px;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                    text-align: center;
                    max-width: 650px;
                    width: 100%;
                    animation: slideIn 0.5s ease-out;
                }}
                
                @keyframes slideIn {{
                    from {{ opacity: 0; transform: translateY(-20px); }}
                    to {{ opacity: 1; transform: translateY(0); }}
                }}
                
                .success-icon {{
                    font-size: 80px;
                    margin-bottom: 20px;
                    animation: bounce 0.6s ease-in-out;
                }}
                
                @keyframes bounce {{
                    0%, 100% {{ transform: scale(1); }}
                    50% {{ transform: scale(1.1); }}
                }}
                
                h1 {{
                    color: #1f2937;
                    font-size: 32px;
                    margin-bottom: 10px;
                    font-weight: 700;
                }}
                
                .subtitle {{
                    color: #6b7280;
                    font-size: 16px;
                    margin-bottom: 30px;
                }}
                
                .info-box {{
                    background: #f9fafb;
                    border: 2px solid #e5e7eb;
                    padding: 20px;
                    border-radius: 10px;
                    margin: 25px 0;
                    text-align: left;
                }}
                
                .info-row {{
                    display: flex;
                    justify-content: space-between;
                    padding: 10px 0;
                    border-bottom: 1px solid #e5e7eb;
                }}
                
                .info-row:last-child {{ border-bottom: none; }}
                
                .info-label {{
                    color: #6b7280;
                    font-weight: 600;
                    font-size: 14px;
                }}
                
                .info-value {{
                    color: #1f2937;
                    font-family: 'Courier New', monospace;
                    font-size: 14px;
                    word-break: break-all;
                }}
                
                .status-badge {{
                    display: inline-block;
                    background: #10b981;
                    color: white;
                    padding: 4px 12px;
                    border-radius: 20px;
                    font-size: 12px;
                    font-weight: 600;
                }}
                
                .next-steps {{
                    background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%);
                    border-left: 4px solid #3b82f6;
                    padding: 25px;
                    margin-top: 30px;
                    text-align: left;
                    border-radius: 8px;
                }}
                
                .next-steps h3 {{
                    margin: 0 0 15px 0;
                    color: #1e40af;
                    font-size: 18px;
                }}
                
                .next-steps ol {{
                    margin: 15px 0;
                    padding-left: 25px;
                    color: #374151;
                    line-height: 1.8;
                }}
                
                .next-steps li {{ margin-bottom: 8px; }}
                
                .api-endpoint {{
                    background: #1f2937;
                    color: #10b981;
                    padding: 12px;
                    border-radius: 6px;
                    font-family: 'Courier New', monospace;
                    font-size: 13px;
                    margin: 15px 0;
                    overflow-x: auto;
                }}
                
                .endpoint-label {{
                    color: #9ca3af;
                    font-size: 12px;
                    margin-bottom: 5px;
                    font-weight: 600;
                }}
                
                .close-note {{
                    margin-top: 30px;
                    font-size: 14px;
                    color: #9ca3af;
                }}
                
                .status-check-btn {{
                    display: inline-block;
                    background: #4267B2;
                    color: white;
                    padding: 12px 24px;
                    border-radius: 8px;
                    text-decoration: none;
                    margin-top: 20px;
                    font-weight: 600;
                    transition: background 0.3s;
                }}
                
                .status-check-btn:hover {{ background: #365899; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="success-icon">✅</div>
                <h1>Facebook Connected Successfully!</h1>
                <p class="subtitle">Your Facebook account is linked and ready to send messages.</p>
                
                <div class="info-box">
                    <div class="info-row">
                        <span class="info-label">Status</span>
                        <span class="status-badge">{"✓ Connected" if is_connected else "⏳ Pending"}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Client ID</span>
                        <span class="info-value">{client_id}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Profile Name</span>
                        <span class="info-value">{fb_data.get("name", "Loading...")}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Connected At</span>
                        <span class="info-value">{fb_data.get("connected_at", "Just now")}</span>
                    </div>
                </div>
                
                <div class="next-steps">
                    <h3>🚀 Next Steps</h3>
                    <ol>
                        <li><strong>Scrape prospects:</strong> Find Facebook pages/profiles</li>
                        <li><strong>Extract profiles:</strong> Get detailed information</li>
                        <li><strong>Enrich with posts:</strong> Analyze recent activity</li>
                        <li><strong>Generate messages:</strong> AI creates personalized messages</li>
                        <li><strong>Send messages:</strong> Deliver via Facebook Messenger</li>
                    </ol>
                    
                    <div class="endpoint-label">📍 Check Connection Status:</div>
                    <div class="api-endpoint">
                        GET /pipeline/facebook-status/{client_id}
                    </div>
                    
                    <div class="endpoint-label">📍 Send Messages:</div>
                    <div class="api-endpoint">
                        POST /pipeline/facebook/send-messages
                    </div>
                </div>
                
                <a href="/pipeline/facebook-status/{client_id}" class="status-check-btn" target="_blank">
                    Check Connection Status
                </a>
                
                <p class="close-note">
                    You can close this window and return to your application.
                </p>
            </div>
        </body>
        </html>
        """
        
        return HTMLResponse(content=html_content)
        
    except Exception as e:
        logger.error(f"Error rendering success page: {str(e)}")
        raise HTTPException(500, f"Error loading page: {str(e)}")


@router.get("/facebook-status/{client_id}")
async def check_facebook_status(client_id: str):
    """
    Check if Facebook account is connected and active via Ayrshare.
    
    Returns:
    - Connection status
    - Profile information
    - Available Facebook pages/profiles
    - Next steps
    """
    try:
        result = verify_facebook_connection(client_id)
        result["client_id"] = client_id
        
        if not result.get("connected"):
            result["connect_url"] = f"{APP_BASE_URL}/pipeline/connect-facebook/{client_id}"
            result["instructions"] = [
                "1. Visit the connect_url to start OAuth flow",
                "2. Log in with your Facebook account",
                "3. Grant permissions to Ayrshare",
                "4. Check this endpoint again to verify"
            ]
        else:
            result["next_steps"] = [
                "1. POST /pipeline/facebook/scrape - Scrape Facebook pages",
                "2. POST /pipeline/facebook/extract-prospects - Extract profiles",
                "3. POST /pipeline/facebook/enrich-posts - Enrich with posts",
                "4. POST /pipeline/facebook/generate-messages - Generate AI messages",
                "5. POST /pipeline/facebook/send-messages - Send messages"
            ]
        
        return result
        
    except Exception as e:
        logger.error(f"Status check error: {str(e)}")
        raise HTTPException(500, f"Failed to check status: {str(e)}")


@router.post("/facebook/send-messages")
async def send_facebook_messages(request: SendMessagesRequest):
    """
    Send pre-generated Facebook messages to selected prospects.
    
    ⚠️ IMPORTANT: Facebook requires recipient Facebook IDs, not profile URLs.
    You'll need to manually map profile URLs to Facebook user IDs.
    
    Request body:
    {
        "client_id": "abc123",
        "profile_urls": ["https://www.facebook.com/username/"],
        "send_all_ready": false,
        "send_delay_seconds": 5,
        "dry_run": false
    }
    """
    from services.db_service import get_prospects_from_audience
    
    client = get_client_data(request.client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    
    # Check Facebook connection
    fb_connection = client.get("facebook_connection", {})
    if not fb_connection or not fb_connection.get("is_active"):
        raise HTTPException(
            400,
            "Facebook not connected. Visit /connect-facebook/{client_id} first"
        )
    
    # Get prospects
    prospects_data = get_prospects_from_audience(request.client_id, "facebook")
    
    if prospects_data["count"] == 0:
        raise HTTPException(404, "No prospects found")
    
    prospects = prospects_data["prospects"]
    
    # Determine which prospects to send to
    to_send = []
    
    if request.send_all_ready:
        # Send to all with generated messages (not yet sent)
        for idx, p in enumerate(prospects):
            if p.get("generated_message") and not p.get("message_sent"):
                to_send.append({
                    "index": idx,
                    "prospect": p,
                    "profile_url": p.get("profile_url"),
                    "name": p.get("name", "Unknown"),
                    "message": p["generated_message"],
                    "facebook_id": p.get("facebook_id")  # This might not exist
                })
    
    elif request.profile_urls:
        # Send to specific profiles
        for url in request.profile_urls:
            norm_url = url.lower().rstrip("/")
            found = False
            
            for idx, p in enumerate(prospects):
                p_url = p.get("profile_url", "").lower().rstrip("/")
                if p_url == norm_url:
                    if not p.get("generated_message"):
                        raise HTTPException(
                            400,
                            f"No message generated for {url}. Run /facebook/generate-messages first"
                        )
                    
                    if p.get("message_sent"):
                        logger.info(f"⚠️ Message already sent to {url}, skipping")
                        continue
                    
                    to_send.append({
                        "index": idx,
                        "prospect": p,
                        "profile_url": url,
                        "name": p.get("name", "Unknown"),
                        "message": p["generated_message"],
                        "facebook_id": p.get("facebook_id")
                    })
                    found = True
                    break
            
            if not found:
                raise HTTPException(404, f"Profile not found: {url}")
    
    else:
        raise HTTPException(
            400,
            "Must provide either 'profile_urls' or set 'send_all_ready=true'"
        )
    
    if not to_send:
        return {
            "status": "no_action",
            "message": "No prospects to send to",
            "ready_count": 0
        }
    
    # Dry run - preview only
    if request.dry_run:
        return {
            "status": "dry_run",
            "message": "Preview only - no messages sent",
            "total_to_send": len(to_send),
            "preview": [
                {
                    "name": item["name"],
                    "profile_url": item["profile_url"],
                    "facebook_id": item.get("facebook_id", "⚠️ NOT SET"),
                    "message_preview": item["message"][:150] + "..."
                }
                for item in to_send
            ],
            "warnings": [
                "⚠️ Facebook requires recipient Facebook IDs, not profile URLs",
                "You must manually set 'facebook_id' field for each prospect",
                "Use extract_facebook_id_from_url() or manually find user IDs"
            ],
            "estimated_time": f"{len(to_send) * request.send_delay_seconds} seconds"
        }
    
    # Actually send messages
    print(f"\n{'='*70}")
    print(f"📤 SENDING {len(to_send)} FACEBOOK MESSAGES")
    print(f"{'='*70}")
    
    results = []
    for i, item in enumerate(to_send):
        idx = item["index"]
        profile_url = item["profile_url"]
        name = item["name"]
        message = item["message"]
        facebook_id = item.get("facebook_id")
        
        print(f"\n📨 Sending {i+1}/{len(to_send)}: {name}")
        
        # Check if we have Facebook ID
        if not facebook_id:
            # Try to extract from URL
            facebook_id = extract_facebook_id_from_url(profile_url)
            
            if not facebook_id:
                print(f"❌ No Facebook ID available for {name}")
                results.append({
                    "profile_url": profile_url,
                    "name": name,
                    "status": "failed",
                    "error": "No Facebook ID - cannot send message via API"
                })
                continue
        
        try:
            send_result = send_facebook_message_to_prospect(
                client_id=request.client_id,
                recipient_id=facebook_id,
                message=message
            )
            
            if send_result.get("status") == "success":
                # Update database
                from db_config import audience_collection
                
                audience_collection.update_one(
                    {
                        "client_id": request.client_id,
                        "platform": "facebook",
                        "type": "prospects"
                    },
                    {
                        "$set": {
                            f"prospects.{idx}.message_sent": True,
                            f"prospects.{idx}.sent_at": datetime.utcnow().isoformat(),
                            f"prospects.{idx}.sent_method": "ayrshare_api",
                            f"prospects.{idx}.status": "sent"
                        }
                    }
                )
                
                print(f"✅ Sent via Ayrshare")
                
                results.append({
                    "profile_url": profile_url,
                    "name": name,
                    "facebook_id": facebook_id,
                    "status": "success",
                    "method": "ayrshare_api"
                })
            else:
                error = send_result.get("error", "Unknown error")
                print(f"❌ Failed: {error}")
                
                results.append({
                    "profile_url": profile_url,
                    "name": name,
                    "facebook_id": facebook_id,
                    "status": "failed",
                    "error": error
                })
            
            # Rate limiting delay
            if i < len(to_send) - 1 and request.send_delay_seconds > 0:
                print(f"⏱️ Waiting {request.send_delay_seconds}s...")
                time.sleep(request.send_delay_seconds)
        
        except Exception as e:
            print(f"❌ Exception: {str(e)}")
            results.append({
                "profile_url": profile_url,
                "name": name,
                "status": "exception",
                "error": str(e)
            })
    
    sent_count = sum(1 for r in results if r["status"] == "success")
    failed_count = len(results) - sent_count
    
    print(f"\n{'='*70}")
    print(f"✅ SENDING COMPLETE: {sent_count} sent, {failed_count} failed")
    print(f"{'='*70}\n")
    
    return {
        "status": "complete",
        "total": len(results),
        "sent": sent_count,
        "failed": failed_count,
        "results": results,
        "note": "Facebook messages sent via Ayrshare API"
    }

# ============================================================
# INSTAGRAM DM ENDPOINTS - ADD TO routes.py
# ============================================================

@router.get("/connect-instagram/{client_id}")
async def initiate_instagram_connection(client_id: str, redirect: bool = Query(True)):
    """
    Step 1: Generate Unipile Hosted Auth Link for Instagram
    
    By default (redirect=true): Returns 302 redirect to Unipile OAuth page
    With redirect=false: Returns JSON with the auth URL
    
    Examples:
    - GET /connect-instagram/{client_id}  → Auto-redirects to Unipile
    - GET /connect-instagram/{client_id}?redirect=false  → Returns JSON with URL
    """
    try:
        # Verify client exists
        client = get_client_data(client_id)
        if not client:
            raise HTTPException(404, "Client not found")
        
        from datetime import datetime, timedelta
        
        # Generate hosted auth link via Unipile API
        headers = {
            "X-API-KEY": UNIPILE_API_TOKEN,
            "accept": "application/json",
            "content-type": "application/json"
        }
        
        # Link expires in 1 hour - MUST use .000Z format (3 decimal places)
        expires_on = (datetime.utcnow() + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
        # Webhook URL where Unipile will notify us after successful connection
        notify_url = f"{APP_BASE_URL}/pipeline/instagram-webhook"
        
        # Payload for Instagram OAuth
        payload = {
            "type": "create",
            "api_url": UNIPILE_DSN,
            "providers": ["INSTAGRAM"],
            "expiresOn": expires_on,
            "notify_url": notify_url,
            "name": client_id,
            "success_redirect_url": f"{APP_BASE_URL}/pipeline/instagram-connection-success?client_id={client_id}",
            "failure_redirect_url": f"{APP_BASE_URL}/pipeline/instagram-connection-failed?client_id={client_id}"
        }
        
        logger.info(f"Generating Instagram hosted auth link for client {client_id}")
        logger.info(f"Payload: {payload}")
        
        response = requests.post(
            f"{UNIPILE_DSN}/api/v1/hosted/accounts/link",
            json=payload,
            headers=headers,
            timeout=15
        )
        
        logger.info(f"Unipile response status: {response.status_code}")
        logger.info(f"Unipile response body: {response.text}")
        
        if response.status_code not in [200, 201]:
            try:
                error_data = response.json()
                error_msg = error_data.get("message", error_data.get("error", "Failed to generate auth link"))
                logger.error(f"Unipile error details: {error_data}")
            except:
                error_msg = response.text or "Failed to generate auth link"
            
            logger.error(f"Unipile error: {response.status_code} - {error_msg}")
            raise HTTPException(500, f"Unipile API error: {error_msg}")
        
        result = response.json()
        hosted_url = result.get("url")
        
        if not hosted_url:
            logger.error(f"No URL in response: {result}")
            raise HTTPException(500, "No URL returned from Unipile")
        
        logger.info(f"✅ Generated Instagram hosted auth URL: {hosted_url}")
        
        if redirect:
            return RedirectResponse(url=hosted_url, status_code=302)
        else:
            return {
                "status": "success",
                "client_id": client_id,
                "oauth_url": hosted_url,
                "expires_at": expires_on,
                "instructions": [
                    "1. Open the 'oauth_url' in a browser",
                    "2. Click 'Connect with Instagram'",
                    "3. Log in with Instagram credentials",
                    "4. After success, webhook will be called (needs ngrok for localhost)",
                    "5. Check connection status with GET /pipeline/instagram-status/{client_id}"
                ],
                "webhook_url": notify_url,
                "webhook_warning": "⚠️ Webhook won't work with localhost. Use ngrok for testing." if "localhost" in notify_url else None
            }
        
    except HTTPException:
        raise
    except requests.RequestException as e:
        logger.error(f"Network error: {str(e)}")
        raise HTTPException(500, f"Failed to connect to Unipile: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(500, f"Failed to initiate Instagram connection: {str(e)}")


@router.post("/instagram-webhook")
async def instagram_webhook(request: Request):
    """
    Webhook that receives account_id from Unipile after Instagram OAuth.
    Automatically saves to database.
    """
    try:
        body = await request.body()
        
        if not body:
            return HTMLResponse(
                content="<h1>Instagram Webhook Endpoint</h1><p>This endpoint receives POST requests from Unipile OAuth.</p>",
                status_code=200
            )
        
        payload = json.loads(body)
        logger.info(f"Instagram webhook received: {payload}")
        
        status = payload.get("status")
        account_id = payload.get("account_id")
        client_id = payload.get("name")
        
        if not account_id or not client_id:
            return {"status": "error", "message": "Missing required fields"}
        
        if status not in ["CREATION_SUCCESS", "RECONNECTED"]:
            return {"status": "error", "message": f"Unexpected status: {status}"}
        
        # Verify account with Unipile
        headers = {"X-API-KEY": UNIPILE_API_TOKEN, "accept": "application/json"}
        response = requests.get(f"{UNIPILE_DSN}/api/v1/accounts/{account_id}", headers=headers, timeout=10)
        
        if response.status_code != 200:
            return {"status": "error", "message": "Account verification failed"}
        
        account_data = response.json()
        
        # Save to database
        clients_collection.update_one(
            {"client_id": client_id},
            {
                "$set": {
                    "instagram": {
                        "account_id": account_id,
                        "provider": "INSTAGRAM",
                        "is_active": True,
                        "connected_at": datetime.utcnow().isoformat(),
                        "account_name": account_data.get("name"),
                        "oauth_completed": True
                    }
                }
            }
        )
        
        logger.info(f"✅ Instagram connected: {client_id} → {account_id}")
        
        return {
            "status": "success",
            "client_id": client_id,
            "account_id": account_id
        }
        
    except Exception as e:
        logger.error(f"Instagram webhook error: {str(e)}")
        return {"status": "error", "message": str(e)}


@router.get("/instagram-connection-success", response_class=HTMLResponse)
async def instagram_connection_success_page(client_id: str = Query(...)):
    """
    Success page after Instagram OAuth completion.
    """
    try:
        client = get_client_data(client_id)
        
        if not client:
            return HTMLResponse(
                content=f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Client Not Found</title>
                    <meta charset="utf-8">
                    <style>
                        body {{
                            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                            display: flex;
                            justify-content: center;
                            align-items: center;
                            height: 100vh;
                            margin: 0;
                            background: #f3f4f6;
                        }}
                        .container {{
                            background: white;
                            padding: 40px;
                            border-radius: 12px;
                            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                            text-align: center;
                            max-width: 500px;
                        }}
                        .error {{ font-size: 48px; margin-bottom: 20px; }}
                        h1 {{ color: #1f2937; margin: 0 0 10px 0; }}
                        p {{ color: #6b7280; line-height: 1.6; }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <div class="error">⚠️</div>
                        <h1>Client Not Found</h1>
                        <p>Client ID: <code>{client_id}</code></p>
                    </div>
                </body>
                </html>
                """,
                status_code=404
            )
        
        instagram_data = client.get("instagram", {})
        account_id = instagram_data.get("account_id", "Not yet connected")
        account_name = instagram_data.get("account_name", "Loading...")
        connected_at = instagram_data.get("connected_at", "Unknown")
        is_connected = instagram_data.get("oauth_completed", False)
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Instagram Connected Successfully</title>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }}
                
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    min-height: 100vh;
                    margin: 0;
                    background: linear-gradient(135deg, #f09433 0%,#e6683c 25%,#dc2743 50%,#cc2366 75%,#bc1888 100%);
                    padding: 20px;
                }}
                
                .container {{
                    background: white;
                    padding: 50px;
                    border-radius: 16px;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                    text-align: center;
                    max-width: 650px;
                    width: 100%;
                    animation: slideIn 0.5s ease-out;
                }}
                
                @keyframes slideIn {{
                    from {{
                        opacity: 0;
                        transform: translateY(-20px);
                    }}
                    to {{
                        opacity: 1;
                        transform: translateY(0);
                    }}
                }}
                
                .success-icon {{
                    font-size: 80px;
                    margin-bottom: 20px;
                    animation: bounce 0.6s ease-in-out;
                }}
                
                @keyframes bounce {{
                    0%, 100% {{ transform: scale(1); }}
                    50% {{ transform: scale(1.1); }}
                }}
                
                h1 {{
                    color: #1f2937;
                    font-size: 32px;
                    margin-bottom: 10px;
                    font-weight: 700;
                }}
                
                .subtitle {{
                    color: #6b7280;
                    font-size: 16px;
                    margin-bottom: 30px;
                }}
                
                .info-box {{
                    background: #f9fafb;
                    border: 2px solid #e5e7eb;
                    padding: 20px;
                    border-radius: 10px;
                    margin: 25px 0;
                    text-align: left;
                }}
                
                .info-row {{
                    display: flex;
                    justify-content: space-between;
                    padding: 10px 0;
                    border-bottom: 1px solid #e5e7eb;
                }}
                
                .info-row:last-child {{
                    border-bottom: none;
                }}
                
                .info-label {{
                    color: #6b7280;
                    font-weight: 600;
                    font-size: 14px;
                }}
                
                .info-value {{
                    color: #1f2937;
                    font-family: 'Courier New', monospace;
                    font-size: 14px;
                    word-break: break-all;
                }}
                
                .status-badge {{
                    display: inline-block;
                    background: #10b981;
                    color: white;
                    padding: 4px 12px;
                    border-radius: 20px;
                    font-size: 12px;
                    font-weight: 600;
                }}
                
                .next-steps {{
                    background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%);
                    border-left: 4px solid #3b82f6;
                    padding: 25px;
                    margin-top: 30px;
                    text-align: left;
                    border-radius: 8px;
                }}
                
                .next-steps h3 {{
                    margin: 0 0 15px 0;
                    color: #1e40af;
                    font-size: 18px;
                }}
                
                .next-steps ol {{
                    margin: 15px 0;
                    padding-left: 25px;
                    color: #374151;
                    line-height: 1.8;
                }}
                
                .next-steps li {{
                    margin-bottom: 8px;
                }}
                
                .api-endpoint {{
                    background: #1f2937;
                    color: #10b981;
                    padding: 12px;
                    border-radius: 6px;
                    font-family: 'Courier New', monospace;
                    font-size: 13px;
                    margin: 15px 0;
                    overflow-x: auto;
                }}
                
                .endpoint-label {{
                    color: #9ca3af;
                    font-size: 12px;
                    margin-bottom: 5px;
                    font-weight: 600;
                }}
                
                .close-note {{
                    margin-top: 30px;
                    font-size: 14px;
                    color: #9ca3af;
                }}
                
                .status-check-btn {{
                    display: inline-block;
                    background: #e1306c;
                    color: white;
                    padding: 12px 24px;
                    border-radius: 8px;
                    text-decoration: none;
                    margin-top: 20px;
                    font-weight: 600;
                    transition: background 0.3s;
                }}
                
                .status-check-btn:hover {{
                    background: #c13584;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="success-icon">✅</div>
                <h1>Instagram Connected Successfully!</h1>
                <p class="subtitle">Your Instagram account has been linked and is ready to send messages.</p>
                
                <div class="info-box">
                    <div class="info-row">
                        <span class="info-label">Status</span>
                        <span class="status-badge">{"✓ Connected" if is_connected else "⏳ Pending"}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Client ID</span>
                        <span class="info-value">{client_id}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Account ID</span>
                        <span class="info-value">{account_id}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Account Name</span>
                        <span class="info-value">{account_name}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Connected At</span>
                        <span class="info-value">{connected_at[:19] if connected_at != "Unknown" else connected_at}</span>
                    </div>
                </div>
                
                <div class="next-steps">
                    <h3>🚀 Next Steps</h3>
                    <ol>
                        <li><strong>Scrape prospects:</strong> Find Instagram profiles</li>
                        <li><strong>Extract profiles:</strong> Get detailed profile information</li>
                        <li><strong>Enrich with posts:</strong> Scrape their recent content</li>
                        <li><strong>Generate messages:</strong> AI creates personalized DMs</li>
                        <li><strong>Send automatically:</strong> Deliver messages via Instagram</li>
                    </ol>
                    
                    <div class="endpoint-label">📍 Check Connection Status:</div>
                    <div class="api-endpoint">
                        GET /pipeline/instagram-status/{client_id}
                    </div>
                    
                    <div class="endpoint-label">📍 Start Campaign:</div>
                    <div class="api-endpoint">
                        POST /pipeline/instagram/generate-and-send
                    </div>
                </div>
                
                <a href="/pipeline/instagram-status/{client_id}" class="status-check-btn" target="_blank">
                    Check Connection Status
                </a>
                
                <p class="close-note">
                    You can close this window and return to your application.
                </p>
            </div>
        </body>
        </html>
        """
        
        return HTMLResponse(content=html_content)
        
    except Exception as e:
        logger.error(f"Error rendering Instagram success page: {str(e)}")
        raise HTTPException(500, f"Error loading page: {str(e)}")



from pydantic import BaseModel
from typing import List
from fastapi import HTTPException
from datetime import datetime

class EnrichInstagramPostsRequest(BaseModel):
    client_id: str
    profile_urls: List[str]

@router.post("/instagram/enrich-posts")
async def enrich_instagram_posts_endpoint(request: EnrichInstagramPostsRequest):
    """
    Scrape Instagram posts for selected profiles.
    Similar to LinkedIn's /enrich-posts but for Instagram.
    
    Request:
    {
        "client_id": "abc123",
        "profile_urls": [
            "https://www.instagram.com/username1/",
            "https://www.instagram.com/username2/"
        ]
    }
    
    Response:
    {
        "status": "success",
        "enriched_count": 2,
        "results": [
            {
                "profile_url": "https://www.instagram.com/username1/",
                "username": "username1",
                "posts_scraped": 12,
                "enrichment_status": "success"
            }
        ]
    }
    """
    from services.instagram_profile_message_service import scrape_instagram_profile
    
    client = get_client_data(request.client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    
    if client["platform"] != "instagram":
        raise HTTPException(400, "Only Instagram is supported for this endpoint")
    
    # Get prospects document
    doc = audience_collection.find_one({
        "client_id": request.client_id,
        "platform": "instagram",
        "type": "prospects"
    })
    
    if not doc:
        raise HTTPException(404, "No prospects found. Add prospects first using /instagram/generate-messages")
    
    prospects = doc.get("prospects", [])
    
    # Find profiles to enrich
    to_enrich = []
    indices_map = {}
    
    for url in request.profile_urls:
        norm_url = url.lower().rstrip("/")
        found = False
        
        for idx, p in enumerate(prospects):
            p_url = p.get("profile_url", "").lower().rstrip("/")
            p_username = p.get("username", "").lower()
            
            # Match by URL or username in URL
            if p_url == norm_url or f"instagram.com/{p_username}" in norm_url:
                to_enrich.append({
                    "url": url,
                    "index": idx,
                    "already_enriched": p.get("enriched", False),
                    "username": p.get("username", "Unknown"),
                    "full_name": p.get("fullName", "")
                })
                indices_map[url] = idx
                found = True
                break
        
        if not found:
            print(f"⚠️ Profile URL not found in prospects: {url}")
    
    if not to_enrich:
        raise HTTPException(404, "No matching prospects found in database")
    
    print(f"\n{'='*70}")
    print(f"📸 ENRICHING INSTAGRAM POSTS FOR {len(to_enrich)} PROFILES")
    print(f"{'='*70}")
    
    results = []
    
    for item in to_enrich:
        url = item["url"]
        idx = item["index"]
        username = item["username"]
        full_name = item["full_name"]
        
        try:
            print(f"\n📊 Profile {len(results)+1}/{len(to_enrich)}: @{username}")
            
            # Check if already enriched recently (within last 24 hours)
            existing_prospect = prospects[idx]
            if item["already_enriched"]:
                enriched_at = existing_prospect.get("enriched_at")
                if enriched_at:
                    from dateutil import parser
                    enriched_time = parser.parse(enriched_at)
                    hours_since = (datetime.utcnow() - enriched_time).total_seconds() / 3600
                    
                    if hours_since < 24:
                        print(f"   ✓ Already enriched {hours_since:.1f} hours ago, skipping...")
                        results.append({
                            "profile_url": url,
                            "username": username,
                            "full_name": full_name,
                            "posts_scraped": len(existing_prospect.get("latestPosts", [])),
                            "enrichment_status": "already_enriched",
                            "enriched_at": enriched_at
                        })
                        continue
            
            # Scrape Instagram profile with posts
            print(f"   📸 Scraping profile and posts...")
            profile_data = scrape_instagram_profile(url)
            
            if not profile_data:
                print(f"   ❌ Failed to scrape profile")
                results.append({
                    "profile_url": url,
                    "username": username,
                    "full_name": full_name,
                    "posts_scraped": 0,
                    "enrichment_status": "scraping_failed",
                    "error": "Failed to scrape Instagram profile"
                })
                continue
            
            posts = profile_data.get("latestPosts", [])
            posts_count = len(posts)
            
            if posts_count == 0:
                print(f"   ⚠️ No posts found (private account or no posts)")
                results.append({
                    "profile_url": url,
                    "username": username,
                    "full_name": full_name,
                    "posts_scraped": 0,
                    "enrichment_status": "no_posts_found",
                    "error": "Profile may be private or have no posts"
                })
                continue
            
            print(f"   ✅ Posts scraped: {posts_count}")
            print(f"   📊 Engagement Rate: {profile_data.get('engagementRate', 0)}%")
            print(f"   💬 Avg Likes: {profile_data.get('avgLikes', 0):,}")
            
            # Update prospect with enriched data
            enriched_profile = {
                **existing_prospect,
                **profile_data,
                "enriched": True,
                "enriched_at": datetime.utcnow().isoformat()
            }
            
            # Update in database
            audience_collection.update_one(
                {
                    "client_id": request.client_id,
                    "platform": "instagram",
                    "type": "prospects"
                },
                {
                    "$set": {
                        f"prospects.{idx}": enriched_profile
                    }
                }
            )
            
            results.append({
                "profile_url": url,
                "username": username,
                "full_name": profile_data.get("fullName", full_name),
                "posts_scraped": posts_count,
                "enrichment_status": "success",
                "engagement_metrics": {
                    "followers": profile_data.get("followersCount", 0),
                    "engagement_rate": profile_data.get("engagementRate", 0),
                    "avg_likes": profile_data.get("avgLikes", 0),
                    "avg_comments": profile_data.get("avgComments", 0)
                }
            })
            
        except Exception as e:
            print(f"   ❌ Error enriching @{username}: {str(e)}")
            import traceback
            traceback.print_exc()
            
            results.append({
                "profile_url": url,
                "username": username,
                "full_name": full_name,
                "posts_scraped": 0,
                "enrichment_status": "exception",
                "error": str(e)
            })
    
    success_count = len([r for r in results if r["enrichment_status"] == "success"])
    
    print(f"\n{'='*70}")
    print(f"✅ ENRICHMENT COMPLETE: {success_count}/{len(results)} successful")
    print(f"{'='*70}\n")
    
    return {
        "status": "success",
        "total": len(results),
        "enriched_count": success_count,
        "results": results
    }

# ============================================================
# INSTAGRAM ENDPOINTS - SPLIT GENERATE & SEND (Like LinkedIn)
# Replace the /instagram/generate-and-send endpoint with these two
# ============================================================

@router.post("/instagram/generate-messages")
async def generate_instagram_messages(request: GenerateMessagesRequest):
    """
    Generate AI-personalized messages for Instagram prospects.
    Messages are saved to database but NOT sent.
    
    IMPORTANT: Profiles must be enriched first using /instagram/enrich-posts
    This endpoint only generates messages, does NOT scrape profiles.
    
    Request:
    {
        "client_id": "abc123",
        "profile_urls": [
            "https://www.instagram.com/username1/",
            "https://www.instagram.com/username2/"
        ],
        "campaign_message": "I help tech leaders optimize cloud costs",
        "debug": false
    }
    
    Response:
    {
        "status": "success",
        "total": 2,
        "generated": 2,
        "failed": 0,
        "results": [...],
        "next_step": "Use POST /instagram/send-messages to send these messages"
    }
    """
    from services.instagram_profile_message_service import generate_personalized_message
    
    client = get_client_data(request.client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    
    # Get prospects document
    doc = audience_collection.find_one({
        "client_id": request.client_id,
        "platform": "instagram",
        "type": "prospects"
    })
    
    if not doc:
        raise HTTPException(404, "No prospects found. Use /instagram/enrich-posts first to add and enrich profiles")
    
    prospects = doc.get("prospects", [])
    
    if not prospects:
        raise HTTPException(404, "No prospects found. Use /instagram/enrich-posts first")
    
    print(f"\n{'='*70}")
    print(f"🤖 GENERATING INSTAGRAM MESSAGES FOR {len(request.profile_urls)} PROFILES")
    print(f"{'='*70}")
    
    results = []
    
    for i, profile_url in enumerate(request.profile_urls, 1):
        try:
            # Extract username
            from insta_dm_sender import extract_username_from_url
            username = extract_username_from_url(profile_url)
            
            print(f"\n📝 Profile {i}/{len(request.profile_urls)}: @{username}")
            
            # Find prospect in database
            prospect_idx = None
            for idx, p in enumerate(prospects):
                p_username = p.get("username", "")
                p_url = p.get("profile_url", "")
                if p_username == username or p_url.lower().rstrip("/") == profile_url.lower().rstrip("/"):
                    prospect_idx = idx
                    break
            
            # Check if prospect exists
            if prospect_idx is None:
                print(f"   ❌ Profile not found in database")
                results.append({
                    "profile_url": profile_url,
                    "username": username,
                    "status": "failed",
                    "error": "Profile not found. Use /instagram/enrich-posts first to add this profile"
                })
                continue
            
            profile_data = prospects[prospect_idx]
            
            # Check if profile is enriched
            if not profile_data.get("enriched"):
                print(f"   ❌ Profile not enriched")
                results.append({
                    "profile_url": profile_url,
                    "username": username,
                    "status": "failed",
                    "error": "Profile not enriched. Use /instagram/enrich-posts first to scrape posts"
                })
                continue
            
            print(f"   ✓ Profile found and enriched")
            
            if request.debug:
                print(f"\n   📊 Profile Data:")
                print(f"      Username: {profile_data.get('username')}")
                print(f"      Full Name: {profile_data.get('fullName')}")
                print(f"      Followers: {profile_data.get('followersCount', 0):,}")
                print(f"      Posts: {profile_data.get('postsCount', 0)}")
                print(f"      Latest Posts: {len(profile_data.get('latestPosts', []))}")
                print(f"      Bio: {profile_data.get('biography', '')[:100]}...")
            
            # Generate personalized message using enriched data
            print(f"   🤖 Generating personalized message...")
            generated_message = generate_personalized_message(
                profile_data,
                request.campaign_message,
                campaign_description=""
            )
            
            if not generated_message:
                print(f"   ❌ Failed to generate message")
                results.append({
                    "profile_url": profile_url,
                    "username": username,
                    "status": "failed",
                    "error": "Failed to generate message"
                })
                continue
            
            print(f"   ✅ Generated ({len(generated_message)} chars)")
            
            # Save message to database
            audience_collection.update_one(
                {
                    "client_id": request.client_id,
                    "platform": "instagram",
                    "type": "prospects"
                },
                {
                    "$set": {
                        f"prospects.{prospect_idx}.generated_message": generated_message,
                        f"prospects.{prospect_idx}.message_generated_at": datetime.utcnow().isoformat(),
                        f"prospects.{prospect_idx}.campaign_message": request.campaign_message,
                        f"prospects.{prospect_idx}.status": "ready_to_send"
                    }
                }
            )
            
            results.append({
                "profile_url": profile_url,
                "username": username,
                "full_name": profile_data.get("fullName", ""),
                "status": "success",
                "message": generated_message,
                "profile_summary": {
                    "followers": profile_data.get("followersCount", 0),
                    "posts": profile_data.get("postsCount", 0),
                    "latest_posts_count": len(profile_data.get("latestPosts", [])),
                    "bio": profile_data.get("biography", "")[:100]
                }
            })
            
        except Exception as e:
            print(f"   ❌ Error processing @{username}: {str(e)}")
            import traceback
            traceback.print_exc()
            results.append({
                "profile_url": profile_url,
                "username": username,
                "status": "exception",
                "error": str(e)
            })
    
    success_count = sum(1 for r in results if r["status"] == "success")
    failed_count = len(results) - success_count
    
    print(f"\n{'='*70}")
    print(f"✅ GENERATION COMPLETE: {success_count}/{len(results)} successful")
    print(f"{'='*70}\n")
    
    return {
        "status": "success",
        "total": len(results),
        "generated": success_count,
        "failed": failed_count,
        "results": results,
        "next_step": "Use POST /instagram/send-messages to send these messages"
    }


# In your routes file, update the send_instagram_messages endpoint

@router.post("/instagram/send-messages")
async def send_instagram_messages(request: SendMessagesRequest):
    """
    Send pre-generated Instagram messages to selected prospects via Unipile.
    """
    from insta_dm_sender import extract_username_from_url, send_instagram_dm_via_unipile
    
    client = get_client_data(request.client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    
    # Get Instagram account_id
    instagram_data = client.get("instagram", {})
    if not instagram_data or not instagram_data.get("is_active"):
        raise HTTPException(400, "Instagram not connected")
    
    account_id = instagram_data.get("account_id")
    
    doc = audience_collection.find_one({
        "client_id": request.client_id,
        "platform": "instagram",
        "type": "prospects"
    })
    if not doc:
        raise HTTPException(404, "No prospects found")
    
    prospects = doc["prospects"]
    
    # Determine which prospects to send to
    to_send = []
    
    if request.send_all_ready:
        for idx, p in enumerate(prospects):
            if p.get("generated_message") and not p.get("message_sent"):
                to_send.append({
                    "index": idx,
                    "username": p.get("username", "Unknown"),
                    "message": p["generated_message"]
                })
    elif request.profile_urls:
        for url in request.profile_urls:
            username = extract_username_from_url(url)
            for idx, p in enumerate(prospects):
                if p.get("username") == username:
                    if not p.get("generated_message"):
                        raise HTTPException(400, f"No message for @{username}")
                    if not p.get("message_sent"):
                        to_send.append({
                            "index": idx,
                            "username": username,
                            "message": p["generated_message"]
                        })
                    break
    
    if not to_send:
        return {"status": "no_action", "message": "No prospects to send"}
    
    # Send messages
    print(f"\n{'='*70}")
    print(f"📤 SENDING {len(to_send)} INSTAGRAM MESSAGES VIA UNIPILE")
    print(f"{'='*70}")
    
    results = []
    for i, item in enumerate(to_send):
        idx = item["index"]
        username = item["username"]
        message = item["message"]
        
        print(f"\n📨 Sending {i+1}/{len(to_send)}: @{username}")
        
        try:
            # ⭐ USE UNIPILE API TO SEND MESSAGE
            send_result = send_instagram_dm_via_unipile(
                username=username,
                message=message,
                account_id=account_id
            )
            
            if send_result.get("status") == "success":
                message_id = send_result.get("message_id")
                print(f"   ✅ Sent (message_id: {message_id})")
                
                # Update database
                audience_collection.update_one(
                    {"client_id": request.client_id, "platform": "instagram", "type": "prospects"},
                    {
                        "$set": {
                            f"prospects.{idx}.message_sent": True,
                            f"prospects.{idx}.sent_at": datetime.utcnow().isoformat(),
                            f"prospects.{idx}.sent_method": "dm",
                            f"prospects.{idx}.message_id": message_id,
                            f"prospects.{idx}.status": "sent"
                        }
                    }
                )
                
                results.append({
                    "username": username,
                    "status": "success",
                    "message_id": message_id
                })
            else:
                error = send_result.get("error")
                print(f"   ❌ Failed: {error}")
                results.append({
                    "username": username,
                    "status": "failed",
                    "error": error
                })
            
            # Rate limiting
            if i < len(to_send) - 1 and request.send_delay_seconds > 0:
                time.sleep(request.send_delay_seconds)
        
        except Exception as e:
            print(f"   ❌ Exception: {str(e)}")
            results.append({
                "username": username,
                "status": "exception",
                "error": str(e)
            })
    
    sent_count = sum(1 for r in results if r["status"] == "success")
    
    print(f"\n{'='*70}")
    print(f"✅ COMPLETE: {sent_count}/{len(results)} sent")
    print(f"{'='*70}\n")
    
    return {
        "status": "complete",
        "total": len(results),
        "sent": sent_count,
        "failed": len(results) - sent_count,
        "results": results
    }

@router.get("/instagram/prospects/{client_id}")
async def get_instagram_prospects(client_id: str):
    """
    Get all Instagram prospects for a client.
    
    Returns:
    - Basic prospect info (username, full name, profile URL)
    - Engagement metrics (followers, posts)
    - Enrichment status (enriched, post count)
    - Message status (has message, message sent)
    - Current status (new, enriched, ready_to_send, sent)
    """
    client = get_client_data(client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    
    if client.get("platform") != "instagram":
        raise HTTPException(400, f"Client is registered for {client.get('platform')}, not Instagram")
    
    doc = audience_collection.find_one({
        "client_id": client_id,
        "platform": "instagram",
        "type": "prospects"
    })
    
    if not doc:
        return {
            "count": 0,
            "prospects": [],
            "statistics": {
                "total": 0,
                "enriched": 0,
                "with_messages": 0,
                "sent": 0,
                "ready_to_send": 0
            },
            "message": "No prospects found",
            "next_steps": [
                "1. Add profile URLs and generate messages",
                "2. Use POST /instagram/generate-messages"
            ]
        }
    
    prospects = doc.get("prospects", [])
    
    # Format prospects
    formatted = []
    for idx, p in enumerate(prospects):
        formatted.append({
            "index": idx,
            "username": p.get("username", ""),
            "full_name": p.get("fullName", ""),
            "profile_url": p.get("profile_url", ""),
            "bio": p.get("biography", "")[:100] + "..." if p.get("biography") else "",
            "followers": p.get("followersCount", 0),
            "following": p.get("followsCount", 0),
            "posts": p.get("postsCount", 0),
            "verified": p.get("verified", False),
            "private": p.get("private", False),
            "enriched": p.get("enriched", False),
            "enriched_at": p.get("enriched_at"),
            "post_count": len(p.get("latestPosts", [])),
            "has_message": bool(p.get("generated_message")),
            "message_preview": p.get("generated_message", "")[:100] if p.get("generated_message") else None,
            "message_sent": p.get("message_sent", False),
            "sent_at": p.get("sent_at"),
            "message_id": p.get("message_id"),
            "status": p.get("status", "new")
        })
    
    # Calculate statistics
    enriched_count = sum(1 for p in prospects if p.get("enriched"))
    with_messages = sum(1 for p in prospects if p.get("generated_message"))
    sent_count = sum(1 for p in prospects if p.get("message_sent"))
    
    return {
        "count": len(formatted),
        "statistics": {
            "total": len(prospects),
            "enriched": enriched_count,
            "with_messages": with_messages,
            "sent": sent_count,
            "ready_to_send": with_messages - sent_count
        },
        "prospects": formatted,
        "next_steps": [
            f"Generate messages for {len(prospects) - with_messages} prospects" if len(prospects) > with_messages else None,
            f"Send {with_messages - sent_count} generated messages" if with_messages > sent_count else None
        ]
    }


@router.get("/instagram/messages/{client_id}")
async def get_instagram_messages(client_id: str):
    """
    View all Instagram messages and their statuses.
    Organizes prospects by status: ready to send, sent, and pending.
    """
    client = get_client_data(client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    
    doc = audience_collection.find_one({
        "client_id": client_id,
        "platform": "instagram",
        "type": "prospects"
    })
    
    if not doc:
        return {"count": 0, "prospects": []}
    
    prospects = doc.get("prospects", [])
    ready, sent, pending = [], [], []
    
    for p in prospects:
        entry = {
            "username": p.get("username", ""),
            "full_name": p.get("fullName", ""),
            "profile_url": p.get("profile_url", ""),
            "message": p.get("generated_message"),
            "enriched": p.get("enriched", False),
            "sent": p.get("message_sent", False),
            "sent_at": p.get("sent_at"),
            "message_id": p.get("message_id"),
            "status": p.get("status", "pending")
        }
        
        if p.get("message_sent"):
            sent.append(entry)
        elif p.get("generated_message"):
            ready.append(entry)
        else:
            pending.append(entry)
    
    return {
        "summary": {
            "ready_to_send": len(ready),
            "sent": len(sent),
            "pending": len(pending)
        },
        "ready": ready,
        "sent": sent,
        "pending": pending
    }