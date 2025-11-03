from email.mime.text import MIMEText
import base64
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request


SCOPES = ['https://www.googleapis.com/auth/gmail.send']

def get_gmail_service():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)

def send_email(to_email: str, subject: str, message_text: str):
    """Send email using Gmail API safely (skip if email missing)"""
    
    # 🧩 Step 1: Check for valid email before trying to send
    if not to_email or not isinstance(to_email, str) or "@" not in to_email:
        print(f"⚠️ Skipping — No valid email provided: {to_email}")
        return {"success": False, "error": "No valid email"}

    try:
        # 🧩 Step 2: Get Gmail API service
        service = get_gmail_service()

        # 🧩 Step 3: Build message
        message = MIMEText(message_text)
        message['to'] = to_email
        message['subject'] = subject
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
        body = {'raw': raw}

        # 🧩 Step 4: Send email
        sent = service.users().messages().send(userId="me", body=body).execute()
        print(f"✅ Email sent to {to_email} | Message ID: {sent['id']}")
        return {"success": True, "id": sent["id"]}

    except Exception as e:
        print(f"❌ Email send failed for {to_email}: {e}")
        return {"success": False, "error": str(e)}
