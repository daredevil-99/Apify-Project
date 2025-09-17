import os
from dotenv import load_dotenv

load_dotenv()

# --- MongoDB ---
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

# --- OpenAI ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# --- Apify ---
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")

# --- Validation ---
required_vars = {
    "MONGO_URI": MONGO_URI,
    "MONGO_DB": MONGO_DB,
    "OPENAI_API_KEY": OPENAI_API_KEY,
    "APIFY_API_TOKEN": APIFY_API_TOKEN,
}

missing = [k for k, v in required_vars.items() if not v]

if missing:
    raise EnvironmentError(f"❌ Missing required environment variables: {', '.join(missing)}. "
                           "Check your .env file before running the server.")
