# db_analysis.py
import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

# -------------------------------
# Load environment variables
# -------------------------------
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "cosmetics_app")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "audience_data")

# -------------------------------
# MongoDB Connection
# -------------------------------
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    print("✅ Successfully connected to MongoDB Atlas!")
except Exception as e:
    print("❌ MongoDB connection failed. Check your MONGO_URI.")
    print("Error details:", e)
    exit(1)

db = client[MONGO_DB]
collection = db[COLLECTION_NAME]

# -------------------------------
# Fetch Data into Pandas DataFrame
# -------------------------------
try:
    print(f"📊 Fetching data from collection: {COLLECTION_NAME} ...")
    data = list(collection.find({}, {"_id": 0}))  # Exclude _id for cleaner DataFrame
    if not data:
        print("⚠️ No data found in collection!")
        exit(0)

    df = pd.DataFrame(data)
    print(f"✅ Loaded {len(df)} records into DataFrame.")
    print("\n🔎 Preview of first 5 records:\n")
    print(df.head())

except Exception as e:
    print("❌ Error while fetching or converting data:", e)
    exit(1)

# -------------------------------
# Platform distribution
# -------------------------------
if "platform" in df.columns:
    print("\n📊 Platform distribution:\n")
    print(df["platform"].value_counts())

# -------------------------------
# Export Facebook Profiles
# -------------------------------
if "platform" in df.columns:
    fb_df = df[df["platform"] == "facebook"].copy()
    print(f"\n✅ Found {len(fb_df)} Facebook profiles in DB")

    # Flatten nested structures safely
    def extract_value(row, key):
        return row.get(key, None) if isinstance(row, dict) else None

    # Some rows may store results in nested "Final Answer" or "original_data"
    if "Final Answer" in fb_df.columns:
        fb_df["bio"] = fb_df["Final Answer"].apply(lambda x: extract_value(x, "bio"))
        fb_df["profile_url"] = fb_df["Final Answer"].apply(lambda x: extract_value(x, "profile_url"))
        fb_df["username"] = fb_df["Final Answer"].apply(lambda x: extract_value(x, "username"))
        fb_df["email"] = fb_df["Final Answer"].apply(lambda x: extract_value(x.get("contact", {}), "email") if isinstance(x, dict) else None)
        fb_df["phone"] = fb_df["Final Answer"].apply(lambda x: extract_value(x.get("contact", {}), "phone") if isinstance(x, dict) else None)
    else:
        # If fields are already top-level
        fb_df["bio"] = fb_df.get("bio")
        fb_df["profile_url"] = fb_df.get("profile_url")
        fb_df["username"] = fb_df.get("username")
        fb_df["email"] = fb_df.get("email")
        fb_df["phone"] = fb_df.get("phone")

    # Select only relevant export columns
    export_cols = ["username", "bio", "profile_url", "email", "phone", "platform"]
    export_df = fb_df[export_cols]

    # Save to CSV
    export_df.to_csv("facebook_profiles.csv", index=False)
    print(f"📂 Exported {len(export_df)} Facebook profiles to facebook_profiles.csv")

# -------------------------------
# Instagram-specific (legacy check)
# -------------------------------
if "ownerId" in df.columns:
    owner_counts = df["ownerId"].value_counts()
    print("\n📋 All Owners by Post Count:\n")
    print(owner_counts)

    owners_df = pd.DataFrame(df["ownerId"].dropna().unique(), columns=["ownerId"])
    owners_df.to_csv("owner_ids.csv", index=False)
    print(f"✅ Saved {len(owners_df)} unique owner IDs to owner_ids.csv")
    print("\n👀 Sample owner IDs:\n")
    print(owners_df.head())
