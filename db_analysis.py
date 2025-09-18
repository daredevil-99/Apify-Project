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
# Example Analysis (Optional)
# -------------------------------
# Count how many posts per platform
if "platform" in df.columns:
    print("\n📊 Platform distribution:\n")
    print(df["platform"].value_counts())

# Show top 5 owners by number of posts
if "ownerId" in df.columns:
    owner_counts = df["ownerId"].value_counts()
    print("\n📋 All Owners by Post Count:\n")
    print(owner_counts)


import pandas as pd

# Assuming df is already fetched from MongoDB
if "ownerId" in df.columns:
    # Get unique owner IDs
    unique_owners = df["ownerId"].dropna().unique()

    # Convert to DataFrame
    owners_df = pd.DataFrame(unique_owners, columns=["ownerId"])

    # Save to CSV
    owners_df.to_csv("owner_ids.csv", index=False)
    print(f"✅ Saved {len(unique_owners)} unique owner IDs to owner_ids.csv")

    # Show first 10 as preview
    print("\n👀 Sample owner IDs:\n")
    print(owners_df)
else:
    print("⚠️ 'ownerId' column not found in DataFrame")
