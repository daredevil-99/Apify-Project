from pipeline_utils import fetch_from_mongo_tool, crew

if __name__ == "__main__":
    # Step 1: Get user request
    user_request = input("📝 Enter your request (e.g., 'Generate message for wedding planners'): ")

    # Step 2: Get platform from user (with validation)
    platform = input("🌐 Enter platform (instagram / facebook / linkedin): ").strip().lower()
    if platform not in ["instagram", "facebook", "linkedin"]:
        print(f"⚠️ Invalid platform '{platform}', defaulting to 'instagram'")
        platform = "instagram"

    # Step 3: Fetch from MongoDB
    mongo_results = fetch_from_mongo_tool._run(platform=platform)
    print(f"✅ Total prospects found in MongoDB for '{platform}': {len(mongo_results)}")
    if not mongo_results:
        print(f"⚠️ No data available for '{platform}'. You may need to run Apify once to populate data.")
    else:
        print("📋 Showing first 3 profiles:")
        print(mongo_results[:3])

    # Step 4: Run Crew pipeline
    print("\n🤖 Generating personalized message...\n")
    crew_result = crew.kickoff(inputs={"user_request": user_request})

    # Step 5: Extract final message safely
    final_message = None
    if isinstance(crew_result, dict):
        final_message = crew_result.get("final_message")
    elif hasattr(crew_result, "output"):
        final_message = crew_result.output

    print("\n=== FINAL PERSONALIZED MESSAGE ===")
    print(final_message if final_message else "⚠️ No message generated.")
