#pipeline_utils.py
import os
import random
import json
import re
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from crewai import Agent, Task, Crew, Process, LLM
from crewai.tools import BaseTool
from typing import List, Dict, Any

# -------------------------------
# MongoDB Atlas Connection Setup
# -------------------------------
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(
    MONGO_URI,
    server_api=ServerApi("1"),
    tls=True,
    tlsAllowInvalidCertificates=False
)

try:
    client.admin.command("ping")
    print("✅ Successfully connected to MongoDB Atlas!")
except Exception as e:
    print("❌ MongoDB Connection Error:", e)

# Database & collection
db = client[os.getenv("MONGO_DB", "cosmetics_app")]
audience_collection = db["audience_data"]

# -------------------------------
# Tool: Enhanced Instagram Profile Fetcher
# -------------------------------
class FetchFromMongoTool(BaseTool):
    name: str = "fetch_from_mongo"
    description: str = (
        "Fetch the most relevant Instagram profile from MongoDB based on client search terms. "
        "Returns one top profile with enhanced profile data and post analysis."
    )

    def _run(self, client_id: str = None, platform: str = None, search_terms: List[str] = None, limit: int = 1):
        try:
            # Build query based on available parameters
            query = {}
            if client_id:
                query["client_id"] = client_id
            if platform:
                query["platform"] = platform.lower()

            print(f"🔍 MongoDB Query: {query}")
            print(f"🔍 Search Terms: {search_terms}")
            
            # Fetch all matching results for relevance scoring
            all_results = list(audience_collection.find(query, {"_id": 0}))
            print(f"📊 Found {len(all_results)} total profiles in MongoDB")
            
            if not all_results:
                return []

            # Score and rank profiles based on search terms relevance
            if search_terms:
                scored_profiles = self._score_profiles_by_relevance(all_results, search_terms, platform)
            else:
                scored_profiles = all_results

            # Get top profile(s)
            top_profiles = scored_profiles[:limit]
            
            # Process and standardize the data based on platform
            processed_profiles = []
            for profile in top_profiles:
                if platform == "instagram":
                    processed_profile = self._standardize_instagram_profile(profile)
                elif platform == "linkedin":
                    processed_profile = self._standardize_linkedin_profile(profile)
                elif platform == "facebook":
                    processed_profile = self._standardize_facebook_profile(profile)
                else:
                    processed_profile = self._standardize_generic_profile(profile)
                    
                if processed_profile:
                    processed_profiles.append(processed_profile)
            
            print(f"✅ Processed {len(processed_profiles)} top relevant profiles")
            return processed_profiles

        except Exception as e:
            print(f"❌ Error in FetchFromMongoTool: {e}")
            return []

    def _score_profiles_by_relevance(self, profiles: List[Dict], search_terms: List[str], platform: str) -> List[Dict]:
        """Score profiles based on search terms relevance for different platforms"""
        if not search_terms:
            return profiles
            
        scored_profiles = []
        search_terms_lower = [term.lower() for term in search_terms]
        
        for profile in profiles:
            score = 0
            
            if platform == "instagram":
                # Instagram specific scoring
                caption = profile.get("caption", "").lower()
                hashtags = [tag.lower() for tag in profile.get("hashtags", [])]
                
                # Score based on hashtag matches (higher weight)
                for hashtag in hashtags:
                    for term in search_terms_lower:
                        if term in hashtag or hashtag in term:
                            score += 3
                
                # Score based on caption content matches
                for term in search_terms_lower:
                    if term in caption:
                        score += 2
                
                # Bonus for engagement metrics
                likes_count = profile.get("likesCount", 0)
                comments_count = profile.get("commentsCount", 0)
                if likes_count > 10:
                    score += 1
                if comments_count > 2:
                    score += 1
                    
            elif platform == "linkedin":
                # LinkedIn specific scoring
                headline = profile.get("headline", "").lower()
                summary = profile.get("summary", "").lower()
                industry = profile.get("industry", "").lower()
                experience = profile.get("experience", [])
                
                # Score based on headline matches (high weight)
                for term in search_terms_lower:
                    if term in headline:
                        score += 4
                    if term in summary:
                        score += 3
                    if term in industry:
                        score += 2
                
                # Score based on experience
                for exp in experience[:3]:  # Check latest 3 experiences
                    exp_text = str(exp).lower()
                    for term in search_terms_lower:
                        if term in exp_text:
                            score += 2
                
                # Bonus for connections
                connections = profile.get("connectionsCount", 0)
                if connections > 500:
                    score += 1
                    
            elif platform == "facebook":
                # Facebook specific scoring
                bio = profile.get("bio", "").lower()
                about = profile.get("about", "").lower()
                work = profile.get("work", [])
                posts = profile.get("posts", [])
                
                # Score based on bio/about matches
                for term in search_terms_lower:
                    if term in bio:
                        score += 3
                    if term in about:
                        score += 3
                
                # Score based on work info
                for work_item in work[:2]:  # Latest 2 work items
                    work_text = str(work_item).lower()
                    for term in search_terms_lower:
                        if term in work_text:
                            score += 2
                
                # Score based on recent posts
                for post in posts[:3]:  # Latest 3 posts
                    post_text = str(post).lower()
                    for term in search_terms_lower:
                        if term in post_text:
                            score += 1
                
                # Bonus for friends count
                friends = profile.get("friendsCount", 0)
                if friends > 200:
                    score += 1
                    
            scored_profiles.append({**profile, "relevance_score": score})
        
        # Sort by relevance score (descending)
        scored_profiles.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
        
        print(f"🎯 Top profile relevance score: {scored_profiles[0].get('relevance_score', 0) if scored_profiles else 0}")
        return scored_profiles

    def _extract_username_from_url(self, url: str) -> str:
        """Extract username from Instagram post URL"""
        if not url:
            return "unknown_user"
        
        # Instagram post URLs are like: https://www.instagram.com/p/CkxzzESOSrJ/
        # We need to get the username, but this URL format doesn't include it
        # We'll use a placeholder approach or try to extract from other fields
        match = re.search(r'instagram\.com/([^/]+)', url)
        if match:
            return match.group(1)
        return "instagram_user"

    def _standardize_instagram_profile(self, raw_profile: Dict) -> Dict:
        """Standardize Instagram profile data with enhanced profile inference"""
        try:
            # Extract username from URL or use ownerId as fallback
            username = self._extract_username_from_url(raw_profile.get("url", ""))
            if username == "instagram_user":
                username = f"user_{raw_profile.get('ownerId', 'unknown')}"
            
            # Analyze caption for bio-like content (first sentence or key info)
            caption = raw_profile.get("caption", "")
            bio_content = self._extract_bio_from_caption(caption)
            
            # Get engagement metrics for profile strength
            engagement_score = self._calculate_engagement_score(raw_profile)
            
            return {
                "username": username,
                "bio": bio_content,
                "hashtags": raw_profile.get("hashtags", [])[:10],  # Top 10 hashtags
                "recent_posts": [{
                    "caption": caption[:150] + "..." if len(caption) > 150 else caption,
                    "likes": raw_profile.get("likesCount", 0),
                    "comments": raw_profile.get("commentsCount", 0),
                    "url": raw_profile.get("url", "")
                }],
                "profile_url": f"https://www.instagram.com/{username}/",
                "platform": "instagram",
                "post_engagement": {
                    "likes": raw_profile.get("likesCount", 0),
                    "comments": raw_profile.get("commentsCount", 0),
                    "engagement_score": engagement_score
                },
                "content_type": raw_profile.get("type", "Unknown"),
                "post_date": raw_profile.get("timestamp", ""),
                "relevance_score": raw_profile.get("relevance_score", 0),
                "owner_id": raw_profile.get("ownerId", "")
            }
                
        except Exception as e:
            print(f"❌ Error standardizing Instagram profile: {e}")
            return None

    def _extract_bio_from_caption(self, caption: str) -> str:
        """Extract meaningful bio-like content from post captions"""
        if not caption:
            return "Instagram content creator"
            
        # Remove excessive hashtags and mentions for cleaner bio
        lines = caption.split('\n')
        bio_lines = []
        
        for line in lines:
            line = line.strip()
            # Skip lines that are mostly hashtags
            if line.startswith('#') or line.count('#') > 2:
                continue
            # Skip empty lines
            if not line:
                continue
            bio_lines.append(line)
            # Take first 2 meaningful lines
            if len(bio_lines) >= 2:
                break
        
        bio = ' '.join(bio_lines)
        
        # If bio is too long, truncate smartly
        if len(bio) > 100:
            bio = bio[:97] + "..."
            
        return bio if bio else "Creative Instagram content creator"

    def _calculate_engagement_score(self, profile: Dict) -> float:
        """Calculate a simple engagement score for the post"""
        likes = profile.get("likesCount", 0)
        comments = profile.get("commentsCount", 0)
        
        # Simple engagement calculation
        engagement = likes + (comments * 5)  # Comments weighted more heavily
        return round(engagement / 100, 2)  # Normalize

    def _standardize_linkedin_profile(self, raw_profile: Dict) -> Dict:
        """Standardize LinkedIn profile data"""
        try:
            return {
                "username": raw_profile.get("fullName", "") or raw_profile.get("name", "LinkedIn User"),
                "bio": raw_profile.get("headline", "") or raw_profile.get("summary", "")[:200] + "..." if len(raw_profile.get("summary", "")) > 200 else raw_profile.get("summary", ""),
                "hashtags": [],  # LinkedIn doesn't typically use hashtags in profiles
                "recent_posts": raw_profile.get("posts", [])[:3],
                "profile_url": raw_profile.get("profileUrl", "") or raw_profile.get("url", ""),
                "platform": "linkedin",
                "experience": raw_profile.get("experience", [])[:2],  # Latest 2 experiences
                "location": raw_profile.get("location", ""),
                "connections": raw_profile.get("connectionsCount", 0),
                "industry": raw_profile.get("industry", ""),
                "company": raw_profile.get("company", ""),
                "skills": raw_profile.get("skills", [])[:5],  # Top 5 skills
                "education": raw_profile.get("education", [])[:2],  # Latest 2 education entries
                "relevance_score": raw_profile.get("relevance_score", 0)
            }
        except Exception as e:
            print(f"❌ Error standardizing LinkedIn profile: {e}")
            return None

    def _standardize_facebook_profile(self, raw_profile: Dict) -> Dict:
        """Standardize Facebook profile data"""
        try:
            return {
                "username": raw_profile.get("name", "") or raw_profile.get("username", "Facebook User"),
                "bio": raw_profile.get("bio", "") or raw_profile.get("about", "")[:200] + "..." if len(raw_profile.get("about", "")) > 200 else raw_profile.get("about", ""),
                "hashtags": [],  # Facebook profiles typically don't use hashtags
                "recent_posts": raw_profile.get("posts", [])[:3],
                "profile_url": raw_profile.get("profileUrl", "") or raw_profile.get("url", ""),
                "platform": "facebook",
                "location": raw_profile.get("location", "") or raw_profile.get("city", ""),
                "friends": raw_profile.get("friendsCount", 0),
                "work": raw_profile.get("work", [])[:2],  # Latest 2 work entries
                "education": raw_profile.get("education", [])[:2],  # Latest 2 education entries
                "interests": raw_profile.get("interests", [])[:5],  # Top 5 interests
                "relationship_status": raw_profile.get("relationshipStatus", ""),
                "relevance_score": raw_profile.get("relevance_score", 0)
            }
        except Exception as e:
            print(f"❌ Error standardizing Facebook profile: {e}")
            return None

    def _standardize_generic_profile(self, raw_profile: Dict) -> Dict:
        """Generic fallback for unknown platforms"""
        try:
            return {
                "username": raw_profile.get("username", "") or raw_profile.get("name", "") or raw_profile.get("ownerUsername", "Unknown User"),
                "bio": raw_profile.get("bio", "") or raw_profile.get("description", "") or raw_profile.get("caption", "")[:200],
                "hashtags": raw_profile.get("hashtags", []),
                "recent_posts": raw_profile.get("posts", []) or raw_profile.get("latestComments", [])[:3],
                "profile_url": raw_profile.get("url", "") or raw_profile.get("profileUrl", "") or raw_profile.get("displayUrl", ""),
                "platform": raw_profile.get("platform", "unknown"),
                "location": raw_profile.get("location", "") or raw_profile.get("locationName", ""),
                "relevance_score": raw_profile.get("relevance_score", 0)
            }
        except Exception as e:
            print(f"❌ Error standardizing generic profile: {e}")
            return None

# Instantiate tool
fetch_from_mongo_tool = FetchFromMongoTool()

# -------------------------------
# LLM Setup
# -------------------------------
llm = LLM(
    model="gpt-4o-mini",
    temperature=0.7,
    max_tokens=800
)

# -------------------------------
# Enhanced Agents
# -------------------------------

platform_router = Agent(
    role="Client Data Analyzer",
    goal="Extract and structure client requirements for targeted audience discovery.",
    backstory=(
        "You are an expert in analyzing client requirements and translating them into actionable "
        "search parameters. You understand how to identify the most relevant targeting criteria "
        "from client registration data."
    ),
    allow_delegation=False,
    verbose=True,
    llm=llm
)

audience_retriever = Agent(
    role="Multi-Platform Audience Retriever",
    goal="Find and retrieve the most relevant prospect profile based on platform and client search criteria.",
    backstory=(
        "You are a data specialist who excels at finding the perfect prospect from social media data "
        "across Instagram, LinkedIn, and Facebook. You understand how to match client requirements "
        "with audience profiles using platform-specific relevance scoring and smart filtering to find "
        "the highest-quality prospects for outreach."
    ),
    tools=[fetch_from_mongo_tool],
    allow_delegation=False,
    verbose=True,
    llm=llm
)

message_generator = Agent(
    role="Multi-Platform Message Specialist",
    goal="Create highly personalized and engaging outreach messages for Instagram, LinkedIn, and Facebook.",
    backstory=(
        "You are an expert outreach specialist who creates authentic, personalized messages across "
        "different social media platforms. You understand the unique communication styles and cultures "
        "of Instagram, LinkedIn, and Facebook, and know how to reference someone's content, interests, "
        "and professional background in a way that builds genuine connection and drives engagement."
    ),
    allow_delegation=False,
    verbose=True,
    llm=llm
)

# -----------------------------
# Enhanced Crew Function
# -----------------------------

def kickoff_message_generation(client_data: dict):
    """
    Run the crew to generate personalized outreach messages for multi-platform prospects.
    
    client_data should include:
    {
        "_id": str (converted to string),
        "name": str,
        "platform": str (instagram/linkedin/facebook),
        "preferred_profession": str,  
        "preferred_location": str,
        "search_terms_with_location": list
    }
    """
    
    # Task 1: Analyze client requirements
    task1 = Task(
        description=(
            f"Analyze the client registration data and extract targeting requirements:\n"
            f"Client Data: {json.dumps(client_data, indent=2)}\n\n"
            f"Your job is to:\n"
            f"1. Extract the client_id from the _id field\n"
            f"2. Identify the platform (should be 'instagram')\n"
            f"3. Extract and clean the search terms from search_terms_with_location\n"
            f"4. Consider preferred_profession and preferred_location for context\n\n"
            f"Return ONLY a JSON object with these keys:\n"
            f"- client_id: string\n"
            f"- platform: string\n"
            f"- search_terms: array of strings\n"
            f"- preferred_profession: string\n"
            f"- preferred_location: string\n"
        ),
        agent=platform_router,
        expected_output="Clean JSON object with client_id, platform, search_terms, preferred_profession, and preferred_location"
    )

    # Task 2: Find most relevant prospect
    task2 = Task(
        description=(
            "Find the most relevant prospect using the client requirements.\n\n"
            "Steps:\n"
            "1. Use the fetch_from_mongo tool with the client_id, platform, and search_terms from Task 1\n"
            "2. The tool will return the top-scoring profile based on platform-specific relevance\n"
            "3. Return the profile data exactly as received from the tool\n\n"
            "Call the tool like this:\n"
            "fetch_from_mongo(client_id='extracted_id', platform='platform_name', search_terms=['term1', 'term2'])\n\n"
            "The tool supports: instagram, linkedin, facebook platforms"
        ),
        agent=audience_retriever,
        context=[task1],
        expected_output=(
            "A single profile dictionary with standardized fields based on platform: "
            "username, bio, platform, profile_url, relevance_score, and platform-specific data"
        )
    )

    # Task 3: Generate personalized message
    task3 = Task(
        description=(
            "Create a highly personalized outreach message based on the prospect's platform and profile data.\n\n"
            "Platform-Specific Guidelines:\n\n"
            "📸 INSTAGRAM:\n"
            "- Use casual, friendly tone with emojis\n"
            "- Reference specific content/hashtags from their posts\n"
            "- Keep it short (2-3 sentences max)\n"
            "- Ask engaging questions about their content\n"
            "- Avoid obvious sales language\n\n"
            "💼 LINKEDIN:\n"
            "- Professional but warm tone\n"
            "- Reference their industry, experience, or recent posts\n"
            "- Connect their background to beauty/cosmetics professionally\n"
            "- Mention shared connections or interests\n"
            "- 3-4 sentences, more detailed than Instagram\n\n"
            "📘 FACEBOOK:\n"
            "- Friendly, conversational tone\n"
            "- Reference shared interests or location\n"
            "- Connect through mutual interests or local community\n"
            "- More personal approach than LinkedIn\n"
            "- 2-4 sentences depending on context\n\n"
            "General Rules:\n"
            "1. Address them by name/username naturally\n"
            "2. Show genuine interest in their content/background\n"
            "3. Subtly connect to beauty/cosmetics when relevant\n"
            "4. Include a natural conversation starter\n"
            "5. Feel authentic, not templated\n\n"
            "Format your response as:\n"
            "Platform: [platform name]\n"
            "Target: [username/name]\n"
            "Message: [your personalized message]\n"
            "Reasoning: [brief explanation of personalization elements used]"
        ),
        agent=message_generator,
        context=[task2],
        expected_output=(
            "A platform-appropriate personalized message with target name, message content, and reasoning"
        )
    )

    # Create and run crew
    crew = Crew(
        agents=[platform_router, audience_retriever, message_generator],
        tasks=[task1, task2, task3],
        process=Process.sequential,
        verbose=True,
        max_iter=3,
    )

    try:
        result = crew.kickoff(inputs={"client_data": client_data})
        print("✅ Multi-platform outreach message generation completed!")
        return result
    except Exception as e:
        print(f"❌ Error in crew execution: {e}")
        return {"error": str(e), "final_output": "Failed to generate personalized message"}

# Helper function for testing
def test_multi_platform_pipeline():
    """Test the pipeline with sample client data for different platforms"""
    
    # Instagram test
    instagram_client = {
        "_id": "test_client_123_ig",
        "name": "Beauty Brand Co",
        "platform": "instagram", 
        "preferred_profession": "lifestyle blogger",
        "preferred_location": "New York",
        "search_terms_with_location": ["beauty", "skincare", "makeup", "lifestyle", "nyc"]
    }
    
    # LinkedIn test
    linkedin_client = {
        "_id": "test_client_456_li",
        "name": "Professional Cosmetics Corp",
        "platform": "linkedin",
        "preferred_profession": "marketing manager",
        "preferred_location": "San Francisco",
        "search_terms_with_location": ["beauty industry", "cosmetics", "marketing", "skincare", "san francisco"]
    }
    
    # Facebook test
    facebook_client = {
        "_id": "test_client_789_fb",
        "name": "Local Beauty Store",
        "platform": "facebook",
        "preferred_profession": "beauty enthusiast",
        "preferred_location": "Los Angeles",
        "search_terms_with_location": ["makeup", "beauty", "cosmetics", "skincare", "los angeles"]
    }
    
    results = {}
    for platform, client_data in [
        ("instagram", instagram_client),
        ("linkedin", linkedin_client), 
        ("facebook", facebook_client)
    ]:
        print(f"\n🧪 Testing {platform.upper()} pipeline...")
        results[platform] = kickoff_message_generation(client_data)
    
    return results