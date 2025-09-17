import os
import random
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from crewai import Agent, Task, Crew, Process, LLM
from crewai.tools import BaseTool

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
# Tool: Read-Only Fetch from DB
# -------------------------------
class FetchFromMongoTool(BaseTool):
    name: str = "fetch_from_mongo"
    description: str = (
        "Fetch a single prospect from MongoDB for a given platform, profession, "
        "and location. Filters audience data and returns one random match."
    )

    def _run(self, platform: str, profession: str = None, location: str = None):
        query = {"platform": platform}
        if profession:
            query["profession"] = profession
        if location:
            query["location"] = location

        results = list(audience_collection.find(query, {"_id": 0}))
        if not results:
            return None  # No data found
        chosen = random.choice(results)  # Randomly pick a profile

        return {
            "username": chosen.get("username"),
            "bio": chosen.get("bio") or chosen.get("caption"),
            "hashtags": chosen.get("hashtags", []),
            "recent_posts": chosen.get("latestComments", []),
            "profile_url": chosen.get("url")
        }

# Instantiate tool
fetch_from_mongo_tool = FetchFromMongoTool()

# -------------------------------
# LLM Setup
# -------------------------------
llm = LLM(
    model="gpt-4o-mini",
    temperature=0.7,
    max_tokens=500
)

# -------------------------------
# Agents
# -------------------------------

platform_router = Agent(
    role="Platform Router",
    goal="Identify platform, professsion, search terms, and location from client registration data.",
    backstory="Expert in routing tasks based on platform and context.",
    allow_delegation=False,
    verbose=True,
)

audience_retriever = Agent(
    role="Audience Retriever",
    goal="Retrieve relevant audience data from MongoDB based on client platform, search terms, and location.",
    backstory="Knows how to query MongoDB efficiently.",
    tools=[fetch_from_mongo_tool],
    allow_delegation=False,
    verbose=True,
)

message_generator = Agent(
    role="Message Generator",
    goal="Generate natural, friendly outreach DMs using audience data with their name, location and interest exposed in that post.",
    backstory="Expert in writing human-like DMs that sound warm and professional.",
    allow_delegation=False,
    verbose=True,
)

# -----------------------------
# 2. Crew
# -----------------------------

def kickoff_message_generation(client_data: dict):
    """
    Run the crew to generate personalized outreach messages.
    client_data should include:
    {
        "name": str,
        "platform": str,
        "preferred_profession": str,
        "preferred_location": str,
        "search_terms_with_location": list
    }
    """
    task1 = Task(
        description=(
            f"Analyze client registration input: {client_data['role']}, "
            f"{client_data['platform']}, {client_data.get('preferred_profession')}, "
            f"{client_data.get('preferred_location')}, {client_data['search_terms_with_location']}. "
            "Return JSON with platform, preferred_profession, and preferred_location."
        ),
        agent=platform_router,
        expected_output=(
            "{'platform': '<platform>', 'preferred_profession': '<preferred_profession>', 'preferred_location': '<preferred_location>'}"
        )
    )

    # Task 2: Fetch audience profiles
    task2 = Task(
        description=(
            "Use fetch_from_mongo to retrieve audience profiles using platform, search terms with location, and preferred location. "
            "Return audience data as a list of dictionaries in JSON format."
            "### Few-shot Example:\n"
            "**Input:**\n"
            "{'platform': 'instagram', 'preferred_profession': 'makeup artist', 'preferred_location': 'Mumbai'}\n\n"
            "**Output:**\n"
            "[\n"
            "  {\n"
            "    'username': 'coffeelover_mumbai',\n"
            "    'bio': 'Something to walk with… warm.. to go.',\n"
            "    'hashtags': ['WinterWarmer', 'Pastry', 'Coffee', 'coffeelover'],\n"
            "    'recent_posts': [],\n"
            "    'profile_url': 'https://www.instagram.com/p/CkxzzESOSrJ/'\n"
            "  }\n"
            "]\n\n"
            "Make sure the output follows the exact same format, with keys: username, bio, hashtags, recent_posts, profile_url."

        ),
        agent=audience_retriever,
        context=[task1],  # Pass Task 1 output
        expected_output="A list of audience profiles in JSON format, each with keys like username, profession, location."
    )

    # Task 3: Generate personalized outreach message
    task3 = Task(
        description=(
            "Generate a friendly, natural DM message for each audience profile. "
            "Mention the prospect username if available, include profession and location context, "
            "and sound human."
        ),
        agent=message_generator,
        context=[task2],  # Pass Task 2 output
        expected_output="A list of 3 personalized outreach messages in plain text."
    )

    crew = Crew(
        agents=[platform_router, audience_retriever, message_generator],
        tasks=[task1, task2, task3],
        process=Process.sequential,
        llm=llm,
        max_iter=3,
        max_time=60,
        verbose=True,
    )

    # ✅ Correct kickoff call
    return crew.kickoff(inputs={"client_data": client_data})
