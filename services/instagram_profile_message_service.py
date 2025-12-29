# services/instagram_profile_message_service.py
"""
Instagram Profile Message Service - Scrape profiles and generate personalized messages
"""
import os
import json
from typing import List, Dict
from dotenv import load_dotenv
from apify_client import ApifyClient
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

load_dotenv()

# Initialize Apify Client
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
if not APIFY_API_TOKEN:
    raise ValueError("Missing APIFY_API_TOKEN in .env")

apify_client = ApifyClient(APIFY_API_TOKEN)

# Initialize OpenAI
openai_api_key = os.getenv("OPENAI_API_KEY")
if not openai_api_key:
    raise ValueError("OPENAI_API_KEY not found in environment variables")

llm = ChatOpenAI(
    model="gpt-3.5-turbo",
    temperature=0.7,
    openai_api_key=openai_api_key
)


def scrape_instagram_profile(profile_url: str) -> Dict:
    """
    Scrape complete Instagram profile data using Apify Instagram Profile Scraper
    
    Args:
        profile_url: Instagram profile URL (e.g., https://www.instagram.com/username/)
    
    Returns:
        dict: Complete profile data including bio, posts, engagement, recent content, etc.
    """
    try:
        print(f"\n📸 Scraping complete Instagram profile: {profile_url}")
        
        # Extract username from URL
        username = profile_url.split('/')[-1].strip('/') if '/' in profile_url else profile_url
        
        actor_id = "apify/instagram-profile-scraper"
        payload = {
            "usernames": [username],
            "resultsLimit": 1,
            "addParentData": True,  # Include additional data
            "extendOutputFunction": "",  # Can be used for custom data extraction
            "extendScraperFunction": ""
        }
        
        print(f"🚀 Running Instagram Profile Scraper for @{username}...")
        run = apify_client.actor(actor_id).call(run_input=payload)
        
        if not run or "defaultDatasetId" not in run:
            print("⚠️ Instagram actor returned no dataset")
            return {}
        
        items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
        
        if not items:
            print("⚠️ No profile data found")
            return {}
        
        profile_data = items[0]
        
        # Extract comprehensive profile information
        comprehensive_data = {
            # Basic Info
            "username": profile_data.get('username', username),
            "fullName": profile_data.get('fullName', ''),
            "biography": profile_data.get('biography', ''),
            "externalUrl": profile_data.get('externalUrl', ''),
            "externalUrlShimmed": profile_data.get('externalUrlShimmed', ''),
            
            # Counts
            "followersCount": profile_data.get('followersCount', 0),
            "followsCount": profile_data.get('followsCount', 0),
            "postsCount": profile_data.get('postsCount', 0),
            
            # Account Type
            "verified": profile_data.get('verified', False),
            "private": profile_data.get('private', False),
            "businessCategoryName": profile_data.get('businessCategoryName', ''),
            "categoryName": profile_data.get('categoryName', ''),
            
            # Profile Picture
            "profilePicUrl": profile_data.get('profilePicUrl', ''),
            "profilePicUrlHD": profile_data.get('profilePicUrlHD', ''),
            
            # Latest Posts (if available)
            "latestPosts": [],
            
            # Engagement metrics
            "engagementRate": 0,
            "avgLikes": 0,
            "avgComments": 0
        }
        
        # Extract latest posts data
        if 'latestPosts' in profile_data and profile_data['latestPosts']:
            posts = profile_data['latestPosts'][:12]  # Get up to 12 recent posts
            comprehensive_data['latestPosts'] = []
            
            total_likes = 0
            total_comments = 0
            post_count = 0
            
            for post in posts:
                post_info = {
                    "caption": post.get('caption', '')[:500],  # First 500 chars
                    "likesCount": post.get('likesCount', 0),
                    "commentsCount": post.get('commentsCount', 0),
                    "timestamp": post.get('timestamp', ''),
                    "type": post.get('type', 'Image'),  # Image, Video, Sidecar
                    "url": post.get('url', ''),
                    "displayUrl": post.get('displayUrl', ''),
                    "hashtags": post.get('hashtags', []),
                    "mentions": post.get('mentions', [])
                }
                comprehensive_data['latestPosts'].append(post_info)
                
                # Calculate engagement
                total_likes += post.get('likesCount', 0)
                total_comments += post.get('commentsCount', 0)
                post_count += 1
            
            # Calculate average engagement
            if post_count > 0:
                comprehensive_data['avgLikes'] = total_likes // post_count
                comprehensive_data['avgComments'] = total_comments // post_count
                
                # Calculate engagement rate (likes + comments) / followers * 100
                if comprehensive_data['followersCount'] > 0:
                    total_engagement = total_likes + total_comments
                    comprehensive_data['engagementRate'] = round(
                        (total_engagement / (post_count * comprehensive_data['followersCount'])) * 100, 
                        2
                    )
        
        print(f"✅ Successfully scraped complete profile: @{comprehensive_data['username']}")
        print(f"   👤 Name: {comprehensive_data['fullName']}")
        print(f"   📊 Followers: {comprehensive_data['followersCount']:,}")
        print(f"   📝 Posts: {comprehensive_data['postsCount']:,}")
        print(f"   📸 Latest Posts Scraped: {len(comprehensive_data['latestPosts'])}")
        print(f"   💬 Avg Engagement: {comprehensive_data['avgLikes']} likes, {comprehensive_data['avgComments']} comments")
        print(f"   📈 Engagement Rate: {comprehensive_data['engagementRate']}%")
        
        return comprehensive_data
        
    except Exception as e:
        print(f"❌ Error scraping Instagram profile: {str(e)}")
        import traceback
        traceback.print_exc()
        return {}


def generate_personalized_message(
    profile_data: Dict,
    campaign_message: str,
    campaign_description: str = ""
) -> str:
    """
    Generate highly personalized message using complete profile data and campaign message template
    **STRICT 280 CHARACTER LIMIT** for Instagram invitations
    
    Args:
        profile_data: Complete scraped Instagram profile data with posts
        campaign_message: Campaign message template saved during campaign creation
        campaign_description: Campaign description for context
    
    Returns:
        str: Highly personalized message (max 280 chars) for Instagram invitations
    """
    try:
        print(f"\n🤖 Generating personalized message (280 char limit for invitations)...")
        
        # Extract comprehensive profile information
        username = profile_data.get('username', 'there')
        full_name = profile_data.get('fullName', '')
        bio = profile_data.get('biography', '')
        followers = profile_data.get('followersCount', 0)
        following = profile_data.get('followsCount', 0)
        posts_count = profile_data.get('postsCount', 0)
        verified = profile_data.get('verified', False)
        business_category = profile_data.get('businessCategoryName', '')
        category = profile_data.get('categoryName', '')
        external_url = profile_data.get('externalUrl', '')
        engagement_rate = profile_data.get('engagementRate', 0)
        avg_likes = profile_data.get('avgLikes', 0)
        avg_comments = profile_data.get('avgComments', 0)
        
        # Analyze recent posts in detail
        recent_posts_analysis = []
        post_themes = []
        hashtags_used = []
        
        if 'latestPosts' in profile_data and profile_data['latestPosts']:
            for idx, post in enumerate(profile_data['latestPosts'][:6], 1):  # Analyze top 6 posts
                post_analysis = {
                    'post_number': idx,
                    'caption': post.get('caption', '')[:300],  # First 300 chars
                    'likes': post.get('likesCount', 0),
                    'comments': post.get('commentsCount', 0),
                    'type': post.get('type', 'Image'),
                    'hashtags': post.get('hashtags', [])[:5],  # Top 5 hashtags
                    'mentions': post.get('mentions', [])[:3]  # Top 3 mentions
                }
                recent_posts_analysis.append(post_analysis)
                
                # Collect hashtags for theme analysis
                hashtags_used.extend(post.get('hashtags', []))
        
        # Get most common hashtags (themes)
        if hashtags_used:
            from collections import Counter
            hashtag_counts = Counter(hashtags_used)
            post_themes = [tag for tag, count in hashtag_counts.most_common(5)]
        
        # Build comprehensive profile summary for AI
        profile_summary = f"""
=== PROFILE OVERVIEW ===
Username: @{username}
Full Name: {full_name}
Bio: {bio}
External Link: {external_url}
Category: {category or business_category or 'Not specified'}
Verified: {'Yes ✓' if verified else 'No'}

=== AUDIENCE & ENGAGEMENT ===
Followers: {followers:,}
Following: {following:,}
Total Posts: {posts_count:,}
Engagement Rate: {engagement_rate}%
Average Likes per Post: {avg_likes:,}
Average Comments per Post: {avg_comments:,}

=== CONTENT THEMES ===
Common Hashtags: {', '.join(post_themes) if post_themes else 'Not available'}

=== RECENT POSTS ANALYSIS ===
{json.dumps(recent_posts_analysis, indent=2) if recent_posts_analysis else 'No recent posts available'}
"""
        
        # Create advanced personalization prompt with STRICT 280 CHAR LIMIT
        prompt_template = ChatPromptTemplate.from_messages([
            (
                "system",
                """
You are an expert in ULTRA-CONCISE Instagram outreach writing.
Your role is to create SHORT, warm, personalized messages for Instagram follow requests.

🚨 CRITICAL CONSTRAINT 🚨
MAXIMUM LENGTH: 280 CHARACTERS (not words, CHARACTERS!)
This is NON-NEGOTIABLE. Messages MUST be under 280 characters to work with Instagram invitations.

THE MOST IMPORTANT LOGIC:
1. First analyze the profile's themes, comments, and hashtags.
2. Select ONLY the ones that are truly relevant to the sender's campaign message.
3. If no relevant themes exist, use their bio and general niche for personalization.
4. Never force irrelevant themes just to personalize.

PROFILE ANALYSIS:
- Understand their niche, tone, audience, and brand identity.
- Identify repeating themes and frequently used hashtags.
- Analyze values they represent.

STRICT RULES:
- MAXIMUM 280 CHARACTERS - Count every character including spaces!
- Do NOT mention specific posts, reels, captions, or stories.
- ABSOLUTELY NO EMOJIS OR EMOTICONS.
- Keep it to 3-4 SHORT sentences maximum.
- Be warm, genuine, and conversational but BRIEF.
- Don't waste characters on generic phrases.
- Get straight to the point with personalized relevance.

MESSAGE STRUCTURE (within 280 chars):
1. Brief warm greeting (use their name if short)
2. ONE specific, relevant compliment/observation
3. Quick connection to your campaign
4. Simple call-to-action

EXAMPLES OF GOOD 280-CHAR MESSAGES:

Example 1 (268 chars):
"Hey Sarah! Fellow content creator in LA here. Your focus on sustainable fashion really stands out. I'm building a network of eco-conscious creators and would love to connect. Open to collaborating on green lifestyle content. Let me know if you're interested!"

Example 2 (275 chars):
"Hi Alex! Your tech tutorials are super helpful. I'm launching a developer community for indie makers and your approach to teaching would be perfect. Would love to have you join and maybe collaborate on some beginner-friendly coding content. Interested in connecting?"

OUTPUT RULE:
Only output the final message text. No formatting, no extra explanation, ZERO EMOJIS, and MUST be under 280 characters.
                """
            ),
            (
                "user",
                """
Use the data below to create ONE highly personalized Instagram message:

=== PROFILE DATA ===
{profile_summary}

=== MY CAMPAIGN MESSAGE ===
{campaign_message}

=== CAMPAIGN CONTEXT ===
{campaign_context}

TASK:
1. Analyze profile to understand their niche and relevant themes.
2. Select ONLY themes that strengthen my campaign message.
3. Write a warm, natural, BRIEF message.
4. Blend campaign message into personalization.
5. End with a soft call-to-action.
6. 🚨 CRITICAL: Keep UNDER 280 CHARACTERS total (count carefully!)
7. No emojis, no formatting, no post references.

Generate the final message now (remember: 280 character limit!):
                """
            )
        ])
        
        chain = prompt_template | llm
        response = chain.invoke({
            "profile_summary": profile_summary,
            "campaign_message": campaign_message,
            "campaign_context": campaign_description or "General outreach campaign"
        })
        
        personalized_message = response.content.strip()
        
        # SAFETY CHECK: Enforce 280 character limit
        if len(personalized_message) > 280:
            print(f"⚠️ Message too long ({len(personalized_message)} chars), truncating to 280...")
            personalized_message = personalized_message[:277] + "..."
        
        print(f"✅ Generated personalized message ({len(personalized_message)}/280 chars)")
        print(f"   📊 Used data from {len(recent_posts_analysis)} posts")
        print(f"   🎯 Identified themes: {', '.join(post_themes[:3]) if post_themes else 'None'}")
        
        return personalized_message
        
    except Exception as e:
        print(f"❌ Error generating personalized message: {str(e)}")
        import traceback
        traceback.print_exc()
        # Fallback: truncate campaign message if generation fails
        fallback = campaign_message[:277] + "..." if len(campaign_message) > 280 else campaign_message
        return fallback


def process_prospects_with_profile_scraping(
    prospects: List[Dict],
    campaign_message: str,
    campaign_description: str = "",
    progress_callback = None
) -> List[Dict]:
    """
    Process multiple prospects: scrape their profiles and generate personalized messages
    
    Args:
        prospects: List of prospect dictionaries with profile_url
        campaign_message: Campaign message template
        campaign_description: Campaign description
        progress_callback: Optional callback function called after each prospect is processed
    
    Returns:
        List of prospects with generated messages (max 280 chars each)
    """
    results = []
    
    for idx, prospect in enumerate(prospects, 1):
        try:
            username = prospect.get('username', '')
            profile_url = prospect.get('profile_url', '')
            
            if not profile_url:
                print(f"⚠️ No profile URL for @{username}, skipping...")
                results.append({
                    'username': username,
                    'success': False,
                    'error': 'No profile URL'
                })
                continue
            
            # Scrape profile
            profile_data = scrape_instagram_profile(profile_url)
            
            if not profile_data:
                print(f"⚠️ Failed to scrape profile for @{username}")
                results.append({
                    'username': username,
                    'success': False,
                    'error': 'Failed to scrape profile'
                })
                continue
            
            # Generate personalized message (280 char limit)
            personalized_message = generate_personalized_message(
                profile_data,
                campaign_message,
                campaign_description
            )
            
            results.append({
                'username': username,
                'success': True,
                'generated_message': personalized_message,
                'message_length': len(personalized_message),
                'profile_data': {
                    'full_name': profile_data.get('fullName', ''),
                    'bio': profile_data.get('biography', ''),
                    'followers': profile_data.get('followersCount', 0),
                    'posts': profile_data.get('postsCount', 0)
                }
            })
            
            print(f"✅ Successfully processed @{username} ({len(personalized_message)}/280 chars)")
            
            # Call progress callback if provided
            if progress_callback:
                progress_callback(idx, len(prospects), username, True)
            
        except Exception as e:
            print(f"❌ Error processing prospect @{prospect.get('username', 'unknown')}: {str(e)}")
            results.append({
                'username': prospect.get('username', ''),
                'success': False,
                'error': str(e)
            })
            
            # Call progress callback even for errors
            if progress_callback:
                progress_callback(idx, len(prospects), prospect.get('username', ''), False)
    
    return results