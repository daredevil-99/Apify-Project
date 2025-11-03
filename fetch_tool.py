from crewai.tools import BaseTool
from typing import List, Dict, Any
from db_config import audience_collection  
import re


# -------------------------------
# Enhanced Tool with Strict Platform Validation
# -------------------------------
class FetchFromMongoTool(BaseTool):
    name: str = "fetch_from_mongo"
    description: str = (
        "Fetch the most relevant profile from MongoDB based on client search terms for the specified platform ONLY. "
        "Returns platform-specific profile data with content validation."
    )
    
    # --- Utility function for LinkedIn ---
    def sanitize_linkedin_profile(self, profile: dict) -> dict:
        """Ensure all string fields are non-None before any further processing"""
        keys_to_strip = ["firstName", "lastName", "fullName", "headline", "summary", "about"]
        cleaned = {k: (profile.get(k) or "").strip() for k in keys_to_strip}
        cleaned["experience"] = profile.get("experience") or []
        cleaned["publicIdentifier"] = profile.get("publicIdentifier")
        cleaned["linkedinUrl"] = profile.get("linkedinUrl")
        cleaned["photo"] = profile.get("photo")
        cleaned["location"] = profile.get("location") or {}
        return cleaned
    
    def _run(self, client_id: str = None, platform: str = None, search_terms: List[str] = None, limit: int = 1):
            try:
                # Build query with strict platform filtering
                query = {}
                if client_id:
                    query["client_id"] = client_id
                if platform:
                    query["platform"] = platform.lower()

                print(f"🔍 MongoDB Query: {query}")
                print(f"🔍 Search Terms: {search_terms}")
                print(f"🔍 Platform Filter: {platform}")
                
                # ✅ UPDATED: Sort by location relevance first, then fetch
                sort_criteria = [
                    ("location_relevance_score", -1),  # Highest location score first
                    ("fetched_at", -1)  # Most recent if scores are equal
                ]
                
                # Fetch all matching results with sorting
                all_results = list(
                    audience_collection.find(query, {"_id": 0})
                    .sort(sort_criteria)
                    .limit(50)  # Get more results to filter from
                )
                
                print(f"📊 Found {len(all_results)} total profiles for {platform.upper()}")
                
                # ✅ Log location scores for debugging
                if all_results:
                    top_scores = [r.get('location_relevance_score', 0) for r in all_results[:5]]
                    print(f"📍 Top 5 location scores: {top_scores}")
                
                if not all_results:
                    print(f"❌ No data found for platform: {platform}")
                    return {"error": f"No {platform} data found", "platform": platform}

                # Validate data quality before processing
                valid_results = self._validate_platform_data(all_results, platform)
                
                if not valid_results:
                    print(f"❌ No valid {platform} data found after validation")
                    return {"error": f"No valid {platform} content found", "platform": platform, "raw_data": all_results[:1]}

                # Score and rank profiles based on search terms relevance
                if search_terms:
                    scored_profiles = self._score_profiles_by_relevance(valid_results, search_terms, platform)
                else:
                    scored_profiles = valid_results

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
                        
                    if processed_profile and processed_profile.get("has_valid_content", False):
                        processed_profiles.append(processed_profile)
                
                if not processed_profiles:
                    print(f"❌ No valid processed profiles for {platform}")
                    return {
                        "error": f"No valid {platform} profiles after processing", 
                        "platform": platform,
                        "raw_data_sample": all_results[:1]
                    }
                
                # ✅ Log the selected profile's location score
                selected_profile = processed_profiles[0]
                location_score = selected_profile.get('location_relevance_score', 0)
                print(f"✅ Selected profile with location score: {location_score}")
                print(f"✅ Processed {len(processed_profiles)} valid {platform} profiles")
                
                return selected_profile

            except Exception as e:
                print(f"❌ Error in FetchFromMongoTool: {e}")
                return {"error": str(e), "platform": platform or "unknown"}


        # ✅ ADD THIS NEW METHOD to _score_profiles_by_relevance (around line 180)
        # Update the existing method to include location score weighting

    def _score_profiles_by_relevance(self, profiles: List[Dict], search_terms: List[str], platform: str) -> List[Dict]:
            """Score profiles based on search terms relevance AND location for different platforms"""
            if not search_terms:
                return profiles

            scored_profiles = []
            search_terms_lower = [term.lower() for term in search_terms]

            for profile in profiles:
                score = 0
                
                # ✅ ADD LOCATION SCORE BONUS (weighted heavily)
                location_score = profile.get('location_relevance_score', 0)
                score += location_score * 2  # Double weight for location relevance

                if platform == "facebook":
                    categories = [c.lower() for c in profile.get("categories", [])]
                    info_text = " ".join(profile.get("info", [])).lower()
                    title = profile.get("title", "").lower()
                    about_me = profile.get("about_me", {}).get("text", "").lower()

                    for term in search_terms_lower:
                        if any(term in c for c in categories):
                            score += 4
                        if term in info_text:
                            score += 3
                        if term in title:
                            score += 2
                        if term in about_me:
                            score += 3

                    # Bonus scoring
                    score += int(profile.get("likes", 0) / 100)
                    score += int(profile.get("followers", 0) / 100)
                    if profile.get("ratingOverall"):
                        score += int(profile["ratingOverall"])
                                            
                elif platform == "instagram":
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
                    for exp in experience[:3]:
                        exp_text = str(exp).lower()
                        for term in search_terms_lower:
                            if term in exp_text:
                                score += 2
                    
                    # Bonus for connections
                    connections = profile.get("connectionsCount", 0)
                    if connections > 500:
                        score += 1
                        
                scored_profiles.append({**profile, "relevance_score": score})
            
            # Sort by combined relevance score (descending)
            scored_profiles.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
            
            if scored_profiles:
                top_profile = scored_profiles[0]
                print(f"🎯 Top {platform} profile - Total score: {top_profile.get('relevance_score', 0)}, "
                    f"Location score: {top_profile.get('location_relevance_score', 0)}")
            
            return scored_profiles


        # ✅ UPDATE _standardize_instagram_profile to preserve location score (around line 350)

    def _standardize_instagram_profile(self, raw_profile: Dict) -> Dict:
            """Standardize Instagram profile data with enhanced validation"""
            try:
                # Extract username from URL or use ownerId as fallback
                username = self._extract_username_from_url(raw_profile.get("url", ""))
                if username == "instagram_user":
                    username = f"user_{raw_profile.get('ownerId', 'unknown')}"
                
                # Analyze caption for bio-like content
                caption = raw_profile.get("caption", "")
                bio_content = self._extract_bio_from_caption(caption)
                
                # Determine content validity
                has_valid_content = bool(
                    caption or 
                    raw_profile.get("hashtags") or 
                    raw_profile.get("ownerId")
                )
                
                # Get engagement metrics
                engagement_score = self._calculate_engagement_score(raw_profile)
                
                return {
                    "username": username,
                    "bio": bio_content,
                    "hashtags": raw_profile.get("hashtags", [])[:10],
                    "platform": "instagram",
                    "has_valid_content": has_valid_content,
                    "recent_posts": [{
                        "caption": caption[:150] + "..." if len(caption) > 150 else caption,
                        "likes": raw_profile.get("likesCount", 0),
                        "comments": raw_profile.get("commentsCount", 0),
                        "url": raw_profile.get("url", "")
                    }],
                    "profile_url": f"https://www.instagram.com/{username}/",
                    "post_engagement": {
                        "likes": raw_profile.get("likesCount", 0),
                        "comments": raw_profile.get("commentsCount", 0),
                        "engagement_score": engagement_score
                    },
                    "content_type": raw_profile.get("type", "Unknown"),
                    "post_date": raw_profile.get("timestamp", ""),
                    "relevance_score": raw_profile.get("relevance_score", 0),
                    "location_relevance_score": raw_profile.get("location_relevance_score", 0),  # ✅ PRESERVE THIS
                    "owner_id": raw_profile.get("ownerId", "")
                }
                    
            except Exception as e:
                print(f"❌ Error standardizing Instagram profile: {e}")
                return None


    def _validate_platform_data(self, profiles: List[Dict], platform: str) -> List[Dict]:
        """Validate that profiles have meaningful content for the specified platform"""
        valid_profiles = []
        
        for profile in profiles:
            is_valid = False

            if platform == "facebook":
                # Apify FB page structure
                categories = profile.get("categories", [])
                info = profile.get("info", [])
                likes = profile.get("likes") or 0
                followers = profile.get("followers") or 0
                about_me = (profile.get("about_me") or {}).get("text", "")

                if categories or info or about_me or likes > 0 or followers > 0:
                    is_valid = True
                    valid_profiles.append(profile)

            elif platform == "instagram":
                caption = profile.get("caption") or ""
                caption = caption.strip() if caption else ""
                hashtags = profile.get("hashtags", [])
                owner_id = profile.get("ownerId")
                if caption or hashtags or owner_id:
                    is_valid = True
                    valid_profiles.append(profile)

            elif platform == "linkedin":
                # Clean & normalize LinkedIn fields
                profile_cleaned = {
                    "firstName": (profile.get("firstName") or "").strip(),
                    "lastName": (profile.get("lastName") or "").strip(),
                    "fullName": (profile.get("fullName") or profile.get("name") or "").strip(),
                    "headline": (profile.get("headline") or "").strip(),
                    "summary": (profile.get("summary") or "").strip(),
                    "about": (profile.get("about") or "").strip(),
                    "experience": profile.get("experience") or [],
                    "publicIdentifier": profile.get("publicIdentifier"),
                    "linkedinUrl": profile.get("linkedinUrl"),
                    "photo": profile.get("photo"),
                    "location": profile.get("location") or {},
                }

                # Validation logic
                name = profile_cleaned["fullName"] or f'{profile_cleaned["firstName"]} {profile_cleaned["lastName"]}'.strip()
                headline = profile_cleaned["headline"]
                summary = profile_cleaned["summary"]
                about = profile_cleaned["about"]
                experience = profile_cleaned["experience"]

                # Check if profile has meaningful content
                if name and name.lower() != "linkedin user" and (headline or summary or about or experience):
                    is_valid = True
                    valid_profiles.append(profile_cleaned)  # Append cleaned version
                
            else:
                # Generic validation for other platforms
                if profile.get("username") or profile.get("bio") or profile.get("caption"):
                    is_valid = True
                    valid_profiles.append(profile)

        print(f"✅ Validated {len(valid_profiles)}/{len(profiles)} {platform} profiles")
        return valid_profiles


    def _score_profiles_by_relevance(self, profiles: List[Dict], search_terms: List[str], platform: str) -> List[Dict]:
        """Score profiles based on search terms relevance for different platforms"""
        if not search_terms:
            return profiles

        scored_profiles = []
        search_terms_lower = [term.lower() for term in search_terms]

        for profile in profiles:
            score = 0

            if platform == "facebook":
                categories = [c.lower() for c in profile.get("categories", [])]
                info_text = " ".join(profile.get("info", [])).lower()
                title = profile.get("title", "").lower()
                about_me = profile.get("about_me", {}).get("text", "").lower()

                for term in search_terms_lower:
                    if any(term in c for c in categories):
                        score += 4
                    if term in info_text:
                        score += 3
                    if term in title:
                        score += 2
                    if term in about_me:
                        score += 3

                # Bonus scoring
                score += int(profile.get("likes", 0) / 100)  # 1 point per 100 likes
                score += int(profile.get("followers", 0) / 100)  # 1 point per 100 followers
                if profile.get("ratingOverall"):
                    score += int(profile["ratingOverall"])
                                        
            elif platform == "instagram":
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
                    
            scored_profiles.append({**profile, "relevance_score": score})
        
        # Sort by relevance score (descending)
        scored_profiles.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
        
        print(f"🎯 Top {platform} profile relevance score: {scored_profiles[0].get('relevance_score', 0) if scored_profiles else 0}")
        return scored_profiles

    def _extract_username_from_url(self, url: str) -> str:
        """Extract username from social media URL"""
        if not url:
            return "unknown_user"
        
        # Try to extract username from various URL formats
        patterns = [
            r'facebook\.com/([^/?]+)',  # Facebook profile
            r'instagram\.com/([^/?]+)',  # Instagram profile
            r'linkedin\.com/in/([^/?]+)'  # LinkedIn profile
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        return "social_user"

    def _standardize_facebook_profile(self, raw_profile: Dict) -> Dict:
        """Standardize Facebook page/profile data from Apify actor"""
        try:
            page_name = raw_profile.get("pageName", "")
            categories = raw_profile.get("categories", [])
            info = " ".join(raw_profile.get("info", []))
            about_me = raw_profile.get("about_me", {}).get("text", "")
            likes = raw_profile.get("likes", 0)
            followers = raw_profile.get("followers", 0)

            username = page_name or self._extract_username_from_url(raw_profile.get("pageUrl", ""))
            bio_parts = []

            if categories:
                bio_parts.append(", ".join(categories))
            if about_me:
                bio_parts.append(about_me[:120])
            elif info:
                bio_parts.append(info[:120])

            bio = " | ".join(bio_parts) if bio_parts else "Facebook business/page"

            has_valid_content = bool(categories or info or about_me or likes or followers)

            standardized_profile = {
                "username": username,
                "bio": bio,
                "platform": "facebook",
                "has_valid_content": has_valid_content,
                "profile_url": raw_profile.get("pageUrl", ""),
                "contact": {
                    "phone": raw_profile.get("phone"),
                    "email": raw_profile.get("email"),
                    "website": raw_profile.get("website")
                },
                "metrics": {
                    "likes": likes,
                    "followers": followers,
                    "rating": raw_profile.get("rating"),
                    "ratingOverall": raw_profile.get("ratingOverall"),
                    "ratingCount": raw_profile.get("ratingCount")
                },
                "page_metadata": {
                    "title": raw_profile.get("title"),
                    "address": raw_profile.get("address"),
                    "creation_date": raw_profile.get("creation_date"),
                    "ad_status": raw_profile.get("ad_status")
                },
                "relevance_score": raw_profile.get("relevance_score", 0),
                "data_quality": "valid" if has_valid_content else "empty",
                "original_data": raw_profile
            }

            print(f"📋 Facebook profile standardized: {username}, Valid content: {has_valid_content}")
            return standardized_profile

        except Exception as e:
            print(f"❌ Error standardizing Facebook profile: {e}")
            return None

    def _create_facebook_bio(self, content: str, author: str, url: str) -> str:
        """Create a meaningful bio from available Facebook data"""
        bio_parts = []
        
        if author and author != "Unknown":
            bio_parts.append(f"Facebook user: {author}")
        
        if content and len(content) > 10:
            # Take first sentence or first 100 chars of content as bio
            sentences = content.split('.')
            first_sentence = sentences[0].strip()
            if first_sentence:
                bio_parts.append(first_sentence[:100])
        
        if url and "hashtag" not in url:
            bio_parts.append("Active Facebook user")
        
        bio = ". ".join(bio_parts) if bio_parts else "Facebook content creator"
        
        # Clean up bio
        if len(bio) > 150:
            bio = bio[:147] + "..."
            
        return bio

    def _standardize_instagram_profile(self, raw_profile: Dict) -> Dict:
        """Standardize Instagram profile data with enhanced validation"""
        try:
            # Extract username from URL or use ownerId as fallback
            username = self._extract_username_from_url(raw_profile.get("url", ""))
            if username == "instagram_user":
                username = f"user_{raw_profile.get('ownerId', 'unknown')}"
            
            # Analyze caption for bio-like content
            caption = raw_profile.get("caption", "")
            bio_content = self._extract_bio_from_caption(caption)
            
            # Determine content validity
            has_valid_content = bool(
                caption or 
                raw_profile.get("hashtags") or 
                raw_profile.get("ownerId")
            )
            
            # Get engagement metrics
            engagement_score = self._calculate_engagement_score(raw_profile)
            
            return {
                "username": username,
                "bio": bio_content,
                "hashtags": raw_profile.get("hashtags", [])[:10],
                "platform": "instagram",
                "has_valid_content": has_valid_content,
                "recent_posts": [{
                    "caption": caption[:150] + "..." if len(caption) > 150 else caption,
                    "likes": raw_profile.get("likesCount", 0),
                    "comments": raw_profile.get("commentsCount", 0),
                    "url": raw_profile.get("url", "")
                }],
                "profile_url": f"https://www.instagram.com/{username}/",
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
        try:
            first = raw_profile.get("firstName", "").strip()
            last = raw_profile.get("lastName", "").strip()
            name = raw_profile.get("fullName") or raw_profile.get("name") or f"{first} {last}".strip() or "LinkedIn User"

            headline = raw_profile.get("headline") or raw_profile.get("summary") or raw_profile.get("about") or "Professional LinkedIn user"

            has_valid_content = bool(name and name != "LinkedIn User" and (headline or raw_profile.get("experience")))

            return {
                "username": name,
                "bio": headline[:200] + "..." if len(headline) > 200 else headline,
                "platform": "linkedin",
                "has_valid_content": has_valid_content,
                "recent_posts": raw_profile.get("posts", [])[:3],
                "profile_url": raw_profile.get("profileUrl") or raw_profile.get("linkedinUrl") or raw_profile.get("url", ""),
                "experience": raw_profile.get("experience", [])[:3],
                "location": raw_profile.get("location") or raw_profile.get("locationName") or "",
                "connections": raw_profile.get("connectionsCount", 0),
                "industry": raw_profile.get("industry", ""),
                "company": raw_profile.get("company", ""),
                "skills": raw_profile.get("skills", [])[:5],
                "education": raw_profile.get("education", [])[:2],
                "relevance_score": raw_profile.get("relevance_score", 0)
            }
        except Exception as e:
            print(f"❌ Error standardizing LinkedIn profile: {e}")
            return None


    def _standardize_generic_profile(self, raw_profile: Dict) -> Dict:
        """Generic fallback for unknown platforms"""
        try:
            return {
                "username": raw_profile.get("username", "") or raw_profile.get("name", "") or raw_profile.get("ownerUsername", "Unknown User"),
                "bio": raw_profile.get("bio", "") or raw_profile.get("description", "") or raw_profile.get("caption", "")[:200],
                "platform": raw_profile.get("platform", "unknown"),
                "has_valid_content": True,  # Assume generic data is valid
                "hashtags": raw_profile.get("hashtags", []),
                "recent_posts": raw_profile.get("posts", []) or raw_profile.get("latestComments", [])[:3],
                "profile_url": raw_profile.get("url", "") or raw_profile.get("profileUrl", "") or raw_profile.get("displayUrl", ""),
                "location": raw_profile.get("location", "") or raw_profile.get("locationName", ""),
                "relevance_score": raw_profile.get("relevance_score", 0)
            }
        except Exception as e:
            print(f"❌ Error standardizing generic profile: {e}")
            return None

# Instantiate tool
fetch_from_mongo_tool = FetchFromMongoTool()
