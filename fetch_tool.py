from crewai.tools import BaseTool
from typing import List, Dict, Any  
import re
from pydantic import BaseModel, Field
from typing import List, Optional

from db_config import get_audience_collection

audience_collection = get_audience_collection()

class FetchFromMongoToolSchema(BaseModel):
    client_id: Optional[str] = Field(default=None, description="Client ID to filter audience data")
    platform: str = Field(..., description="Platform name like linkedin, instagram, facebook")
    search_terms: List[str] = Field(default_factory=list, description="Search keywords to find relevant profiles")
    limit: int = Field(default=5, description="Number of top profiles to return (default = 5)")  # ✅ CHANGED from 1 to 5


class FetchFromMongoTool(BaseTool):
    name: str = "fetch_from_mongo"
    description: str = (
        "Fetch the most relevant profile from MongoDB based on client search terms for the specified platform ONLY. "
        "Returns platform-specific profile data with content validation."
    )
    args_schema: type = FetchFromMongoToolSchema
    
    def _get_field(self, profile: Dict, *field_names: str) -> str:
        """
        🔧 Flexible field getter that tries multiple field name variations
        Handles both camelCase and snake_case
        """
        for field_name in field_names:
            value = profile.get(field_name)
            if value and isinstance(value, str):
                stripped = value.strip()
                if stripped:
                    return stripped
        return ""
    
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
    
    def _run(self, client_id: str = None, platform: str = None, search_terms: List[str] = None, limit: int = 5):  # ✅ KEEP default as 5
        try:
            # Build query - fetch prospects without generated messages
            query = {"type": "prospects"}
            
            if client_id:
                query["client_id"] = client_id
            if platform:
                query["platform"] = platform.lower()

            print(f"🔍 MongoDB Query: {query}")
            print(f"📊 Requested limit: {limit}")  # ✅ ADDED: Debug print
            
            # Get the prospects document
            prospects_doc = audience_collection.find_one(query)
            
            if not prospects_doc:
                print(f"❌ No prospects document found")
                return {"error": f"No {platform} prospects found", "platform": platform}
            
            # ✅ Filter prospects that don't have messages yet
            all_prospects = prospects_doc.get("prospects", [])
            prospects_without_messages = [
                p for p in all_prospects 
                if not p.get("generated_message")
            ]
            
            print(f"📊 Found {len(all_prospects)} total prospects")
            print(f"📝 {len(prospects_without_messages)} prospects need messages")
            
            if not prospects_without_messages:
                return {
                    "error": "All prospects already have messages",
                    "platform": platform
                }
            
            # ✅ ADDED: Debug - show sample prospect data
            if prospects_without_messages and platform == "linkedin":
                sample = prospects_without_messages[0]
                print(f"🔍 Sample LinkedIn prospect keys: {list(sample.keys())}")
                print(f"🔍 Sample data:")
                print(f"   - first_name: '{sample.get('first_name', 'MISSING')}'")
                print(f"   - firstName: '{sample.get('firstName', 'MISSING')}'")
                print(f"   - last_name: '{sample.get('last_name', 'MISSING')}'")
                print(f"   - lastName: '{sample.get('lastName', 'MISSING')}'")
                print(f"   - headline: '{sample.get('headline', 'MISSING')}'")
                print(f"   - position: '{sample.get('position', 'MISSING')}'")
                print(f"   - summary: '{sample.get('summary', 'MISSING')}'")
                print(f"   - about: '{sample.get('about', 'MISSING')}'")
            
            # Validate data quality on filtered prospects
            valid_results = self._validate_platform_data(prospects_without_messages, platform)
            
            if not valid_results:
                print(f"❌ No valid {platform} data found after validation")
                return {"error": f"No valid {platform} content found", "platform": platform}

            # Score and rank profiles
            if search_terms:
                scored_profiles = self._score_profiles_by_relevance(valid_results, search_terms, platform)
            else:
                scored_profiles = valid_results

            # ✅ FIXED: Use the limit parameter correctly
            requested_limit = min(limit, len(scored_profiles))
            print(f"📊 Returning top {requested_limit} profiles out of {len(scored_profiles)} valid profiles")
            top_profiles = scored_profiles[:requested_limit]
            
            # Process based on platform
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
                    "platform": platform
                }
            
            # ✅ FIXED: Return ALL processed profiles, not just the first one
            print(f"✅ Returning {len(processed_profiles)} processed profiles")
            
            for idx, profile in enumerate(processed_profiles, 1):
                if platform == "linkedin":
                    identifier = profile.get('fullName') or f"{profile.get('firstName', '')} {profile.get('lastName', '')}".strip()
                    print(f"   {idx}. {identifier} - {profile.get('headline', 'No headline')[:50]}")
                else:
                    identifier = f"@{profile.get('username')}"
                    print(f"   {idx}. {identifier}")
            
            # ✅ Return list if multiple, single if limit=1
            return processed_profiles if len(processed_profiles) > 1 else processed_profiles[0]

        except Exception as e:
            print(f"❌ Error in FetchFromMongoTool: {e}")
            import traceback
            traceback.print_exc()
            return {"error": str(e), "platform": platform or "unknown"}


    def _validate_platform_data(self, profiles: List[Dict], platform: str) -> List[Dict]:
        """Validate that profiles have meaningful content"""
        valid_profiles = []
        
        for profile in profiles:
            if platform == "facebook":
                categories = profile.get("categories", [])
                info = profile.get("info", [])
                likes = profile.get("likes") or 0
                about_me = (profile.get("about_me") or {}).get("text", "")

                if categories or info or about_me or likes > 0:
                    valid_profiles.append(profile)

            elif platform == "instagram":
                caption = (profile.get("caption") or "").strip()
                hashtags = profile.get("hashtags", [])
                owner_id = profile.get("ownerId")
                if caption or hashtags or owner_id:
                    valid_profiles.append(profile)

            # 🔥🔥🔥 LINKEDIN UPDATED BLOCK — START
            elif platform == "linkedin":
                # --- 1️⃣ Extract Name
                first_name = self._get_field(profile, "first_name", "firstName")
                last_name = self._get_field(profile, "last_name", "lastName")
                full_name = self._get_field(profile, "full_name", "fullName", "name")

                if not full_name and first_name and last_name:
                    full_name = f"{first_name} {last_name}"

                # --- 2️⃣ Extract location / URL (minimal mode)
                profile_url = profile.get("profile_url") or profile.get("linkedinUrl") or profile.get("url")
                location = profile.get("location") or {}

                location_str = ""
                if isinstance(location, dict):
                    location_str = (
                        location.get("city")
                        or location.get("defaultLocalizedName")
                        or location.get("country")
                    )
                elif isinstance(location, str):
                    location_str = location

                # --- 3️⃣ Extract content options
                headline = self._get_field(profile, "headline", "position", "title", "current_role", "currentRole")
                summary = self._get_field(profile, "summary", "about", "bio", "description")
                company = self._get_field(profile, "company", "companyName", "current_company")
                position = self._get_field(profile, "position", "title", "current_position", "role")

                experience = (
                    profile.get("experience")
                    or profile.get("current_positions")
                    or profile.get("positions")
                    or []
                )

                # Fallback to first experience entry
                if isinstance(experience, list) and len(experience) > 0:
                    first_exp = experience[0]
                    if isinstance(first_exp, dict):
                        position = position or first_exp.get("title") or first_exp.get("position")
                        company = company or first_exp.get("company") or first_exp.get("companyName")
                    elif isinstance(first_exp, str):
                        position = position or first_exp[:100]

                # --- VALIDATION ---
                has_name = bool(
                    full_name
                    and full_name.strip()
                    and len(full_name.strip()) > 1
                    and full_name.lower() not in ["linkedin user", "none", "null", "n/a", "user"]
                )

                has_minimal_data = bool(profile_url or location_str)

                has_content = (
                    headline or summary or position or company or (experience and len(experience) > 0)
                )

                print(f"🔍 Checking {full_name or '(no name)'}")
                print(f"   name_ok={has_name} minimal_ok={has_minimal_data} content_ok={bool(has_content)}")
                print(f"   url={profile_url} location={location_str}")
                print(f"   headline={headline} company={company} pos={position}")

                # ---------------------------------------------
                # ACCEPT IF:
                #   ✔ name AND (URL OR location)
                # OR
                #   ✔ name AND has content
                # ---------------------------------------------
                if has_name and (has_minimal_data or has_content):

                    # Build fallback headline
                    if not headline and location_str:
                        headline = f"LinkedIn professional in {location_str}"
                    elif not headline:
                        headline = "LinkedIn Professional"

                    profile["_normalized"] = {
                        "firstName": first_name,
                        "lastName": last_name,
                        "full_name": full_name,
                        "headline": headline,
                        "company": company,
                        "position": position,
                        "summary": summary,
                        "experience": experience,
                        "location": location_str,
                        "profile_url": profile_url,
                    }
                    valid_profiles.append(profile)

                    print("   ✅ VALID (MINIMAL / CONTENT MODE)")
                else:
                    print("   ⏭️ SKIPPED (insufficient data)")

            # OTHER PLATFORMS (KEEP)
            else:
                if profile.get("username") or profile.get("bio") or profile.get("caption"):
                    valid_profiles.append(profile)

        print(f"✅ Validated {len(valid_profiles)}/{len(profiles)} {platform} profiles")
        return valid_profiles

    # ... [Rest of the methods remain the same - _score_profiles_by_relevance, etc.]
    
    def _score_profiles_by_relevance(self, profiles: List[Dict], search_terms: List[str], platform: str) -> List[Dict]:
        """Score profiles based on search terms relevance AND location"""
        if not search_terms:
            return profiles

        scored_profiles = []
        search_terms_lower = [term.lower() for term in search_terms]

        for profile in profiles:
            score = 0
            
            location_score = profile.get('location_relevance_score', 0)
            score += location_score * 2

            if platform == "linkedin":
                headline = self._get_field(profile, "headline", "position", "title").lower()
                summary = self._get_field(profile, "summary", "about", "bio").lower()
                industry = self._get_field(profile, "industry").lower()
                
                for term in search_terms_lower:
                    if term in headline:
                        score += 5
                    if term in summary:
                        score += 3
                    if term in industry:
                        score += 2
                
                experience = profile.get("experience") or profile.get("current_positions") or []
                for exp in experience[:3]:
                    if isinstance(exp, dict):
                        exp_title = (exp.get("title") or "").lower()
                        exp_company = (exp.get("company") or exp.get("companyName") or "").lower()
                        exp_desc = (exp.get("description") or "").lower()
                        
                        for term in search_terms_lower:
                            if term in exp_title:
                                score += 3
                            if term in exp_company:
                                score += 2
                            if term in exp_desc:
                                score += 1
                
                connections = profile.get("connectionsCount", 0)
                if connections > 500:
                    score += 1
                    
            scored_profiles.append({**profile, "relevance_score": score})
        
        scored_profiles.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
        
        if scored_profiles:
            top_profile = scored_profiles[0]
            if platform == "linkedin":
                name = self._get_field(top_profile, "full_name", "fullName") or f"{self._get_field(top_profile, 'first_name', 'firstName')} {self._get_field(top_profile, 'last_name', 'lastName')}"
                print(f"🎯 Top LinkedIn profile: {name} - Score: {top_profile.get('relevance_score', 0)}")
        
        return scored_profiles

    def _standardize_linkedin_profile(self, profile):
        """Standardize LinkedIn profile - handles both snake_case and camelCase"""
        # ✅ IMPROVED: Use _get_field for flexible extraction
        first = self._get_field(profile, "first_name", "firstName")
        last = self._get_field(profile, "last_name", "lastName")
        full = self._get_field(profile, "full_name", "fullName", "name") or f"{first} {last}".strip()

        headline = self._get_field(profile, "headline", "position", "currentRole", "title")
        summary = self._get_field(profile, "summary", "about", "bio")
        company = self._get_field(profile, "company", "companyName", "current_company")
        
        exp = profile.get("experience") or profile.get("positions") or profile.get("current_positions") or []
        url = profile.get("profile_url") or profile.get("linkedinUrl") or profile.get("url")

        # ✅ ENSURE we have valid content
        has_valid = bool(full and (headline or summary or company or exp))

        return {
            "platform": "linkedin",
            "fullName": full,
            "firstName": first,
            "lastName": last,
            "headline": headline or "LinkedIn Professional",
            "summary": summary,
            "company": company,
            "experience": exp,
            "profile_url": url,
            "has_valid_content": has_valid,
            "relevance_score": profile.get("relevance_score", 0),
            "location_relevance_score": profile.get("location_relevance_score", 0)
        }

    # ... [Keep all other methods unchanged]
    def _standardize_generic_profile(self, raw_profile: Dict) -> Dict:
        """Generic fallback"""
        try:
            return {
                "username": raw_profile.get("username", "") or raw_profile.get("name", "") or "Unknown User",
                "bio": raw_profile.get("bio", "") or raw_profile.get("description", "") or raw_profile.get("caption", "")[:200],
                "platform": raw_profile.get("platform", "unknown"),
                "has_valid_content": True,
                "hashtags": raw_profile.get("hashtags", []),
                "recent_posts": raw_profile.get("posts", [])[:3],
                "profile_url": raw_profile.get("url", "") or raw_profile.get("profileUrl", ""),
                "location": raw_profile.get("location", ""),
                "relevance_score": raw_profile.get("relevance_score", 0),
                "location_relevance_score": raw_profile.get("location_relevance_score", 0)
            }
        except Exception as e:
            print(f"❌ Error standardizing generic profile: {e}")
            return None

# Instantiate tool
fetch_from_mongo_tool = FetchFromMongoTool()