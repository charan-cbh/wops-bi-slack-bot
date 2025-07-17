#!/usr/bin/env python3
"""
Slack User Service - Fetches user information from Slack API
Dynamically retrieves user names, emails, and profile data
"""

import os
import json
from typing import Dict, Optional, Any
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv

load_dotenv()

class SlackUserService:
    """Service to fetch user information from Slack API"""
    
    def __init__(self):
        self.slack_token = os.getenv("SLACK_BOT_TOKEN")
        if not self.slack_token:
            print("⚠️ SLACK_BOT_TOKEN not found in environment variables")
            self.client = None
        else:
            self.client = WebClient(token=self.slack_token)
            print("✅ Slack client initialized")
        
        # Cache for user info to avoid repeated API calls
        self._user_cache = {}
    
    async def get_user_info(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get user information from Slack API
        
        Args:
            user_id: Slack user ID (e.g., 'U123ABC456')
            
        Returns:
            Dict with user info or None if error
        """
        if not self.client:
            print("❌ Slack client not available")
            return None
        
        # Check cache first
        if user_id in self._user_cache:
            print(f"📋 Using cached info for {user_id}")
            return self._user_cache[user_id]
        
        try:
            print(f"🔍 Fetching user info for {user_id} from Slack...")
            
            # Call Slack API
            response = self.client.users_info(user=user_id)
            
            if response["ok"]:
                user_data = response["user"]
                
                # Extract relevant information
                user_info = {
                    "id": user_data.get("id"),
                    "name": user_data.get("real_name") or user_data.get("name"),
                    "display_name": user_data.get("profile", {}).get("display_name"),
                    "email": user_data.get("profile", {}).get("email"),
                    "title": user_data.get("profile", {}).get("title"),
                    "department": user_data.get("profile", {}).get("fields", {}).get("department", {}).get("value"),
                    "manager": user_data.get("profile", {}).get("fields", {}).get("manager", {}).get("value"),
                    "team": user_data.get("profile", {}).get("fields", {}).get("team", {}).get("value"),
                    "is_admin": user_data.get("is_admin", False),
                    "is_owner": user_data.get("is_owner", False),
                    "timezone": user_data.get("tz"),
                    "timezone_label": user_data.get("tz_label"),
                    "profile_image": user_data.get("profile", {}).get("image_192")
                }
                
                # Cache the result
                self._user_cache[user_id] = user_info
                
                print(f"✅ Retrieved user info: {user_info['name']} ({user_info.get('email', 'no email')})")
                return user_info
            else:
                print(f"❌ Slack API error: {response['error']}")
                return None
                
        except SlackApiError as e:
            error_msg = e.response['error']
            print(f"❌ Slack API error: {error_msg}")
            
            if error_msg == "missing_scope":
                print("⚠️ Bot missing 'users:read' scope. Contact admin to add this scope.")
                print("💡 For now, using fallback user data...")
                
                # Return mock data for testing
                fallback_info = {
                    "id": user_id,
                    "name": f"User_{user_id[-6:]}",  # Use last 6 chars of ID
                    "display_name": f"Display_{user_id[-6:]}",
                    "email": f"user_{user_id[-6:]}@company.com",
                    "title": "Customer Support Specialist",
                    "department": "Customer Support",
                    "manager": "Team Lead",
                    "team": "Support Team",
                    "is_admin": False,
                    "is_owner": False,
                    "timezone": "America/Los_Angeles",
                    "timezone_label": "Pacific Standard Time"
                }
                
                # Cache the fallback result
                self._user_cache[user_id] = fallback_info
                print(f"✅ Using fallback user info: {fallback_info['name']}")
                return fallback_info
            
            return None
        except Exception as e:
            print(f"❌ Unexpected error fetching user info: {e}")
            return None
    
    async def get_user_name(self, user_id: str) -> Optional[str]:
        """Get just the user's name (shortcut method)"""
        user_info = await self.get_user_info(user_id)
        return user_info.get("name") if user_info else None
    
    async def get_user_email(self, user_id: str) -> Optional[str]:
        """Get just the user's email (shortcut method)"""
        user_info = await self.get_user_info(user_id)
        return user_info.get("email") if user_info else None
    
    def clear_cache(self):
        """Clear the user info cache"""
        self._user_cache.clear()
        print("🗑️ User cache cleared")
    
    def get_cached_users(self) -> Dict[str, Dict]:
        """Get all cached user info"""
        return self._user_cache.copy()
    
    async def test_connection(self) -> bool:
        """Test if Slack connection is working"""
        if not self.client:
            return False
        
        try:
            response = self.client.auth_test()
            if response["ok"]:
                print(f"✅ Slack connection test successful")
                print(f"   Bot User ID: {response.get('user_id')}")
                print(f"   Team: {response.get('team')}")
                print(f"   Bot Name: {response.get('user')}")
                return True
            else:
                print(f"❌ Slack auth test failed: {response.get('error')}")
                return False
        except Exception as e:
            print(f"❌ Slack connection test error: {e}")
            return False


# Global instance
slack_user_service = SlackUserService()


# Convenience functions
async def get_user_info(user_id: str) -> Optional[Dict[str, Any]]:
    """Get user information from Slack"""
    return await slack_user_service.get_user_info(user_id)

async def get_user_name(user_id: str) -> Optional[str]:
    """Get user name from Slack"""
    return await slack_user_service.get_user_name(user_id)

async def get_user_email(user_id: str) -> Optional[str]:
    """Get user email from Slack"""
    return await slack_user_service.get_user_email(user_id)

async def test_slack_connection() -> bool:
    """Test Slack connection"""
    return await slack_user_service.test_connection()