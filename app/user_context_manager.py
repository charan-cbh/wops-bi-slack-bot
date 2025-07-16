#!/usr/bin/env python3
"""
User Context Manager - Handles user identification and role-based data access
Enables natural questions like "show me my team" and "my KPIs"
"""

import os
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

# Configuration
ENABLE_USER_RECOGNITION = os.getenv("ENABLE_USER_RECOGNITION", "false").lower() == "true"


@dataclass
class UserContext:
    """User context information"""
    user_id: str
    name: str
    email: str
    role: str  # agent, supervisor, manager, admin
    team: str
    supervisor: Optional[str] = None
    direct_reports: List[str] = None
    department: Optional[str] = None
    permissions: List[str] = None
    
    def __post_init__(self):
        if self.direct_reports is None:
            self.direct_reports = []
        if self.permissions is None:
            self.permissions = []


class UserContextManager:
    """Manages user context and role-based data access"""
    
    def __init__(self):
        # Static mapping for demo/testing - will be enhanced with Slack integration
        # In production, this would come from your HR system, Active Directory, or employee database
        self.user_mapping = {
            # Test user for demonstration - represents Ricardo Birck (supervisor)
            'U123USER': UserContext(
                user_id='U123USER',
                name='Ricardo Birck',
                email='ricardo.birck@clipboardhealth.com',
                role='supervisor',
                team='Customer Support Team',
                supervisor='Joan Mallari',
                direct_reports=['Cristine Joy Arcayna', 'Kenneth Aringoy', 'Emil Sagaral', 'Mikka Humilde', 'Lavinia Layson', 'Ardilyn B.', 'Naeto Alomefuna'],
                department='Customer Support',
                permissions=['team_data', 'agent_metrics', 'supervisor_reports']
            ),
            
            # Test agent user - represents Lavinia Layson (agent under Ricardo Birck)
            'U456AGENT': UserContext(
                user_id='U456AGENT', 
                name='Lavinia Layson',
                email='lavinia.layson@clipboardhealth.com',
                role='agent',
                team='Customer Support Team',
                supervisor='Ricardo Birck',
                direct_reports=[],
                department='Customer Support',
                permissions=['personal_metrics']
            ),
            
            'U789MANAGER': UserContext(
                user_id='U789MANAGER',
                name='Mary Johnson', 
                email='mary.johnson@company.com',
                role='manager',
                team='Customer Support',
                supervisor='Executive Team',
                direct_reports=['John Smith', 'Lisa Brown', 'Mike Davis'],
                department='Customer Support',
                permissions=['department_data', 'manager_reports', 'team_data', 'agent_metrics']
            )
        }
        
        # Add your provided test user
        self.user_mapping['U019NNZPPME'] = UserContext(
            user_id='U019NNZPPME',
            name='Dynamic User',  # Will be fetched from Slack
            email='dynamic@company.com',  # Will be fetched from Slack
            role='supervisor',  # Assumed for testing
            team='Dynamic Team',
            supervisor='Manager',
            direct_reports=['Agent1', 'Agent2', 'Agent3'],
            department='Customer Support',
            permissions=['team_data', 'agent_metrics', 'supervisor_reports']
        )
        
        # Reverse mapping for name lookups
        self.name_to_user = {}
        self._build_name_mapping()
    
    def _build_name_mapping(self):
        """Build reverse mapping from names to user contexts"""
        for user_context in self.user_mapping.values():
            # Full name mapping
            self.name_to_user[user_context.name.lower()] = user_context
            
            # First name mapping (if unique)
            first_name = user_context.name.split()[0].lower()
            if first_name not in self.name_to_user:
                self.name_to_user[first_name] = user_context
    
    async def get_user_context(self, user_id: str) -> Optional[UserContext]:
        """Get user context by Slack user ID, fetching from Slack if needed"""
        # Check if user recognition is enabled
        if not ENABLE_USER_RECOGNITION:
            print("⚠️ User recognition feature disabled. Set ENABLE_USER_RECOGNITION=true to enable.")
            return None
        
        # Check if we have the user in our mapping
        if user_id in self.user_mapping:
            user_context = self.user_mapping[user_id]
            
            # If the name is still 'Dynamic User', fetch real data from Slack
            if user_context.name == 'Dynamic User':
                await self._update_user_from_slack(user_id)
                
            return self.user_mapping[user_id]
        else:
            # Try to create user context from Slack data
            return await self._create_user_from_slack(user_id)
    
    async def _update_user_from_slack(self, user_id: str):
        """Update existing user context with Slack data"""
        if not ENABLE_USER_RECOGNITION:
            return
        
        try:
            from app.slack_user_service import get_user_info
            slack_info = await get_user_info(user_id)
            
            if slack_info and user_id in self.user_mapping:
                user_context = self.user_mapping[user_id]
                # Update with real Slack data
                user_context.name = slack_info.get('name', user_context.name)
                user_context.email = slack_info.get('email', user_context.email)
                
                # Try to infer role from title if available
                title = slack_info.get('title', '').lower()
                if 'manager' in title or 'supervisor' in title:
                    user_context.role = 'supervisor'
                elif 'agent' in title or 'support' in title:
                    user_context.role = 'agent'
                
                print(f"✅ Updated user context with Slack data: {user_context.name}")
                
        except Exception as e:
            print(f"⚠️ Could not fetch Slack data for {user_id}: {e}")
    
    async def _create_user_from_slack(self, user_id: str) -> Optional[UserContext]:
        """Create new user context from Slack data"""
        if not ENABLE_USER_RECOGNITION:
            return None
        
        try:
            from app.slack_user_service import get_user_info
            slack_info = await get_user_info(user_id)
            
            if slack_info:
                # Create new user context from Slack data
                user_context = UserContext(
                    user_id=user_id,
                    name=slack_info.get('name', f'User_{user_id}'),
                    email=slack_info.get('email', ''),
                    role='agent',  # Default role, can be customized
                    team=slack_info.get('department', 'Unknown Team'),
                    supervisor=slack_info.get('manager', 'Unknown Supervisor'),
                    direct_reports=[],
                    department=slack_info.get('department', 'Unknown Department'),
                    permissions=['personal_metrics']  # Default permissions
                )
                
                # Try to infer role from title
                title = slack_info.get('title', '').lower()
                if 'manager' in title or 'supervisor' in title:
                    user_context.role = 'supervisor'
                    user_context.permissions = ['team_data', 'agent_metrics', 'supervisor_reports']
                
                # Add to mapping
                self.user_mapping[user_id] = user_context
                self._build_name_mapping()  # Rebuild name mapping
                
                print(f"✅ Created new user context from Slack: {user_context.name}")
                return user_context
                
        except Exception as e:
            print(f"⚠️ Could not create user from Slack data for {user_id}: {e}")
        
        return None
    
    def get_user_by_name(self, name: str) -> Optional[UserContext]:
        """Get user context by name (fuzzy matching)"""
        name_lower = name.lower()
        
        # Exact match
        if name_lower in self.name_to_user:
            return self.name_to_user[name_lower]
        
        # Partial match
        for stored_name, user_context in self.name_to_user.items():
            if name_lower in stored_name or stored_name in name_lower:
                return user_context
        
        return None
    
    def is_supervisor(self, user_id: str) -> bool:
        """Check if user is a supervisor"""
        context = self.get_user_context(user_id)
        return context and context.role in ['supervisor', 'manager', 'admin']
    
    def is_agent(self, user_id: str) -> bool:
        """Check if user is an agent"""
        context = self.get_user_context(user_id)
        return context and context.role == 'agent'
    
    def get_team_members(self, user_id: str) -> List[str]:
        """Get team members for a user"""
        context = self.get_user_context(user_id)
        if not context:
            return []
        
        if context.role in ['supervisor', 'manager']:
            # Return direct reports
            return context.direct_reports
        elif context.role == 'agent':
            # Return team members (agents under same supervisor)
            supervisor_name = context.supervisor
            if supervisor_name:
                supervisor = self.get_user_by_name(supervisor_name)
                if supervisor:
                    return supervisor.direct_reports
        
        return []
    
    def get_my_supervisor(self, user_id: str) -> Optional[str]:
        """Get user's supervisor"""
        context = self.get_user_context(user_id)
        return context.supervisor if context else None
    
    def has_permission(self, user_id: str, permission: str) -> bool:
        """Check if user has specific permission"""
        context = self.get_user_context(user_id)
        return context and permission in context.permissions
    
    async def get_personal_filters(self, user_id: str) -> Dict[str, Any]:
        """Get SQL filters for personal data based on user context"""
        context = await self.get_user_context(user_id)
        if not context:
            return {}
        
        filters = {
            'user_name': context.name,
            'user_email': context.email,
            'team': context.team,
            'department': context.department
        }
        
        # Add role-specific filters
        if context.role == 'agent':
            filters['agent_filters'] = {
                'ASSIGNEE_NAME': context.name,
                'USER_NAME': context.name,
                'AGENT_NAME': context.name
            }
        elif context.role in ['supervisor', 'manager']:
            filters['team_filters'] = {
                'ASSIGNEE_SUPERVISOR': context.name,
                'SUPERVISOR': context.name,
                'team_members': context.direct_reports
            }
        
        return filters
    
    def explain_user_context(self, user_id: str) -> str:
        """Explain what data the user can access"""
        context = self.get_user_context(user_id)
        if not context:
            return "❌ User not found in system"
        
        explanation = f"""
👤 **User Context for {context.name}**

**Role**: {context.role.title()}
**Team**: {context.team}
**Department**: {context.department}

**Data Access**:
"""
        
        if context.role == 'agent':
            explanation += """
• ✅ Your personal metrics (tickets, AHT, FCR, etc.)
• ✅ Your team's general performance  
• ❌ Individual metrics of other agents
"""
        elif context.role == 'supervisor':
            explanation += f"""
• ✅ Your personal metrics
• ✅ Your team's performance ({len(context.direct_reports)} direct reports)
• ✅ Individual agent metrics for your reports
• ❌ Other teams' detailed data
"""
        elif context.role == 'manager':
            explanation += f"""
• ✅ Department-wide metrics ({context.department})
• ✅ All supervisor and team data under you
• ✅ Cross-team analytics
• ✅ Historical trending data
"""
        
        if context.direct_reports:
            explanation += f"\n**Your Team**: {', '.join(context.direct_reports)}"
        
        return explanation


# Global instance
user_context_manager = UserContextManager()


# Convenience functions for backward compatibility
async def get_user_context(user_id: str) -> Optional[UserContext]:
    """Get user context"""
    return await user_context_manager.get_user_context(user_id)

async def get_personal_filters(user_id: str) -> Dict[str, Any]:
    """Get personal filters for user"""
    return await user_context_manager.get_personal_filters(user_id)

async def is_supervisor(user_id: str) -> bool:
    """Check if user is supervisor"""
    user_context = await user_context_manager.get_user_context(user_id)
    return user_context and user_context.role in ['supervisor', 'manager', 'admin']

async def get_team_members(user_id: str) -> List[str]:
    """Get team members"""
    user_context = await user_context_manager.get_user_context(user_id)
    return user_context.direct_reports if user_context else []