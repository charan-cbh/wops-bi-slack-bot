"""
Valkey Manager - Handles all cache operations for the BI Slack Bot
"""
import os
import json
import time
from typing import Optional, Dict, Any
from glide import GlideClient, GlideClientConfiguration, NodeAddress, GlideClusterClient, \
    GlideClusterClientConfiguration
from dotenv import load_dotenv
from .logging_config import get_bot_logger

load_dotenv()

class ValkeyManager:
    def __init__(self):
        self.logger = get_bot_logger(__name__)
        
        # Valkey configuration
        self.valkey_host = os.getenv("VALKEY_HOST", "localhost")
        self.valkey_port = int(os.getenv("VALKEY_PORT", 6379))
        self.valkey_use_tls = os.getenv("VALKEY_USE_TLS", "true").lower() == "true"
        self.is_local_dev = os.getenv("IS_LOCAL_DEV", "false").lower() == "true"
        self.valkey_username = os.getenv("VALKEY_USERNAME")
        self.valkey_password = os.getenv("VALKEY_PASSWORD")
        self.aws_region = os.getenv("AWS_REGION", "us-east-1")
        
        # Cache TTL settings (in seconds)
        self.THREAD_CACHE_TTL = 3600  # 1 hour
        self.SQL_CACHE_TTL = 86400  # 24 hours
        self.SCHEMA_CACHE_TTL = 604800  # 7 days for table schema cache
        self.CONVERSATION_CACHE_TTL = 600  # 10 minutes
        self.TABLE_SELECTION_CACHE_TTL = 2592000  # 30 days for table selection patterns
        self.FEEDBACK_CACHE_TTL = 7776000  # 90 days for feedback data
        self.TOKEN_USAGE_CACHE_TTL = 86400  # 24 hours for daily limits
        
        # Cache key prefixes
        self.CACHE_PREFIX = "bi_slack_bot"
        self.THREAD_CACHE_PREFIX = f"{self.CACHE_PREFIX}:thread"
        self.SQL_CACHE_PREFIX = f"{self.CACHE_PREFIX}:sql"
        self.SCHEMA_CACHE_PREFIX = f"{self.CACHE_PREFIX}:schema"
        self.CONVERSATION_CACHE_PREFIX = f"{self.CACHE_PREFIX}:conversation"
        self.TABLE_SELECTION_PREFIX = f"{self.CACHE_PREFIX}:table_selection"
        self.TABLE_SAMPLES_PREFIX = f"{self.CACHE_PREFIX}:table_samples"
        self.FEEDBACK_PREFIX = f"{self.CACHE_PREFIX}:feedback"
        self.TOKEN_USAGE_PREFIX = f"{self.CACHE_PREFIX}:token_usage"
        self.DAILY_USAGE_PREFIX = f"{self.TOKEN_USAGE_PREFIX}:daily"
        self.HOURLY_USAGE_PREFIX = f"{self.TOKEN_USAGE_PREFIX}:hourly"
        self.THREAD_USAGE_PREFIX = f"{self.TOKEN_USAGE_PREFIX}:thread"
        
        # Client and local cache
        self.valkey_client = None
        self._local_cache = {
            'thread': {},
            'sql': {},
            'schema': {},
            'conversation': {},
            'table_selection': {},
            'table_samples': {},
            'feedback': {}
        }
    
    async def init_valkey_client(self):
        """Initialize Valkey client connection"""
        if self.valkey_client is not None:
            return self.valkey_client
        
        try:
            # Check if this is a serverless ElastiCache endpoint
            is_serverless = 'serverless' in self.valkey_host.lower()
            
            if self.is_local_dev or is_serverless:
                # Local development or serverless - use single node
                self.logger.info(f"Connecting to Valkey as single node: {self.valkey_host}:{self.valkey_port} (TLS: {self.valkey_use_tls})")
                
                # Prepare configuration with authentication if available
                config_params = {
                    "addresses": [NodeAddress(host=self.valkey_host, port=self.valkey_port)],
                    "use_tls": self.valkey_use_tls,
                    "request_timeout": 15000  # Increased timeout for serverless
                }
                
                # Add authentication if provided
                if self.valkey_username and self.valkey_password:
                    self.logger.info("Using username/password authentication")
                    config_params["credentials"] = {
                        "username": self.valkey_username,
                        "password": self.valkey_password
                    }
                
                config = GlideClientConfiguration(**config_params)
                self.valkey_client = await GlideClient.create(config)
            else:
                # Production cluster
                self.logger.info(f"Connecting to Valkey as cluster: {self.valkey_host}:{self.valkey_port} (TLS: {self.valkey_use_tls})")
                
                # Prepare configuration with authentication if available
                config_params = {
                    "addresses": [NodeAddress(host=self.valkey_host, port=self.valkey_port)],
                    "use_tls": self.valkey_use_tls,
                    "request_timeout": 15000  # Increased timeout
                }
                
                # Add authentication if provided
                if self.valkey_username and self.valkey_password:
                    self.logger.info("Using username/password authentication")
                    config_params["credentials"] = {
                        "username": self.valkey_username,
                        "password": self.valkey_password
                    }
                
                config = GlideClusterClientConfiguration(**config_params)
                self.valkey_client = await GlideClusterClient.create(config)
            
            self.logger.info("Valkey client initialized successfully")
            return self.valkey_client
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Valkey client: {e}")
            self.valkey_client = None
            return None
    
    async def ensure_valkey_connection(self):
        """Ensure Valkey connection is available"""
        if self.valkey_client is None:
            await self.init_valkey_client()
        return self.valkey_client is not None
    
    def convert_to_serializable(self, obj):
        """Convert object to JSON serializable format"""
        if isinstance(obj, dict):
            return {k: self.convert_to_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_to_serializable(item) for item in obj]
        elif isinstance(obj, (int, float, str, bool)) or obj is None:
            return obj
        else:
            return str(obj)
    
    async def safe_valkey_get(self, key: str, default=None):
        """Safely get value from Valkey with local cache fallback"""
        try:
            await self.ensure_valkey_connection()
            if self.valkey_client:
                value = await self.valkey_client.get(key)
                if value is not None:
                    return json.loads(value)
                return default
            else:
                # Fallback to local cache
                cache_type = key.split(':')[1] if ':' in key else 'default'
                if cache_type in self._local_cache:
                    return self._local_cache[cache_type].get(key, default)
                return default
        except Exception as e:
            self.logger.error(f"Error getting from Valkey: {e}")
            return default
    
    async def safe_valkey_set(self, key: str, value: Any, ex: int = None):
        """Safely set value in Valkey with local cache fallback"""
        try:
            await self.ensure_valkey_connection()
            serializable_value = self.convert_to_serializable(value)
            
            if self.valkey_client:
                json_value = json.dumps(serializable_value)
                if ex:
                    await self.valkey_client.set(key, json_value, ex=ex)
                else:
                    await self.valkey_client.set(key, json_value)
                return True
            else:
                # Fallback to local cache
                cache_type = key.split(':')[1] if ':' in key else 'default'
                if cache_type in self._local_cache:
                    self._local_cache[cache_type][key] = serializable_value
                    
                    # Set expiration in local cache
                    if ex:
                        import asyncio
                        async def expire_key():
                            await asyncio.sleep(ex)
                            if key in self._local_cache[cache_type]:
                                del self._local_cache[cache_type][key]
                        asyncio.create_task(expire_key())
                return True
        except Exception as e:
            self.logger.error(f"Error setting in Valkey: {e}")
            return False
    
    async def safe_valkey_delete(self, key: str):
        """Safely delete value from Valkey with local cache fallback"""
        try:
            await self.ensure_valkey_connection()
            if self.valkey_client:
                await self.valkey_client.delete([key])
                return True
            else:
                # Fallback to local cache
                cache_type = key.split(':')[1] if ':' in key else 'default'
                if cache_type in self._local_cache and key in self._local_cache[cache_type]:
                    del self._local_cache[cache_type][key]
                return True
        except Exception as e:
            self.logger.error(f"Error deleting from Valkey: {e}")
            return False
    
    async def safe_valkey_exists(self, key: str) -> bool:
        """Check if key exists in Valkey with local cache fallback"""
        try:
            await self.ensure_valkey_connection()
            if self.valkey_client:
                result = await self.valkey_client.exists([key])
                return result > 0
            else:
                # Fallback to local cache
                cache_type = key.split(':')[1] if ':' in key else 'default'
                if cache_type in self._local_cache:
                    return key in self._local_cache[cache_type]
                return False
        except Exception as e:
            self.logger.error(f"Error checking existence in Valkey: {e}")
            return False
    
    async def close_valkey_connection(self):
        """Close Valkey connection"""
        if self.valkey_client:
            try:
                await self.valkey_client.close()
                self.valkey_client = None
                self.logger.debug("Valkey connection closed")
            except Exception as e:
                self.logger.error(f"Error closing Valkey connection: {e}")