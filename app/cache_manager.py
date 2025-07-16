import os
import json
import asyncio
import time
from typing import Optional, Dict, Any
from dotenv import load_dotenv
try:
    from glide import GlideClient, GlideClientConfiguration, NodeAddress, GlideClusterClient, \
        GlideClusterClientConfiguration
    GLIDE_AVAILABLE = True
except ImportError:
    print("⚠️ Glide not available, using local cache only")
    GLIDE_AVAILABLE = False

load_dotenv()

# Configuration
VALKEY_HOST = os.getenv("VALKEY_HOST", "localhost")
VALKEY_PORT = int(os.getenv("VALKEY_PORT", 6379))
VALKEY_USE_TLS = os.getenv("VALKEY_USE_TLS", "true").lower() == "true"
IS_LOCAL_DEV = os.getenv("IS_LOCAL_DEV", "false").lower() == "true"
ENABLE_CACHE = os.getenv("ENABLE_CACHE", "true").lower() == "true"

# Cache TTL settings (in seconds)
THREAD_CACHE_TTL = 3600  # 1 hour
SQL_CACHE_TTL = 86400  # 24 hours
SCHEMA_CACHE_TTL = 604800  # 7 days for table schema cache
CONVERSATION_CACHE_TTL = 600  # 10 minutes
TABLE_SELECTION_CACHE_TTL = 2592000  # 30 days for table selection patterns
FEEDBACK_CACHE_TTL = 7776000  # 90 days for feedback data
TOKEN_USAGE_CACHE_TTL = 86400  # 24 hours for daily limits

# Cache key prefixes
CACHE_PREFIX = "bi_slack_bot"
THREAD_CACHE_PREFIX = f"{CACHE_PREFIX}:thread"
SQL_CACHE_PREFIX = f"{CACHE_PREFIX}:sql"
SCHEMA_CACHE_PREFIX = f"{CACHE_PREFIX}:schema"
CONVERSATION_CACHE_PREFIX = f"{CACHE_PREFIX}:conversation"
TABLE_SELECTION_PREFIX = f"{CACHE_PREFIX}:table_selection"
TABLE_SAMPLES_PREFIX = f"{CACHE_PREFIX}:table_samples"
FEEDBACK_PREFIX = f"{CACHE_PREFIX}:feedback"
TOKEN_USAGE_PREFIX = f"{CACHE_PREFIX}:token_usage"
DAILY_USAGE_PREFIX = f"{TOKEN_USAGE_PREFIX}:daily"
HOURLY_USAGE_PREFIX = f"{TOKEN_USAGE_PREFIX}:hourly"
THREAD_USAGE_PREFIX = f"{TOKEN_USAGE_PREFIX}:thread"


class CacheManager:
    """Manages all caching operations with Valkey/Redis and local fallback"""
    
    def __init__(self):
        self.valkey_client = None
        self._local_cache = {
            'thread': {},
            'sql': {},
            'schema': {},
            'conversation': {},
            'table_selection': {},
            'table_samples': {},
            'feedback': {},
            'token_usage': {}
        }
    
    async def init_valkey_client(self):
        """Initialize Valkey client - must be called in async context"""
        if IS_LOCAL_DEV or not GLIDE_AVAILABLE:
            if not GLIDE_AVAILABLE:
                print("🏠 Glide not available - using local cache only")
            else:
                print("🏠 Local development mode - skipping Valkey connection")
            self.valkey_client = None
            return

        try:
            addresses = [NodeAddress(VALKEY_HOST, VALKEY_PORT)]
            config = GlideClusterClientConfiguration(
                addresses=addresses,
                use_tls=VALKEY_USE_TLS,
                request_timeout=10000,
            )

            self.valkey_client = await GlideClusterClient.create(config)
            pong = await self.valkey_client.ping()
            print(f"✅ Valkey connection established: {pong}")

        except Exception as e:
            print(f"❌ Valkey connection failed: {e}")
            self.valkey_client = None

    async def ensure_valkey_connection(self):
        """Ensure Valkey client is initialized"""
        if self.valkey_client is None:
            await self.init_valkey_client()

    def convert_to_serializable(self, obj):
        """Convert non-serializable objects to serializable format"""
        if isinstance(obj, dict):
            return {k: self.convert_to_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_to_serializable(item) for item in obj]
        elif hasattr(obj, 'isoformat'):  # datetime, Timestamp
            return obj.isoformat()
        elif hasattr(obj, 'to_dict'):  # DataFrame
            return obj.to_dict()
        elif isinstance(obj, (int, float, str, bool, type(None))):
            return obj
        else:
            return str(obj)

    async def get(self, key: str, default=None):
        """Safely get value from cache with fallback"""
        if not ENABLE_CACHE:
            return default
            
        await self.ensure_valkey_connection()

        if self.valkey_client:
            try:
                value = await self.valkey_client.get(key)
                if value:
                    return json.loads(value)
                return default
            except Exception as e:
                print(f"⚠️ Valkey GET error for {key}: {e}")
                return default
        else:
            cache_type = key.split(':')[1] if ':' in key else 'thread'
            return self._local_cache.get(cache_type, {}).get(key, default)

    async def set(self, key: str, value: Any, ex: int = None):
        """Safely set value in cache with fallback"""
        if not ENABLE_CACHE:
            return True
            
        await self.ensure_valkey_connection()

        if self.valkey_client:
            try:
                json_value = json.dumps(value)
                if ex:
                    await self.valkey_client.set(key, json_value)
                    await self.valkey_client.expire(key, ex)
                else:
                    await self.valkey_client.set(key, json_value)
                return True
            except TypeError as te:
                print(f"⚠️ JSON serialization error for {key}: {te}")
                print(f"⚠️ Problematic value type: {type(value)}")
                # Try to convert to a serializable format
                try:
                    if isinstance(value, dict):
                        value = self.convert_to_serializable(value)
                        json_value = json.dumps(value)
                        if ex:
                            await self.valkey_client.set(key, json_value)
                            await self.valkey_client.expire(key, ex)
                        else:
                            await self.valkey_client.set(key, json_value)
                        return True
                except Exception as e2:
                    print(f"⚠️ Failed to convert to serializable: {e2}")
                    return False
            except Exception as e:
                print(f"⚠️ Valkey SET error for {key}: {e}")
                return False
        else:
            # Fallback to local cache
            cache_type = key.split(':')[1] if ':' in key else 'thread'
            if cache_type not in self._local_cache:
                self._local_cache[cache_type] = {}
            try:
                # Ensure value is serializable for consistency
                _ = json.dumps(value)
                self._local_cache[cache_type][key] = value
            except TypeError:
                # Convert to serializable format
                if isinstance(value, dict):
                    value = self.convert_to_serializable(value)
                self._local_cache[cache_type][key] = value
            return True

    async def delete(self, key: str):
        """Safely delete key from cache with fallback"""
        await self.ensure_valkey_connection()

        if self.valkey_client:
            try:
                await self.valkey_client.delete([key])
                return True
            except Exception as e:
                print(f"⚠️ Valkey DELETE error for {key}: {e}")
                return False
        else:
            cache_type = key.split(':')[1] if ':' in key else 'thread'
            if cache_type in self._local_cache and key in self._local_cache[cache_type]:
                del self._local_cache[cache_type][key]
            return True

    async def exists(self, key: str) -> bool:
        """Check if key exists in cache with fallback"""
        await self.ensure_valkey_connection()

        if self.valkey_client:
            try:
                result = await self.valkey_client.exists([key])
                return result > 0
            except Exception as e:
                print(f"⚠️ Valkey EXISTS error for {key}: {e}")
                return False
        else:
            cache_type = key.split(':')[1] if ':' in key else 'thread'
            return key in self._local_cache.get(cache_type, {})

    async def close_connection(self):
        """Close Valkey connection gracefully"""
        if self.valkey_client:
            try:
                await self.valkey_client.close()
                print("🔌 Valkey connection closed")
            except Exception as e:
                print(f"❌ Error closing Valkey connection: {e}")
            self.valkey_client = None

    async def clear_cache_type(self, cache_type: str):
        """Clear all keys for a specific cache type"""
        if self.valkey_client:
            try:
                # For production, we'd need to scan and delete by pattern
                # For now, this is a simple implementation
                print(f"🧹 Cache type {cache_type} clearing requested (Valkey)")
            except Exception as e:
                print(f"❌ Error clearing {cache_type} cache: {e}")
        else:
            # Clear local cache
            if cache_type in self._local_cache:
                self._local_cache[cache_type].clear()
                print(f"🧹 {cache_type} cache cleared (local)")

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        stats = {
            "cache_enabled": ENABLE_CACHE,
            "valkey_connected": self.valkey_client is not None,
            "local_cache_sizes": {}
        }
        
        for cache_type, cache_data in self._local_cache.items():
            stats["local_cache_sizes"][cache_type] = len(cache_data)
            
        return stats

    async def check_valkey_health(self) -> Dict[str, Any]:
        """Check Valkey health status"""
        if IS_LOCAL_DEV:
            return {
                "status": "fallback",
                "mode": "local_development",
                "message": "Using local cache in development mode"
            }
        
        await self.ensure_valkey_connection()
        
        if self.valkey_client:
            try:
                await self.valkey_client.ping()
                return {
                    "status": "healthy",
                    "connection": "active",
                    "host": VALKEY_HOST,
                    "port": VALKEY_PORT,
                    "tls": VALKEY_USE_TLS
                }
            except Exception as e:
                return {
                    "status": "unhealthy",
                    "error": str(e),
                    "fallback": "local_cache"
                }
        else:
            return {
                "status": "fallback",
                "connection": "failed",
                "mode": "local_cache"
            }


# Global cache manager instance
cache_manager = CacheManager()

# Convenience functions for backward compatibility
async def init_valkey_client():
    """Initialize the global cache manager"""
    return await cache_manager.init_valkey_client()

async def check_valkey_health():
    """Check Valkey health"""
    return await cache_manager.check_valkey_health()

async def safe_valkey_get(key: str, default=None):
    """Get from cache"""
    return await cache_manager.get(key, default)

async def safe_valkey_set(key: str, value: Any, ex: int = None):
    """Set in cache"""
    return await cache_manager.set(key, value, ex)

async def safe_valkey_delete(key: str):
    """Delete from cache"""
    return await cache_manager.delete(key)

async def safe_valkey_exists(key: str) -> bool:
    """Check if key exists"""
    return await cache_manager.exists(key)

async def close_valkey_connection():
    """Close connection"""
    return await cache_manager.close_connection()