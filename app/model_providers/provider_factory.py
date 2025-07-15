#!/usr/bin/env python3
"""
Model Provider Factory
Creates the appropriate model provider based on configuration
"""

import os
from typing import Optional
from .base_provider import BaseModelProvider, ModelProviderError
from .openai_provider import create_openai_provider
from .anthropic_provider import create_anthropic_provider


class ModelProviderFactory:
    """Factory for creating model provider instances"""
    
    _instance = None
    _provider = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self._provider = None
    
    def get_provider(self, force_refresh: bool = False) -> BaseModelProvider:
        """Get the configured model provider instance"""
        
        if self._provider and not force_refresh:
            return self._provider
        
        provider_name = os.getenv('MODEL_PROVIDER', 'openai').lower()
        
        print(f"🏭 Creating model provider: {provider_name}")
        
        if provider_name == 'openai':
            self._provider = create_openai_provider()
        elif provider_name == 'anthropic':
            self._provider = create_anthropic_provider()
        else:
            raise ModelProviderError(f"Unknown model provider: {provider_name}. Supported: 'openai', 'anthropic'")
        
        return self._provider
    
    def switch_provider(self, provider_name: str) -> BaseModelProvider:
        """Switch to a different provider at runtime"""
        
        provider_name = provider_name.lower()
        print(f"🔄 Switching to model provider: {provider_name}")
        
        # Temporarily set the environment variable
        original_provider = os.getenv('MODEL_PROVIDER')
        os.environ['MODEL_PROVIDER'] = provider_name
        
        try:
            self._provider = None  # Clear cache
            provider = self.get_provider(force_refresh=True)
            return provider
        except Exception as e:
            # Restore original provider on error
            if original_provider:
                os.environ['MODEL_PROVIDER'] = original_provider
            else:
                os.environ.pop('MODEL_PROVIDER', None)
            raise e
    
    def get_available_providers(self) -> list:
        """Get list of available providers"""
        providers = []
        
        # Check OpenAI availability
        try:
            import openai
            if os.getenv('OPENAI_API_KEY'):
                providers.append('openai')
        except ImportError:
            pass
        
        # Check Anthropic availability
        try:
            import anthropic
            if os.getenv('ANTHROPIC_API_KEY') and os.getenv('ANTHROPIC_API_KEY') != 'your_anthropic_api_key_here':
                providers.append('anthropic')
        except ImportError:
            pass
        
        return providers
    
    def get_current_provider_info(self) -> dict:
        """Get information about the current provider"""
        
        provider = self.get_provider()
        
        return {
            'provider_name': provider.provider_name,
            'model_name': provider.model_name,
            'api_key_configured': bool(provider.api_key and provider.api_key != 'your_anthropic_api_key_here'),
            'available_providers': self.get_available_providers()
        }


# Global factory instance
provider_factory = ModelProviderFactory()


# Convenience functions
def get_model_provider(force_refresh: bool = False) -> BaseModelProvider:
    """Get the configured model provider"""
    return provider_factory.get_provider(force_refresh)


def switch_model_provider(provider_name: str) -> BaseModelProvider:
    """Switch to a different provider"""
    return provider_factory.switch_provider(provider_name)


def get_provider_info() -> dict:
    """Get current provider information"""
    return provider_factory.get_current_provider_info()