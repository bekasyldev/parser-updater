from abc import ABC, abstractmethod
import logging
import time
import asyncio
from typing import Dict, Optional, List, Tuple
from services.proxy_service import ProxyService

class BaseParser(ABC):
    def __init__(self, context, semaphore):
        self.context = context
        self.semaphore = semaphore
        self.proxy_service = ProxyService()

    async def create_page(self):
        """Create a new page with proxy configuration"""
        proxy_config = await self.proxy_service.get_proxy_config()
        
        # Create a new context with proxy
        context = await self.context.browser.new_context(
            proxy=proxy_config,
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        # Create and return the page
        page = await context.new_page()
        return page

    @abstractmethod
    async def parse_product(self, url: str) -> Dict:
        pass

    async def parse_urls(self, urls: List[str]) -> List[Tuple[Dict, float]]:
        """Parse a batch of URLs concurrently"""
        tasks = [self.parse_product(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out errors and format results
        valid_results = []
        for result in results:
            if isinstance(result, tuple) and not isinstance(result, Exception):
                valid_results.append(result)
            else:
                logging.error(f"Error parsing URL: {result}")
        
        return valid_results

    async def _safe_get_text(self, selector: str) -> Optional[str]:
        try:
            element = await self.page.query_selector(selector)
            if element:
                return await element.text_content()
            return None
        except Exception as e:
            logging.debug(f"Error getting text for selector {selector}: {e}")
            return None 