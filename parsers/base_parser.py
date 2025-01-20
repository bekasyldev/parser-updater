from abc import ABC, abstractmethod
import logging
import time
import asyncio
from typing import Dict, Optional, List, Tuple

class BaseParser(ABC):
    def __init__(self, context, semaphore):
        self.context = context
        self.semaphore = semaphore

    async def create_page(self):
        """Create a new page"""
        try:
            # Create and configure the page
            page = await self.context.new_page()
            
            # Set headers
            await page.set_extra_http_headers({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            })
            
            return page
        except Exception as e:
            logging.error(f"Error creating page: {str(e)}")
            raise

    @abstractmethod
    async def parse_product(self, url: str) -> Dict:
        pass

    async def parse_urls(self, urls: List[str]) -> List[Tuple[Dict, float]]:
        """Parse a batch of URLs concurrently"""
        tasks = [self.parse_product(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
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