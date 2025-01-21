from abc import ABC, abstractmethod
import logging
import time
import asyncio
from typing import Dict, Optional, List, Tuple
from services.proxy_service import get_random_proxy_ip

class BaseParser(ABC):
    def __init__(self, context, semaphore):
        self.context = context
        self.semaphore = semaphore
        self.proxy_file = "proxy_http_ip.txt"

    async def create_page(self):
        """Create a new page with proxy"""
        try:
            # Get random proxy IP
            proxy_ip = get_random_proxy_ip(self.proxy_file)
            
            # Create new context with proxy
            context = await self.context.browser.new_context(
                proxy={
                    "server": f"http://{proxy_ip}",
                    "username": "PrsRUSGF6FZF1",
                    "password": "JhSiykag"
                },
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            # Create and configure the page
            page = await context.new_page()
            
            # Set headers
            await page.set_extra_http_headers({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            })
            
            logging.info(f"Created page with proxy: {proxy_ip}")
            return page
            
        except Exception as e:
            logging.error(f"Error creating page with proxy: {str(e)}")
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