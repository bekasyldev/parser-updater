import logging
import time
import re
import json
import random
from urllib.parse import urlparse, parse_qs
from typing import Dict
from .base_parser import BaseParser

class OzonParser(BaseParser):
    def __init__(self, context, semaphore):
        super().__init__(context, semaphore)
        # Load proxies from file
        try:
            with open('proxy_http_ip_100.txt', 'r') as f:
                self.proxies = [line.strip() for line in f if line.strip()]
        except Exception as e:
            logging.error(f"Error loading proxies: {str(e)}")
            self.proxies = []

    def clean_url(self, url: str) -> str:
        """Clean Ozon URL to remove tracking parameters"""
        parsed = urlparse(url)
        # Get only essential query parameters
        query = parse_qs(parsed.query)
        clean_query = {}
        # Keep only important parameters, remove tracking ones
        for key in ['keywords']:
            if key in query:
                clean_query[key] = query[key][0]
        
        # Reconstruct URL with minimal parameters
        path = parsed.path
        if clean_query:
            path += '?' + '&'.join(f"{k}={v}" for k, v in clean_query.items())
        return f"https://www.ozon.ru{path}"

    async def parse_product(self, url: str) -> Dict:
        async with self.semaphore:
            start_time = time.time()
            try:
                # Clean the URL
                url = self.clean_url(url)
                
                # Get random proxy
                if self.proxies:
                    proxy = random.choice(self.proxies)
                    proxy_parts = proxy.split(':')
                    if len(proxy_parts) == 4:  # proxy with auth
                        proxy_url = f"http://{proxy_parts[2]}:{proxy_parts[3]}@{proxy_parts[0]}:{proxy_parts[1]}"
                    else:  # proxy without auth
                        proxy_url = f"http://{proxy_parts[0]}:{proxy_parts[1]}"
                    
                    # Create new page with proxy
                    context = await self.context.browser.new_context(
                        proxy={
                            "server": proxy_url,
                            "bypass": "<-loopback>" # Don't use proxy for localhost
                        }
                    )
                    page = await context.new_page()
                else:
                    page = await self.context.new_page()

                # Configure page to look more like a real browser
                await page.set_extra_http_headers({
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:134.0) Gecko/20100101 Firefox/134.0",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Accept-Encoding": "gzip, deflate, br",
                    "DNT": "1",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1"
                })

                # Set required cookies
                await page.context.add_cookies([
                    {
                        'name': 'xcid',
                        'value': 'random_value',
                        'domain': '.ozon.ru',
                        'path': '/'
                    },
                    {
                        'name': 'js', 
                        'value': '1',
                        'domain': '.ozon.ru',
                        'path': '/'
                    }
                ])

                # Configure page to enable JavaScript
                await page.evaluate('''() => {
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                }''')

                # Try to load page with retries
                max_load_retries = 3
                for attempt in range(max_load_retries):
                    try:
                        await page.goto(url, timeout=30000)  # Reduced timeout
                        await page.wait_for_load_state('domcontentloaded', timeout=30000)
                        break
                    except Exception as e:
                        if attempt == max_load_retries - 1:
                            raise
                        logging.warning(f"Load attempt {attempt + 1} failed: {str(e)}")
                        await page.wait_for_timeout(5000)

                content = await page.content()
                logging.info(f"Page source for {url}:")
                logging.info(content)

                # Handle challenge page with more patience
                max_retries = 5
                retry_count = 0
                
                while retry_count < max_retries:
                    if await page.query_selector('.container .message'):
                        logging.warning(f"Challenge detected for {url}, attempt {retry_count + 1}/{max_retries}")
                        
                        # Try to solve challenge
                        try:
                            reload_button = await page.wait_for_selector('#reload-button', timeout=5000)
                            if reload_button:
                                await reload_button.click()
                                await page.wait_for_timeout(10000)  # Longer wait after click
                        except Exception as e:
                            logging.debug(f"Error clicking reload: {str(e)}")

                        # Wait longer between retries
                        await page.wait_for_timeout(15000)
                        await page.reload(wait_until='networkidle')
                        await page.wait_for_timeout(8000)
                        
                        retry_count += 1
                    else:
                        break

                if retry_count == max_retries:
                    logging.error(f"Failed to bypass challenge for {url} after {max_retries} attempts")
                    return {'product_url': url, 'error': 'Challenge page persistent'}, time.time() - start_time

                # Try to find JSON-LD data
                content = await page.content()
                match = re.search(r'<script[^>]*type="application/ld\+json"[^>]*>(\{[^<]+\})</script>', content)

                if not match:
                    # Try alternative selectors for price and reviews
                    price = await page.evaluate('''() => {
                        try {
                            const selectors = [
                                'span[data-widget="webPrice"]',
                                '.price-block__final-price',
                                '.price-block__price'
                            ];
                            
                            for (const selector of selectors) {
                                const elem = document.querySelector(selector);
                                if (elem) {
                                    const price = elem.textContent.replace(/[^\d]/g, '');
                                    if (price) return parseInt(price);
                                }
                            }
                            return null;
                        } catch (e) {
                            console.error('Error getting price:', e);
                            return null;
                        }
                    }''')

                    reviews_data = await page.evaluate('''() => {
                        try {
                            const reviewsElem = document.querySelector('[data-widget="webReviewProductScore"]');
                            if (reviewsElem) {
                                const text = reviewsElem.textContent;
                                const ratingMatch = text.match(/(\d+\.?\d*)/);
                                const reviewsMatch = text.match(/(\d+)\s+отзыв/);
                                return {
                                    rating: ratingMatch ? parseFloat(ratingMatch[1]) : 0,
                                    total: reviewsMatch ? parseInt(reviewsMatch[1]) : 0
                                };
                            }
                            return null;
                        } catch (e) {
                            console.error('Error getting reviews:', e);
                            return null;
                        }
                    }''')

                    # Get SKU from URL
                    sku = url.split('/')[-1].split('-')[-1].split('?')[0]

                    result = {
                        'product_url': url,
                        'articul': sku,
                        'is_available': price is not None,
                        'price': price or 0,
                        'reviews': reviews_data['total'] if reviews_data else 0,
                        'rating': reviews_data['rating'] if reviews_data else 0
                    }
                else:
                    try:
                        json_data = json.loads(match.group(1))
                        result = {
                            'product_url': url,
                            'articul': json_data.get('sku'),
                            'is_available': 'InStock' in json_data.get('offers', {}).get('availability', ''),
                            'price': int(json_data.get('offers', {}).get('price', 0)),
                            'reviews': int(json_data.get('aggregateRating', {}).get('reviewCount', 0)),
                            'rating': float(json_data.get('aggregateRating', {}).get('ratingValue', 0))
                        }
                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        logging.error(f"Error parsing JSON data for {url}: {str(e)}")
                        return {'product_url': url, 'error': str(e)}, time.time() - start_time

                parse_time = time.time() - start_time
                logging.info(f"Successfully parsed data in {parse_time:.2f}s: {result}")
                # Clean up contexts and pages
                try:
                    if self.proxies:
                        await context.close()
                    await page.close()
                except:
                    pass
                return result, parse_time

            except Exception as e:
                parse_time = time.time() - start_time
                logging.error(f"Error parsing {url} in {parse_time:.2f}s: {str(e)}")
                # Clean up contexts and pages
                try:
                    if 'context' in locals():
                        await context.close()
                    if 'page' in locals():
                        await page.close()
                except:
                    pass
                return {'product_url': url, 'error': str(e)}, parse_time 