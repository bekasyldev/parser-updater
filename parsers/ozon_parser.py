import logging
import time
import re
import json
from typing import Dict
from .base_parser import BaseParser

class OzonParser(BaseParser):
    async def parse_product(self, url: str) -> Dict:
        async with self.semaphore:
            start_time = time.time()
            try:
                # Use proxy-enabled page
                page = await self.create_page()
                
                # Configure page headers
                await page.set_extra_http_headers({
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1'
                })

                await page.goto(url, wait_until='networkidle')
                await page.wait_for_timeout(2000)  # Wait longer for initial load

                # Check for challenge page
                if await page.query_selector('.container .message'):
                    logging.warning(f"Challenge detected for {url}, attempting to bypass...")
                    
                    # Wait for challenge to complete
                    await page.wait_for_timeout(2000)
                    await page.reload()
                    await page.wait_for_timeout(2000)

                # Try to find JSON-LD data
                content = await page.content()
                match = re.search(r'<script[^>]*type="application/ld\+json"[^>]*>(\{[^<]+\})</script>', content)

                if not match:
                    # Try alternative selectors for price and reviews
                    price = await page.evaluate('''() => {
                        const priceElem = document.querySelector('span[data-widget="webPrice"]');
                        if (priceElem) {
                            return parseInt(priceElem.textContent.replace(/[^\d]/g, ''));
                        }
                        return null;
                    }''')

                    reviews_data = await page.evaluate('''() => {
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
                    }''')

                    # Get SKU from URL
                    sku = url.split('/')[-1].split('-')[-1].split('?')[0]

                    result = {
                        'product_url': url,
                        'articul': sku,
                        'is_available': price is not None,
                        'price': price or 0,
                        'total_reviews': reviews_data['total'] if reviews_data else 0,
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
                            'total_reviews': int(json_data.get('aggregateRating', {}).get('reviewCount', 0)),
                            'rating': float(json_data.get('aggregateRating', {}).get('ratingValue', 0))
                        }
                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        logging.error(f"Error parsing JSON data for {url}: {str(e)}")
                        return {'product_url': url, 'error': str(e)}, time.time() - start_time

                parse_time = time.time() - start_time
                logging.info(f"Successfully parsed data in {parse_time:.2f}s: {result}")
                await page.close()
                return result, parse_time

            except Exception as e:
                parse_time = time.time() - start_time
                logging.error(f"Error parsing {url} in {parse_time:.2f}s: {str(e)}")
                if 'page' in locals():
                    await page.close()
                return {'product_url': url, 'error': str(e)}, parse_time 