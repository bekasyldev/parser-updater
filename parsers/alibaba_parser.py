import logging
import time
import json
import re
from typing import Dict
import asyncio
from .base_parser import BaseParser

class AlibabaParser(BaseParser):
    def __init__(self, context=None, semaphore=None):
        if semaphore is None:
            semaphore = asyncio.Semaphore(10)
        super().__init__(context, semaphore)

    async def parse_product(self, url: str) -> Dict:
        async with self.semaphore:
            start_time = time.time()
            try:
                page = await self.context.new_page()
                await page.set_extra_http_headers({
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:134.0) Gecko/20100101 Firefox/134.0",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "DNT": "1",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1"
                })
                await page.goto(url, wait_until='networkidle')
                await page.wait_for_timeout(1000)

                # Get JSON-LD data
                content = await page.content()
                match = re.search(r'<script[^>]*type="application/ld\+json"[^>]*>(\[?\{[^<]+\}?\]?)</script>', content)
                
                if match:
                    try:
                        json_data = json.loads(match.group(1))
                        # Handle both single object and array cases
                        if isinstance(json_data, list):
                            # Find the Product object in the array
                            for item in json_data:
                                if item.get('@type') == 'Product':
                                    json_data = item
                                    break
                        
                        # Extract data from JSON-LD
                        price = float(json_data.get('offers', {}).get('price', 0))
                        is_available = 'InStock' in json_data.get('offers', {}).get('availability', '')
                        articul = json_data.get('sku') or json_data.get('mpn')
                        
                        # Get rating from review array
                        reviews = json_data.get('review', [])
                        rating = 0
                        if reviews:
                            if isinstance(reviews, list):
                                review = reviews[0]
                            else:
                                review = reviews
                            rating = float(review.get('reviewRating', {}).get('ratingValue', 0))

                        result = {
                            'product_url': url,
                            'articul': articul,
                            'is_available': is_available,
                            'price': price,
                            'total_reviews': len(reviews) if isinstance(reviews, list) else 1 if reviews else 0,
                            'rating': rating
                        }

                        parse_time = time.time() - start_time
                        logging.info(f"Successfully parsed data in {parse_time:.2f}s: {result}")
                        await page.close()
                        return result, parse_time

                    except json.JSONDecodeError as e:
                        logging.error(f"Error parsing JSON-LD data for {url}: {str(e)}")
                        return {'product_url': url, 'error': f'JSON parse error: {str(e)}'}, time.time() - start_time

                # Fallback to DOM parsing if no JSON-LD found
                price = await page.evaluate('''() => {
                    try {
                        const priceElem = document.querySelector('div.price-list .price');
                        return priceElem ? parseFloat(priceElem.textContent.replace(/[^\d.]/g, '')) : 0;
                    } catch (e) {
                        console.error('Error getting price:', e);
                        return 0;
                    }
                }''')
                
                reviews_data = await page.evaluate('''() => {
                    try {
                        const reviewsElem = document.querySelector('div.verified-reviews');
                        const ratingElem = document.querySelector('div.score');
                        
                        let total = 0;
                        let rating = 0;
                        
                        if (reviewsElem) {
                            // Extract number from text like "Based on 190 reviews"
                            const reviewText = reviewsElem.textContent;
                            const match = reviewText.match(/Based on\s+(\d+)\s+reviews/);
                            if (match) {
                                total = parseInt(match[1]);
                            }
                        }
                        
                        if (ratingElem) {
                            // Get rating value directly
                            rating = parseFloat(ratingElem.textContent.trim());
                        }
                        
                        return {
                            total: total,
                            rating: rating
                        };
                    } catch (e) {
                        console.error('Error getting reviews:', e);
                        return {
                            total: 0,
                            rating: 0
                        };
                    }
                }''')

                print('reviews_data', reviews_data)

                # Get SKU from URL
                articul = url.split('_')[-1].split('.')[0]

                result = {
                    'product_url': url,
                    'articul': articul,
                    'is_available': True,
                    'price': price,
                    'total_reviews': reviews_data['total'],
                    'rating': reviews_data['rating']
                }

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