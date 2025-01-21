import logging
import time
import re
from typing import Dict
from .base_parser import BaseParser

class WildberriesParser(BaseParser):
    async def parse_product(self, url: str) -> Dict:
        async with self.semaphore:
            start_time = time.time()
            try:
                page = await self.context.new_page()
                await page.goto(url, wait_until='networkidle')
                await page.wait_for_timeout(2000)  # Wait for dynamic content

                # Check availability
                is_available = not await page.query_selector('p.sold-out-product')
                
                if not is_available:
                    return {'product_url': url, 'is_available': False}

                articul = url.split('/')[-1]

                # Get price
                price = await page.evaluate('''() => {
                    const selectors = [
                        "span.price-block__wallet-price",
                        "ins.price-block__final-price"
                    ];
                    
                    for (const selector of selectors) {
                        const elem = document.querySelector(selector);
                        if (elem && elem.textContent) {
                            const price = elem.textContent.replace(/[^\d]/g, '');
                            if (price) return parseInt(price);
                        }
                    }
                    return 0;
                }''')

                # Get reviews data
                reviews_data = await page.evaluate('''() => {
                    try {
                        const reviewsCount = document.querySelector('span.product-review__count-review');
                        const rating = document.querySelector('span.product-review__rating');
                        
                        return {
                            total: reviewsCount ? reviewsCount.textContent.replace(/[^\d]/g, '') : '',
                            rating: rating ? rating.textContent : ''
                        };
                    } catch (e) {
                        console.error('Error getting reviews:', e);
                        return null;
                    }
                }''')

                result = {
                    'product_url': url,
                    'is_available': True,
                    'price': price,
                    'total_reviews': int(reviews_data['total']) if reviews_data else '',
                    'rating': float(reviews_data['rating'].replace(',', '.')) if reviews_data else '',
                    'articul': articul
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