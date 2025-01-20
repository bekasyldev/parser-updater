import logging
import time
import json
import re
from typing import Dict
from .base_parser import BaseParser

class KaspiParser(BaseParser):
    async def parse_product(self, url: str) -> Dict:
        async with self.semaphore:
            start_time = time.time()
            try:
                page = await self.context.new_page()
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
                await page.goto(url, wait_until='networkidle')
                await page.wait_for_timeout(2000)

                # Get JSON-LD data
                content = await page.content()

                print('content', content)
                # Find all JSON-LD scripts
                matches = re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', content, re.DOTALL)
                print('matches', matches)
                
                product_data = None
                for match in matches:
                    try:
                        json_data = json.loads(match.group(1))
                        # Check if this is the product data we want
                        if json_data.get('@type') == 'Product' and json_data.get('offers'):
                            product_data = json_data
                            break
                    except json.JSONDecodeError:
                        continue

                if product_data:
                    try:
                        # Extract data from JSON-LD
                        articul = product_data.get('productID') or product_data.get('sku')
                        
                        # Get price from offers array
                        offers = product_data.get('offers', [])
                        price = None
                        is_available = False

                        if isinstance(offers, list):
                            # Find first offer with price
                            for offer in offers:
                                if isinstance(offer, dict) and offer.get('price'):
                                    try:
                                        price = int(float(offer['price']))
                                        is_available = offer.get('availability') == 'InStock'
                                        break
                                    except (ValueError, TypeError):
                                        continue
                        else:
                            # Single offer object
                            try:
                                price = int(float(offers.get('price', 0)))
                                is_available = offers.get('availability') == 'InStock'
                            except (ValueError, TypeError):
                                pass

                        # Get reviews data from backend state
                        reviews_data = await page.evaluate('''() => {
                            try {
                                const scripts = Array.from(document.querySelectorAll('script'));
                                for (const script of scripts) {
                                    const content = script.textContent || '';
                                    if (content.includes('BACKEND.components.productReviews')) {
                                        const match = content.match(/BACKEND\.components\.productReviews\s*=\s*({[^;]+})/);
                                        if (match) {
                                            const data = JSON.parse(match[1]);
                                            return {
                                                total: data.productReviews?.reviewsCount || data.rating?.reviewsCount || 0,
                                                rating: data.productReviews?.rating?.global || data.rating?.global || 0
                                            };
                                        }
                                    }
                                }
                                return null;
                            } catch (e) {
                                console.error('Error parsing reviews data:', e);
                                return null;
                            }
                        }''')

                        result = {
                            'product_url': url,
                            'articul': articul,
                            'is_available': is_available,
                            'price': price,
                            'total_reviews': reviews_data['total'] if reviews_data else None,
                            'rating': reviews_data['rating'] if reviews_data else None
                        }

                        parse_time = time.time() - start_time
                        logging.info(f"Successfully parsed data in {parse_time:.2f}s: {result}")
                        await page.close()
                        return result, parse_time

                    except Exception as e:
                        logging.error(f"Error processing JSON-LD data for {url}: {str(e)}")
                        return {'product_url': url, 'error': f'Data processing error: {str(e)}'}, time.time() - start_time

                # Fallback to DOM parsing if no JSON-LD found
                logging.warning(f"No valid JSON-LD product data found for {url}, falling back to DOM parsing")
                
                # Get price from DOM
                price = await page.evaluate('''() => {
                    try {
                        const priceElement = document.querySelector('[data-test-id="product-price"]');
                        if (priceElement) {
                            const priceText = priceElement.textContent.replace(/[^\d]/g, '');
                            return parseInt(priceText);
                        }
                        return null;
                    } catch (e) {
                        console.error('Error getting price:', e);
                        return null;
                    }
                }''')

                # Get articul from URL
                articul = url.split('/')[-1].split('-')[-1].split('?')[0]

                # Get reviews data
                reviews_data = await page.evaluate('''() => {
                    try {
                        const scripts = Array.from(document.querySelectorAll('script'));
                        for (const script of scripts) {
                            const content = script.textContent || '';
                            if (content.includes('BACKEND.components.productReviews')) {
                                const match = content.match(/BACKEND\.components\.productReviews\s*=\s*({[^;]+})/);
                                if (match) {
                                    const data = JSON.parse(match[1]);
                                    return {
                                        total: data.productReviews?.reviewsCount || data.rating?.reviewsCount || 0,
                                        rating: data.productReviews?.rating?.global || data.rating?.global || 0
                                    };
                                }
                            }
                        }
                        return null;
                    } catch (e) {
                        console.error('Error parsing reviews data:', e);
                        return null;
                    }
                }''')

                result = {
                    'product_url': url,
                    'articul': articul,
                    'is_available': True if price else False,
                    'price': price,
                    'total_reviews': reviews_data['total'] if reviews_data else None,
                    'rating': reviews_data['rating'] if reviews_data else None
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