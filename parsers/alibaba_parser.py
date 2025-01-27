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
                
                # Set headers
                await page.set_extra_http_headers({
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:134.0) Gecko/20100101 Firefox/134.0",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "DNT": "1",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1"
                })

                await page.goto(url, wait_until='networkidle')
                await page.wait_for_timeout(3000)

                # Check if product is unavailable
                try:
                    unavailable = await page.query_selector('.product-unsafe')
                    if unavailable and await unavailable.is_visible():
                        return {
                            'product_url': url,
                            'articul': url.split('_')[-1].split('.')[0],
                            'is_available': False,
                            'price': 0,
                            'reviews': 0,
                            'rating': 0
                        }, time.time() - start_time
                except Exception as e:
                    logging.debug(f"Error checking availability: {str(e)}")

                # Extract data using JavaScript
                data = await page.evaluate('''() => {
                    try {
                        // Get price - handle multiple price formats
                        let price = 0;
                        
                        // Try promotion price first
                        const promotionPrice = document.querySelector('.promotion-price strong.normal');
                        if (promotionPrice) {
                            const priceText = promotionPrice.textContent.trim();
                            price = parseFloat(priceText.replace(/[^0-9.]/g, ''));
                        }
                        
                        // If no promotion price, try price list
                        if (!price) {
                            const priceList = document.querySelector('.price-list');
                            if (priceList) {
                                const firstPrice = priceList.querySelector('.price span');
                                if (firstPrice) {
                                    const priceText = firstPrice.textContent.trim();
                                    price = parseFloat(priceText.replace(/[^0-9.]/g, ''));
                                }
                            }
                        }

                        // Get reviews and rating
                        let rating = 0;
                        let reviews = 0;
                        let hasReviews = false;
                        
                        // Check for "No reviews yet" text
                        const noReviewsText = document.querySelector('.detail-review-item.detail-separator');
                        if (noReviewsText && noReviewsText.textContent.includes('No reviews yet')) {
                            hasReviews = false;
                        } else {
                            // Try to get star rating
                            const starElement = document.querySelector('.detail-star');
                            if (starElement) {
                                const ratingText = starElement.textContent.trim();
                                rating = parseFloat(ratingText);
                                hasReviews = true;
                            }

                            // Try to get review count
                            const reviewElement = document.querySelector('.detail-review');
                            if (reviewElement) {
                                const reviewText = reviewElement.textContent.trim();
                                const match = reviewText.match(/\\((\\d+)\\s+review/);
                                if (match) {
                                    reviews = parseInt(match[1]);
                                    hasReviews = true;
                                }
                            }
                        }

                        return {
                            price: price || 0,
                            rating: hasReviews ? rating : 0,
                            reviews: hasReviews ? reviews : 0,
                            is_available: price > 0
                        };
                    } catch (e) {
                        console.error('Error extracting data:', e);
                        return null;
                    }
                }''')

                if not data:
                    result = {
                        'product_url': url,
                        'articul': url.split('_')[-1].split('.')[0],
                        'is_available': False,
                        'price': 0,
                        'reviews': 0,
                        'rating': 0
                    }
                else:
                    result = {
                        'product_url': url,
                        'articul': url.split('_')[-1].split('.')[0],
                        'is_available': data['is_available'],
                        'price': data['price'],
                        'reviews': data['reviews'],
                        'rating': data['rating']
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