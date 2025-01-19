import asyncio
import aiohttp
from playwright.async_api import async_playwright
import logging
from typing import List, Dict
from datetime import datetime
import json
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'parser_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)

class AsyncKaspiParser:
    def __init__(self, max_concurrent: int = 10):
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.browser = None
        self.context = None
    
    async def init_browser(self):
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=True)
        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )

    async def close(self):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()

    async def parse_product(self, url: str) -> Dict:
        async with self.semaphore:
            start_time = time.time()
            try:
                page = await self.context.new_page()
                await page.goto(url, wait_until='networkidle')
                
                # Get articul from URL or page content
                articul = await page.evaluate('''() => {
                    try {
                        // Try to get articul from structured data
                        const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
                        for (const script of scripts) {
                            const data = JSON.parse(script.textContent);
                            if (data.sku) return data.sku;
                            if (data.mpn) return data.mpn;
                        }
                        
                        // Try to get from URL
                        const urlMatch = window.location.pathname.match(/\/p\/([^\/]+)/);
                        if (urlMatch) return urlMatch[1];
                        
                        // Try to get from page content
                        const articleElement = document.querySelector('[data-item-id]');
                        if (articleElement) return articleElement.getAttribute('data-item-id');
                        
                        return null;
                    } catch (e) {
                        console.error('Error getting articul:', e);
                        return null;
                    }
                }''')

                # Get other product data
                price = await page.evaluate('''() => {
                    scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
                    for (const script of scripts) {
                        try {
                            const data = JSON.parse(script.textContent);
                            if (data['@type'] === 'Product' && Array.isArray(data.offers)) {
                                for (const offer of data.offers) {
                                    if (offer.price && offer.priceCurrency === 'KZT') {
                                        return parseInt(offer.price);
                                    }
                                }
                            }
                        } catch (e) {
                            console.error('Error parsing JSON-LD:', e);
                        }
                    }
                    return null;
                }''')

                # Get delivery info
                delivery_info = await page.evaluate('''() => {
                    const deliveryElement = document.querySelector('.delivery-info');
                    if (!deliveryElement) return {};
                    return {
                        date: deliveryElement.querySelector('.delivery-date')?.textContent?.trim(),
                        price: deliveryElement.querySelector('.delivery-price')?.textContent?.trim()
                    };
                }''')

                # Get reviews data
                reviews_data = await page.evaluate('''() => {
                    try {
                        // Find the script containing product reviews data
                        const scripts = Array.from(document.querySelectorAll('script'));
                        for (const script of scripts) {
                            const content = script.textContent || '';
                            if (content.includes('BACKEND.components.productReviews')) {
                                // Extract the JSON object
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
                    'delivery_date': delivery_info.get('date'),
                    'delivery_price': delivery_info.get('price'),
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
                return {'product_url': url, 'error': str(e)}, parse_time

    async def parse_urls(self, urls: List[str]) -> List[Dict]:
        await self.init_browser()
        tasks = [self.parse_product(url) for url in urls]
        results = await asyncio.gather(*tasks)
        await self.close()
        return results

async def main():
    # Example usage
    urls = [
        "https://kaspi.kz/shop/p/product1",
        "https://kaspi.kz/shop/p/product2",
        # ... more URLs
    ]
    
    parser = AsyncKaspiParser(max_concurrent=10)
    results = await parser.parse_urls(urls)
    
    # Save results
    with open('results.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    asyncio.run(main())