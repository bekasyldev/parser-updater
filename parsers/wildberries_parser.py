import logging
import time
import json
import requests
from typing import Dict
from .base_parser import BaseParser

class WildberriesParser(BaseParser):
    async def parse_product(self, url: str) -> Dict:
        async with self.semaphore:
            start_time = time.time()
            try:
                # Extract product ID from URL
                product_id = url.split('/')[-2]
                
                # Use WB API v2 to get product data
                api_url = f'https://card.wb.ru/cards/v2/detail?appType=1&curr=rub&dest=82&spp=30&nm={product_id}'
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0)",
                    "Accept": "*/*"
                }
                
                response = requests.get(api_url, headers=headers)
                
                if not response.ok:
                    return {
                        'product_url': url,
                        'articul': product_id,
                        'is_available': False,
                        'price': 0,
                        'reviews': 0,
                        'rating': 0
                    }, time.time() - start_time

                data = response.json()
                if not data.get('data', {}).get('products'):
                    return {
                        'product_url': url,
                        'articul': product_id,
                        'is_available': False,
                        'price': 0,
                        'reviews': 0,
                        'rating': 0
                    }, time.time() - start_time

                product = data['data']['products'][0]
                
                # Get price from sizes[0].price.total
                price = 0
                if product.get('sizes') and len(product['sizes']) > 0:
                    price = int(product['sizes'][0].get('price', {}).get('total', 0) / 100)  # Convert kopeks to rubles
                
                result = {
                    'product_url': url,
                    'articul': product_id,
                    'is_available': product.get('totalQuantity', 0) > 0,
                    'price': price,
                    'reviews': product.get('feedbacks', 0),
                    'rating': product.get('rating', 0)
                }

                parse_time = time.time() - start_time
                logging.info(f"Successfully parsed data in {parse_time:.2f}s: {result}")
                return result, parse_time

            except Exception as e:
                parse_time = time.time() - start_time
                logging.error(f"Error parsing {url} in {parse_time:.2f}s: {str(e)}")
                return {'product_url': url, 'error': str(e)}, parse_time 