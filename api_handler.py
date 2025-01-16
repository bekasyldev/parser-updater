import requests
import logging
from datetime import datetime
import json
from typing import List, Dict, Optional
from pydantic import BaseModel

class Product(BaseModel):
    articul: str
    product_url: str


class APIHandler:
    def __init__(self):
        self.base_url = "https://parse.trendoanalytics.kz"
        self.headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def get_urls(self, marketplace: str) -> List[tuple[str, str]]:
        """
        Get product URLs and articuls from the API for a specific marketplace
        Returns: List of tuples (url, articul)
        """
        try:
            logging.info(f"Fetching URLs for {marketplace} from {self.base_url}")
            
            response = requests.get(
                f"{self.base_url}/api_table/get_data/{marketplace}",
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                logging.debug(f"Raw response: {data[:2]}")  # Log first 2 items for debugging
                
                # Convert raw data to list of (url, articul) tuples
                products = []
                for item in data:
                    # Check both 'url' and 'product_url' fields
                    url = item.get('url') or item.get('product_url')
                    articul = item.get('articul')
                    if url and articul:
                        products.append((url, articul))
                    else:
                        logging.warning(f"Skipping item due to missing data: {item}")
                
                logging.info(f"Found {len(products)} products for {marketplace}")
                if products:
                    logging.debug(f"Sample product: {products[0]}")
                return products
            else:
                logging.error(f"API returned status code {response.status_code}")
                return []
            
        except Exception as e:
            logging.error(f"Error getting URLs for {marketplace}: {str(e)}", exc_info=True)
            return []

    def send_kaspi_data(self, data: List[Dict]) -> bool:
        """
        Send parsed Kaspi data to the API
        
        Args:
            data (List[Dict]): List of parsed product data
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Format data according to API requirements
            formatted_data = []
            for item in data:
                formatted_item = {
                    "articul": "",  # You might want to add article number if available
                    "product_url": item.get('product_url', ''),
                    "is_available": item.get('is_available', False),
                    "price": item.get('price', 0),
                    "delivery_price": item.get('delivery_price', ''),
                    "delivery_date": item.get('delivery_date', ''),
                    "total_reviews": item.get('total_reviews', 0),
                    "rating": item.get('rating', 0),
                    "updated_at": datetime.now().isoformat()
                }
                formatted_data.append(formatted_item)

            response = requests.post(
                f"{self.base_url}/api_table/set_data/kaspi",
                headers=self.headers,
                data=json.dumps(formatted_data)
            )
            response.raise_for_status()
            
            logging.info(f"Successfully sent {len(data)} items to API")
            return True
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error sending data to API: {str(e)}")
            return False

    def send_wildberries_data(self, data: List[Dict]) -> bool:
        """Send parsed Wildberries data to the API"""
        try:
            formatted_data = [{
                "product_url": item.get('product_url', ''),
                "is_available": item.get('is_available', False),
                "price": item.get('price', 0),
                "rating": item.get('rating', '0'),
                "reviews": item.get('reviews', '0'),
                "updated_at": datetime.now().isoformat()
            } for item in data]

            response = requests.post(
                f"{self.base_url}/api_table/set_data/wb",
                headers=self.headers,
                data=json.dumps(formatted_data)
            )
            response.raise_for_status()
            logging.info(f"Successfully sent {len(data)} WB items to API")
            return True
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error sending WB data to API: {str(e)}")
            return False

    def send_ozon_data(self, data: List[Dict]) -> bool:
        """Send parsed Ozon data to the API"""
        try:
            formatted_data = [{
                "product_url": item.get('product_url', ''),
                "is_available": item.get('is_available', False),
                "price": item.get('price', 0),
                "rating": item.get('rating', '0'),
                "reviews": item.get('reviews', '0'),
                "updated_at": datetime.now().isoformat()
            } for item in data]

            response = requests.post(
                f"{self.base_url}/api_table/set_data/ozon",
                headers=self.headers,
                data=json.dumps(formatted_data)
            )
            response.raise_for_status()
            logging.info(f"Successfully sent {len(data)} Ozon items to API")
            return True
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error sending Ozon data to API: {str(e)}")
            return False

    def send_alibaba_data(self, data: List[Dict]) -> bool:
        """Send parsed Alibaba data to the API"""
        try:
            formatted_data = [{
                "product_url": item.get('product_url', ''),
                "is_available": item.get('is_available', False),
                "price": item.get('price', ''),
                "reviews": item.get('reviews', '0'),
                "rating": item.get('rating', '0'),
                "delivery_speed": item.get('delivery_speed', ''),
                "updated_at": datetime.now().isoformat()
            } for item in data]

            response = requests.post(
                f"{self.base_url}/api_table/set_data/alibaba",
                headers=self.headers,
                data=json.dumps(formatted_data)
            )
            response.raise_for_status()
            logging.info(f"Successfully sent {len(data)} Alibaba items to API")
            return True
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error sending Alibaba data to API: {str(e)}")
            return False 