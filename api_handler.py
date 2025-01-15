import requests
import logging
from datetime import datetime
import json
from typing import List, Dict, Optional

class APIHandler:
    def __init__(self):
        self.base_url = "https://parse.trendoanalytics.kz"
        self.headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def get_urls(self, marketplace: str) -> List[str]:
        """
        Get product URLs from the API for a specific marketplace
        Args:
            marketplace (str): One of 'kaspi', 'ozon', 'wb', 'alibaba'
        Returns:
            List[str]: List of product URLs
        """
        try:
            response = requests.get(
                f"{self.base_url}/api_table/get_data/{marketplace}",
                headers=self.headers
            )
            response.raise_for_status()
            data = response.json()
            
            # Extract URLs from response
            urls = [item.get('product_url') for item in data if item.get('product_url')]
            logging.info(f"Retrieved {len(urls)} URLs for {marketplace}")
            return urls
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error getting URLs for {marketplace}: {str(e)}")
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