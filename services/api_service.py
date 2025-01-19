import aiohttp
import logging
from datetime import datetime
from typing import Dict, List

class APIService:
    BASE_URL = "https://parse.trendoanalytics.kz/api_table/set_data"
    
    @staticmethod
    def validate_data(data: Dict, marketplace: str) -> Dict:
        """Validate and format data according to API requirements"""
        try:
            # Basic validation
            if not data.get('product_url') or not data.get('articul'):
                raise ValueError("Missing required fields: product_url or articul")

            # Convert rating and reviews to proper format
            rating = float(data.get('rating', 0) or 0)
            total_reviews = int(data.get('total_reviews', 0) or 0)

            # Base structure for all marketplaces
            formatted_data = {
                "articul": str(data.get('articul', '')),
                "product_url": str(data.get('product_url', '')),
                "is_available": bool(data.get('is_available', False)),
                "delivery_price": "",  # Always empty string as per requirement
                "delivery_date": "",   # Always empty string as per requirement
                "updated_at": datetime.utcnow().isoformat()
            }

            # If product is not available, set default values
            if not formatted_data['is_available']:
                formatted_data.update({
                    "price": 0,
                    "total_reviews": 0,
                    "rating": 0
                })
            else:
                formatted_data.update({
                    "price": int(float(data.get('price', 0) or 0)),
                    "total_reviews": total_reviews,
                    "rating": rating
                })

            return formatted_data

        except Exception as e:
            logging.error(f"Error validating data: {str(e)}")
            return None

    @staticmethod
    def get_marketplace_endpoint(marketplace: str) -> str:
        """Get the appropriate endpoint for the marketplace"""
        marketplace_map = {
            'kaspi': 'kaspi',
            'ozon': 'ozon',
            'wildberries': 'wb',
            'alibaba': 'alibaba'
        }
        return marketplace_map.get(marketplace, '')

    async def send_data(self, data: List[Dict], marketplace: str):
        """Send data to API endpoint"""
        if not data:
            logging.warning("No data to send")
            return

        endpoint = self.get_marketplace_endpoint(marketplace)
        if not endpoint:
            logging.error(f"Invalid marketplace: {marketplace}")
            return

        # Validate and format all data
        formatted_data = []
        for item in data:
            validated_item = self.validate_data(item, marketplace)
            if validated_item:
                formatted_data.append(validated_item)

        if not formatted_data:
            logging.error("No valid data to send")
            return

        url = f"{self.BASE_URL}/{endpoint}"
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json'
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=formatted_data, headers=headers) as response:
                    if response.status == 200:
                        logging.info(f"Successfully sent {len(formatted_data)} items to {endpoint}")
                    else:
                        response_text = await response.text()
                        logging.error(f"Error sending data to API: {response.status} - {response_text}")
        except Exception as e:
            logging.error(f"Error sending data to API: {str(e)}") 