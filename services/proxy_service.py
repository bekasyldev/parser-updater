import aiohttp
import logging
import asyncio
from datetime import datetime
import json

class ProxyService:
    def __init__(self):
        self.login = "PrsRUSGF6FZF1"
        self.password = "JhSiykag"
        self.current_ip = "2.76.176.224"  # Will be updated dynamically
        self.last_ip_update = None
        self.proxy_list = []
        self.proxy_index = 0
        
    async def update_ip_binding(self, new_ip):
        """Update IP binding (can be used once per 10 minutes)"""
        if self.last_ip_update and (datetime.now() - self.last_ip_update).total_seconds() < 600:
            logging.warning("Cannot update IP yet - must wait 10 minutes between updates")
            return False
            
        url = f"https://papaproxy.net/api/getproxy/?action=setip&login={self.login}&password={self.password}&ip={new_ip}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("result") == "success":
                            self.current_ip = new_ip
                            self.last_ip_update = datetime.now()
                            logging.info(f"Successfully updated IP binding to {new_ip}")
                            return True
                    logging.error(f"Failed to update IP binding: {await response.text()}")
                    return False
        except Exception as e:
            logging.error(f"Error updating IP binding: {str(e)}")
            return False

    async def get_proxy_config(self):
        """Get proxy configuration for Playwright"""
        return {
            'server': 'http://45.130.43.9:8085',  # Using HTTP/HTTPS proxy
            'username': self.login,
            'password': self.password
        }

    def get_proxy_url(self):
        """Get proxy URL for direct usage"""
        return f"http://{self.login}:{self.password}@45.130.43.9:8085" 