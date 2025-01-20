import aiohttp
import logging
import asyncio
from datetime import datetime
import json
import requests
from urllib.parse import quote

class ProxyService:
    def __init__(self):
        self.login = "PrsRUSGF6FZF1"
        self.password = "JhSiykag"
        self.current_ip = None
        self.last_ip_update = None
        
    async def get_current_ip(self):
        """Get current IP address"""
        try:
            # Try multiple IP services in case one fails
            services = [
                'https://api.ipify.org?format=json',
                'https://ifconfig.me/ip',
                'https://icanhazip.com'
            ]
            
            async with aiohttp.ClientSession() as session:
                for service in services:
                    try:
                        async with session.get(service) as response:
                            if response.status == 200:
                                if 'json' in service:
                                    data = await response.json()
                                    return data.get('ip')
                                else:
                                    return (await response.text()).strip()
                    except:
                        continue
            return None
        except Exception as e:
            logging.error(f"Error getting current IP: {str(e)}")
            return None

    async def update_ip_binding(self, new_ip=None):
        """Update IP binding (can be used once per 10 minutes)"""
        try:
            if not new_ip:
                new_ip = await self.get_current_ip()
                if not new_ip:
                    return False

            if self.last_ip_update and (datetime.now() - self.last_ip_update).total_seconds() < 600:
                logging.warning("Cannot update IP yet - must wait 10 minutes between updates")
                return False
            
            # Encode parameters properly
            encoded_ip = quote(new_ip)
            url = f"https://papaproxy.net/api/getproxy/?action=setip&login={self.login}&password={self.password}&ip={encoded_ip}"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json, text/plain, */*'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        self.current_ip = new_ip
                        self.last_ip_update = datetime.now()
                        logging.info(f"Successfully updated IP binding to {new_ip}")
                        return True
                        
                    logging.error(f"Failed to update IP binding. Status: {response.status}, Response: {await response.text()}")
                    return False
            
        except Exception as e:
            logging.error(f"Error updating IP binding: {str(e)}")
            return False

    async def get_proxy_config(self):
        """Get proxy configuration for Playwright"""
        return {
            'server': 'http://45.130.43.9:8085',
            'username': self.login,
            'password': self.password
        }

    async def get_proxy_context_options(self):
        """Get complete context options with proxy"""
        return {
            'proxy': await self.get_proxy_config(),
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'viewport': {'width': 1920, 'height': 1080},
            'ignore_https_errors': True,
            'bypass_csp': True,  # Bypass Content Security Policy
            'java_script_enabled': True
        }

    def get_proxy_url(self):
        """Get proxy URL for direct usage"""
        return f"http://{self.login}:{self.password}@45.130.43.9:8085"

    async def test_proxy(self):
        """Test if proxy is working"""
        try:
            proxy_url = self.get_proxy_url()
            async with aiohttp.ClientSession() as session:
                async with session.get('https://api.ipify.org?format=json', proxy=proxy_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        logging.info(f"Proxy test successful. IP: {data.get('ip')}")
                        return True
                    logging.error(f"Proxy test failed. Status: {response.status}")
                    return False
        except Exception as e:
            logging.error(f"Error testing proxy: {str(e)}")
            return False 