from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
from bs4 import BeautifulSoup
import re
import json
import time
import logging
import os
import requests
import random

class MarketplaceParser:
    def __init__(self):
        self.options = webdriver.ChromeOptions()
        
        # Proxy configuration for papaproxy.net
        PROXY_HOST = os.getenv('PROXY_HOST')
        PROXY_PORT = os.getenv('PROXY_PORT')         # For HTTP/HTTPS proxy
        
        # Configure proxy
        self.options.add_argument(f'--proxy-server=http://{PROXY_HOST}:{PROXY_PORT}')
        
        # Basic settings
        self.options.add_argument('--headless=new')  # Using newer headless mode
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        
        # Additional anti-detection measures
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)
        
        # Set more realistic window size
        self.options.add_argument('--window-size=1920,1080')
        self.options.add_argument('--start-maximized')
        
        # Add more realistic user agent
        self.options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Additional stealth settings
        self.options.add_argument('--disable-notifications')
        self.options.add_argument('--ignore-certificate-errors')
        self.options.add_argument('--disable-popup-blocking')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--lang=ru-RU,ru')
        
        # Set Chrome binary location
        CHROME_BINARY = '/usr/bin/google-chrome-stable'
        if not os.path.exists(CHROME_BINARY):
            CHROME_BINARY = '/usr/bin/google-chrome'
        self.options.binary_location = CHROME_BINARY

        try:
            self.driver = webdriver.Chrome(options=self.options)
            
            # Additional stealth using CDP
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                "platform": "MacOS"
            })
            
            # Mask webdriver presence
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5]
                    });
                    window.chrome = {
                        runtime: {}
                    };
                '''
            })
            
            logging.info(f"Chrome driver initialized successfully with binary: {CHROME_BINARY}")
        except Exception as e:
            logging.error(f"Failed to initialize Chrome driver: {e}")
            raise

    def parse_kaspi(self, url):
        try:
            logging.info(f"Opening URL in Chrome: {url}")
            self.driver.get(url)
            
            # Longer initial wait
            time.sleep(8)
            
            # Check if we got the anti-bot page
            if "robots" in self.driver.page_source.lower() or len(self.driver.page_source) < 1000:
                logging.error("Possible bot detection, got minimal page")
                # Try refreshing the page
                self.driver.refresh()
                time.sleep(5)
            
            # Save page source for debugging
            with open(f'page_source_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html', 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            
            # Log page length for debugging
            logging.info(f"Page source length: {len(self.driver.page_source)}")
            
            logging.info("Checking availability...")

            data = {
                'product_url': url,
                'is_available': True
            }

            # Check availability first
            try:
                out_of_stock = self.driver.find_elements(By.CSS_SELECTOR, '.sold-out-text')
                if out_of_stock and any(elem.is_displayed() for elem in out_of_stock):
                    logging.info("Product is out of stock")
                    return {'product_url': url, 'is_available': False}
            except Exception as e:
                logging.debug(f"Availability check exception: {e}")

            # Get price - updated selectors
            try:
                logging.info("Getting price...")
                price_selectors = [
                    'div.item__price-once',  # Old selector
                    'div.offer__price',      # New selector
                    'span.price',            # Alternative selector
                    'div[data-zone-name="price"]'  # Another possible selector
                ]
                
                price_elem = None
                for selector in price_selectors:
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for elem in elements:
                            if elem.is_displayed() and elem.text:
                                price_elem = elem
                                break
                        if price_elem:
                            break
                    except:
                        continue

                if price_elem:
                    price_text = price_elem.text.replace('₸', '').replace(' ', '')
                    data['price'] = int(''.join(filter(str.isdigit, price_text)))
                    logging.info(f"Found price: {data['price']}")
                else:
                    logging.error("No price element found with any selector")
                    data['price'] = 0
                    
            except Exception as e:
                logging.error(f"Error getting price: {e}")
                data['price'] = 0

            # Get delivery info - updated selectors
            try:
                logging.info("Getting delivery info...")
                delivery_selectors = [
                    'span.sellers-table__delivery-date',  # Old selector
                    'div.delivery-info',                  # New selector
                    'div[data-zone-name="delivery"]'      # Alternative selector
                ]
                
                delivery_date = None
                for selector in delivery_selectors:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        delivery_date = elements[0].text
                        break
                    
                data['delivery_date'] = delivery_date if delivery_date else ''
                
                # Delivery price
                delivery_price_selectors = [
                    'span.sellers-table__delivery-price',
                    'div.delivery-price',
                    'span[data-zone-name="delivery-price"]'
                ]
                
                delivery_price = None
                for selector in delivery_price_selectors:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        delivery_price = elements[0].text
                        break
                        
                data['delivery_price'] = delivery_price if delivery_price else ''
                
            except Exception as e:
                logging.error(f"Error getting delivery info: {e}")
                data['delivery_date'] = ''
                data['delivery_price'] = ''

            # Get reviews data - updated method
            try:
                logging.info("Getting reviews data...")
                
                # Try to find reviews in the page source
                page_source = self.driver.page_source
                
                # Method 1: Look for JSON data
                reviews_pattern = r'BACKEND\.components\.productReviews\s*=\s*({[^;]+})'
                match = re.search(reviews_pattern, page_source)
                
                if match:
                    reviews_data = json.loads(match.group(1))
                    rating_data = reviews_data.get('rating', {})
                    data['total_reviews'] = rating_data.get('ratingCount', 0)
                    data['rating'] = rating_data.get('global', 0.0)
                else:
                    # Method 2: Try to find reviews in HTML
                    try:
                        reviews_elem = self.driver.find_element(By.CSS_SELECTOR, 'div.rating__counter')
                        data['total_reviews'] = int(''.join(filter(str.isdigit, reviews_elem.text)))
                        
                        rating_elem = self.driver.find_element(By.CSS_SELECTOR, 'div.rating__digits')
                        data['rating'] = float(rating_elem.text)
                    except:
                        data['total_reviews'] = 0
                        data['rating'] = 0.0
                    
                logging.info(f"Reviews data: total={data['total_reviews']}, rating={data['rating']}")
                
            except Exception as e:
                logging.error(f"Error getting reviews: {e}")
                data.update({
                    'total_reviews': 0,
                    'rating': 0.0
                })

            logging.info(f"Successfully parsed data: {data}")
            return data

        except Exception as e:
            logging.error(f"Error parsing Kaspi: {e}")
            return None

    def parse_alibaba(self, url):
        try:
            self.driver.get(url)
            time.sleep(2)

            # Check availability
            try:
                unavailable = self.driver.find_element(By.CLASS_NAME, "product-unsafe")
                if unavailable.is_displayed():
                    return {'product_url': url, 'is_available': False}
            except:
                pass

            data = {
                'product_url': url,
                'is_available': True
            }

            # Get price
            try:
                price_elem = self.driver.find_element(By.CSS_SELECTOR, "div.price-list .price")
                data['price'] = price_elem.text
            except:
                data['price'] = ''

            # Get reviews and rating
            try:
                reviews = self.driver.find_element(By.CSS_SELECTOR, "div.verified-reviews")
                data['reviews'] = ''.join(filter(str.isdigit, reviews.text))
                
                rating = self.driver.find_element(By.CSS_SELECTOR, "div.score")
                data['rating'] = rating.text
            except:
                data['reviews'] = '0'
                data['rating'] = '0'

            # Get delivery speed
            try:
                delivery = self.driver.find_element(By.CSS_SELECTOR, "div.detail-next-progress-line-text")
                data['delivery_speed'] = delivery.text
            except:
                data['delivery_speed'] = ''

            return data

        except Exception as e:
            print(f"Error parsing Alibaba: {e}")
            return None
        

    def parse_wildberries(self, url):
        try:
            self.driver.get(url)
            time.sleep(10)

            data = {
                'product_url': url,
                'is_available': True
            }

            # Check availability
            try:
                out_of_stock = self.driver.find_elements(By.CSS_SELECTOR, "p.sold-out-product")
                if out_of_stock:
                    return {'product_url': url, 'is_available': False}
            except:
                pass

            # Get price - try multiple selectors
            try:
                # Try different price selectors
                price_selectors = [
                    "span.price-block__wallet-price",
                    "ins.price-block__final-price",
                ]

                price_text = None
                for selector in price_selectors:
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if elements:
                            for elem in elements:
                                if elem.text and any(c.isdigit() for c in elem.text):
                                    price_text = elem.text
                                    break
                        if price_text:
                            break
                    except:
                        continue

                if price_text:
                    # Clean price text (remove currency and spaces)
                    cleaned_price = ''.join(filter(str.isdigit, price_text))
                    data['price'] = int(cleaned_price) if cleaned_price else 0
                else:
                    data['price'] = 0
                    

            except Exception as e:
                data['price'] = 0

            # Get reviews and rating
            try:
                reviews_count_elem = self.driver.find_element(By.CSS_SELECTOR, "span.product-review__count-review")
                if reviews_count_elem:
                    print(reviews_count_elem.text)
                    data['reviews'] = ''.join(re.findall(r'\d+', reviews_count_elem.text))
                else:
                    data['reviews'] = '0'

                rating_elem = self.driver.find_element(By.CSS_SELECTOR, "span.product-review__rating")
                if rating_elem:
                    # reviews.text is like "4.88" float number
                    print(rating_elem.text)
                    data['rating'] = rating_elem.text
                else:
                    data['rating'] = '0'
                
                
            except:
                data['rating'] = '0'
                data['reviews'] = '0'

            logging.info(f"Successfully parsed data: {data}")
            return data

        except Exception as e:
            logging.error(f"Error parsing Wildberries: {e}")
            return None

    def parse_ozon(self, url):
        try:
            self.driver.get(url)
            time.sleep(2)

            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(2)

            data = {
                'product_url': url,
                'is_available': True
            }

            try:
                is_out_of_stock = self.driver.execute_script("""
                    return document.querySelector('div[data-widget="webOutOfStock"]') !== null
                """)
                if is_out_of_stock:
                    return {'product_url': url, 'is_available': False}
            except:
                pass

            try:
                # find element span elemement with class l8t_27 tl8_27 l2u_27
                price_elem = self.driver.find_element(By.CSS_SELECTOR, "span.l8t_27.tl8_27.l2u_27")
                if price_elem:
                    price_text = ''.join(filter(str.isdigit, price_elem.text))
                    data['price'] = int(price_text) if price_text else 0
                else:
                    data['price'] = 0

            except Exception as e:
                print(f"Error getting price/stock: {e}")
                data['price'] = 0
            
            # Get reviews and rating
            try:
                reviews_elem = self.driver.find_elements(By.CSS_SELECTOR, "div.ga121-a2.tsBodyControl500Medium")
                # reviews_elem.text is like "4.8 • 14 006 reviews" format "rating • reviews_count reviews"
                data['reviews'] = ''.join(re.findall(r'\d+', reviews_elem[0].text.split('•')[1]))
                data['rating'] = reviews_elem[0].text.split()[0]
            except:
                data['reviews'] = '0'
                data['rating'] = '0'
                
            return data

        except Exception as e:
            print(f"Error parsing Ozon: {e}")
            return None

    def get_proxy_list(self):
        """Get proxy list from papaproxy.net"""
        PROXY_API_KEY = os.getenv('PROXY_API_KEY')
        proxies = []
        try:
            response = requests.get(
                'http://api.papaproxy.net/api/v1/proxy/list/txt',
                headers={
                    'Authorization': 'Bearer ' + PROXY_API_KEY
                }
            )
            if response.status_code == 200:
                proxies = response.text.strip().split('\n')
        except Exception as e:
            logging.error(f"Failed to get proxy list: {e}")
        return proxies

    def rotate_proxy(self):
        """Rotate to a new proxy"""
        proxies = self.get_proxy_list()
        if proxies:
            proxy = random.choice(proxies)
            self.options.add_argument(f'--proxy-server=http://{proxy}')
            self.driver = webdriver.Chrome(options=self.options)
            logging.info(f"Rotated to new proxy: {proxy}")
            return True
        return False

    def __del__(self):
        if hasattr(self, 'driver'):
            self.driver.quit()
