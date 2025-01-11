from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
from bs4 import BeautifulSoup
import re
import json
import time
import logging

class MarketplaceParser:
    def __init__(self):
        self.options = webdriver.ChromeOptions()
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--disable-gpu")
        self.options.add_argument("--window-size=1920,1080")
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)
        self.options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        self.driver = webdriver.Chrome(options=self.options)
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            '''
        })

    def parse_kaspi(self, url):
        try:
            logging.info(f"Opening URL in Chrome: {url}")
            self.driver.get(url)
            
            # Ждем загрузки страницы
            time.sleep(4)
            
            # Проверяем, что страница полностью загрузилась
            page_state = self.driver.execute_script('return document.readyState;')
            logging.info(f"Page state: {page_state}")
            
            if page_state != 'complete':
                logging.warning("Page not fully loaded, waiting additional time")
                time.sleep(3)
            
            logging.info("Checking availability...")

            # click close modal button
            try:
                close_modal = self.driver.find_element(By.CSS_SELECTOR, 'i.icon.icon_close')
                close_modal.click()
            except:
                pass

            try:
                out_of_stock = self.driver.find_element(By.CSS_SELECTOR, '.out-of-stock')
                if out_of_stock.is_displayed():
                    logging.info("Product is out of stock")
                    return {'product_url': url, 'is_available': False}
            except:
                logging.info("Product is in stock")
                pass

            data = {
                'product_url': url,
                'is_available': True
            }

            # Get price and delivery info
            try:
                logging.info("Getting price...")
                price_elem = self.driver.find_element(By.CSS_SELECTOR, 'div.item__price-once')
                data['price'] = int(price_elem.text.replace('₸', '').replace(' ', ''))
                logging.info(f"Found price: {data['price']}")
            except Exception as e:
                logging.error(f"Error getting price: {e}")
                data['price'] = 0

            # Get delivery info
            try:
                logging.info("Getting delivery info...")
                delivery = self.driver.find_elements(By.CSS_SELECTOR, 'span.sellers-table__delivery-date')
                data['delivery_date'] = delivery[0].text
                
                delivery_price = self.driver.find_elements(By.CSS_SELECTOR, 'span.sellers-table__delivery-price')
                data['delivery_price'] = delivery_price[0].text
            except Exception as e:
                logging.error(f"Error getting delivery info: {e}")
                data['delivery_date'] = ''
                data['delivery_price'] = 0

            # Get reviews data from page source
            try:
                logging.info("Getting reviews data from page source...")
                page_source = self.driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')

                # Ищем все скрипты на странице
                all_scripts = soup.find_all('script')
                reviews_data = None
                
                # Перебираем все скрипты в поисках данных об отзывах
                for script in all_scripts:
                    script_text = script.string
                    if script_text and 'BACKEND.components.productReviews' in script_text:
                        try:
                            # Извлекаем JSON часть
                            json_str = script_text.split('BACKEND.components.productReviews = ')[1].strip()
                            reviews_data = json.loads(json_str)
                            logging.info("Found reviews data in script")
                            break
                        except Exception as e:
                            logging.error(f"Error parsing script content: {e}")
                            continue

                if reviews_data:
                    rating_data = reviews_data.get('rating', {})
                    data['total_reviews'] = rating_data.get('ratingCount', 0)
                    data['rating'] = rating_data.get('global', 0.0)
                    logging.info(f"Successfully parsed reviews data: total={data['total_reviews']}, rating={data['rating']}")
                else:
                    logging.warning("Reviews data not found in any script tags")
                    data['total_reviews'] = 0
                    data['rating'] = 0.0

            except Exception as e:
                logging.error(f"Error getting reviews from page source: {e}")
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

    def __del__(self):
        if hasattr(self, 'driver'):
            self.driver.quit()