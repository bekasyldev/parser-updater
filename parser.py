from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import time

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
        self.options.add_argument("--headless")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--no-sandbox")
        self.options.binary_location = "/usr/bin/chromium-browser"
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
            self.driver.get(url)
            time.sleep(2)

            # Check availability
            try:
                out_of_stock = self.driver.find_element(By.CSS_SELECTOR, '.out-of-stock')
                if out_of_stock.is_displayed():
                    return {'product_url': url, 'is_available': False}
            except:
                pass

            data = {
                'product_url': url,
                'is_available': True
            }

            # Get price and delivery info
            try:
                price_elem = self.driver.find_element(By.CSS_SELECTOR, 'div.sellers-table__price-cell-text')
                data['price'] = int(price_elem.text.replace('₸', '').replace(' ', ''))
            except:
                data['price'] = 0

            # Get delivery info
            try:
                delivery = self.driver.find_element(By.CSS_SELECTOR, 'span.sellers-table__delivery-date')
                data['delivery_type'] = delivery.text
                
                delivery_price = self.driver.find_element(By.CSS_SELECTOR, 'span.sellers-table__delivery-price')
                data['delivery_price'] = int(delivery_price.text.replace('₸', '').replace(' ', ''))
            except:
                data['delivery_type'] = ''
                data['delivery_price'] = 0

            # Get sellers count
            try:
                sellers = self.driver.find_elements(By.CSS_SELECTOR, 'tr.sellers-table__row')
                data['sellers_count'] = len(sellers)
            except:
                data['sellers_count'] = 0

            # Get reviews data
            try:
                reviews_tab = self.driver.find_element(By.CSS_SELECTOR, 'li[data-tab="reviews"]')
                data['total_reviews'] = int(''.join(filter(str.isdigit, reviews_tab.text)))
                
                if data['total_reviews'] > 0:
                    reviews_tab.click()
                    time.sleep(1)
                    
                    dates = []
                    review_dates = self.driver.find_elements(By.CSS_SELECTOR, "div.reviews__date")
                    for date in review_dates:
                        try:
                            dates.append(datetime.strptime(date.text, '%d.%m.%Y'))
                        except:
                            continue

                    if dates:
                        now = datetime.now()
                        data['reviews_3days'] = sum(1 for d in dates if (now - d).days <= 3)
                        data['reviews_5days'] = sum(1 for d in dates if (now - d).days <= 5)
                        data['reviews_15days'] = sum(1 for d in dates if (now - d).days <= 15)
                        data['reviews_30days'] = sum(1 for d in dates if (now - d).days <= 30)
                        data['last_review_date'] = dates[0].strftime('%d.%m.%Y')
                        
                        rating = self.driver.find_element(By.CSS_SELECTOR, "div.rating")
                        data['rating'] = float(rating.text)
            except Exception as e:
                print(f"Error getting reviews: {e}")
                data.update({
                    'total_reviews': 0,
                    'reviews_3days': 0,
                    'reviews_5days': 0,
                    'reviews_15days': 0,
                    'reviews_30days': 0,
                    'last_review_date': None,
                    'rating': 0.0
                })

            return data

        except Exception as e:
            print(f"Error parsing Kaspi: {e}")
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

    def parse_1688(self, url):
        try:
            self.driver.get(url)
            time.sleep(5)

            # Handle slider verification
            try:
                slider = self.driver.find_element(By.CSS_SELECTOR, "span.nc-lang-cnt")
                if slider and "slide to verify" in slider.text.lower():
                    print("Found slider verification, waiting for manual input...")
                    # Wait for manual verification (30 seconds)
                    time.sleep(30)
            except:
                pass

            # Check if still on verification page
            if "slide to verify" in self.driver.page_source.lower():
                print("Still on verification page, skipping...")
                return None

            data = {
                'product_url': url,
                'is_available': True
            }

            # Get price
            try:
                price_elem = self.driver.find_element(By.CSS_SELECTOR, "span.price")
                data['price'] = price_elem.text.replace('¥', '').strip()
            except:
                data['price'] = ''

            # Get rating
            try:
                rating_elem = self.driver.find_element(By.CSS_SELECTOR, "span.star-rating")
                data['rating'] = rating_elem.get_attribute('title')
            except:
                data['rating'] = '0'

            print(data)
            return data

        except Exception as e:
            print(f"Error parsing 1688: {e}")
            return None

    def parse_wildberries(self, url):
        try:
            self.driver.get(url)
            time.sleep(10)  # Initial wait

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
                    print(f"Cleaned price: {data['price']}")
                else:
                    print("No price element found with any selector")
                    data['price'] = 0

            except Exception as e:
                print(f"Error getting price: {e}")
                data['price'] = 0

            print(f"Final data: {data}")
            return data

        except Exception as e:
            print(f"Error parsing Wildberries: {e}")
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
            print(data)
            return data

        except Exception as e:
            print(f"Error parsing Ozon: {e}")
            return None

    def __del__(self):
        if hasattr(self, 'driver'):
            self.driver.quit()