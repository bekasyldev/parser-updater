import psycopg2
from dotenv import load_dotenv
import os
import logging
load_dotenv()

class DatabaseHandler:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        self.create_tables()

    def create_tables(self):
        logging.info("Creating tables")
        with self.conn.cursor() as cur:
            # Kaspi table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS kaspi_products (
                    articul TEXT NOT NULL UNIQUE,
                    product_url TEXT,
                    is_available BOOLEAN DEFAULT false,
                    price INTEGER DEFAULT 0,
                    delivery_price TEXT DEFAULT '',
                    delivery_date TEXT DEFAULT '',
                    total_reviews INTEGER DEFAULT 0,
                    rating FLOAT DEFAULT 0.0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Alibaba table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alibaba_products (
                    articul TEXT NOT NULL UNIQUE,
                    product_url TEXT,
                    is_available BOOLEAN DEFAULT false,
                    price TEXT DEFAULT '',
                    reviews TEXT DEFAULT '0',
                    rating TEXT DEFAULT '0',
                    delivery_speed TEXT DEFAULT '',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Wildberries table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wildberries_products (
                    articul TEXT NOT NULL UNIQUE,
                    product_url TEXT,
                    is_available BOOLEAN DEFAULT false,
                    price INTEGER DEFAULT 0,
                    rating TEXT DEFAULT '0',
                    reviews TEXT DEFAULT '0',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Ozon table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ozon_products (
                    articul TEXT NOT NULL UNIQUE,
                    product_url TEXT,
                    is_available BOOLEAN DEFAULT false,
                    price INTEGER DEFAULT 0,
                    rating TEXT DEFAULT '0',
                    reviews TEXT DEFAULT '0',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indexes for better performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_kaspi_url ON kaspi_products(product_url)",
                "CREATE INDEX IF NOT EXISTS idx_alibaba_url ON alibaba_products(product_url)",
                "CREATE INDEX IF NOT EXISTS idx_wb_url ON wildberries_products(product_url)",
                "CREATE INDEX IF NOT EXISTS idx_ozon_url ON ozon_products(product_url)"
            ]
            
            for index in indexes:
                cur.execute(index)

            self.conn.commit()

    def update_kaspi_product(self, data):
        logging.info("Updating kaspi product")
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE kaspi_products SET 
                    is_available = %s,
                    price = %s,
                    delivery_price = %s,
                    delivery_date = %s,
                    total_reviews = %s,
                    rating = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE product_url = %s
            """, (
                data.get('is_available', False),
                data.get('price', 0),
                data.get('delivery_price', ''),
                data.get('delivery_date', ''),
                data.get('total_reviews', 0),
                data.get('rating', 0.0),
                data.get('product_url')
            ))
            self.conn.commit()

    def update_alibaba_product(self, data):
        logging.info("Updating alibaba product")
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE alibaba_products SET 
                    is_available = %s,
                    price = %s,
                    reviews = %s,
                    rating = %s,
                    delivery_speed = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE product_url = %s
            """, (
                data.get('is_available', False),
                data.get('price', ''),
                data.get('reviews', '0'),
                data.get('rating', '0'),
                data.get('delivery_speed', ''),
                data.get('product_url')
            ))
            self.conn.commit()

    def update_wildberries_product(self, data):
        logging.info("Updating wildberries product")
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE wildberries_products SET 
                    is_available = %s,
                    price = %s,
                    rating = %s,
                    reviews = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE product_url = %s
            """, (
                data.get('is_available', False),
                data.get('price', 0),
                data.get('rating', '0'),
                data.get('reviews', '0'),
                data.get('product_url')
            ))
            self.conn.commit()

    def update_ozon_product(self, data):
        logging.info("Updating ozon product")
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE ozon_products SET 
                    is_available = %s,
                    price = %s,
                    rating = %s,
                    reviews = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE product_url = %s
            """, (
                data.get('is_available', False),
                data.get('price', 0),
                data.get('rating', '0'),
                data.get('reviews', '0'),
                data.get('product_url')
            ))
            self.conn.commit()

    def get_kaspi_urls(self):
        logging.info("Getting kaspi urls")
        with self.conn.cursor() as cur:
            cur.execute("SELECT product_url FROM kaspi_products")
            return [row[0] for row in cur.fetchall()]
    
    def get_alibaba_urls(self):
        logging.info("Getting alibaba urls")
        with self.conn.cursor() as cur:
            cur.execute("SELECT product_url FROM alibaba_products")
            return [row[0] for row in cur.fetchall()]
    
    def get_wildberries_urls(self):
        logging.info("Getting wildberries urls")
        with self.conn.cursor() as cur:
            cur.execute("SELECT product_url FROM wildberries_products")
            return [row[0] for row in cur.fetchall()]
    
    def get_ozon_urls(self):
        logging.info("Getting ozon urls")
        with self.conn.cursor() as cur:
            cur.execute("SELECT product_url FROM ozon_products")
            return [row[0] for row in cur.fetchall()]

    def add_kaspi_url(self, articul, url):
        logging.info("Adding kaspi url")
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO kaspi_products (articul, product_url, is_available)
                VALUES (%s, %s, false)
                ON CONFLICT (product_url) DO NOTHING
            """, (articul, url))
            self.conn.commit()

    def add_alibaba_url(self, articul, url):
        logging.info("Adding alibaba url")
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO alibaba_products (articul, product_url, is_available)
                VALUES (%s, %s, false)
                ON CONFLICT (product_url) DO NOTHING
            """, (articul, url))
            self.conn.commit()


    def add_wildberries_url(self, articul, url):
        logging.info("Adding wildberries url")
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO wildberries_products (articul,  product_url, is_available)
                VALUES (%s, %s, false)
                ON CONFLICT (product_url) DO NOTHING
            """, (articul, url))
            self.conn.commit()

    def add_ozon_url(self, articul, url):
        logging.info("Adding ozon url")
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ozon_products (articul,     product_url, is_available)
                VALUES (%s, %s, false)
                ON CONFLICT (product_url) DO NOTHING
            """, (articul, url))
            self.conn.commit()

    def __del__(self):
        if hasattr(self, 'conn'):
            self.conn.close() 