import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

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
        with self.conn.cursor() as cur:
            # Kaspi table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS kaspi_products (
                    id SERIAL PRIMARY KEY,
                    product_url TEXT,
                    is_available BOOLEAN,
                    price INTEGER,
                    delivery_price INTEGER,
                    delivery_type TEXT,
                    sellers_count INTEGER,
                    total_reviews INTEGER,
                    reviews_3days INTEGER,
                    reviews_5days INTEGER,
                    reviews_15days INTEGER,
                    reviews_30days INTEGER,
                    last_review_date TEXT,
                    rating FLOAT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Alibaba table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alibaba_products (
                    id SERIAL PRIMARY KEY,
                    product_url TEXT,
                    is_available BOOLEAN,
                    price TEXT,
                    reviews TEXT,
                    rating TEXT,
                    delivery_speed TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 1688 table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS market_1688_products (
                    id SERIAL PRIMARY KEY,
                    product_url TEXT,
                    is_available BOOLEAN,
                    price TEXT,
                    rating TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Wildberries table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wildberries_products (
                    id SERIAL PRIMARY KEY,
                    product_url TEXT,
                    is_available BOOLEAN,
                    price INTEGER,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Ozon table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ozon_products (
                    id SERIAL PRIMARY KEY,
                    product_url TEXT,
                    is_available BOOLEAN,
                    price INTEGER,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indexes for better performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_kaspi_url ON kaspi_products(product_url)",
                "CREATE INDEX IF NOT EXISTS idx_alibaba_url ON alibaba_products(product_url)",
                "CREATE INDEX IF NOT EXISTS idx_1688_url ON market_1688_products(product_url)",
                "CREATE INDEX IF NOT EXISTS idx_wb_url ON wildberries_products(product_url)",
                "CREATE INDEX IF NOT EXISTS idx_ozon_url ON ozon_products(product_url)"
            ]
            
            for index in indexes:
                cur.execute(index)

            self.conn.commit()

    def update_kaspi_product(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO kaspi_products (
                    product_url, is_available, price, delivery_price,
                    delivery_type, sellers_count, total_reviews,
                    reviews_3days, reviews_5days, reviews_15days,
                    reviews_30days, last_review_date, rating
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) RETURNING id
            """, (
                data.get('product_url'),
                data.get('is_available', False),
                data.get('price', 0),
                data.get('delivery_price', 0),
                data.get('delivery_type'),
                data.get('sellers_count', 0),
                data.get('total_reviews', 0),
                data.get('reviews_3days', 0),
                data.get('reviews_5days', 0),
                data.get('reviews_15days', 0),
                data.get('reviews_30days', 0),
                data.get('last_review_date'),
                data.get('rating', 0.0)
            ))
            self.conn.commit()
            return cur.fetchone()[0]

    def update_alibaba_product(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO alibaba_products (
                    product_url, is_available, price, reviews,
                    rating, delivery_speed
                ) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
            """, (
                data.get('product_url'),
                data.get('is_available', False),
                data.get('price', ''),
                data.get('reviews', '0'),
                data.get('rating', '0'),
                data.get('delivery_speed', '')
            ))
            self.conn.commit()
            return cur.fetchone()[0]

    def update_1688_product(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO market_1688_products (
                    product_url, is_available, price, rating
                ) VALUES (%s, %s, %s, %s) RETURNING id
            """, (
                data.get('product_url'),
                data.get('is_available', False),
                data.get('price', ''),
                data.get('rating', '0')
            ))
            self.conn.commit()
            return cur.fetchone()[0]

    def update_wildberries_product(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO wildberries_products (
                    product_url, is_available, price
                ) VALUES (%s, %s, %s) RETURNING id
            """, (
                data.get('product_url'),
                data.get('is_available', False),
                data.get('price', 0)
            ))
            self.conn.commit()
            return cur.fetchone()[0]

    def update_ozon_product(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ozon_products (
                    product_url, is_available, price
                ) VALUES (%s, %s, %s) RETURNING id
            """, (
                data.get('product_url'),
                data.get('is_available', False),
                data.get('price', 0)
            ))
            self.conn.commit()
            return cur.fetchone()[0]

    def get_kaspi_urls(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT product_url FROM kaspi_products")
            return [row[0] for row in cur.fetchall()]
    
    def get_alibaba_urls(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT product_url FROM alibaba_products")
            return [row[0] for row in cur.fetchall()]
    
    def get_1688_urls(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT product_url FROM market_1688_products")
            return [row[0] for row in cur.fetchall()]
    
    def get_wildberries_urls(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT product_url FROM wildberries_products")
            return [row[0] for row in cur.fetchall()]
    
    def get_ozon_urls(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT product_url FROM ozon_products")
            return [row[0] for row in cur.fetchall()]

    def __del__(self):
        if hasattr(self, 'conn'):
            self.conn.close() 