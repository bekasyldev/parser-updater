import asyncpg
import logging
from typing import List, Dict
import os
import dotenv

dotenv.load_dotenv()

class DatabaseHandler:
    def __init__(self):
        self.pool = None
        self.dsn = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

    async def get_pool(self):
        if not self.pool:
            self.pool = await asyncpg.create_pool(self.dsn)
            await self.create_tables()
        return self.pool

    async def create_tables(self):
        """Create necessary tables if they don't exist"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS kaspi_products (
                    id SERIAL PRIMARY KEY,
                    product_url TEXT UNIQUE NOT NULL,
                    articul TEXT NOT NULL,
                    is_available BOOLEAN DEFAULT false,
                    price INTEGER,
                    total_reviews INTEGER,
                    rating FLOAT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_kaspi_url ON kaspi_products(product_url);
                CREATE INDEX IF NOT EXISTS idx_kaspi_articul ON kaspi_products(articul);
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ozon_products (
                    id SERIAL PRIMARY KEY,
                    product_url TEXT UNIQUE NOT NULL,
                    articul TEXT NOT NULL,
                    is_available BOOLEAN DEFAULT false,
                    price INTEGER,
                    total_reviews INTEGER,
                    rating FLOAT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_ozon_url ON ozon_products(product_url);
                CREATE INDEX IF NOT EXISTS idx_ozon_articul ON ozon_products(articul);
            """)
            logging.info("Created tables")

    async def get_kaspi_urls(self) -> List[str]:
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT product_url FROM kaspi_products")
            return [row['product_url'] for row in rows]
    
    async def get_alibaba_urls(self) -> List[str]:
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT product_url FROM alibaba_products")
            return [row['product_url'] for row in rows]
    
    async def get_ozon_urls(self) -> List[str]:
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT product_url FROM ozon_products")
            return [row['product_url'] for row in rows]
    
    async def get_wildberries_urls(self) -> List[str]:
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT product_url FROM wildberries_products")
            return [row['product_url'] for row in rows]

    async def update_kaspi_product(self, data: Dict):
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            try:
                await conn.execute("""
                    INSERT INTO kaspi_products (
                        product_url,
                        articul,
                        price,
                        is_available,
                        total_reviews,
                        rating,
                        updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, NOW())
                    ON CONFLICT (product_url) 
                    DO UPDATE SET 
                        articul = EXCLUDED.articul,
                        price = EXCLUDED.price,
                        is_available = EXCLUDED.is_available,
                        total_reviews = EXCLUDED.total_reviews,
                        rating = EXCLUDED.rating,
                        updated_at = NOW()
                """, 
                data['product_url'],
                data['articul'],
                data.get('price'),
                data.get('is_available', False),
                data.get('total_reviews'),
                data.get('rating')
                )
                logging.info(f"Successfully updated product in database: {data['product_url']}")
            except Exception as e:
                logging.error(f"Error updating product in database: {str(e)}")

    async def update_alibaba_product(self, data: Dict):
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO alibaba_products (product_url, is_available, price, reviews, rating, delivery_speed, updated_at, articul) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", data['product_url'], data['is_available'], data['price'], data['reviews'], data['rating'], data['delivery_speed'], data['updated_at'], data['articul'])
            logging.info(f"Successfully updated product in database: {data['product_url']}")
        
    async def update_ozon_product(self, data: Dict):
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            try:
                await conn.execute("""
                    INSERT INTO ozon_products (
                        product_url,
                        articul,
                        price,
                        is_available,
                        total_reviews,
                        rating,
                        updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, NOW())
                    ON CONFLICT (product_url) 
                    DO UPDATE SET 
                        articul = EXCLUDED.articul,
                        price = EXCLUDED.price,
                        is_available = EXCLUDED.is_available,
                        total_reviews = EXCLUDED.total_reviews,
                        rating = EXCLUDED.rating,
                        updated_at = NOW()
                """, 
                data['product_url'],
                data['articul'],
                data.get('price'),
                data.get('is_available', False),
                data.get('total_reviews'),
                data.get('rating')
                )
                logging.info(f"Successfully updated product in database: {data['product_url']}")
            except Exception as e:
                logging.error(f"Error updating product in database: {str(e)}")
    
    async def update_wildberries_product(self, data: Dict):
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO wildberries_products (product_url, is_available, price, total_reviews, rating, updated_at, articul) VALUES ($1, $2, $3, $4, $5, $6, $7)", data['product_url'], data['is_available'], data['price'], data['total_reviews'], data['rating'], data['updated_at'], data['articul'])
            logging.info(f"Successfully updated product in database: {data['product_url']}")

    async def add_kaspi_url(self, url: str):
        """Add a new URL to track"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO kaspi_products (product_url, is_available)
                VALUES ($1, false)
                ON CONFLICT (product_url) DO NOTHING
            """, url)
            logging.info(f"Added new URL to track: {url}")

    async def close(self):
        if self.pool:
            await self.pool.close() 