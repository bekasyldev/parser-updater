import asyncio
from typing import List, Dict
import logging
from datetime import datetime
import time
from db_handler import DatabaseHandler
from parser import MarketplaceParser
from api_handler import APIHandler
from concurrent.futures import ThreadPoolExecutor

class AsyncMarketplaceParser:
    def __init__(self, max_concurrent_parsers=3):
        self.max_parsers = max_concurrent_parsers
        self.parser_semaphore = asyncio.Semaphore(max_concurrent_parsers)
        self.db = DatabaseHandler()
        self.api_handler = APIHandler()
        self.parsers = []
        self.thread_pool = ThreadPoolExecutor(max_workers=max_concurrent_parsers)
        self.loop = asyncio.get_event_loop()

    async def initialize_parser(self):
        """Initialize and return a MarketplaceParser instance"""
        parser = MarketplaceParser()
        self.parsers.append(parser)
        return parser

    async def get_parser(self):
        """Get an available parser from the pool"""
        async with self.parser_semaphore:
            if not self.parsers:
                return await self.initialize_parser()
            return self.parsers.pop()

    async def release_parser(self, parser):
        """Release parser back to the pool"""
        self.parsers.append(parser)
        self.parser_semaphore.release()

    def _sync_parse(self, parser, url: str, marketplace: str):
        """Synchronous parsing method to run in thread pool"""
        try:
            if marketplace == 'kaspi':
                return parser.parse_kaspi(url)
            elif marketplace == 'alibaba':
                return parser.parse_alibaba(url)
            elif marketplace == 'wildberries':
                return parser.parse_wildberries(url)
            elif marketplace == 'ozon':
                return parser.parse_ozon(url)
        except Exception as e:
            logging.error(f"Error parsing {marketplace} URL {url}: {str(e)}")
            return None

    async def parse_url(self, url: str, marketplace: str):
        """Parse a single URL with automatic parser management"""
        parser = await self.get_parser()
        try:
            start_time = time.time()
            
            # Run the synchronous parsing in a thread pool
            result = await self.loop.run_in_executor(
                self.thread_pool,
                self._sync_parse,
                parser,
                url,
                marketplace
            )
            
            parse_time = time.time() - start_time
            if result:
                logging.info(f"Parsed {marketplace} URL {url} in {parse_time:.2f}s")
            else:
                logging.error(f"Failed to parse {marketplace} URL {url} after {parse_time:.2f}s")
            
            return result
        finally:
            await self.release_parser(parser)

    async def process_marketplace_urls(self, marketplace: str, products: List[tuple[str, str]]):
        """Process all URLs for a marketplace concurrently"""
        tasks = []
        for url, articul in products:
            # Create task for parsing
            parse_task = asyncio.create_task(self.parse_url(url, marketplace))
            tasks.append((parse_task, url, articul))

        results = []
        for parse_task, url, articul in tasks:
            try:
                result = await parse_task
                if result:
                    result['articul'] = articul
                    results.append(result)
                    # Update database
                    if marketplace == 'kaspi':
                        self.db.update_kaspi_product(result)
                    elif marketplace == 'alibaba':
                        self.db.update_alibaba_product(result)
                    elif marketplace == 'wildberries':
                        self.db.update_wildberries_product(result)
                    elif marketplace == 'ozon':
                        self.db.update_ozon_product(result)
            except Exception as e:
                logging.error(f"Error processing {url}: {str(e)}")

        return results

    async def run(self):
        """Main run loop"""
        while True:
            try:
                start_time = time.time()
                logging.info("Starting new update cycle")

                # Get URLs from API
                marketplaces = {}
                for marketplace in ['kaspi', 'alibaba', 'wb', 'ozon']:
                    products = self.api_handler.get_urls(marketplace)
                    if products:
                        marketplaces[marketplace] = products
                        logging.info(f"Retrieved {len(products)} products for {marketplace}")

                if not marketplaces:
                    logging.warning("No products received from API")
                    await asyncio.sleep(60)
                    continue

                # Process each marketplace
                tasks = []
                for marketplace, products in marketplaces.items():
                    task = asyncio.create_task(
                        self.process_marketplace_urls(marketplace, products)
                    )
                    tasks.append((marketplace, task))

                # Wait for all tasks to complete
                for marketplace, task in tasks:
                    try:
                        results = await task
                        logging.info(f"Completed {marketplace}, processed {len(results)} items")
                    except Exception as e:
                        logging.error(f"Error processing {marketplace}: {str(e)}")

                # Calculate sleep time
                execution_time = time.time() - start_time
                sleep_time = max(0, 900 - execution_time)
                logging.info(f"Cycle completed in {execution_time:.2f}s, sleeping for {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)

            except Exception as e:
                logging.error(f"Error in main loop: {str(e)}")
                await asyncio.sleep(60)

    def cleanup(self):
        """Cleanup resources"""
        self.thread_pool.shutdown(wait=True)
        for parser in self.parsers:
            try:
                parser.driver.quit()
            except:
                pass

async def main():
    parser = AsyncMarketplaceParser(max_concurrent_parsers=3)
    try:
        await parser.run()
    finally:
        parser.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
    except Exception as e:
        logging.critical(f"Critical error: {str(e)}", exc_info=True) 