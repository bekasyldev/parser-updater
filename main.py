import asyncio
import logging
from datetime import datetime
from playwright.async_api import async_playwright
from parsers.parser_factory import ParserFactory
from services.api_service import APIService
from db_handler import DatabaseHandler

async def process_urls(urls, marketplace):
    """Process URLs and send to API"""
    if not urls:
        logging.warning(f"No URLs to process for {marketplace}")
        return
        
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            context = await browser.new_context()
            
            # Initialize services
            api_service = APIService()
            semaphore = asyncio.Semaphore(10)
            
            # Get appropriate parser
            parser = ParserFactory.get_parser(urls[0], context, semaphore)
            if not parser:
                logging.error(f"No parser found for marketplace: {marketplace}")
                return

            # Process URLs in batches
            batch_size = 100
            total_processed = 0
            
            for i in range(0, len(urls), batch_size):
                batch = urls[i:i + batch_size]
                try:
                    results = await parser.parse_urls(batch)
                    
                    # Format results for API
                    valid_results = [result[0] for result in results if isinstance(result, tuple) and not result[0].get('error')]
                    
                    # Send to API
                    if valid_results:
                        await api_service.send_data(valid_results, marketplace)
                        total_processed += len(valid_results)
                    
                    logging.info(f"Processed batch {i//batch_size + 1} for {marketplace}: {len(valid_results)} valid results")
                    
                except Exception as e:
                    logging.error(f"Error processing batch for {marketplace}: {str(e)}")
                    continue
            
            logging.info(f"Completed processing {total_processed} items for {marketplace}")
            await browser.close()
            
    except Exception as e:
        logging.error(f"Error in process_urls for {marketplace}: {str(e)}")

async def process_marketplace(marketplace):
    """Process all URLs for a specific marketplace"""
    try:
        db = DatabaseHandler()
        
        # Get URLs based on marketplace
        urls = []
        if marketplace == 'kaspi':
            urls = await db.get_kaspi_urls()
        elif marketplace == 'ozon':
            urls = await db.get_ozon_urls()
        elif marketplace == 'wb':
            urls = await db.get_wildberries_urls()
        elif marketplace == 'alibaba':
            urls = await db.get_alibaba_urls()
            
        if urls:
            logging.info(f"Starting update for {marketplace} with {len(urls)} URLs")
            await process_urls(urls, marketplace)
            logging.info(f"Completed update for {marketplace}")
        else:
            logging.warning(f"No URLs found for {marketplace}")
            
    except Exception as e:
        logging.error(f"Error processing {marketplace}: {str(e)}")

async def main():
    """Main function to process all marketplaces"""
    try:
        # Initialize database
        db = DatabaseHandler()
        await db.create_tables()
        
        while True:
            # Process each marketplace
            marketplaces = ['kaspi', 'ozon', 'wb', 'alibaba']
            
            for marketplace in marketplaces:
                try:
                    logging.info(f"Starting marketplace: {marketplace}")
                    await process_marketplace(marketplace)
                except Exception as e:
                    logging.error(f"Error in marketplace {marketplace}: {str(e)}")
                    continue
                
                # Wait between marketplaces to avoid overwhelming the system
                await asyncio.sleep(5)
            
            # Wait before next round of updates
            logging.info("Completed all marketplaces, waiting for next cycle")
            await asyncio.sleep(300)  # 5 minutes between cycles
            
    except Exception as e:
        logging.error(f"Error in main loop: {str(e)}")
    finally:
        if 'db' in locals():
            await db.close()

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'parser_{datetime.now().strftime("%Y%m%d")}.log')
        ]
    )
    
    # Run the main loop
    asyncio.run(main())