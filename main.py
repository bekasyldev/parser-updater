from parser import MarketplaceParser
from db_handler import DatabaseHandler
import concurrent.futures
import time
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    filename=f'parser_{datetime.now().strftime("%Y%m%d")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def process_urls(urls, marketplace):
    """Process a batch of URLs for a specific marketplace"""
    try:
        parser = MarketplaceParser()
        db = DatabaseHandler()
        results = []
        
        for url in urls:
            try:
                data = None
                logging.info(f"Starting to parse {marketplace} URL: {url}")
                
                if marketplace == 'kaspi':
                    data = parser.parse_kaspi(url)
                    if data:
                        logging.info(f"Parsed Kaspi data: {data}")
                        db.update_kaspi_product(data)
                elif marketplace == 'alibaba':
                    data = parser.parse_alibaba(url)
                    if data:
                        db.update_alibaba_product(data)
                elif marketplace == 'wildberries':
                    data = parser.parse_wildberries(url)
                    if data:
                        db.update_wildberries_product(data)
                elif marketplace == 'ozon':
                    data = parser.parse_ozon(url)
                    if data:
                        db.update_ozon_product(data)
                
                if data:
                    logging.info(f"Successfully parsed and updated {marketplace} product: {url}")
                    results.append(data)
                else:
                    logging.error(f"Failed to parse {marketplace} product: {url}")
                
                # Add delay between requests to avoid blocking
                time.sleep(2)
                
            except Exception as e:
                logging.error(f"Error processing {url}: {str(e)}", exc_info=True)
                continue
        
        return results
    except Exception as e:
        logging.error(f"Critical error in process_urls: {str(e)}", exc_info=True)
        raise
    finally:
        if 'parser' in locals():
            parser.driver.quit()

def chunk_urls(urls, chunk_size=100):
    """Split URLs into chunks"""
    for i in range(0, len(urls), chunk_size):
        yield urls[i:i + chunk_size]

def main():
    db = DatabaseHandler()
    
    while True:
        start_time = time.time()
        
        try:
            # Get URLs for each marketplace
            marketplaces = {
                'kaspi': db.get_kaspi_urls(),
                'alibaba': db.get_alibaba_urls(),
                'wildberries': db.get_wildberries_urls(),
                'ozon': db.get_ozon_urls()
            }
            
            # Process each marketplace
            for marketplace, urls in marketplaces.items():
                if urls:  # Добавим проверку на наличие URLs
                    logging.info(f"Starting update for {marketplace} with {len(urls)} URLs")
                    
                    # Split URLs into chunks
                    url_chunks = list(chunk_urls(urls))
                    
                    # Process chunks in parallel
                    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                        futures = [
                            executor.submit(process_urls, chunk, marketplace)
                            for chunk in url_chunks
                        ]
                        concurrent.futures.wait(futures)
                    
                    logging.info(f"Completed update for {marketplace}")
                else:
                    logging.warning(f"No URLs found for {marketplace}")
            
            # Calculate time until next run (15 minutes = 900 seconds)
            execution_time = time.time() - start_time
            sleep_time = max(0, 900 - execution_time)  # Changed from 1200 to 900
            
            logging.info(f"Update cycle completed in {execution_time:.2f} seconds")
            logging.info(f"Sleeping for {sleep_time:.2f} seconds")
            
            time.sleep(sleep_time)
            
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
            time.sleep(60)  # Wait a minute before retrying

if __name__ == "__main__":
    main()