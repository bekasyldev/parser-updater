from parser import MarketplaceParser
from db_handler import DatabaseHandler
import concurrent.futures
import time
import logging
from datetime import datetime
from api_handler import APIHandler

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
        api_handler = APIHandler()
        results = []
        
        for url in urls:
            try:
                start_time = time.time()
                data = None
                logging.info(f"Starting to parse {marketplace} URL: {url}")
                
                if marketplace == 'kaspi':
                    data = parser.parse_kaspi(url)
                    if data:
                        parse_time = time.time() - start_time
                        logging.info(f"Parsed Kaspi data in {parse_time:.2f}s: {data}")
                        results.append(data)
                elif marketplace == 'alibaba':
                    data = parser.parse_alibaba(url)
                    if data:
                        parse_time = time.time() - start_time
                        logging.info(f"Parsed Alibaba data in {parse_time:.2f}s: {data}")
                        results.append(data)
                elif marketplace == 'wildberries':
                    data = parser.parse_wildberries(url)
                    if data:
                        parse_time = time.time() - start_time
                        logging.info(f"Parsed Wildberries data in {parse_time:.2f}s: {data}")
                        results.append(data)
                elif marketplace == 'ozon':
                    data = parser.parse_ozon(url)
                    if data:
                        parse_time = time.time() - start_time
                        logging.info(f"Parsed Ozon data in {parse_time:.2f}s: {data}")
                        results.append(data)
                
            except Exception as e:
                parse_time = time.time() - start_time
                logging.error(f"Error processing {url} after {parse_time:.2f}s: {str(e)}", exc_info=True)
                continue
        
        # Send batch results to API
        if results:
            if marketplace == 'kaspi':
                api_handler.send_kaspi_data(results)
            elif marketplace == 'alibaba':
                api_handler.send_alibaba_data(results)
            elif marketplace == 'wildberries':
                api_handler.send_wildberries_data(results)
            elif marketplace == 'ozon':
                api_handler.send_ozon_data(results)
        
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
    api_handler = APIHandler()
    
    while True:
        start_time = time.time()
        
        try:
            # Get URLs from API instead of database
            marketplaces = {
                'kaspi': api_handler.get_urls('kaspi'),
                'alibaba': api_handler.get_urls('alibaba'),
                'wildberries': api_handler.get_urls('wb'),
                'ozon': api_handler.get_urls('ozon')
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