# Marketplace Price Parser

A robust Python-based web scraper that monitors prices and product information from multiple marketplaces:

- Kaspi.kz
- Alibaba.com
- Wildberries
- Ozon

## Features

- Real-time price monitoring
- Multi-marketplace support
- Automated data collection every 15 minutes
- PostgreSQL database storage
- Multi-threaded processing
- Detailed logging system

## Requirements

- Python 3.10+
- PostgreSQL 12+
- Chrome Browser
- ChromeDriver

## Installation

1. Clone the repository

2. Create and activate virtual environment

3. Install dependencies

4. Run the script

## Database Structure

### Kaspi Products Table

- product_url (TEXT, UNIQUE)
- is_available (BOOLEAN)
- price (INTEGER)
- delivery_price (TEXT)
- delivery_date (TEXT)
- total_reviews (INTEGER)
- rating (FLOAT)
- updated_at (TIMESTAMP)

### Alibaba Products Table

- product_url (TEXT, UNIQUE)
- is_available (BOOLEAN)
- price (TEXT)
- reviews (TEXT)
- rating (TEXT)
- delivery_speed (TEXT)
- updated_at (TIMESTAMP)

### Wildberries Products Table

- product_url (TEXT, UNIQUE)
- is_available (BOOLEAN)
- price (INTEGER)
- rating (TEXT)
- reviews (TEXT)
- updated_at (TIMESTAMP)

### Ozon Products Table

- product_url (TEXT, UNIQUE)
- is_available (BOOLEAN)
- price (INTEGER)
- rating (TEXT)
- reviews (TEXT)
- updated_at (TIMESTAMP)

## Usage

1. Add URLs to monitor
2. Run the script
3. Check the database for updated data

## Logging

Logs are stored in `parser_YYYYMMDD.log` files with the following information:

- Parsing status for each URL
- Error messages
- Update cycles timing
- Data collection results

## Important Notes

1. **URL Management**:

   - URLs are stored uniquely in each table
   - First insertion creates record with default values
   - Subsequent updates modify existing records

2. **Data Updates**:

   - Automatic updates every 15 minutes
   - Each update cycle processes all marketplaces
   - Failed updates are logged but don't stop the process

3. **Error Handling**:
   - Connection errors are automatically retried
   - Invalid URLs are logged but skipped
   - Database connection issues trigger automatic reconnection

## Troubleshooting

1. **ChromeDriver Issues**:

   - Ensure Chrome browser is installed
   - Update ChromeDriver if needed
   - Check ChromeDriver permissions

2. **Database Issues**:

   - Verify PostgreSQL is running
   - Check database credentials in .env
   - Ensure database user has proper permissions

3. **Network Issues**:
   - Check internet connection
   - Verify marketplace websites are accessible
   - Consider using VPN if needed

## License

MIT - See LICENSE file for details

## Support

For support, please contact me at [email](devbekasyl@gmail.com)
