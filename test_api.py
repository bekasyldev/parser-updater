import requests
import logging
import json

def test_api_connection():
    base_url = "https://parse.trendoanalytics.kz"
    endpoints = ['kaspi', 'ozon', 'wb', 'alibaba']
    
    print("Testing API connectivity...")
    
    for endpoint in endpoints:
        try:
            url = f"{base_url}/api_table/get_data/{endpoint}"
            print(f"\nTesting {url}")
            
            response = requests.get(url, timeout=10)
            print(f"Status code: {response.status_code}")
            
            # Parse and print the actual data structure
            if response.status_code == 200:
                data = response.json()
                print(f"Number of items: {len(data)}")
                if len(data) > 0:
                    print(f"First item structure: {json.dumps(data[0], indent=2)}")
            
        except Exception as e:
            print(f"Error testing {endpoint}: {str(e)}")

if __name__ == "__main__":
    test_api_connection() 