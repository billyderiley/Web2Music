from flask import Flask, request, jsonify
from threading import Thread
from IdeasForV3 import DiscogsSearchScraper
import os

app = Flask(__name__)
scraper = DiscogsSearchScraper("https://www.discogs.com/search/")

# Simple token-based authentication
API_TOKEN = os.environ.get("API_TOKEN", "your_default_token")

def token_required(f):
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token or token != API_TOKEN:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated_function

def start_scraper_in_background():
    try:
        scraper.start_scraping()  # Start scraping in a separate thread
    except Exception as e:
        print(f"Error during scraping: {e}")

@app.route('/start_scrape', methods=['POST'])
@token_required
def start_scrape():
    params = request.json
    try:
        scraper.set_search_parameters(params)
        thread = Thread(target=start_scraper_in_background)
        thread.start()
        return jsonify({"status": "Scraping started"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Additional endpoints with token_required decorator






import motor.motor_asyncio
import asyncio
from bson.json_util import dumps
import json

# Async MongoDB connection string from Atlas
async_connection_string = "YOUR_MONGODB_ATLAS_CONNECTION_STRING"

# Async connection to MongoDB Atlas
async_client = motor.motor_asyncio.AsyncIOMotorClient(async_connection_string)

# Access database
async_db = async_client['your_database_name']  # Replace with your database name

# Access collection
async_collection = async_db['your_collection_name']  # Replace with your collection name

# Async function to insert data into the collection
async def async_insert_data(data):
    await async_collection.insert_one(data)

# Async function to query and fetch data from the collection
async def async_fetch_data(query):
    documents = []
    async for document in async_collection.find(query):
        documents.append(document)
    return json.loads(dumps(documents))  # Converts BSON to JSON

# Example usage of the async functions
async def main():
    await async_insert_data({'name': 'Jane Doe', 'email': 'jane@example.com'})
    results = await async_fetch_data({'name': 'Jane Doe'})
    print(results)

# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())

if __name__ == '__main__':
    app.run(debug=True)
