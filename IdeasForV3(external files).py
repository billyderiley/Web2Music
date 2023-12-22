from flask import Flask, request, jsonify
from threading import Thread
from IdeasForV3 import DiscogsSearchScraper
import os

app = Flask(__name__)
scraper = DiscogsSearchScraper("https://www.discogs.com/search")

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

if __name__ == '__main__':
    app.run(debug=True)
