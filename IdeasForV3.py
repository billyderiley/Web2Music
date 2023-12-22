import requests
from bs4 import BeautifulSoup
import time
import logging

from typing import Dict, List, Tuple, Optional
from bs4 import BeautifulSoup

import time
import requests
from requests.exceptions import RequestException

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from requests.exceptions import RequestException
from typing import Dict, Any, List

from fake_useragent import UserAgent

import aiohttp
from aiohttp import ClientSession
import asyncio
from bs4 import BeautifulSoup

class BaseScraper:
    def __init__(self, base_url, rate_limit=1.0):
        """
        Initialize the BaseScraper class.
        :param base_url: The base URL for scraping operations.
        :param rate_limit: Time in seconds to wait between requests.
        """
        self.user_agents = UserAgent()
        self.base_url = base_url
        self.rate_limit = rate_limit
        self.headers = {'User-Agent': 'Your User-Agent'}
        self.session = requests.Session()  # Using session for persistent configurations
        # Additional instance variables can be initialized here if needed

    def get_random_user_agent(self):
        """
        Returns a random user-agent string.
        """
        return self.user_agents.random

    def get_soup_from_url(self, url):
        """
        Fetch and parse the HTML content of a given URL.

        :param url: URL to fetch the content from.
        :return: BeautifulSoup object of the parsed HTML content.
        """
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()  # Raises HTTPError for bad responses
            return BeautifulSoup(response.content, 'html.parser')
        except requests.exceptions.RequestException as e:
            self.handle_request_errors(e)
            return None

    async def get_soup_from_url_async(self, url):
        """
        Fetch and parse the HTML content of a given URL asynchronously.
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers={'User-Agent': self.get_random_user_agent()}) as response:
                if response.status == 200:
                    html = await response.text()
                    return BeautifulSoup(html, 'html.parser')
                else:
                    print(f"Error fetching {url}: {response.status}")
                    return None

    """# Usage example
    async def main():
        scraper = DiscogsSearchScraper("https://www.discogs.com/search")
        soup = await scraper.get_soup_from_url_async(scraper.current_url)

    asyncio.run(main())"""

    def handle_request_errors(self, error):
        """
        Handle errors related to HTTP requests.
        :param error: Exception object representing the error.
        """
        logging.error(f"Request Error: {error}")
        # Additional error handling logic goes here

    def rate_limit_control(self, response=None):
        """
        Enhanced rate limit control using server response headers.
        """
        if response and 'X-RateLimit-Reset' in response.headers:
            reset_time = int(response.headers['X-RateLimit-Reset'])
            wait_time = max(reset_time - time.time(), 1)  # Ensure at least 1 second wait
            time.sleep(wait_time)

    def extract_data(self, soup):
        """
        Extract data from a BeautifulSoup object. (To be implemented in derived classes)
        :param soup: BeautifulSoup object to extract data from.
        """
        raise NotImplementedError("This method should be overridden in a derived class.")

# Example Usage
# scraper = BaseScraper("http://example.com", rate_limit=2.0)
# soup = scraper.get_soup_from_url(scraper.base_url)
# data = scraper.extract_data(soup)  # Assuming this is implemented in a subclass

# DiscogsSearchScraper Class
class DiscogsSearchScraper(BaseScraper):
    def __init__(self, start_url: str, Search_Dataframe=None):
        """
        Initialize the DiscogsSearchScraper class.
        :param start_url: The initial URL to start scraping from.
        :param Search_Dataframe: A pandas DataFrame to store search results.
        """
        super().__init__(start_url)
        self.search_module = SearchModule(self)
        self.search_options_dict: Dict[str, Dict[str, str]] = {}
        self.current_url: str = start_url
        self.Search_Dataframe = Search_Dataframe if Search_Dataframe else self.create_search_dataframe()

    async def scrape_all_pages(self, urls):
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_and_process_page(session, url) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return results

    async def fetch_and_process_page(self, session, url, retries=3):
        for attempt in range(1, retries + 1):
            try:
                response = await session.get(url)
                response.raise_for_status()  # Raise an exception for HTTP errors
                soup = BeautifulSoup(await response.text(), 'html.parser')
                # Process soup to extract data
                data = self.extract_data(soup)
                # Update DataFrame or other data structures
                return data
            except aiohttp.ClientError as e:
                logging.error(f"Error fetching {url}: {e}, Attempt {attempt} of {retries}")
                if attempt == retries:
                    logging.error(f"Failed to fetch {url} after {retries} attempts")
                    return None  # Or a default value
                await asyncio.sleep(2)  # Wait before retrying

    def fetch_search_results(self, url):
        """
        Fetch search results from a given URL, with rate limiting and error handling.
        """
        self.rate_limit_control()

        try_count = 0
        max_retries = 3  # or any other appropriate number
        while try_count < max_retries:
            try:
                soup = self.get_soup_from_url(url)
                # Process the soup object to extract search results
                # ...
                break  # Exit loop if successful
            except RequestException as e:
                # Log the error along with the URL for better troubleshooting
                print(f"Error fetching {url}: {e}")
                try_count += 1
                time.sleep(2)  # Wait for 2 seconds before retrying

    def scrape_search_results(self, url: str):
        """
        Scrape search results from a given URL.
        :param url: URL to scrape from.
        """
        soup = self.get_soup_from_url(url)
        if soup:
            data = self.extract_data(soup)
            self.update_search_dataframe(data)
        else:
            print(f"Failed to scrape data from {url}")

    def create_search_dataframe(self) -> pd.DataFrame:
        """
        Create a DataFrame to store search results.
        :return: A pandas DataFrame with specified columns.
        """
        columns = ["Discogs_Artists", "Discogs_Titles", "Discogs_Labels", ...]  # Add more columns as needed
        return pd.DataFrame(columns=columns)

    def get_search_url_content_dict(self) -> Dict[str, Any]:
        """
        Fetch and parse the current search page's content.
        :return: A dictionary containing different parts of the page content.
        """
        soup = self.get_soup_from_url(self.current_url)
        if soup:
            return self.parse_search_page(soup)
        return {}

    def parse_search_page(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Parse different sections of the search page.
        :param soup: BeautifulSoup object of the search page.
        :return: A dictionary containing parsed content from the page.
        """
        artists = self.parse_artists_section(soup)
        titles = self.parse_titles_section(soup)
        labels = self.parse_labels_section(soup)
        # Add other sections as needed
        return {'artists': artists, 'titles': titles, 'labels': labels}

    def parse_artists_section(self, soup: BeautifulSoup) -> List[str]:
        """
        Parse the artist section of the page.
        :param soup: BeautifulSoup object.
        :return: List of artists.
        """
        # Implement the artist parsing logic
        return []

    def parse_titles_section(self, soup: BeautifulSoup) -> List[str]:
        """
        Parse the title section of the page.
        :param soup: BeautifulSoup object.
        :return: List of titles.
        """
        # Implement the title parsing logic
        return []

    def parse_labels_section(self, soup: BeautifulSoup) -> List[str]:
        """
        Parse the label section of the page.
        :param soup: BeautifulSoup object.
        :return: List of labels.
        """
        # Implement the label parsing logic
        return []

    def update_search_dataframe(self, new_data: List[Dict[str, Any]]):
        """
        Update the search DataFrame with new data.
        :param new_data: List of dictionaries containing new data to be added.
        """
        # Implement DataFrame update logic here

    def navigate_to_next_page(self) -> bool:
        """
        Navigate to the next page in the search results.
        :return: True if navigation is successful, False otherwise.
        """
        soup = self.get_soup_from_url(self.current_url)
        next_page_link = soup.find('a', {'class': 'pagination_next'})
        if next_page_link:
            self.current_url = self.base_url + next_page_link['href']
            return True
        return False  # End of search results



class SearchModule:
    def __init__(self, base_scraper):
        self.base_scraper = base_scraper

    def construct_search_url(self, genre=None, year=None, artist=None):
        """
        Dynamically construct a search URL based on input parameters.
        """
        # Construct URL based on the provided parameters
        return self.base_scraper.base_url + "/search?" + "genre=" + genre + "&year=" + year + "&artist=" + artist

    def navigate_search_results(self, url):
        # logic to navigate

    def extract_summary_data(self, soup):
        # logic to extract data from search page

    def update_search_parameters(self, genre=None, year=None, artist=None):
# Update internal search parameters based on provided arguments

class SearchAnalytics:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def most_common_labels(self):
        return self.dataframe['Discogs_Labels'].value_counts().head(10)

    def most_common_genres(self):
        return self.dataframe['Discogs_Genres'].value_counts().head(10)

    def most_common_artists(self):
        return self.dataframe['Discogs_Artists'].value_counts().head(10)

class ReleaseDetailsModule:
    def __init__(self, base_scraper):
        self.base_scraper = base_scraper

    def extract_release_details(self, url):

    # logic to extract details

    def parse_tracklist(self, soup):

    # logic to parse tracklist

    def parse_additional_info(self, soup):
# logic to parse additional release info

class DiscogsReleaseScraper(BaseScraper):
    def __init__(self, data_handler):
        super().__init__(...)
        self.release_details_module = ReleaseDetailsModule(self)

    def navigate_to_next_page(self) -> bool:
        """
        Navigate to the next page in the search results.
        :return: True if navigation is successful, False otherwise.
        """
        soup = self.get_soup_from_url(self.current_url)
        next_page_link = soup.find('a', {'class': 'pagination_next'})
        if next_page_link:
            self.current_url = self.base_url + next_page_link['href']
            return True
        return False  # End of search results

# SpotifyScraper and SpotifyPlaylistCreation Classes
class SpotifyScraper:
    def __init__(self, spotify_api):
        self.spotify_api = spotify_api

    def get_artist_metrics(self, artist_id):
        # Retrieve artist metrics from Spotify
        pass

class SpotifyPlaylistCreation(SpotifyScraper):
    def __init__(self, spotify_api, data_handler):
        super().__init__(spotify_api)
        self.data_handler = data_handler

    def create_playlist(self, playlist_name, track_uris):
        # Create a playlist on Spotify
        pass

    def generate_playlist_from_dataframe(self):
        # Logic to generate playlist from data
        pass

# DataHandler Class
class DataHandler:
    def __init__(self):
        self.search_dataframe = None
        self.release_dataframe = None
        self.spotify_dataframe = None

    def load_data(self, file_path):
        # Load data into DataFrame
        pass

    def save_data(self, dataframe, file_path):
        # Save DataFrame to file
        pass

    def transfer_data_to_dto(self, dataframe):
        # Convert DataFrame to DTOs
        pass

# DTO Classes
class SpotifyDTO:
    def __init__(self, artist_name, track_name, popularity_metrics):
        self.artist_name = artist_name
        self.track_name = track_name
        self.popularity_metrics = popularity_metrics

# DataExtractor and DataProcessor Classes (New)
class DataExtractor:
    def extract_from_html(self, soup):
        # Extract data from HTML
        pass

    def extract_from_api_response(self, response):
        # Extract data from API response
        pass

class DataProcessor:
    def process_data(self, raw_data):
        # Process raw data
        pass

    def transform_data(self, processed_data):
        # Transform processed data
        pass


class DataExporter:
    @staticmethod
    def export_to_csv(dataframe, filename):
        dataframe.to_csv(filename, index=False)

    @staticmethod
    def export_to_json(dataframe, filename):
        dataframe.to_json(filename, orient='records')

    # Add methods for other formats or database export as needed.

# API Client Classes (e.g., SpotifyApiClient)
class SpotifyApiClient:
    def __init__(self, credentials):
        self.credentials = credentials

    def make_api_call(self, endpoint, params):
        # Make an API call
        pass

    def handle_api_response(self, response):
        # Handle the response from API
        pass


async def main():
    scraper = DiscogsSearchScraper("https://www.discogs.com/search")
    urls_to_scrape = ["URL1", "URL2", ...]  # List of URLs to scrape
    results = await scraper.scrape_all_pages(urls_to_scrape)


# Usage Example
def main():
    # Instantiate classes
    search_scraper = DiscogsSearchScraper(search_dataframe=pd.DataFrame())
    spotify_api_client = SpotifyApiClient(credentials={"client_id": "xyz", "client_secret": "abc"})
    spotify_playlist_creator = SpotifyPlaylistCreation(spotify_api_client, DataHandler())
    data_extractor = DataExtractor()
    data_processor = DataProcessor()



    # Perform operations
    asyncio.run(main())

    search_scraper.scrape_search_results("some_url")
    playlist = spotify_playlist_creator.create_playlist("My Playlist", ["track_uri1", "track_uri2"])

if __name__ == "__main__":
    main()