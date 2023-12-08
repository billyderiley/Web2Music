import unittest
from unittest.mock import patch
from main import BaseScraper, DiscogsSearchScraper, DiscogsSearch, DiscogsReleaseScraper, DataHandler
discogs_base_url = "https://www.discogs.com/search"
youtube_api_key = 'AIzaSyAUCZgYUeP4Xcj-kw88V6X7VfcjQdBPtAg' # Use this key in your application by passing it with the key=API_KEY parameter.

class TestDiscogsSearch(unittest.TestCase):

    def test_set_up(self):
        test_url = discogs_base_url
        self.Discogs_Search = DiscogsSearch(test_url)

    def test_getSearchOptions(self):
        test_url = discogs_base_url
        self.Discogs_Search = DiscogsSearch(test_url)
        self.Discogs_Search.get_search_options() ###must call parse aside cotent before gettnig search options

    def test_searchPage(self):
        test_url = discogs_base_url
        self.Discogs_Search = DiscogsSearch(test_url)
        self.Discogs_Search.get_search_options()  ###must call parse aside cotent before gettnig search options
        self.Discogs_Search.search_page_user_interaction()


class TestDataHandler(unittest.TestCase):
    def test(self):
        test_url = discogs_base_url
        # Create an instance of DataHandler
        data_handler = DataHandler()

        # Create an instance of DiscogsSearch and pass data_handler to it
        discogs_search = DiscogsSearch(test_url, data_handler)
        discogs_search.user_interaction()
        #discogs_search.search_page_user_interaction()
        #discogs_search.search_page_user_interaction()
        discogs_search.data_handler.save_dataframe()
        # At this point, data_handler's DataFrame should be updated with the content
        #data_handler.display_dataframe()

if __name__ == '__main__':
    unittest.main()