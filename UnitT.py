import unittest
from unittest.mock import patch
from main import BaseScraper, DiscogsSearchScraper, DiscogsSearch, DiscogsReleaseScraper, DataHandler
discogs_base_url = "https://www.discogs.com/search"
youtube_api_key = 'AIzaSyAUCZgYUeP4Xcj-kw88V6X7VfcjQdBPtAg' # Use this key in your application by passing it with the key=API_KEY parameter.

# Test Class for BaseScraper
class TestBaseScraper(unittest.TestCase):

    def test_setUp(self):
        self.Base_Scraper = BaseScraper()
        test_url = discogs_base_url
        self.Base_Scraper.createSoupObjFromUrl(test_url)


# Test Class for DiscogsScraper
class TestDiscogsSearchScraper(unittest.TestCase):

    def test_setUp(self):
        test_url = discogs_base_url
        self.Discogs_Search_Scraper = DiscogsSearchScraper()
        self.Discogs_Search_Scraper.get_search_page_content(test_url)


class TestDiscogsSearch(unittest.TestCase):

    def test_set_up(self):
        test_url = discogs_base_url
        self.Discogs_Search = DiscogsSearch(test_url)

    def test_getSearchOptions(self):
        test_url = discogs_base_url
        self.Discogs_Search = DiscogsSearch(test_url)
        #self.Discogs_Search.get_search_options() ###must call parse aside cotent before gettnig search options

    def test_searchPage(self):
        test_url = discogs_base_url
        self.Discogs_Search = DiscogsSearch(test_url)
        #self.Discogs_Search.get_search_options()  ###must call parse aside cotent before gettnig search options
        self.Discogs_Search.user_interaction_add_filters()

class TestDiscogsReleaseScraper(unittest.TestCase):

    def test_set_up(self):
        test_release = "https://www.discogs.com/release/28624954-Jon-Hopkins-LateNightTales"
        my_youtube_API_Key = 'AIzaSyAUCZgYUeP4Xcj-kw88V6X7VfcjQdBPtAg'
        self.Discogs_Release_Scraper = DiscogsReleaseScraper()

    def test_process_release(self):
        # with youtube api
        my_youtube_API_Key = 'AIzaSyCBZ6lIgO9qQdVou_aAyONBEngCsWG5-eg'
        test_release = "https://www.discogs.com/release/28624954-Jon-Hopkins-LateNightTales"
        self.Discogs_Release_Scraper = DiscogsReleaseScraper()
        self.Discogs_Release_Scraper.get_release_page_content(test_release)

class TestDataHandler(unittest.TestCase):
    def test(self):
        test_url = discogs_base_url
        # Create an instance of DataHandler
        data_handler = DataHandler()

        # Create an instance of DiscogsSearch and pass data_handler to it
        discogs_search = DiscogsSearch(test_url, data_handler)

        # At this point, data_handler's DataFrame should be updated with the content
        data_handler.display_dataframe()

if __name__ == '__main__':
    unittest.main()