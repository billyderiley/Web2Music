import unittest
from SpotifyPlaylistCreation import SpotifyPlaylistCreation
from DiscogsReleaseScraper import DiscogsReleaseScraper
from DiscogsSearchGUI import DiscogsSearchGUI
from ScrapeDataHandler import DataHandler

discogs_base_url = "https://www.discogs.com/search/"
youtube_api_key = 'AIzaSyAUCZgYUeP4Xcj-kw88V6X7VfcjQdBPtAg' # Use this key in your application by passing it with the key=API_KEY parameter.


class TestSpotifyAPI(unittest.TestCase):
    def test_(self):
        client_id = '2ea9899462614265a2b26b43c68cf72a'
        client_secret = 'fb29534f29134f618623b02cfe8dbc65'
        redirect_uri = 'https://open.spotify.com'
        spotify_api = SpotifyPlaylistCreation(client_id, client_secret, redirect_uri)
        spotify_api.user_menu()


class TestDiscogsReleaseScraper(unittest.TestCase):

        """def test_set_up(self):
            test_release = "https://www.discogs.com/release/28855378-Lana-Del-Rey-Lust-For-Life"
            my_youtube_API_Key = 'AIzaSyCBZ6lIgO9qQdVou_aAyONBEngCsWG5-eg'
            self.Discogs_Release_Scraper = DiscogsReleaseScraper(test_release)"""

        def test_process_release(self):
            # with youtube api
            my_youtube_API_Key = 'AIzaSyCBZ6lIgO9qQdVou_aAyONBEngCsWG5-eg'
            test_release = "https://www.discogs.com/release/28855378-Lana-Del-Rey-Lust-For-Life"
            self.Discogs_Release_Scraper = DiscogsReleaseScraper()
            current_url_release_info_dict = self.Discogs_Release_Scraper.get_Soup_from_url(test_release)
            self.Discogs_Release_Scraper.add_new_release(current_url_release_info_dict)
            self.Discogs_Release_Scraper.save_release_dataframe()

class TestDiscogsSearch(unittest.TestCase):
    def test_set_up(self):
        #test_url = discogs_base_url
        test_url = 'https://www.discogs.com/search/'
 # The code snippet is a unit test for various classes related to Spotify playlist creation, Discogs release scraping, Discogs search, and data handling. It tests various methods and functionalities of these classes to ensure they are working correctly.
        self.Discogs_Search = DiscogsSearchGUI(test_url)
        self.Discogs_Search.user_interaction_menu()
"""
    def test_getSearchOptions(self):
        test_url = discogs_base_url
        self.Discogs_Search = DiscogsSearch(test_url)
        #self.Discogs_Search.get_search_options() ###must call parse aside cotent before gettnig search options

    def test_searchPage(self):
        test_url = discogs_base_url
        self.Discogs_Search = DiscogsSearch(test_url)
        self.Discogs_Search.get_search_options()  ###must call parse aside cotent before gettnig search options
        #self.Discogs_Search.search_page_user_interaction()
        #self.Discogs_Search.clean_applied_filters()
"""

class TestDataHandler(unittest.TestCase):
    def test(self):
        #test_url = discogs_base_url
        # Create an instance of DataHandler
        #data_handler = DataHandler()
        # Create an instance of DiscogsSearch and pass data_handler to it
        ztest_search = "https://www.discogs.com/search/?style_exact=Ambient&style_exact=Techno&format_exact=Vinyl&genre_exact=Electronic"
        #test_search = "https://www.discogs.com/search/"
        #discogs_search = DiscogsSearch(start_url=test_search)
        #discogs_search.user_interaction()
        #discogs_search.test_function()

        #discogs_search.search_page_user_interaction()
        #discogs_search.search_page_user_interaction()
        #discogs_search.data_handler.save_dataframe()
        # At this point, data_handler's DataFrame should be updated with the content
        #data_handler.display_dataframe()



if __name__ == '__main__':
    unittest.main()