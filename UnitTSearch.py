import unittest
from unittest.mock import patch
from main import BaseScraper, DiscogsSearchScraper, DiscogsSearch, DiscogsReleaseScraper, DataHandler, SessionDataManager, MusicScraperApp
discogs_base_url = "https://www.discogs.com/search/"
youtube_api_key = 'AIzaSyAUCZgYUeP4Xcj-kw88V6X7VfcjQdBPtAg' # Use this key in your application by passing it with the key=API_KEY parameter.

class TestDiscogsSearch(unittest.TestCase):
    def test_set_up(self):
        #test_url = discogs_base_url
        test_url = 'https://www.discogs.com/search/?genre_exact=Electronic&style_exact=Techno&style_exact=Ambient&page=5'
        self.Discogs_Search = DiscogsSearch(test_url)
        self.Discogs_Search.test_function(max_rows_to_update=5)
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
        test_url = discogs_base_url
        # Create an instance of DataHandler
        data_handler = DataHandler()
        # Create an instance of DiscogsSearch and pass data_handler to it
        discogs_search = DiscogsSearch(test_url, data_handler)
        discogs_search.user_interaction()

        #discogs_search.search_page_user_interaction()
        #discogs_search.search_page_user_interaction()
        #discogs_search.data_handler.save_dataframe()
        # At this point, data_handler's DataFrame should be updated with the content
        #data_handler.display_dataframe()


class TestSessionHandler(unittest.TestCase):

    def test_setupSessionHandler(self):
        # Usage example
        data_manager = SessionDataManager('saved_data.csv')
        print(data_manager.dataframe)
        # Add new data, compare dataframes, save data, etc.

class TestMusicScraperApp(unittest.TestCase):
    def test_123(self):
        load_save = 'default_df_save.csv'
        session_data_manager = SessionDataManager(load_save)
        discogs_search = DiscogsSearch(discogs_base_url)
        discogs_search.test_function(max_rows_to_update=3)
        session_data_manager.compare_and_update_data(discogs_search.data_handler.df)
        print('done')
        session_data_manager.save_data_to_csv()

        #app = MusicScraperApp()
        #app.start_app()
        #app.test_function(file_path="saved_data.csv")
        #app.data_handler.display_dataframe()

if __name__ == '__main__':
    unittest.main()