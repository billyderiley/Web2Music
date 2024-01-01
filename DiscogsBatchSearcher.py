from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.parse
from DataframeFilter import DataframeFilter

from DiscogsSearchScraper import DiscogsSearchScraper
from DiscogsReleaseScraper import DiscogsReleaseScraper
from ScrapeDataHandler import DataHandler


class DiscogsBatchSearcher:
    def __init__(self):
        self.base_search_url = "https://www.discogs.com/search/?"
        # Initialize other necessary attributes here
        self.Discogs_Search_Scraper = DiscogsSearchScraper()
        self.Discogs_Release_Scraper = DiscogsReleaseScraper()
        self.data_handler = DataHandler()


    def batch_search_discogs(self, search_items):
        """
        Performs a batch search on Discogs for each item in search_items.

        :param search_items: A list of tuples, each containing (u_id, artist, album).
        :return: A list of tuples, each containing (u_id, release_data).
        """
        # Use ThreadPoolExecutor for concurrent execution of searches
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Create a future object for each search item
            futures = [executor.submit(self.search_and_fetch_release, u_id, artist, album)
                       for u_id, artist, album in search_items]

            # Collect the results as they complete
            results = [future.result() for future in as_completed(futures)]

        return results

    def search_and_fetch_release(self, u_id, artist, album):
        """
        Searches for a specific album by an artist on Discogs and fetches the release details.

        :param u_id: The unique identifier for the album.
        :param artist: The artist's name.
        :param album: The album title.
        :return: A tuple containing the u_id and the release data.
        """
        # Construct the search URL using the artist and album
        search_url = self.construct_release_search_url(artist, album)
        searched_for = (artist, album)
        # Find the best match URL from the search results
        best_search_release_info = self.find_best_match(search_url, searched_for=searched_for)


        # If a best match is found, scrape the release page for detailed information
        if best_search_release_info:
            url = best_search_release_info['Discogs_Urls']
            release_data = self.Discogs_Release_Scraper.get_current_release_url_content(url)
            return (u_id, release_data, best_search_release_info, searched_for)

        # If no match is found, return the u_id with None for release data
        return None

    def construct_release_search_url(self, artist, album):
        """
        Constructs a URL for searching a specific album by an artist on Discogs.
        """
        encoded_artist = urllib.parse.quote(artist)
        encoded_album = urllib.parse.quote(album)

        search_url = f"{self.base_search_url}type=release&title={encoded_album}&artist={encoded_artist}"
        return search_url

    def find_best_match(self, search_url, searched_for: tuple):
        """
        Finds the best match for a release from the Discogs search results.
        """
        # Fetch the HTML content of the search results page
        content = self.fetch_page_content(search_url)
        if content:
            # Parse the HTML to find the best match
            best_release_info = self.parse_search_results(content, searched_for)
            return best_release_info
        else:
            return None

    def fetch_page_content(self, url):
        """
        Fetches the HTML content of a given URL.
        """
        # Logic to fetch the page content
        soup = self.Discogs_Search_Scraper.get_Soup_from_url(url)
        center_releases_content = self.Discogs_Search_Scraper.get_center_releases_content(soup)
        return center_releases_content

    def parse_search_results(self, content, searched_for: tuple):
        """
        Parses the search results page and finds the URL of the best matching release.
        """
        # Logic to parse the search results page
        artist = searched_for[0]
        album = searched_for[1]

        for release_info in content:
            artist, album, d_artist, d_title = DataframeFilter.normalize_str(
                to_normalize_strings=[artist, album, release_info['Discogs_Artists'], release_info['Discogs_Titles']])
            # Need to check if 'Discogs_Artists" is equal to artist, or similar, or contains artist
            if d_artist == artist:
                if d_title == album:
                    return release_info
            if DataframeFilter.similarity(
                    d_artist, artist) > 0.6:
                if DataframeFilter.similarity(
                        d_title, album) > 0.6:
                    return release_info
                continue


class DiscogsBatchSearch(DiscogsBatchSearcher):
    def __init__(self, data_handler):
        DiscogsBatchSearcher.__init__(self)
        if not data_handler:
            self.data_handler = DataHandler()  # Assuming DataHandler is already defined
        else:
            self.data_handler = data_handler


    def process_search_and_update(self, dataframe, search_columns: list):
        """
        Processes the entire search and update cycle.

        :param search_columns: Columns to use for searching (artist, album, etc.)
        """
        # Generate search items from the DataFrame
        search_items = self.generate_search_items(dataframe, search_columns)

        # Perform batch search
        aggregated_results = self.batch_search_discogs(search_items)

        counter = 0
        release_data_list = []
        if aggregated_results:
            for result in aggregated_results:
                u_id, release_data, best_search_release_info, searched_for = result
                counter += 1
                # Remap the table content to match the database schema
                processed_release_data = self.Discogs_Release_Scraper.process_release_data_to_dict(u_id, release_data, best_search_release_info)

                release_data_list.append(processed_release_data)
            df = self.data_handler.create_new_discogs_dataframe(release_data_list)
            #self.data_handler.save_dataframe(df, 'release_data_test.csv')
            return df


    def generate_search_items(self, dataframe, search_columns):
        """
        Generates search items from the DataFrame.

        :param search_columns: Columns to use for generating search items.
        :return: List of tuples for searching (u_id, artist, album).
        """

        df_filter = DataframeFilter(dataframe)
        search_items = df_filter.get_search_items(dataframe=dataframe ,search_columns=search_columns,  keep_unique_ids=True, master_u_ids_list=self.data_handler.master_u_id_Dataframe['u_id'].tolist())
        return search_items