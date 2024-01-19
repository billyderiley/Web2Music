from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import urllib.parse
from DataframeFilter import DataframeFilter
from DiscogsSearchScraper import DiscogsSearchScraper
from DiscogsReleaseScraper import DiscogsReleaseScraper
from ScrapeDataHandler import DataHandler

class BatchSearcher:
    def __init__(self):
        pass

class DiscogsBatchSearcher:
    def __init__(self):
        self.base_search_url = "https://www.discogs.com/search/?"
        # Initialize other necessary attributes here
        self.Discogs_Search_Scraper = DiscogsSearchScraper()
        self.Discogs_Release_Scraper = DiscogsReleaseScraper()
        self.data_handler = DataHandler()

        self.batch_size = 5
    """
    Methods for searching the Discogs search page using filters (mechanism via altering the terms in the url)
    """
    def fetch_search_page_content(self, url):
        """
        Fetches the HTML content of a given search page (url).
        """
        self.Discogs_Search_Scraper.current_url = url

        # Logic to fetch the page content
        soup = self.Discogs_Search_Scraper.get_Soup_from_url(url)
        aside_navbar_content, applied_filters, new_applied_filters_list = self.Discogs_Search_Scraper.get_aside_navbar_content(soup)
        self.Discogs_Search_Scraper.updateAppliedFilters(applied_filters=self.Discogs_Search_Scraper.getAppliedFiltersFromUrl(self.Discogs_Search_Scraper.current_url))
        self.Discogs_Search_Scraper.sort_by = self.Discogs_Search_Scraper.get_sort_by(soup)
        selected_sort_by = self.Discogs_Search_Scraper.sort_by['Selected']
        center_releases_content = self.Discogs_Search_Scraper.get_center_releases_content(soup)
        return center_releases_content, aside_navbar_content, selected_sort_by

    """
    Using a discogs search page or release page, and batching the process via multithreading
    """
    def fetch_and_aggregate_release_content(self, urls, batch_size=5):
        """
        Fetches and aggregates release content from multiple URLs using multithreading.

        :param urls: List[str] - A list of Discogs release search URLs.
        :param batch_size: int - Maximum number of concurrent fetches to perform.
        :return: List[Dict] - A list of release_info dictionaries.
        """
        aggregated_releases_list = []
        aggregation_lock = Lock()

        def fetch_and_aggregate(url):
            try:
                center_releases_content, _, _ = self.fetch_search_page_content(url)
                with aggregation_lock:
                    for release_info in center_releases_content:
                        if not any(release['Discogs_Titles'] == release_info['Discogs_Titles'] for release in aggregated_releases_list):
                            aggregated_releases_list.append(release_info)
            except Exception as exc:
                print(f"Exception occurred for URL {url}: {exc}")

        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = [executor.submit(fetch_and_aggregate, url) for url in urls]
            for future in as_completed(futures):
                future.result()  # Ensures any exceptions are raised

        return aggregated_releases_list

    """
    Methods for searching for artist and title on Discogs search page
    """
    def batch_search_releases_discogs(self, search_items):
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
        center_releases_content, aside_navbar_content, sort_by_dict = self.fetch_search_page_content(search_url)
        if center_releases_content:
            # Parse the HTML to find the best match
            best_release_info = self.parse_search_results(center_releases_content, searched_for)
            return best_release_info
        else:
            return None


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
    """
    End to end process for creating a new dataframe from using search 
    filters and page numbers
    """
    def process_search_page_filters_and_update(self, search_urls):
        """
        Fetches and aggregates release content based on search page filters and updates the Search DataFrame.

        :param search_urls: List[str] - A list of Discogs URLs representing search pages to be processed.
        :return: DataFrame - The updated Search DataFrame.
        """
        print(f" the following urls will be processed: {search_urls}")
        # Fetch and aggregate release content
        aggregated_releases = self.fetch_and_aggregate_release_content(search_urls)

        #aggregated_releases = self.data_handler_generate_u_ids_per_album(aggregated_releases)

        # Initialize an empty DataFrame
        search_df = DataHandler.create_new_discogs_dataframe(aggregated_releases)
        self.data_handler.set_search_dataframe(search_df)


    """
    End to end process for passing an existing dataframe, and searching discogs 
    for titles by passing the values from the given columns in the dataframe.
    """
    def process_search_and_update(self, dataframe, search_columns: list):
        """
        Processes the entire search and update cycle.

        :param search_columns: Columns to use for searching (artist, album, etc.)
        """
        # Generate search items from the DataFrame
        search_items = self.generate_search_items(dataframe, search_columns)

        # Perform batch search
        aggregated_results = self.batch_search_releases_discogs(search_items)

        counter = 0
        release_data_list = []
        if aggregated_results:
            for result in aggregated_results:
                u_id, release_data, best_search_release_info, searched_for = result
                counter += 1
                # Remap the table content to match the database schema
                processed_release_data = self.Discogs_Release_Scraper.process_release_data_to_dict(u_id, release_data, best_search_release_info)

                release_data_list.append(processed_release_data)
            df = self.Discogs_Release_Scraper.create_Release_Dataframe(release_data_list)
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




class ReleaseBatchSearcher(DiscogsBatchSearcher):
    def __init__(self, data_handler):
        DiscogsBatchSearcher.__init__(self)
        self.data_handler = data_handler

    def process_release_search_queue_from_dataframe(self, filtered_df, batch_size=5):
        # Prepare index and URL pairs for batch search
        index_url_pairs = [(index, row['Discogs_Urls']) for index, row in filtered_df.iterrows() if
                           'Discogs_Urls' in row]

        # Perform batch search and get results
        index_release_data_mapping = self.process_release_urls_and_update(index_url_pairs, batch_size=batch_size)
        # Update the original DataFrame with the fetched release data
        for index, release_data in index_release_data_mapping.items():
            if release_data:
                self.data_handler.add_new_release_to_dataframe(index, release_data)


    def process_release_urls_and_update(self, index_url_pairs, batch_size=5):
        # Create a ReleaseBatchSearcher instance and perform batch search
        index_release_data_mapping = self.batch_search_releases(index_url_pairs, batch_size=batch_size)
        return index_release_data_mapping

    def batch_search_releases(self, index_url_pairs, batch_size=5):
        """
        Performs a batch search on Discogs for release URLs along with their respective DataFrame index.

        :param index_url_pairs: A list of tuples, each containing (index, Discogs release URL).
        :return: A dictionary mapping each index to its corresponding release_data result.
        """
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            # Create a future object for each index and release URL pair
            future_to_index = {executor.submit(self.fetch_release_data, url): index for index, url in index_url_pairs}

            # Collect the results as they complete
            index_release_data_mapping = {}
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    release_info_dict = future.result()
                    index_release_data_mapping[index] = release_info_dict
                except Exception as exc:
                    print(f"An error occurred for index {index}: {exc}")

        return index_release_data_mapping


    def fetch_release_data(self, release_url):
        """
        Fetches release data for a specific Discogs release URL.

        :param release_url: The Discogs release URL.
        :return: Release data.
        """
        # Fetch release data from the release URL
        release_info_dict = self.Discogs_Release_Scraper.get_current_release_url_content(release_url)
        return release_info_dict




    """
    Backups
    """

    def fetch_and_aggregate_release_content_Backup(self, urls):
        """
        Fetches and aggregates release content from multiple URLs.

        :param urls: A list of Discogs release search URLs.
        :return: A list of release_info dictionaries.
        """
        aggregated_releases_list = []

        for url in urls:
            center_releases_content, _, _ = self.fetch_search_page_content(url)

            for release_info in center_releases_content:
                # Check if the title already exists in the aggregated list
                if not any(release['Discogs_Titles'] == release_info['Discogs_Titles'] for release in
                           aggregated_releases_list):
                    aggregated_releases_list.append(release_info)

        return aggregated_releases_list


    def process_release_urls_and_update_backup(self, release_urls):
        # Create a ReleaseBatchSearcher instance and perform batch search
        aggregated_results = self.batch_search_releases(release_urls)
        return aggregated_results


    def batch_search_releases_backup(self, release_urls, batch_size=5):
        """
        Performs a batch search on Discogs for release URLs.

        :param release_urls: A list of Discogs release URLs.
        :return: A list of release_data results.
        """
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            # Create a future object for each release URL
            futures = [executor.submit(self.fetch_release_data, release_url)
                       for release_url in release_urls]

            # Collect the results as they complete
            results = [future.result() for future in as_completed(futures)]

        return results

    def process_release_search_queue_from_dataframe_backup(self, rows_to_search):
        release_urlz = []
        for index, row in rows_to_search:
            if 'Discogs_Urls' in row:
                release_urlz.append(row['Discogs_Urls'])
            else:
                print("No 'Discogs_Urls' key in the row.")
        release_urls = [row[1]['Discogs_Urls'] for row in rows_to_search]

        aggregated_results = self.process_release_urls_and_update(release_urls)

        # Prepare a dictionary with the index and corresponding release data
        index_release_data_mapping = {index: release_data for index, release_data in enumerate(aggregated_results)}

        for index, row in rows_to_search:
            # Add each release data to the DataFrame using the corresponding index
            release_data = index_release_data_mapping.get(index)
            if release_data:
                self.data_handler.add_new_release_to_dataframe(index, release_data)


    def process_release_search_queue_from_dataframe_Backup2(self, rows_to_search):
        # Prepare index and URL pairs for batch search
        index_url_pairs = []
        for index, row in rows_to_search:
            if 'Discogs_Urls' in row:
                index_url_pairs.append((index, row['Discogs_Urls']))
            else:
                print(f"No 'Discogs_Urls' key in the row at index {index}.")

        # Perform batch search and get results
        index_release_data_mapping = self.process_release_urls_and_update(index_url_pairs)

        # Update the DataFrame with the fetched release data
        for index, _ in rows_to_search:
            release_info_dict = index_release_data_mapping.get(index)
            if release_info_dict:
                release_data = release_info_dict['table_release_content']
                self.data_handler.add_new_release_to_dataframe(index, release_data)

    def execute_search_urls_in_batches(self, urls, action, batch_size=5):
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            # Submit each URL to the executor
            future_to_url = {executor.submit(action, url): url for url in urls}

            # Process the results as they complete
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    future.result()  # This is where the action function is executed
                    # print(f"Loaded webpage: {future.result()}")  # Print the URL returned by the action function
                except Exception as exc:
                    print(f"{url} generated an exception: {exc}")
