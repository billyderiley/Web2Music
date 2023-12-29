from DiscogsSearchScraper import DiscogsSearchScraper
from DiscogsReleaseScraper import DiscogsReleaseScraper
from ScrapeDataHandler import DataHandler
from SpotifyPlaylistCreation import SpotifyPlaylistCreation
from DataframeFilter import DataframeFilter
from UserInteraction import UserInteraction

import time

import re

client_id = '2ea9899462614265a2b26b43c68cf72a'
client_secret = 'fb29534f29134f618623b02cfe8dbc65'
redirect_uri = 'https://open.spotify.com'

class DiscogsSearchGUI(DiscogsSearchScraper):
    def __init__(self, start_url = None, data_handler=None):
        super().__init__(start_url)
        if start_url is None:
            start_url = self.base_discogs_search_url
        else:
            self.start_url = start_url
        #self.start_up_search()
        self.user_interaction = UserInteraction()

        # self.applied_filters = applied_filters
        # super(DiscogsSearch, self).get_search_page_content(start_url)

        # self.update_center_releases_content(center_releases_content)
        # self.search_page_user_interaction()
        if data_handler is not None:
            self.data_handler = data_handler
        else:
            self.data_handler = DataHandler()

    def user_interaction_load_search_dataframe(self):
        csv_files = self.data_handler.list_csv_files()
        selected_file = self.user_interaction.select_csv_file(csv_files)
        if selected_file:
            search_df = self.data_handler.load_data(selected_file)
            self.data_handler.set_search_dataframe(search_df)
            self.data_handler.set_loaded_search_csv_file(selected_file)
            print(f"Search DataFrame loaded from {selected_file}")

    def user_interaction_load_search_dataframe_backup(self):
        # List available CSV files and prompt user to select one
        csv_files = self.data_handler.list_csv_files()
        if not csv_files:
            print("No CSV files found in the current directory.")
        file_index = int(input("Enter the number of the CSV file to load: ")) - 1
        if 0 <= file_index < len(csv_files):
            """Load a CSV file into the Spotify Dataframe managed by DataHandler."""
            # Load CSV file using DataHandler and set it as the Spotify Dataframe
            search_df = self.data_handler.load_data(csv_files[file_index])
            self.data_handler.set_search_dataframe(search_df)
            self.data_handler.set_loaded_search_csv_file(csv_files[file_index])
            print(f"Search DataFrame loaded from {csv_files[file_index]}")
        else:
            print("Invalid file number.")



    def start_up_search(self):
        self.current_url = self.start_url
        print(self.current_url)
        self.navigate_to_search_url(self.current_url, update_dataframes=False)
        # self.updateAppliedFilters(applied_filters=self.getAppliedFiltersFromUrl(self.current_url))
        # self.fetchCurrentSearchPageContent()
        # self.aside_navbar_content, self.center_releases_content, __applied_filters, __new_applied_filters_list \
        #    = self.get_current_search_page_content()
        # self.updateSearchOptions()

    def navigate_to_search_url(self, url, update_dataframes=True):
        self.current_url = url
        self.updateAppliedFilters(applied_filters=self.getAppliedFiltersFromUrl(self.current_url))
        #self.aside_navbar_content, self.center_releases_content, __applied_filters, __new_applied_filters_list = self.get_current_search_page_content()
        self.search_url_content_dict = self.get_search_url_content_dict()
        self.updateSearchOptions(aside_navbar_content=self.search_url_content_dict['aside_navbar_content'])
        self.updateCenterReleasesContent(self.search_url_content_dict['center_releases_content'])
        self.updateSortByDict(self.search_url_content_dict['sort_by_dict'])
        if update_dataframes is True:
            self.updateDataFrame()

        # self.updateCurrentPageAndNextPage()
        # self.fetchCurrentSearchPageContent()

    def updateSearchOptions(self, aside_navbar_content):
        self.search_options_dict = aside_navbar_content

    def updateCenterReleasesContent(self, center_releases_content):
        self.center_releases_content = center_releases_content

    def updateDataFrame(self):
        self.data_handler.update_search_dataframe(self.center_releases_content)

    def updateSortByDict(self, sort_by_dict):
        self.sort_by_dict = sort_by_dict

    def user_interaction_menu(self):
        # Initialize user input variable
        u_i = ''

        # Mapping of user input to corresponding functions
        switch = {
            '1': self.user_interaction_add_filters,
            '2': self.user_interaction_update_sort_by,
            '3': self.user_interaction_display_dataframe,
            '4': self.user_interaction_release_dataframe_deep_search,
            '5': self.updateDataFrame,
            '6': self.user_interaction_save_dataframe,
            '7': self.user_interaction_select_pages,
            '8': self.user_interaction_view_applied_filters,
            '9': self.user_interaction_remove_filters,
            '10': self.user_interaction_create_spotify_playlist,
            '11': self.user_interaction_load_search_dataframe,  # New function for loading the search DataFrame
            '12': self.user_interaction_reload_all_webpages,  # New method for reloading webpages
            '13': self.user_interaction_filter_dataframe
        }

        # Main loop for user interaction
        while u_i != "Q":
            # Check if the Search DataFrame is loaded and display its status
            if self.data_handler.Search_Dataframe is not None and not self.data_handler.Search_Dataframe.empty:
                print("\n" + "=" * 30)
                message = "Search DataFrame present."
                # If a CSV file was used to load the DataFrame, display its name
                if self.data_handler.loaded_search_csv_file:
                    message += " (Loaded from: " + self.data_handler.loaded_search_csv_file + ")"
                print(f"  {message}")
                print("=" * 30)
            else:
                # Message when no Search DataFrame is loaded
                print("\n" + "=" * 30)
                print("  No Search DataFrame currently loaded.")
                print("=" * 30)

            # Display menu options to the user
            print("1: Apply Filters Page\n"
                  "2: Update Sort By\n"
                  "3: Display DataFrame\n"
                  "4: Do Release Info Deep Search\n"
                  "5: Update DataFrame\n"
                  "6: Save DataFrame to CSV\n"
                  "7: Scrape Pages\n"
                  "8: View Applied Filters\n"
                  "9: Remove Applied Filters\n"
                  "10: Create Spotify Playlist from Search Results\n"
                  "11: Load Search DataFrame\n"
                  "12: Reload Search Pages\n"
                  "13: Filter DataFrame\n")
            #u_i = input("Enter Q to Quit, or any other key to continue: ")
            u_i = self.user_interaction.get_user_input("Enter Q to Quit, or any other key to continue: ")

            # Execute the function based on user input
            func = switch.get(u_i)
            if func:
                func()
            else:
                # Handle invalid choices
                print('Invalid choice. Please enter a number between 1 and 13, or Q to quit.')

    def user_interaction_add_filters(self):
        if self.current_url == self.start_url:
            #print(self.current_url)
            self.start_up_search()

        print(
            f"Applied filters: {[applied_filter for i, applied_filter in reversed(list(enumerate(self.applied_filters, 1)))]} ")
        print([f"{i}: {label_type}" for i, label_type in enumerate(self.search_dict_get_label_type_keys(), 1)])
        if len(self.search_dict_get_label_type_keys()) == 0:
            return
        enter_key1 = int(input("")) - 1  # -1 to get index number
        label_type, values = list(self.search_dict_get_label_type_items())[enter_key1]
        print([f"{i}: {label_data}" for i, label_data in
               reversed(list(enumerate(self.search_dict_get_label_url_keys(label_type), 1)))])
        enter_key2 = int(input("")) - 1
        new_search_term_1 = self.search_dict_get_search_term(label_type, enter_key2)
        new_discogs_search_url = self.base_discogs_url + new_search_term_1

        self.navigate_to_search_url(new_discogs_search_url)

    """def user_interaction_update_sort_by(self):
        if self.current_url == self.start_url:
            self.start_up_search()
        print([f"{i}: {sort_by}" for i, sort_by in
               reversed(list(enumerate(self.sort_by_dict.keys(), 1)))])
        enter_key2 = int(input("")) - 1
        new_sort_by = self.sort_by_dict.get(list(self.sort_by_dict.keys())[enter_key2])
        print(new_sort_by)
        print('testing')
        new_discogs_search_url = self.current_url + new_sort_by
        print(new_discogs_search_url)
        self.navigate_to_search_url(new_discogs_search_url)"""

    def user_interaction_update_sort_by(self):
        if self.current_url == self.start_url:
            self.start_up_search()

        # Print the selected option
        selected_option = self.sort_by_dict.get('Selected', 'None Selected')
        print(f"Selected: {selected_option}")

        # Print the options with numbers
        options = self.sort_by_dict.get('Options', {})
        for i, (key, value) in enumerate(options.items(), start=1):
            print(f"{i}: {key}")

        # Get user input
        user_choice = int(input("Choose an option number: ")) - 1

        # Retrieve the selected sort-by value based on user's choice
        if user_choice in range(len(options)):
            chosen_sort_by = list(options.values())[user_choice]
            #new_sort_by = f"sort={chosen_sort_by}"
            print(f"You selected: {chosen_sort_by}")

            new_discogs_search_url = self.get_sorted_url(self.current_url, chosen_sort_by)
            print(f"New URL: {new_discogs_search_url}")
            self.navigate_to_search_url(new_discogs_search_url)
        else:
            print("Invalid selection. Please choose a valid option.")


    """def get_sorted_url(self, current_url, selected_sort_by_value, new_sort_by):
        # Combine to form the old sort_by term in the URL
        #old_sort_by_term = f"?sort={selected_sort_by_value}"
       # print("now here")
        #print(current_url)
        #cleaned_url = current_url.strip(old_sort_by_term)
        #print(cleaned_url)
        # Remove the existing sort_by term
        #cleaned_url = re.sub(r'\?sort=[^&]*', '', current_url)
        # use regex to find the characters appearing after sort= and replace them with the selected sort_by value


        cleaned_url = re.sub(r'\?sort=[^&]*', '', current_url)
        #print(cleaned_url)

        # Add the new sort_by term
        new_discogs_search_url = cleaned_url +'?'+ new_sort_by if cleaned_url.endswith(
            '/') else cleaned_url + '&' + new_sort_by

        return new_discogs_search_url"""

    def user_interaction_remove_filters(self):
        # Display applied filters starting from the second item (index 1)
        if len(self.applied_filters) >= 2:
            # Using dictionary comprehension to display every second item starting from index 1
            displayed_filters = {i // 2 + 1: self.applied_filters[i] for i in
                                 range(0, len(self.applied_filters), 2)}
            print(f"Applied filters: {displayed_filters}")
        else:
            print(f"Applied filters: {self.applied_filters}")

        if len(self.search_dict_get_label_type_keys()) == 0:
            return

        remove_index = int(input("Enter index to remove: ")) - 1
        # Adjust the remove_index to point to the correct element in applied_filters
        adjusted_remove_index = remove_index * 2 + 1

        # Check if adjusted_remove_index is valid
        if 0 <= adjusted_remove_index < len(self.applied_filters):
            # Remove the element and its preceding element
            self.applied_filters = self.remove_applied_filter(self.applied_filters, adjusted_remove_index)
            url = self.getUrlFromAppliedFilters(self.applied_filters)
            self.navigate_to_search_url(url)
        else:
            print("Invalid index")

    # Helper function to remove element and its previous element
    def remove_applied_filter(self, filters, index):
        if index > 0 and index < len(filters):
            del filters[index - 1:index + 1]
        return filters

    def test_function(self, max_rows_to_update=None):
        enter_key1 = '2'
        print(f"page range is {enter_key1}")
        search_pages = self.get_page_range(self.current_url, enter_key1)
        #TEST_URL = 'https://www.discogs.com/search/'
        for page in search_pages:
            print(page)
            self.navigate_to_search_url(page)
            self.updateDataFrame()
            #print(self.display_Search_Dataframedata())
        self.save_Search_Dataframe(path='test_dataframe.csv')
        # max_rows = 3
        # print(" max number of rows is 5")
        #self.data_handler.fill_in_missing_data(max_rows_to_update=max_rows_to_update)
        # self.data_handler.display_dataframe()
        # self.saveDataFrameToCSV(save_as_file_name="saved_data.csv")

    def user_interaction_select_pages(self, update_applied_filters=False):
        # self.current_url
        results_per_page = self.get_results_per_search_page(self.current_url)
        number_of_search_pages = self.get_number_of_search_pages(self.current_url)
        max_page_number = int(number_of_search_pages.split(' ')[-1].replace(",", "")) // int(results_per_page)+1
        print(f"Results per page is {results_per_page}")
        print(f"Number of results you can search is {number_of_search_pages}")
        print(f"You can search up to page {max_page_number}")
        page_number_input = input("Enter a page number, or range (number seperated by a space)")
        search_pages = self.get_page_range(self.current_url, page_number_input, max_page_number)
        # Use execute_in_batches to process each URL in search_pages
        self.execute_in_batches(urls=search_pages, action=self.navigate_to_search_url)

        """for search_page_url in search_pages:
            time.sleep(0.5)
            print(f"Navigating to : \n {search_page_url}")
            self.navigate_to_search_url(search_page_url)
            self.updateDataFrame()"""

    def user_interaction_view_applied_filters(self):
        print(
            f"Applied filters: {[applied_filter for i, applied_filter in reversed(list(enumerate(self.applied_filters, 1)))]}")

    def user_interaction_release_dataframe_deep_search(self):
        if self.data_handler.Search_Dataframe is None:
            print("No Search DataFrame loaded.")
            return

        # initialize DiscogsReleaseScraper object with DataHandler instance
        self.data_handler.get_release_dataframe_from_search_dataframe()
        self.data_handler.save_Release_Dataframe(path=self.data_handler.loaded_search_csv_file)

    def user_interaction_filter_dataframe(self):
        #path = '/Users/Billy/Documents/Documents – Billy’s MacBook Pro (2)/Projects/Web2Music/HardcoreReggaeton.csv_ReleaseDataframe.csv'
        #self.data_handler.set_release_dataframe(self.data_handler.load_data(path))
        #self.data_handler.l
        # Display columns and get user choice
       # if self.data_handler.Release_Dataframe is None:
        #    print("No Release DataFrame loaded.")
        #    return
        dataframe = self.data_handler.Search_Dataframe
        DataframeFilter_obj = DataframeFilter(dataframe=dataframe)
        DataframeFilter_obj.dataframe_alteration_menu()
        self.data_handler.set_release_dataframe(DataframeFilter_obj.filtered_dataframe)
        self.data_handler.set_search_dataframe(DataframeFilter_obj.filtered_dataframe)






        #max_rows = int(input("Enter the number of rows to fill in: "))
       # if max_rows == '':
       #     max_rows = None
       # self.data_handler.fill_in_missing_data(max_rows_to_update=max_rows)

    def user_interaction_display_dataframe(self):
        self.data_handler.display_Search_Dataframedata()

    def user_interaction_save_dataframe(self):
        default_save_name = self.data_handler.loaded_search_csv_file
        prompt_message = f"Enter a filename to save the DataFrame, or press Enter to use the default ('{default_save_name}'):"
        save_name = self.user_interaction.get_user_input(prompt_message)
        self.data_handler.save_Search_Dataframe(save_name)
        print(f"DataFrame saved as: {save_name}")
        # enter 1 to save
       # if save_name is None:
       #     save_name = input("Enter the name of the file to save as: ")
        #self.data_handler.save_Search_Dataframe(save_name)
       # self.data_handler.save_dataframe(self.data_handler.Search_Dataframe, save_name)

        # self.updateCurrentPageAndNextPage()
        # print(f"now updating applied filters with this {new_applied_filters_list}")
        # self.updateAppliedFilters(new_applied_filters_list)
        # if update_applied_filters is True:
        # self.updateAppliedFilters(new_applied_filters_list)
        # self.getUrlFromAppliedFilters(self.applied_filters)
        # print(self.get_number_of_search_pages(new_discogs_search_url))
        # print(self.get_current_page_from_url(new_discogs_search_url))
        # print(self.get_next_search_page_url(new_discogs_search_url))
        # self.data_handler.update_dataframe(center_releases_content)
        # self.update_center_releases_content(center_releases_content)

    def user_interaction_create_spotify_playlist(self):
        # Initialize SpotifyPlaylistCreation with necessary Spotify credentials and DataHandler instance
        SpotifyPlaylistCreation_obj = SpotifyPlaylistCreation(client_id=client_id,
                                              client_secret=client_secret,
                                              redirect_uri=redirect_uri,
                                              data_handler=self.data_handler)

        # If there is a search DataFrame available, set it as the Spotify DataFrame in DataHandler
        if self.data_handler.Search_Dataframe is not None:
            SpotifyPlaylistCreation_obj.data_handler.set_search_dataframe(self.data_handler.Search_Dataframe)

        # Call the user menu of SpotifyPlaylistCreation for further actions
        SpotifyPlaylistCreation_obj.user_menu()


    def user_interaction_reload_all_webpages(self):
        print("Reloading all webpages. This might take some time...")
        self.reload_all_webpages(batch_size=5)  # You can adjust the batch size
        print("All webpages reloaded successfully.")

    """def user_interaction_create_spotify_playlist_backup(self):
        spotify_api = SpotifyPlaylistCreation(client_id=client_id,
                                              client_secret=client_secret,
                                              redirect_uri=redirect_uri)

        spotify_api.df = self.data_handler.Search_Dataframe  # Pass DataFrame to SpotifyAPI
        spotify_api.user_menu()  # Call the SpotifyAPI user menu for further actions"""


    def remove_discogs_search_term(self, remove_search_term):
        new_discogs_search_url = self.current_url.strip(remove_search_term)
        return new_discogs_search_url
