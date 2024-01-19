from DiscogsSearchGUI import DiscogsSearchGUI

import threading
import queue


class Web2MusicPipeline:
    """
    Pipeline to process user prompts into data for Discogs and Spotify searches.

    Attributes:
        user_interaction (UserInteraction): Interface for user interactions.
        discogs_gui (DiscogsSearchGUI): Interface for Discogs searches.
        spotify_creator (SpotifyPlaylistCreation): Interface for Spotify playlist creation.
        task_queue (queue.Queue): Thread-safe queue for tasks.
        result_queue (queue.Queue): Thread-safe queue for results.
    """

    def __init__(self):
        self.user_interaction = UserInteraction()
        self.discogs_gui = DiscogsSearchGUI()
        self.spotify_creator = SpotifyPlaylistCreation(client_id, client_secret, redirect_uri, DataHandler())
        self.task_queue = queue.Queue()
        self.result_queue = queue.Queue()

    def start_pipeline(self):
        """
        Start the pipeline for processing user inputs and managing search tasks.
        """
        threading.Thread(target=self.process_user_input, daemon=True).start()
        threading.Thread(target=self.process_tasks, daemon=True).start()

    def process_user_input(self):
        """
        Continuously process user input and add tasks to the task queue.
        """
        while True:
            user_input = self.user_interaction.get_user_input("Enter your search query: ")
            if user_input:
                self.task_queue.put(user_input)

    def process_tasks(self):
        """
        Continuously process tasks from the task queue and add results to the result queue.
        """
        while True:
            task = self.task_queue.get()
            if task:
                result = self.process_prompt(task)
                self.result_queue.put(result)

    def process_prompt(self, prompt):
        """
        Process a given prompt to generate actionable data for Discogs and Spotify.

        Parameters:
            prompt (str): The user input prompt to be processed.

        Returns:
            dict: A dictionary with processed data for Discogs and Spotify.
        """
        # Process the prompt to generate search queries
        discogs_search_query = self.discogs_gui.process_prompt(prompt)
        spotify_search_query = self.spotify_creator.process_prompt(prompt)

        return {
            "discogs": discogs_search_query,
            "spotify": spotify_search_query
        }

    def retrieve_results(self):
        """
        Retrieve results from the result queue.

        Returns:
            list: A list of results from processed tasks.
        """
        results = []
        while not self.result_queue.empty():
            results.append(self.result_queue.get())

        return results


"""# Example usage
pipeline = Web2MusicPipeline()
pipeline.start_pipeline()"""

# In a real-world application, the main thread could periodically check for new results:
# results = pipeline.retrieve_results()
# (Process the results as needed)

from DiscogsSearchGUI import DiscogsSearchGUI

import Levenshtein as lv


class DiscogsFilterApplier:
    """
    Class to process prompts and apply appropriate filters for Discogs searches.

    Attributes:
        search_dict (dict): Dictionary containing available search options and their filters.
    """

    def __init__(self):
        self.search_dict = {}  # Initialize with actual search options and their corresponding filters

    def process_prompt(self, prompt):
        """
        Process a given prompt and extract relevant filters based on the search_dict.

        Parameters:
            prompt (str): The user input prompt to be processed.

        Returns:
            list: A list of filters based on the prompt.
        """
        words = prompt.split()
        filters = []
        for word in words:
            filter = self.find_closest_match(word)
            if filter:
                filters.append(filter)
        return filters

    def find_closest_match(self, word):
        """
        Find the closest match for a word in the search_dict keys based on similarity.

        Parameters:
            word (str): Word to find a match for.

        Returns:
            str: The closest matching filter or None if no close match is found.
        """
        max_similarity = 0.0
        closest_match = None
        for key in self.search_dict.keys():
            similarity = lv.ratio(word.lower(), key.lower())
            if similarity > max_similarity and similarity > 0.9:
                max_similarity = similarity
                closest_match = key
        return closest_match


class DiscogsPageCreator:
    """
    Class to create a Discogs search page URL based on applied filters.

    Attributes:
        base_url (str): The base URL for Discogs searches.
    """

    def __init__(self):
        self.base_url = "https://www.discogs.com/search/"

    def generate_search_page(self, filters):
        """
        Generate a Discogs search page URL based on given filters.

        Parameters:
            filters (list): A list of filters to apply.

        Returns:
            str: A URL for the Discogs search page with applied filters.
        """
        query_parts = ["?ev=em_rs"]
        for filter in filters:
            query_parts.append(f"{filter}_exact={self.search_dict[filter]}")
        query = "&".join(query_parts)
        return self.base_url + query


# Example usage
filter_applier = DiscogsFilterApplier()
prompt = "Show me some techno that has minimal and dubstep qualities"
filters = filter_applier.process_prompt(prompt)

page_creator = DiscogsPageCreator()
search_page_url = page_creator.generate_search_page(filters)
print(search_page_url)