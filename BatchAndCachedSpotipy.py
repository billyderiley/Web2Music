from DatabaseManager import DatabaseManager
from SpotifyScraper import SpotifyScraper
import spotipy
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import RequestException
import Levenshtein as lv


class BatchAndCachedSpotipy(spotipy.Spotify, SpotifyScraper):
    def __init__(self, database_manager=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_manager = DatabaseManager() if not database_manager else database_manager # DatabaseManager instance
        self.tasks_queue = []
        self.batch_size = 5
        self.last_process_time = time.time()




    def batch_album_search(self, search_items, request_type='album'):
        aggregated_uris = []
        tasks_queue = []
        # Process each search item and add to queue
        for artist, title in search_items:
            if "," in artist:
                artist = artist.split(",")[0].rstrip(" ").lstrip(" ")
            # Format the search query
            query = f"artist:{artist} {request_type}:{title}"
            self.add_to_tasks_queue(query, request_type)

        # Execute the batch search
        if self.tasks_queue:
            #aggregated_uris.extend(self.execute_in_batches_track_searches(self.track_search_queue))
            aggregated_uris = self.execute_in_batches_album_track_searches(self.tasks_queue)
            print(f"Aggregated URIs: {aggregated_uris}")
             # Clear the queue after processing
            #self.tasks_queue = []
        return aggregated_uris

    def add_to_tasks_queue(self, query, request_type):
        # Create a task tuple with args and kwargs for the search
        task = ((query,), {'type': request_type})  # args is a tuple containing the query, kwargs contains the type
        # Append the task to the search queue
        self.tasks_queue.append(task)

    def execute_in_batches_album_track_searches(self, tasks):
        aggregated_results = []
        print("Executing track searches in batches...")
        with ThreadPoolExecutor(max_workers=self.batch_size) as executor:
            # Submit each task in the batch for execution
            futures = [executor.submit(self.perform_album_track_search, *task) for task in tasks]
            completed_batches = 0

            total_batches = len(futures) // self.batch_size + (1 if len(futures) % self.batch_size != 0 else 0)
            # Process the completed tasks and aggregate results
            for future in as_completed(futures):
                result = future.result()
                print(type(result))
                if result:
                    aggregated_results.append(result)
                # aggregated_results.extend(result if result else [])
                # Update and print the completed batch count
                completed_batches += 1
                if completed_batches % self.batch_size == 0 or completed_batches == len(futures):
                    print(f"Completed batch {completed_batches // self.batch_size} of {total_batches}")
        print("All batches completed.")
        return aggregated_results

    """def execute_in_batches_track_searches(self, tasks):
        aggregated_results = []
        print("Executing track searches in batches...")
        with ThreadPoolExecutor(max_workers=self.batch_size) as executor:
            # Submit each task in the batch for execution
            futures = [executor.submit(self.perform_track_search, *task) for task in tasks]
            completed_batches = 0

            total_batches = len(futures) // self.batch_size + (1 if len(futures) % self.batch_size != 0 else 0)
            # Process the completed tasks and aggregate results
            for future in as_completed(futures):
                result = future.result()
                if result:
                    aggregated_results.append(result)
                #aggregated_results.extend(result if result else [])
                # Update and print the completed batch count
                completed_batches += 1
                if completed_batches % self.batch_size == 0 or completed_batches == len(futures):
                    print(f"Completed batch {completed_batches // self.batch_size} of {total_batches}")
        print("All batches completed.")
        return aggregated_results"""

    def search_spotify(self, artist, title, search_type='track', max_retries=2, delay=1):
        if "," in artist:
            artist = artist.split(",")[0]
        query = f"artist:{artist} {search_type}:{title}"
        print(f"Searching Spotify with query: {query}")  # Debugging info

        retries = 0
        while retries < max_retries:
            try:
                results = super().search(q=query, type=search_type)
                #print(results)

                items = results['tracks']['items'] if search_type == 'track' else results['albums']['items']
                if items:
                    return items
                else:
                    return None
            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429:  # Rate limiting
                    retry_after = int(e.headers.get('Retry-After', 1))  # Default to 1 second if header is missing
                    print(f"Rate limit reached. Retrying after {retry_after} seconds.")
                    time.sleep(retry_after)
                    continue  # Retry the request
                else:
                    print(f"Spotify API error for query {query}: {e}")
                    return None
            except RequestException as e:
                retries += 1
                if retries < max_retries:
                    print(
                        f"Network error encountered. Retrying in {delay} seconds... (Attempt {retries}/{max_retries})")
                    time.sleep(delay)
                else:
                    print(f"Network error during Spotify search after {max_retries} attempts: {e}")
                    return None
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                return None

    def perform_album_track_search(self, args, kwargs):
        track_uris_list = []
        track_uris_set = set()
        track_metadata_list = []
        # Perform the search
        # Construct a unique query identifier
        query = f"search {args} {kwargs}"
        request_type = 'search'

        # Check for existing request in the database
        """existing_response = self.db_manager.check_existing_spotify_request(query, request_type)
        if existing_response:
            return existing_response"""

        # Perform the search and save the response
        print(f"Performing track search with args: {args} and kwargs: {kwargs}")
        items = []
        response = super().search(*args, **kwargs)
        if 'tracks' in response:
            items = response['tracks']['items']
        elif 'albums' in response:
            items = response['albums']['items']
        if items:
            #self.db_manager.save_spotify_api_request(query, request_type, 'API Response', response)
            album_id = items[0]['id']
            album_tracks = super().album_tracks(album_id)['items']
            album_name = items[0]['name']
            album_release_date = items[0]['release_date']
        else:
            # alter the *args to search for a track instead of an album
            # Extract the first element and replace 'album' with 'track'
            modified_arg = args[0].replace('album', 'track')
            # Reconstruct args as a tuple with the modified argument
            args = (modified_arg,)
            kwargs['type'] = 'track'
            print(f"Performing track search with args: {args} and kwargs: {kwargs}")
            # If no album found, search for a track
            track_search_result = super().search(*args, **kwargs)
            if track_search_result:
                try:
                    album_id = track_search_result[0]['album']['id']
                except KeyError:
                    return None

                album_id = track_search_result[0]['album']['id']
                album_tracks = super().album_tracks(album_id)['items']
                album_name = track_search_result[0]['album']['name']
                album_release_date = track_search_result[0]['album']['release_date']
            else:
                # If no track found, continue to the next row in DataFrame
                return None
        #self.db_manager.save_spotify_api_request(query, request_type, 'API Response', response)

        title = args[0].split(':')[1]
        artist = args[0].split(':')[0].split('artist')[1]
        if album_name.lower() != title.lower():
            # print(f"Album name '{album_name}' does not match title exactly '{title}'.")
            # continue
            # CHECK ALBUM NAME AGAINST TITLE FOR SIMILARITY
            if self.similarity(album_name.lower(), title.lower()) < 0.6:
                # print(f"Album name '{album_name}' does not match title by 60% '{title}'.")
                # check if album_name is in title or vice versa
                if album_name.lower() in title.lower() or title.lower() in album_name.lower():
                    # print("but the album name is in the title or vice versa")
                    if artist.lower() == 'various':
                        # print("but the artist is various")
                        return None
                    else:
                        pass
                else:
                    return  None

            # Add tracks to the playlist
        for track in album_tracks:
            # Append the preview URL to the list
            # preview_url = track.get('preview_url', None)
            if track['uri'] not in track_uris_set:
                track_uris_set.add(track['uri'])
                track_uris_list.append(track['uri'])
                track_metadata_list.append(SpotifyScraper.get_spotify_metadata_from_track(track, album_name, album_release_date))

        return track_uris_list

    def similarity(self, str1, str2):
        if str1 is None or str2 is None:
            return 0  # Return 0 similarity if either string is None
        return lv.ratio(str(str1).lower(), str(str2).lower())

    def process_search_queue(self):
        with ThreadPoolExecutor(max_workers=self.batch_size) as executor:
            futures = [executor.submit(self.perform_search, args, kwargs) for args, kwargs in self.search_queue]
            for future in as_completed(futures):
                # Handle the search results
                pass
        self.search_queue = []

    def perform_api_call(self, task):
        # Extract action and arguments from the task
        action = task.get('action')
        args = task.get('args', ())
        kwargs = task.get('kwargs', {})

        # Perform the action with the provided arguments
        return action(*args, **kwargs)

    """def search(self, *args, **kwargs):
        # Construct a unique query identifier
        query = f"search {args} {kwargs}"
        request_type = 'search'

        # Check for existing request
        existing_response = self.db_manager.check_existing_spotify_request(query, request_type)
        if existing_response:
            return existing_response

        # Make API request
        response = super().search(*args, **kwargs)

        # Save the request and response to the database
        self.db_manager.save_spotify_api_request(query, request_type, 'API Response', response)

        return response

   def batch_track_search(self, search_items):
        # Process each search item and add to queue
        for artist, title in search_items:
            self.add_to_search_queue(artist, title)

        # Execute the batch search
        self.execute_in_batches_spotify_api_call(self.track_search_queue)
        self.track_search_queue = []  # Clear the queue after processing"""

    """def add_to_track_search_queue(self, artist, title):
        # Create a task for each search item
        task = {
            'action': self.perform_search,  # Assuming search_spotify is a method in CachedSpotipy
            'args': (artist, title),
            'kwargs': {}
        }
        self.search_queue.append(task)"""

    """def perform_search(self, args, kwargs):
        # Construct a unique query identifier
        query = f"search {args} {kwargs}"
        request_type = 'search'

        # Check for existing request
        existing_response = self.db_manager.check_existing_spotify_request(query, request_type)
        if existing_response:
            return existing_response

        # Make API request
        response = super().search(*args, **kwargs)

        # Save the request and response to the database
        self.db_manager.save_spotify_api_request(query, request_type, 'API Response', response)

        return response"""

    """def search(self, *args, **kwargs):
        # Construct a unique query identifier
        query = f"search {args} {kwargs}"
        request_type = 'search'

        # Check for existing request in the database
        existing_response = self.db_manager.check_existing_spotify_request(query, request_type)
        if existing_response:
            return existing_response

        # Create a task for the search
        task = {
            'action': super().search,  # Reference the original search method of spotipy
            'args': args,
            'kwargs': kwargs
        }

        # Add the task to the queue
        self.search_queue.append(task)

        # Process the queue if conditions are met
        if len(self.search_queue) >= self.batch_size or (time.time() - self.last_process_time) > 2:
            self.execute_in_batches_spotify_api_call(self.search_queue)
            self.last_process_time = time.time()
            self.search_queue = []  # Clear the queue after processing"""

    """def search(self, *args, **kwargs):
        # Construct a unique query identifier
        query = f"search {args} {kwargs}"
        request_type = 'search'

        # Check for existing request in the database
        existing_response = self.db_manager.check_existing_spotify_request(query, request_type)
        if existing_response:
            # Handle the existing response immediately
            # You might want to return this response or process it as required
            return existing_response

        # If no existing response, add the search to the queue
        self.search_queue.append((args, kwargs))

        # Process the queue if it reaches the batch size or if 2 seconds have passed
        if len(self.search_queue) >= self.batch_size or (time.time() - self.last_process_time) > 2:
            self.process_search_queue()
            self.last_process_time = time.time()"""

    """def add_to_search_queue(self, artist, title):
        # Create a task for each search item
        task = {
            'action': self.perform_search,
            'args': (artist, title),
            'kwargs': {}
        }
        self.search_queue.append(task)"""

    # Add other overridden methods as needed...


    """def execute_in_batches_spotify_api_call(self, tasks):
        with ThreadPoolExecutor(max_workers=self.batch_size) as executor:
            # Submit each task in the batch for execution
            futures = [executor.submit(self.perform_api_call, task) for task in tasks]

            # Process the completed tasks
            for future in as_completed(futures):
                result = future.result()
                # Handle the search results
                # Process the results here, such as saving them to the database or other operations

        # Clear the queue after processing
        self.search_queue = []"""
