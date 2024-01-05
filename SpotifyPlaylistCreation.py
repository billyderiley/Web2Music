from spotipy.oauth2 import SpotifyOAuth
from BatchAndCachedSpotipy import BatchAndCachedSpotipy
from DataframeFilter import DataframeFilter
import re
from requests.exceptions import RequestException
import time
from datetime import datetime
from SpotifyScraper import SpotifyScraper
from Audio import SpotifyPreviewPlayer
import Levenshtein as lv
import spotipy

class SpotifyPlaylistCreation(SpotifyScraper):
    def __init__(self, client_id, client_secret, redirect_uri, data_handler):
        self.sp = BatchAndCachedSpotipy(auth_manager=SpotifyOAuth(client_id=client_id,
                                                            client_secret=client_secret,
                                                            redirect_uri=redirect_uri,
                                                            scope="playlist-modify-public"))
        super().__init__(self.sp)
        self.data_handler = data_handler  # DataHandler instance


    def user_menu(self):
        #preview_player = SpotifyPreviewPlayer()  # Create an instance of SpotifyPreviewPlayer

        while True:
            self.display_dataframe_status()

            print("Spotify Playlist Creator Menu")
            print("1. Load Search CSV file")
            print("2. Load Spotify CSV file")  # New option for loading Spotify DataFrame
            print("3. Create a playlist from DataFrame")
            print("4. Create a playlist from selected artists")
            print("5. Save Spotify DataFrame to CSV")
            print("6. Exit")
            print("7. Get Spotify Metadata from DataFrame")  # Adjusted option number
            print("8. Play Previews")  # New menu item for playing previews
            choice = input("Enter your choice: ")

            if choice == '1':
                df, csv = self.handle_load_search_csv()
                self.data_handler.set_search_dataframe(df)
                self.data_handler.set_loaded_search_csv_file(csv)

            elif choice == '2':
                self.handle_load_spotify_csv()
            elif choice in ['3', '4'] and self.is_dataframe_loaded():
                self.handle_playlist_creation(choice)
            elif choice == '5':
                self.handle_save_spotify_csv()
            elif choice == '6':
                print("Exiting...")
                break
            elif choice == '7':
                self.update_dataframe_and_create_playlist(self.data_handler.Search_Dataframe, create_playlist=False, save_Spotify_Dataframe=True)
            elif choice == '8':
                self.play_previews()
            else:
                print("Invalid choice, please enter a number between 1 and 7.")

    def handle_load_spotify_csv(self):
        csv_files = self.data_handler.list_csv_files()
        if not csv_files:
            print("No CSV files found in the current directory.")
            return

        csv_files = [file for file in csv_files if "SpotifyDataframe" in file]

        print("Available Spotify CSV Files:")
        for i, file in enumerate(csv_files, 1):
            print(f"{i}. {file}")

        file_index = int(input("Enter the number of the Spotify CSV file to load: ")) - 1
        if 0 <= file_index < len(csv_files):
            print(csv_files[file_index])

            self.data_handler.load_Spotify_Dataframe(csv_files[file_index])

            #self.data_handler.set_spotify_dataframe(df)
        else:
            print("Invalid file number.")

    def load_spotify_dataframe(self, csv_file):
        """Load a CSV file into the Spotify Dataframe managed by DataHandler."""
        # Load CSV file using DataHandler and set it as the Spotify Dataframe
        self.data_handler.load_Spotify_Dataframe(csv_file)
        print(f"Spotify DataFrame loaded from {csv_file}")

    def load_search_dataframe(self, csv_file):
        """Load a CSV file into the Spotify Dataframe managed by DataHandler."""
        # Load CSV file using DataHandler and set it as the Spotify Dataframe
        self.data_handler.load_Search_Dataframe(csv_file)
        print(f"Search DataFrame loaded from {csv_file}")


    def update_dataframe_and_create_playlist(self, dataframe, create_playlist=True, save_Spotify_Dataframe=True):
        """Update the Spotify DataFrame with metadata and create a playlist."""
        if create_playlist:
            playlist_name = self.get_user_input_for_action("playlist", "Enter named for spotify playlist")
        # Example usage
        master_u_ids_list = self.data_handler.get_master_Spotify_Dataframe_u_ids_list()
        dataframe_filter = DataframeFilter(dataframe, master_u_ids_list=master_u_ids_list)
        u_id_search_items = dataframe_filter.get_search_items(dataframe,['Discogs_Artists', 'Discogs_Titles'], keep_unique_ids=True)

        # Perform batch album search
        aggregated_albums_list = self.sp.batch_album_search(search_items=u_id_search_items)
        #print(f"{len(aggregated_albums_list)} albums found out of {len(u_id_search_items)}")

        track_uris_list = []
        track_uris_set = set()
        track_metadata_list = []
        # iterate through the aggregated albums keys and get the values
        #print(f"check here")
        #print(aggregated_albums_list)
        for album_tuple in aggregated_albums_list:
            #print(album_tuple)
            u_id = album_tuple[0]
            album = album_tuple[1]
            #print(u_id, album)


            for track in album['album_tracks']:

                #print(track)
                if track['uri'] not in track_uris_set:
                    track_uris_set.add(track['uri'])
                    track_uris_list.append(track['uri'])
                    #print(f"Added track: {track['name']}")
                    track_metadata = self.get_spotify_metadata_from_track(track,u_id=u_id, album_name=album['album_name'],
                                                                      album_release_date=album['album_release_date'],
                                                                      album_artists=album['album_artists'],
                                                                      album_artist_ids=album['album_artist_ids'],
                                                                      album_id=album['album_id'])
                    track_metadata_list.append(track_metadata)
            self.data_handler.update_spotify_dataframe_with_track_metadata(track_metadata_list)
        if track_uris_list and create_playlist:
            self.create_playlist(playlist_name, track_uris_list)
        if save_Spotify_Dataframe:
            self.data_handler.save_Spotify_Dataframe(self.data_handler.loaded_search_csv_file)

        def search_spotify(self, artist, title, search_type='track', max_retries=2, delay=1):
            if "," in artist:
                artist = artist.split(",")[0]
            query = f"artist:{artist} {search_type}:{title}"
            #print(f"Searching Spotify with query: {query}")  # Debugging info

            retries = 0
            while retries < max_retries:
                try:
                    results = self.sp.search(q=query, type=search_type)
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

    def process_spotify_search_result(self, search_result, search_type):
        if search_type == 'track':
            if search_result:
                artist_id = search_result[0]['artists'][0]['id']  # Fetch artist ID from track
                return artist_id
            else:
                return None
        elif search_type == 'album':
            if search_result:
                artist_id = search_result[0]['artists'][0]['id']  # Fetch artist ID from album
                return artist_id
            else:
                return None
        else:
            return None

    """def search_spotify_backup(self, artist, title):
        query = f"artist:{artist} track:{title}"
        results = self.sp.search(q=query, type='track')
        tracks = results['tracks']['items']
        return tracks if tracks else None"""

    def create_playlist(self, name, track_uris):
        if not track_uris:
            print("No tracks to add to the playlist. Playlist creation skipped.")
            return
        user_id = self.sp.current_user()['id']
        playlist = self.sp.user_playlist_create(user_id, name)

        # Split track_uris into chunks of 100
        track_chunks = self.chunk_list(track_uris, 100)
        for chunk in track_chunks:
            self.sp.playlist_add_items(playlist['id'], chunk)

    def chunk_list(self, input_list, chunk_size):
        """Yield successive chunk_size-sized chunks from input_list."""
        for i in range(0, len(input_list), chunk_size):
            yield input_list[i:i + chunk_size]


    def similarity(self, str1, str2):
        if str1 is None or str2 is None:
            return 0  # Return 0 similarity if either string is None
        return lv.ratio(str(str1).lower(), str(str2).lower())

    def generate_single_track_playlist_from_dataframe(self):
        """Generate a playlist with single tracks from the loaded Spotify DataFrame."""
        # Ensure the Spotify DataFrame is loaded
        if self.data_handler.Spotify_Dataframe is None:
            print("Spotify DataFrame not loaded.")
            return

        playlist_name = self.get_user_input_for_action("playlist", self.data_handler.loaded_csv_file)
        track_uris = []

        # Iterate over each row in the Spotify DataFrame
        for _, row in self.data_handler.Spotify_Dataframe.iterrows():
            tracks = self.search_spotify(row['Discogs_Artists'], row['Discogs_Titles'])
            if tracks:
                # Fetch artist metrics and update the DataFrame
                artist_id = tracks[0]['artists'][0]['id']
                artist_metrics = self.get_artist_metrics(artist_id)
                artist = row['Discogs_Artists']
                self.data_handler.update_spotify_dataframe_with_artist_metrics(artist, artist_metrics)

                # Add the first track's URI to the playlist
                track_uris.append(tracks[0]['uri'])

        # Create the playlist with the collected track URIs
        self.create_playlist(playlist_name, track_uris)

    def generate_playlist_from_selected_artists(self, dataframe, create_playlist=True):
        """Create a playlist from selected artists in the loaded Spotify DataFrame."""
        # Ensure the Spotify DataFrame is loaded
        if dataframe is None:
            print("DataFrame not loaded.")
            return

        #unique_artists = self.data_handler.Spotify_Dataframe['Discogs_Artists'].unique()
        unique_artists = dataframe['Discogs_Artists'].unique()

        # Display and select artists
        for idx, artist in enumerate(unique_artists, 1):
            print(f"{idx}. {artist}")

        selected_indices = input(
            "Enter the numbers of the artists you want to select, separated by commas (e.g., 1,3,5): ")
        selected_indices = [int(i.strip()) for i in selected_indices.split(',')]

        track_uris_list = []
        track_uris_set = set()
        track_metadata_list = []

        # Iterate over selected artist indices
        for index in selected_indices:
            artist = unique_artists[index - 1]
            #titles = self.data_handler.Spotify_Dataframe[self.data_handler.Spotify_Dataframe['Discogs_Artists'] == artist][ 'Discogs_Titles']
            titles = dataframe[dataframe['Discogs_Artists'] == artist]['Discogs_Titles']

            for title in titles:
                print(f"Searching top track for {artist} - {title}...")
                top_tracks = self.search_spotify(artist, title)
                #print(top_tracks)

                if top_tracks:
                    # Fetch artist metrics and update the DataFrame
                    artist_spotify_id = top_tracks[0]['artists'][0]['id']
                    #artist_metrics = self.get_artist_metrics(artist_spotify_id)
                    #self.data_handler.update_spotify_dataframe_with_artist_metrics(artist, artist_metrics)

                    # Add unique track URIs to the playlist
                    for track in self.search_spotify_tracks_by_artist_id(artist_spotify_id):
                        if track['uri'] not in track_uris_set:
                            track_uris_set.add(track['uri'])
                            track_uris_list.append(track['uri'])
                            album_name = track['album']['name']  if 'album' in track else  'Unknown Album'
                            album_release_date = track['album']['release_date'] if 'album' in track else 'Unknown Release Date'
                            track_metadata_list.append(
                                self.get_spotify_metadata_from_track(track, album_name, album_release_date))

        # Create the playlist if there are tracks to add
        if track_uris_list:
            playlist_name = self.get_user_input_for_action("playlist", default_suggestion=artist)
            if create_playlist:
                self.create_playlist(playlist_name, track_uris_list)
            self.data_handler.update_spotify_dataframe_with_metadata(track_metadata_list)
            save_file = artist+"_"+self.data_handler.loaded_search_csv_file
            self.data_handler.save_Spotify_Dataframe(self.data_handler.loaded_search_csv_file)

        else:
            print("No tracks to add to the playlist. Playlist creation skipped.")

    """def create_playlist_from_selected_artists_backup(self):
        # Retrieve unique artist names from the DataFrame
        unique_artists = self.df['Discogs_Artists'].unique()
        for idx, artist in enumerate(unique_artists, 1):
            print(f"{idx}. {artist}")

        # Prompt user to select artists by their indices
        selected_indices = input(
            "Enter the numbers of the artists you want to select, separated by commas (e.g., 1,3,5): ")
        selected_indices = [int(i.strip()) for i in selected_indices.split(',')]

        # Initialize a list to maintain the order of track URIs and a set to check for duplicates
        track_uris_list = []
        track_uris_set = set()

        # Iterate over selected artist indices
        for index in selected_indices:
            artist = unique_artists[index - 1]  # Get artist name by index
            titles = self.df[self.df['Discogs_Artists'] == artist]['Discogs_Titles']  # Retrieve titles for the artist

            # Iterate over the titles of the selected artist
            for title in titles:
                print(f"Searching top track for {artist} - {title}...")

                # Search for the top Spotify track for the given artist and title
                top_tracks = self.search_spotify(artist, title)
                if top_tracks:

                    artist_spotify_id = top_tracks[0]['artists'][0]['id']  # Extract artist's Spotify ID

                    print(f"Searching all tracks for artist ID {artist_spotify_id}...")
                    all_tracks = self.search_spotify_tracks_by_artist_id(artist_spotify_id)
                    if all_tracks:
                        # Use inherited method to get artist metrics
                        artist_metrics = self.get_artist_metrics(artist_spotify_id)

                        # Update DataFrame with the new metrics
                        self.update_dataframe_with_artist_metrics(artist, artist_metrics)
                    # Iterate over found tracks and add unique URIs to the list and set
                    for track in all_tracks:
                        if track['uri'] not in track_uris_set:
                            track_uris_set.add(track['uri'])
                            track_uris_list.append(track['uri'])

        # Check if the list of track URIs is empty
        if not track_uris_list:
            print("No tracks to add to the playlist. Playlist creation skipped.")
            return
        else:
            # Prompt user for playlist name and create the playlist
            playlist_name = self.get_user_input_playlist_name(suggestion=artist)
            self.create_playlist(playlist_name, track_uris_list)"""

    """def create_playlist_from_selected_artists_backup(self):

        unique_artists = self.df['Discogs_Artists'].unique()
        for idx, artist in enumerate(unique_artists, 1):
            print(f"{idx}. {artist}")

        selected_indices = input(
            "Enter the numbers of the artists you want to select, separated by commas (e.g., 1,3,5): ")
        selected_indices = [int(i.strip()) for i in selected_indices.split(',')]

        track_uris = set()
        existing_tracks = []  # To store track info for comparison

        for index in selected_indices:
            artist = unique_artists[index - 1]
            titles = self.df[self.df['Discogs_Artists'] == artist]['Discogs_Titles']
            for title in titles:
                print(f"Searching top track for {artist} - {title}...")

                top_tracks = self.search_spotify(artist, title)
                if top_tracks:
                    artist_spotify_id = top_tracks[0]['artists'][0]['id']
                    print(f"Searching all tracks for artist ID {artist_spotify_id}...")
                    all_tracks = self.search_spotify_tracks_by_artist_id(artist_spotify_id)
                    print(len(all_tracks))
                    for track in all_tracks:
                        if not self.is_duplicate_track(existing_tracks, track):
                            track_uris.add(track['uri'])
                            existing_tracks.append(track)
        if not track_uris:
            print("No tracks to add to the playlist. Playlist creation skipped.")
            return
        else:
            playlist_name = self.get_user_input_playlist_name(suggestion=artist)
            self.create_playlist(playlist_name, list(track_uris))"""

    def search_spotify_tracks_by_artist_id(self, artist_id):
        track_objects = []
        albums = self.sp.artist_albums(artist_id, limit=20)
        #print(f"these are the albums found  {albums['items']}")
        while albums:
            for album in albums['items']:
                #print(album['artists'])
                if any(artist['id'] == artist_id for artist in album['artists']):
                    tracks = self.sp.album_tracks(album['id'], limit=50)
                    track_objects.extend(tracks['items'])  # Add the track objects
                elif  any(artist['id'] == '0LyfQWJT6nXafLPZqxe9Of' for artist in album['artists']):
                    #print("various found")
                    tracks = self.sp.album_tracks(album['id'], limit=50)
                    counter = 0
                    for track in tracks['items']:
                        #print(counter)
                        if track['artists'][0]['id'] == artist_id:
                            track_objects.append(track)
                            #track_objects.extend(tracks['items'])
                    #if tracks['items'][0]['artists'][0]['id'] == artist_id:
                        #print("found the artist")
                        #track_objects.extend(tracks['items'])  # Add the track objects
                   # counter += 1


            if albums['next']:
                albums = self.sp.next(albums)
            else:
                break

        return track_objects

    def create_playlist_from_all_artists(self):
        track_uris = set()
        existing_tracks = []  # To store track info for comparison

        for _, row in self.df.drop_duplicates(subset=['Discogs_Artists']).iterrows():
            artist = row['Discogs_Artists']
            title = row['Discogs_Titles']
            print(f"Searching for track: {artist} - {title}...")

            top_tracks = self.search_spotify(artist, title)
            if top_tracks:
                artist_spotify_id = top_tracks[0]['artists'][0]['id']
                print(f"Fetching tracks for artist ID: {artist_spotify_id}...")
                all_tracks = self.search_spotify_tracks_by_artist_id(artist_spotify_id)

                for track in all_tracks:
                    if not self.is_duplicate_track(existing_tracks, track):
                        track_uris.add(track['uri'])
                        existing_tracks.append(track)
            else:
                print(f"No results found for {artist} - {title}")

        self.create_playlist(self.get_user_input_playlist_name(), list(track_uris))

    def search_spotify_artist_tracks(self, artist):
        results = self.sp.search(q=f"artist:{artist}", type='track', limit=50)
        return results['tracks']['items']

    def display_dataframe_status(self):
        print("\n" + "=" * 30)
        if self.is_dataframe_loaded():
            message = f"DataFrame present (Loaded)"
        else:
            message = "No DataFrame currently loaded."
        print(f"  {message}\n" + "=" * 30)

    def is_dataframe_loaded(self):
        if self.data_handler.Search_Dataframe is None and self.data_handler.Spotify_Dataframe is None:
            return False
        elif self.data_handler.Search_Dataframe is not None or self.data_handler.Spotify_Dataframe is not None:
            return True
        else:
             return False

    def handle_load_search_csv(self):
        csv_files = self.data_handler.list_csv_files()
        if not csv_files:
            print("No CSV files found.")
            return
        return self.prompt_user_to_load_csv(csv_files)

    def prompt_user_to_load_csv(self, csv_files):
        for index, file in enumerate(csv_files, start=1):
            print(f"{index}. {file}")
        file_index = int(input("Enter the number of the CSV file to load: ")) - 1
        if 0 <= file_index < len(csv_files):
            df = self.data_handler.load_data(csv_files[file_index])
            #self.data_handler.set_loaded_search_csv_file(csv_files[file_index])
            return df, csv_files[file_index]

    def handle_playlist_creation(self, choice):
        if choice == '3':
            self.update_dataframe_and_create_playlist(self.data_handler.Search_Dataframe, create_playlist=True, save_Spotify_Dataframe=True)
        elif choice == '4':
            self.generate_playlist_from_selected_artists(self.data_handler.Search_Dataframe)

    def handle_save_spotify_csv(self):
        save_name = self.get_user_input_for_action("save file", self.data_handler.loaded_csv_file)
        self.data_handler.save_Spotify_Dataframe(save_name)

    """def user_menu(self):
        while True:
            # Check if the Spotify DataFrame is present and display an appropriate message
            if self.data_handler.Search_Dataframe is not None and not self.data_handler.Search_Dataframe.empty:
                print("\n" + "=" * 30)
                message = "Search DataFrame present\n."
                if self.data_handler.loaded_csv_file:
                    message += " (Loaded from: " + self.data_handler.loaded_csv_file + ")"
                print(f"  {message}")
                print("=" * 30)
            else:
                print("\n" + "=" * 30)
                print("  No Spotify DataFrame currently loaded.")
                print("=" * 30)

            print("Spotify Playlist Creator Menu")
            print("1. Load Search CSV file")
            print("2. Create a playlist from the entire DataFrame")
            print("3. Create a playlist from selected artists")
            print("4. Save Spotify DataFrame to CSV")
            print("5. Exit")
            choice = input("Enter your choice: ")

            if choice == '1':
                # List available CSV files and prompt user to select one
                csv_files = self.data_handler.list_csv_files()
                if not csv_files:
                    print("No CSV files found in the current directory.")
                    continue

                file_index = int(input("Enter the number of the CSV file to load: ")) - 1
                if 0 <= file_index < len(csv_files):
                    # Load the selected CSV file into the Spotify DataFrame
                    self.load_search_dataframe(csv_files[file_index])
                    self.data_handler.set_loaded_search_csv_file(csv_files[file_index])
                    #self.data_handler.loaded_csv_file = csv_files[file_index]
                else:
                    print("Invalid file number.")
            elif choice == '2':
                # Generate a playlist based on the entire loaded Spotify DataFrame
                self.generate_playlist_from_dataframe()
            elif choice == '3':
                # Create a playlist from selected artists within the loaded Spotify DataFrame
                self.generate_playlist_from_selected_artists()
            elif choice == '4':
                # Get user input for the file name to save the Spotify DataFrame
                save_name = self.get_user_input_for_action("save file", self.data_handler.loaded_csv_file)
                self.data_handler.save_Spotify_Dataframe(save_name)
            elif choice == '5':
                print("Exiting...")
                break
            else:
                print("Invalid choice, please enter a number between 1 and 5.")"""

    def normalize_track_name(self, name):
        """ Normalize track names for comparison """
        return re.sub(r'\s*\([^)]*\)', '', name.lower()).strip()  # Remove parentheses and lowercase

    def is_duplicate_track(self, existing_tracks, track):
        """ Check if a track is a duplicate based on name and duration """
        norm_name = self.normalize_track_name(track['name'])
        duration = track['duration_ms']
        for existing in existing_tracks:
            if self.normalize_track_name(existing['name']) == norm_name:
                # Example condition: durations within 5 seconds of each other
                if abs(existing['duration_ms'] - duration) < 5000:
                    return True
        return False

    def get_user_input_for_action(self, action_type, default_suggestion=None):
        """
        Get user input for a specified action (e.g., playlist name, save file name) with suggestions.

        :param action_type: A string indicating the type of action (e.g., 'playlist', 'save file')
        :param default_suggestion: A default suggestion if any (e.g., loaded file name)
        :return: User input or chosen suggestion for the action
        """
        suggestions = []
        default_name = default_suggestion if default_suggestion else f"Spotify_{action_type.capitalize()}"

        # Generate a suggestion based on the current date and time
        date_suggestion = f"Created_" + datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        default_name = f"{default_name} Discography " + datetime.now().strftime("%Y-%m-%d")
        suggestions.append(default_name)
        suggestions.append(date_suggestion)

        # Display the suggestions to the user
        for i, suggestion in enumerate(suggestions, start=1):
            print(f"{i}. {suggestion}")

        # Prompt the user for input
        user_input = input(
            f"Enter a name for your {action_type}, or a digit corresponding to a suggested name: ").strip()

        # Validate the user input
        if user_input.isdigit() and int(user_input) <= len(suggestions):
            return suggestions[int(user_input) - 1]
        elif user_input:
            return user_input
        else:
            return default_name

    def play_previews(self):

        if self.data_handler.Spotify_Dataframe.empty:
            print("Spotify DataFrame is empty. Load a DataFrame first.")
            return
        audio_player = SpotifyPreviewPlayer()
        audio_player.set_spotify_dataframe(self.data_handler.Spotify_Dataframe)
        audio_player.preview_menu()

        print("Exiting Preview Player.")

    """def get_user_input_playlist_name(self, suggestion: str = None):
        suggestions = []
        default_name = ""

        # Check if a suggestion is provided
        if suggestion:
            if suggestion.endswith('.csv'):
                suggestion = suggestion.replace('.csv', '')
                default_name = suggestion
            else:
                default_name = suggestion + " Discography"
            suggestions.append(default_name)

        # Generate a suggestion based on the current date and time
        date = datetime.datetime.now()
        suggestion = "Discogify " + date.strftime("%Y-%m-%d %H:%M:%S")
        suggestions.append(suggestion)

        # Display the suggestions to the user
        for i, suggestion in enumerate(suggestions, start=1):
            print(f"{i}. {suggestion}")

        # Prompt the user for input
        user_input = input("Enter a name for your playlist, or a digit corresponding to a suggested name: ").strip()

        # Validate the user input
        if user_input.isdigit() and int(user_input) <= len(suggestions):
            return suggestions[int(user_input) - 1]
        elif user_input:
            return user_input
        else:
            return default_name"""

    """def create_playlist_from_selected_artists_old(self):
            unique_artists = self.df['Discogs_Artists'].unique()
            for idx, artist in enumerate(unique_artists, 1):
                print(f"{idx}. {artist}")

            selected_indices = input(
                "Enter the numbers of the artists you want to select, separated by commas (e.g., 1,3,5): ")
            selected_indices = [int(i.strip()) for i in selected_indices.split(',')]

            track_uris = []
            for index in selected_indices:
                artist = unique_artists[index - 1]
                title = self.df[self.df['Discogs_Artists'] == artist]['Discogs_Titles'].iloc[
                    0]  # Get the first title for the artist
                print(f"Searching top track for {artist} - {title}...")

                # Search using artist and title, and get the top result
                top_tracks = self.search_spotify(artist, title)
                if top_tracks:
                    artist_spotify_id = top_tracks[0]['artists'][0]['id']  # Get Spotify ID of the artist
                    print(f"Searching all tracks for artist ID {artist_spotify_id}...")
                    all_tracks = self.search_spotify_tracks_by_artist_id(
                        artist_spotify_id)  # Search all tracks for this artist by Spotify ID
                    track_uris.extend([track['uri'] for track in all_tracks])

            self.create_playlist("Selected Artists Playlist", track_uris)"""

    """def search_spotify_tracks_by_artist_id_old(self, artist_id):
            track_objects = []
            albums = self.sp.artist_albums(artist_id, limit=50)

            while albums:
                album_ids = [album['id'] for album in albums['items']]
                for album_id in album_ids:
                    tracks = self.sp.album_tracks(album_id, limit=50)
                    track_objects.extend(tracks['items'])

                if albums['next']:
                    albums = self.sp.next(albums)
                else:
                    break

            return track_objects"""

    """def search_spotify_tracks_by_artist_id_old_old(self, artist_id):
        track_uris = []

        # Fetch albums by artist
        albums = self.sp.artist_albums(artist_id, album_type='album', limit=50)
        while albums:
            for album in albums['items']:
                # Fetch tracks from album
                tracks = self.sp.album_tracks(album['id'], limit=50)
                track_uris.extend([track['uri'] for track in tracks['items']])

            # Check for more albums (pagination)
            if albums['next']:
                albums = self.sp.next(albums)
            else:
                albums = None

        return track_uris"""
