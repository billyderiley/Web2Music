from spotipy.oauth2 import SpotifyOAuth
import spotipy
import pandas as pd
import os
import re

class SpotifyPlaylistCreation:
    def __init__(self, client_id, client_secret, redirect_uri):
        self.sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id,
                                                            client_secret=client_secret,
                                                            redirect_uri=redirect_uri,
                                                            scope="playlist-modify-public"))
        self.df = None

    def load_data(self, csv_file):
        self.df = pd.read_csv(csv_file)
        print(f"Data loaded from {csv_file}")


    def list_csv_files(self):
        print("Available CSV files:")
        csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
        for idx, file in enumerate(csv_files, 1):
            print(f"{idx}. {file}")
        return csv_files

    def search_spotify(self, artist, title):
        query = f"artist:{artist} track:{title}"
        results = self.sp.search(q=query, type='track')
        tracks = results['tracks']['items']
        return tracks if tracks else None



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

    def generate_playlist_from_dataframe(self):
        track_uris = set()
        for _, row in self.df.iterrows():
            artist = row['Discogs_Artists']
            title = row['Discogs_Titles']
            top_tracks = self.search_spotify(artist, title)

            if top_tracks:
                album_id = top_tracks[0]['album']['id']  # Get the album ID of the top track
                album_tracks = self.sp.album_tracks(album_id)['items']  # Fetch tracks of the album

                for track in album_tracks:
                    track_uris.add(track['uri'])  # Add track URIs to the set

        self.create_playlist(self.get_user_input_playlist_name(), list(track_uris))

    def generate_single_track_playlist_from_dataframe(self):
        track_uris = []
        for _, row in self.df.iterrows():
            tracks = self.search_spotify(row['Discogs_Artists'], row['Discogs_Titles'])
            if tracks:
                track_uris.append(tracks[0]['uri'])  # Assuming you want the first search result
        self.create_playlist(self.get_user_input_playlist_name(), track_uris)



    def create_playlist_from_selected_artists(self):
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

        self.create_playlist(self.get_user_input_playlist_name(), list(track_uris))


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

    def user_menu(self):
        while True:
            print("\nSpotify Playlist Creator Menu")
            print("1. Load CSV file")
            print("2. Create a playlist from the entire DataFrame")
            print("3. Create a playlist from selected artists")
            print("4. Exit")
            choice = input("Enter your choice: ")

            if choice == '1':
                csv_files = self.list_csv_files()
                if not csv_files:
                    print("No CSV files found in the current directory.")
                    continue

                file_index = int(input("Enter the number of the CSV file to load: ")) - 1
                if 0 <= file_index < len(csv_files):
                    self.load_data(csv_files[file_index])
                else:
                    print("Invalid file number.")
            elif choice == '2':
                self.generate_playlist_from_dataframe()
            elif choice == '3':
                self.create_playlist_from_selected_artists()
            elif choice == '4':
                print("Exiting...")
                break
            else:
                print("Invalid choice, please enter a number between 1 and 4.")


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

    def get_user_input_playlist_name(self):
        default_name = "My Spotify Playlist"
        user_input_name = input("Enter a name for your playlist (leave blank for default): ").strip()
        return user_input_name if user_input_name else default_name

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
