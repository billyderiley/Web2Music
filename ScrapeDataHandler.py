from SpotifyScraper import SpotifyScraper
import pandas as pd
import os
from datetime import datetime
import difflib
from Master_u_ID_Universe import MasterUIDUniverse
import logging

class DataHandler(MasterUIDUniverse):

    @staticmethod
    def save_dataframe(dataframe, save_as_file_name):
        dataframe.to_csv(path_or_buf=save_as_file_name, index=False)

    @staticmethod
    def load_data(csv_file):
        df = pd.read_csv(csv_file)
        print(f"Data loaded from {csv_file}")
        return df

    @staticmethod
    def list_csv_files():
        print("Available CSV files:")
        csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]

        # Sort files by modification time (most recent first)
        csv_files.sort(key=lambda x: os.path.getmtime(x), reverse=False)

        for idx, file in enumerate(csv_files, 1):
            print(f"{idx}. {file} (Last Modified: {os.path.getmtime(file)})")
        return csv_files

    def __init__(self, Search_Dataframe = None, Release_Dataframe = None, Spotify_Dataframe = None,
                 loaded_search_csv_file = None,  loaded_spotify_csv_file = None):
        super().__init__()
        self.Search_Dataframe = Search_Dataframe
        self.Release_Dataframe = Release_Dataframe
        self.Spotify_Dataframe = Spotify_Dataframe
        self.loaded_search_csv_file = loaded_search_csv_file
        self.loaded_spotify_csv_file = loaded_spotify_csv_file

        # Not sure if this is needed
        self.Track_Release_Dataframe = None

        self.master_u_id_Dataframe_path = "master_u_id_Dataframe.csv"
        self.master_u_id_Dataframe = (
            self.loadfromCSV(self.master_u_id_Dataframe_path)) \
            if os.path.exists(self.master_u_id_Dataframe_path) \
            else self.create_new_u_id_dataframe()

        self.master_Spotify_Dataframe_path = "master_Spotify_Dataframe.csv"
        self.master_Spotify_Dataframe = (
            self.loadfromCSV(self.master_Spotify_Dataframe_path)) \
            if os.path.exists(self.master_Spotify_Dataframe_path) \
            else self.create_new_spotify_dataframe()

        self.master_Discogs_Dataframe_path = "master_Discogs_Dataframe.csv"
        self.master_Discogs_Dataframe = (
            self.loadfromCSV(self.master_Discogs_Dataframe_path)) \
            if os.path.exists(self.master_Discogs_Dataframe_path) \
            else DataHandler.create_new_discogs_dataframe()

    def loadfromCSV(self, csv_file):
        try:
            return pd.read_csv(csv_file)
        except FileNotFoundError:
            return None

    def load_Search_Dataframe(self, csv_file):
        self.Search_Dataframe = self.loadfromCSV(csv_file)
        self.set_loaded_search_csv_file(csv_file)
        return self.Search_Dataframe

    def load_Spotify_Dataframe(self, csv_file):
        self.Spotify_Dataframe = self.loadfromCSV(csv_file)
        self.set_loaded_spotify_csv_file(csv_file)
        return self.Spotify_Dataframe

    def save_dataframe(self, dataframe, path):
        dataframe.to_csv(path, index=False)
        print(f"DataFrame saved to {path}")

    """
    Spotify Dataframes
    """

    def create_new_spotify_dataframe(self, data=None):
        """Create an empty Spotify DataFrame."""
        spotify_dataframe = pd.DataFrame(data if data else None,
        columns= ['u_id', 'Spotify_Track_ID','Spotify_Track_Name','Spotify_Track_Artist',
                   'Spotify_Album_Name', 'Spotify_Track_Artist_ID',
                  'Spotify_Track_Duration','Spotify_Track_Preview_Url', 'Spotify_Track_Number',
                  'Spotify_Album_ID', 'Spotify_Album_Artists'
            ,'Spotify_Album_Artist_IDs', 'Spotify_Album_Release_Date'])
        return spotify_dataframe

    def set_loaded_spotify_csv_file(self, csv_file):
        """Set the loaded CSV file."""
        self.loaded_spotify_csv_file = csv_file

    def set_spotify_dataframe(self, df):
        """Set the Spotify DataFrame."""
        self.Spotify_Dataframe = df

    def update_spotify_dataframes_with_album_metadata(self, album_metadata):
        if self.Spotify_Dataframe is None:
            self.Spotify_Dataframe = self.create_new_spotify_dataframe()
        # Check if the album metadata is a list
        if isinstance(album_metadata, list):
            # Iterate through the list of album metadata
            for album in album_metadata:
                # Get the album's tracks
                album_tracks = album.get('album_tracks')
                album_id = album.get('album_id')
                album_name = album.get('name')
                album_release_date = album.get('album_release_date')
                album_artists = album.get('album_artists')
                album_artist_ids = album.get('album_artist_ids')

                # Iterate through the tracks
                for track in album_tracks:
                    # Get the track's metadata
                    track_metadata_dict = SpotifyScraper.get_spotify_metadata_from_track(track, album_id=album_id, album_name=album_name,
                                                                          album_release_date=album_release_date, album_artists=album_artists, album_artist_ids=album_artist_ids)
                    # Update the Spotify DataFrame with the track metadata
                    self.update_spotify_dataframe_with_track_metadata([track_metadata_dict])
        else:
            raise ValueError("album_metadata must be a list")

    def update_spotify_dataframe_with_track_metadata(self,track_metadata_dict):
        # Check if a Spotify DataFrame exists and if not, create one
        if self.Spotify_Dataframe is None:
            self.Spotify_Dataframe = self.create_new_spotify_dataframe()

        # Define the columns to check for duplicates
        check_columns = ['u_id', 'Spotify_Track_ID', 'Spotify_Track_Name', 'Spotify_Album_Name', 'Spotify_Track_Artist', 'Spotify_Track_Duration']

        # Create a DataFrame from the list of metadata dictionaries
        new_rows_df = pd.DataFrame(track_metadata_dict)

        # Filter out rows that already exist in Spotify_Dataframe
        filtered_new_rows_df = new_rows_df[~new_rows_df.apply(
            lambda row: self.row_exists_in_spotify_df(row, check_columns), axis=1
        )]

        # Concatenate the new rows with the existing DataFrame
        self.Spotify_Dataframe = pd.concat([self.Spotify_Dataframe, filtered_new_rows_df], ignore_index=True)

    def row_exists_in_spotify_df(self, row, check_columns):
        if self.Spotify_Dataframe.empty:
            return False

        # Check each row in Spotify_Dataframe for a match
        for _, existing_row in self.Spotify_Dataframe.iterrows():
            if all(existing_row[col] == row[col] for col in check_columns if col in row):
                return True

        return False

    def update_spotify_dataframe_with_artist_metrics(self, artist, artist_metrics):
        """Update the Spotify DataFrame with artist's Spotify popularity and followers."""
        if 'Spotify_Popularity_Followers' not in self.Spotify_Dataframe.columns:
            self.Spotify_Dataframe['Spotify_Popularity_Followers'] = None

        popularity, followers = artist_metrics
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Update the DataFrame with the artist metrics and the current timestamp
        self.Spotify_Dataframe.loc[self.Spotify_Dataframe[
                                       'Discogs_Artists'] == artist, 'Spotify_Popularity_Followers'] = f"{popularity}, {followers}, {current_time}"

    def save_Spotify_Dataframe(self, path, update_master_Spotify_Dataframe=True):
        """Save the Spotify DataFrame to a CSV file."""
        # Ensure the file name ends with .csv

        if 'SpotifyDataframe' not in path:
            # a str representation of the current date
            current_date = datetime.now().strftime("%Y-%m-%d")
            path += '_SpotifyDataframe'
        if not path.endswith('.csv'):
            path += '.csv'
        # Check if the Spotify DataFrame is not empty
        if self.Spotify_Dataframe is not None:
            # Save the DataFrame to the specified path
            self.Spotify_Dataframe.to_csv(path_or_buf=path, index=False)
            self.set_loaded_spotify_csv_file(path) # Set the loaded CSV file
            print(f"Spotify DataFrame saved to {path}")
            if update_master_Spotify_Dataframe:
                self.update_and_save_master_Spotify_Dataframe(self.Spotify_Dataframe)
        else:
            print("No Spotify DataFrame to save.")

    def update_and_save_master_Spotify_Dataframe(self, dataframe):
        if dataframe is not None:
            if self.master_Spotify_Dataframe_path is not None:
                master_Spotify_Dataframe = self.loadfromCSV(self.master_Spotify_Dataframe_path)
                if master_Spotify_Dataframe is not None:
                    # Add only new rows to the master Spotify DataFrame
                    master_Spotify_Dataframe = pd.concat([master_Spotify_Dataframe, dataframe], ignore_index=True).drop_duplicates(
                        subset=['u_id','Spotify_Track_ID', 'Spotify_Track_Name', 'Spotify_Album_Name', 'Spotify_Album_ID', 'Spotify_Track_Duration', 'Spotify_Track_Number'])
                else:
                    master_Spotify_Dataframe = dataframe
                self.save_dataframe(master_Spotify_Dataframe, self.master_Spotify_Dataframe_path)
                print(f"Master Spotify DataFrame saved to {self.master_Spotify_Dataframe_path}")
            self.order_master_Spotify_Dataframe_by_Spotify_Album_ID_then_Spotify_Track_Number()

    def order_master_Spotify_Dataframe_by_Spotify_Album_ID_then_Spotify_Track_Number(self):
        if self.master_Spotify_Dataframe_path is not None:
            master_Spotify_Dataframe = self.loadfromCSV(self.master_Spotify_Dataframe_path)
            if master_Spotify_Dataframe is not None:
                master_Spotify_Dataframe = master_Spotify_Dataframe.sort_values(by=[ 'Spotify_Album_Release_Date','Spotify_Album_ID' ], ascending=False)
                self.save_dataframe(master_Spotify_Dataframe, self.master_Spotify_Dataframe_path)
                print(f"Master Spotify DataFrame saved to {self.master_Spotify_Dataframe_path}")
            else:
                print("No Master Spotify DataFrame to save.")

    def get_master_Spotify_Dataframe_u_ids_list(self):
        if self.master_Spotify_Dataframe['u_id'] is not None:
            return self.master_Spotify_Dataframe['u_id'].tolist()
        else:
            return []

    """
    Discogs Dataframes
    """

    def set_search_dataframe(self, df):
        """Set the Search DataFrame."""
        self.Search_Dataframe = df

    def set_release_dataframe(self, df):
        """Set the Release DataFrame."""
        self.Release_Dataframe = df

    def set_loaded_search_csv_file(self, csv_file):
        """Set the loaded CSV file."""
        self.loaded_search_csv_file = csv_file

    @staticmethod
    def create_new_discogs_dataframe(data=None):
        """Create an empty Discogs DataFrame."""
        discogs_dataframe = pd.DataFrame(data if data else None, columns=['u_id','Discogs_Artists', 'Discogs_Titles', 'Discogs_Labels', 'Discogs_Genres', 'Discogs_Styles',
                                                'Discogs_Countries', 'Discogs_Years', 'Discogs_Search_Filters', 'Discogs_Urls',
                                                'Discogs_Formats', 'Discogs_Tracklist',  'Discogs_YouTube_Videos'])
        return discogs_dataframe

    def display_Search_Dataframedata(self):
        print(self.Search_Dataframe)

    def save_Search_Dataframe(self, path, update_master=True):
        if path.endswith('.csv'):
            pass
        else:
            path = path+'.csv'
        self.Search_Dataframe.to_csv(path_or_buf=path, index=False)
        self.set_loaded_search_csv_file(path)  # Set the loaded CSV file
        if update_master:
            self.update_and_save_master_Discogs_Dataframe(self.Search_Dataframe)

    def update_search_dataframe(self, center_releases_content):
        if center_releases_content is None or not isinstance(center_releases_content, list):
            raise ValueError("center_releases_content must be a non-empty list of dictionaries")

        # Check that each dictionary has the expected keys
        expected_keys = ['Discogs_Artists', 'Discogs_Titles']
        for item in center_releases_content:
            if not all(key in item for key in expected_keys):
                raise ValueError(f"Each dictionary must contain the keys {expected_keys}")

        # Convert the list of dictionaries to a DataFrame
        new_df = pd.DataFrame(center_releases_content)

        # Use apply function to check and update the u_id
        if 'Discogs_Tracks' in new_df.columns:
            new_df['u_id'] = new_df.apply(
                lambda row: self.check_and_update_u_id(row['Discogs_Artists'], row['Discogs_Titles'],
                                                       row['Discogs_Tracks']), axis=1)
        else:
            new_df['u_id'] = new_df.apply(
                lambda row: self.check_and_update_u_id(row['Discogs_Artists'], row['Discogs_Titles']), axis=1)

        # Concatenate new_df with Search_Dataframe and drop duplicates
        self.Search_Dataframe = pd.concat([self.Search_Dataframe, new_df]).drop_duplicates()

        # Log the completion of the method
        logging.info("update_search_dataframe completed successfully")

    def check_and_update_u_id(self, artist_name, album_name=None, track_name=None):
        if album_name:
            u_id = self.check_u_id_in_master_list(artist_name, album_name)
        elif track_name:
            u_id = self.check_u_id_in_master_list(artist_name, track_name)
        else:
            raise ValueError("Either album_name or track_name must be provided")

        if not u_id:
            u_id = self.generate_unique_id(artist_name, album_name or track_name)
        return u_id

    def update_search_dataframe_backup(self, center_releases_content):
        if center_releases_content is None or not isinstance(center_releases_content, list):
            raise ValueError("center_releases_content must be a non-empty list of dictionaries")

        # Convert the list of dictionaries to a DataFrame
        new_df = pd.DataFrame(center_releases_content)

        # Iterate over the new data and check against the master_u_id_list
        for index, row in new_df.iterrows():
            url = row['Discogs_Urls']
            u_id, u_id_type = self.check_url_in_master_list(url)

            if u_id:
                # Update the u_id in the new dataframe
                row['u_id'] = u_id
                # TODO: Handle u_id_type if needed
            else:
                # Generate a new u_id and update both new_df and master_u_id_dataframe
                new_u_id = self.generate_unique_id(url)  # Implement this method
                row['u_id'] = new_u_id
                # TODO: Add new_u_id to master_u_id_dataframe

        # Concatenate new_df with Search_Dataframe and drop duplicates
        self.Search_Dataframe = pd.concat([self.Search_Dataframe, new_df]).drop_duplicates()

    def save_Release_Dataframe(self, path):
        """Save the Release DataFrame to a CSV file."""
        # Ensure the file name ends with .csv
        if not path:
            # get the date and time to add to ed of string
            current_date = datetime.now().strftime("%Y-%m-%d")
            path = self.Search_Dataframe['Discogs_Search_Filters'][0].strip("_exact") + current_date
        if 'ReleaseDataframe' not in path:
            # a str representation of the current date
            current_date = datetime.now().strftime("%Y-%m-%d")
            path = path+'_ReleaseDataframe'
        if not path.endswith('.csv'):
            path += '.csv'
        # Check if the Release DataFrame is not empty
        if self.Release_Dataframe is not None:
            # Save the DataFrame to the specified path
            self.Release_Dataframe.to_csv(path_or_buf=path, index=False)
            self.set_loaded_search_csv_file(path) # Set the loaded CSV file
            print(f"Release DataFrame saved to {path}")
        else:
            print("No Release DataFrame to save.")

    def get_deep_search_dataframe(self):
        if self.Search_Dataframe is None:
            print("DataHandler has no Search Dataframe to transform")
            return
        filtered_df = self.find_search_rows_needing_update()
        return filtered_df

    def get_expanded_tracklist_search_dataframe_backup(self,tracklist_col='Discogs_Tracklist'):
        df = self.Release_Dataframe if self.Release_Dataframe is not None else self.Search_Dataframe
        # Prepare a container for the new DataFrame rows
        new_rows = []

        # Iterate over the rows in the DataFrame
        for idx, row in df.iterrows():
            # Split the tracklist into individual track strings
            tracks = row[tracklist_col].split(' | ')

            # For each track, extract its details and create a new row
            for track in tracks:
                if track:
                    track_details = track.split(', ')
                    # Ensure that the track details list has four items (Artist, Trackname, Duration, Position)
                    if len(track_details) == 4:
                        new_row = row.to_dict()
                        new_row.update({
                            'Discogs_Tracks_Artists': track_details[0],
                            'Discogs_Tracks': track_details[1],
                            'Discogs_Tracks_Durations': track_details[2],
                            'Discogs_Tracks_Positions': track_details[3]
                        })
                        new_rows.append(new_row)

        # Create a new DataFrame from the new_rows list
        new_df = pd.DataFrame(new_rows)
        # Reorder the columns so the track details start from the second column
        col_order = ['u_id', 'Discogs_Tracks_Artists', 'Discogs_Tracks', 'Discogs_Tracks_Durations',
                     'Discogs_Tracks_Positions'] + \
                    [col for col in new_df.columns if
                     col not in ['u_id', 'Discogs_Tracks_Artists', 'Discogs_Tracks', 'Discogs_Tracks_Durations',
                                 'Discogs_Tracks_Positions']]
        new_df = new_df[col_order]

        # Drop the original tracklist column if needed
        new_df = new_df.drop(columns=[tracklist_col])

        self.Release_Dataframe = new_df
        return new_df

    def get_expanded_tracklist_search_dataframe(self, tracklist_col='Discogs_Tracklist', youtube_col='Discogs_YouTube_Videos', generate_u_id=True):
        df = self.Release_Dataframe if self.Release_Dataframe is not None else self.Search_Dataframe
        # Prepare a container for the new DataFrame rows
        new_rows = []

        # Iterate over the rows in the DataFrame
        for idx, row in df.iterrows():
            # Split the tracklist into individual track strings
            tracks = row[tracklist_col].split(' | ')

            # Prepare a list to store YouTube video matches for the tracks
            track_youtube_matches = []

            # For each track, extract its details and create a new row
            for track in tracks:
                if track:
                    track_details = track.split(', ')
                    # Ensure that the track details list has four items (Artist, Trackname, Duration, Position)
                    if len(track_details) == 4:
                        new_row = row.to_dict()
                        new_row.update({
                            'Discogs_Tracks_Artists': track_details[0],
                            'Discogs_Tracks': track_details[1],
                            'Discogs_Tracks_Durations': track_details[2],
                            'Discogs_Tracks_Positions': track_details[3]
                        })

                        # Match the track name with the closest YouTube video title and URL
                        youtube_matches = self.match_youtube_videos(track_details[1], row[youtube_col])
                        if youtube_matches:
                            #print(f"this is youtube matches {youtube_matches}")
                            # Extract just the title from the match for appending
                            #youtube_title = youtube_match.split(',')[0]
                            new_row['Discogs_Tracks_Youtube'] = youtube_matches
                            track_youtube_matches.append(youtube_matches)

                        new_rows.append(new_row)

        # Create a new DataFrame from the new_rows list
        new_df = pd.DataFrame(new_rows)
        # Reorder the columns so the track details start from the second column
        col_order = ['u_id_track', 'Discogs_Tracks_Artists', 'Discogs_Tracks', 'Discogs_Tracks_Durations',
                     'Discogs_Tracks_Positions', 'Discogs_Tracks_Youtube'''] + \
                    [col for col in new_df.columns if
                     col not in ['u_id_track', 'Discogs_Tracks_Artists', 'Discogs_Tracks', 'Discogs_Tracks_Durations',
                                 'Discogs_Tracks_Positions', 'Discogs_Tracks_Youtube''']]
        new_df = new_df[col_order]

        self.Track_Release_Dataframe = new_df

        return new_df


    def match_youtube_videos(self, track_name, youtube_videos):
        if not isinstance(track_name, str) or not isinstance(youtube_videos, str):
            if youtube_videos == ' ':
                return None
            return None
        # Split the list of videos into individual video strings
        video_pairs = youtube_videos.split(' | ') if youtube_videos else []

        # Extract video titles and URLs
        video_titles = [pair.split(',')[0] for pair in video_pairs]

        # Use difflib to get the closest matching in video titles, and return as many as found above cutoff
        closest_match_titles = difflib.get_close_matches(track_name, video_titles, n=len(video_titles), cutoff=0.5)

        # Find the indexes of the matching titles
        match_indexes = [video_titles.index(title) for title in closest_match_titles]

        # Return the full video pairs (titles, URL) for all the closest matches
        return [video_pairs[index] for index in match_indexes] if match_indexes else None

    def add_new_release_to_dataframe(self, index, release_content):

        # Check if a Release DataFrame exists and if not, create one
        if self.Search_Dataframe is not None and self.Release_Dataframe is None:
            self.Release_Dataframe = self.Search_Dataframe
        elif self.Release_Dataframe is not None:
            """# Check if the release already exists in the DataFrame
            if self.is_duplicate(release_content['Discogs_Urls']):
                return  # Avoid adding duplicate entry"""
        for col in self.Release_Dataframe.columns:
            self.Release_Dataframe[col] = self.Release_Dataframe[col].astype('object')

        #self.Release_Dataframe.at[index, 'u_id'] = u_id
        # Update the DataFrame with the new release content by index
        self.update_release_Dataframe_table_content(index, release_content['table_content'])
        self.update_release_Dataframe_tracklist_content(index, release_content['tracklist_content'])
        self.update_release_Dataframe_video_links_content(index, release_content['video_links_content'])

    def update_release_Dataframe_table_content(self, index, release_table_content):
        """
        Updates the release DataFrame with content from the release table obtained from the Discogs page.

        :param index: The index in the DataFrame where the release content needs to be updated.
        :type index: int
        :param release_table_content: A dictionary containing key-value pairs of release information.
                                      Keys are labels like 'Label', 'Format', etc., and values are their corresponding details.
        :type release_table_content: dict
        """

        # Define a mapping between the label names from the release table content and the DataFrame column names
        label_to_column_mapping = {
            'Label': 'Discogs_Labels',
            'Format': 'Discogs_Formats',
            'Country': 'Discogs_Countries',
            'Released': 'Discogs_Years',
            'Genre': 'Discogs_Genres',
            'Style': 'Discogs_Styles',
            'Tracklist': 'Discogs_Tracklist',
            # Additional mappings can be added here as needed
        }

        # Initialize a dictionary to store updates for the DataFrame row
        row_updates = {}
        # Iterate over the items in the release_table_content dictionary
        for info_label, info in release_table_content.items():
            # Find the corresponding column name in the DataFrame for the given label
            column_name = label_to_column_mapping.get(info_label)
            if not info:
                continue

            # If a column name is found in the mapping
            if column_name:
                # Update the row_updates dictionary with the new information
                row_updates[column_name] = info
        # Iterate over the items in the row_updates dictionary
        for key, value in row_updates.items():
            # Update the corresponding column of the DataFrame at the specified index with the new value
            self.Release_Dataframe.at[index, key] = str(value)

        # Check if any columns in the row are still blank
        blank_columns = self.Release_Dataframe.columns[self.Release_Dataframe.iloc[index].isnull()]

        # Iterate over the blank columns and see if row_updates has information for them
        for column in blank_columns:
            if column in row_updates:
                # If row_updates has information for this column, update the DataFrame again
                self.Release_Dataframe.at[index, column] = str(row_updates[column])

    def update_release_Dataframe_tracklist_content(self, index, release_tracklist_content):
        # Convert each inner list to a string and then join all strings
        #tracklist_str = ', '.join([' - '.join(item) for item in release_tracklist_content])
        #tracklist_str = ', '.join([item for sublist in release_tracklist_content for item in sublist])
        tracklist_str = ' | '.join(', '.join(sublist) for sublist in release_tracklist_content)
        # Assign the string to the DataFrame
        self.Release_Dataframe.at[index, 'Discogs_Tracklist'] = str(tracklist_str)

    def update_release_Dataframe_video_links_content(self, index, release_video_links_content):
        # Convert the list of tuples to a string format where each tuple becomes "title, url"
        video_links_str = ' | '.join([f"{title},{url}" for title, url in release_video_links_content])
        self.Release_Dataframe.at[index, 'Discogs_YouTube_Videos'] = video_links_str

    def is_duplicate(self, discogs_url):
        # Assuming 'Discogs_URLS' is a unique identifier for each release
        #url = table_content.get('Discogs_URLS')
        if discogs_url in self.Release_Dataframe['Discogs_Urls'].values:
            return True
        return False

    def find_search_rows_needing_update(self):
        """
        Identify rows in the Search DataFrame that need more information.
        :return: DataFrame - Filtered DataFrame with rows needing updates.
        """
        # Define columns to check for missing values
        columns_to_check = ['Discogs_Genres', 'Discogs_Styles', 'Discogs_Countries', 'Discogs_Years',
                            'Discogs_Tracklist', 'Discogs_Labels']

        # Check if any of the specified columns have missing values
        missing_info = self.Search_Dataframe[columns_to_check].isnull().any(axis=1)

        # Filter rows where 'Discogs_Urls' is not null and other info is missing
        filtered_df = self.Search_Dataframe[missing_info & self.Search_Dataframe['Discogs_Urls'].notnull()]

        return filtered_df

    def find_search_rows_needing_update_backup(self):
        # Identify rows that need more information
        search_queue = []
        for index, row in self.Search_Dataframe.iterrows():
            if pd.isnull(row['Discogs_Genres']) or pd.isnull(row['Discogs_Styles']) or \
                    pd.isnull(row['Discogs_Countries']) or pd.isnull(row['Discogs_Years']) or \
                    pd.isnull(row['Discogs_Tracklist']) or pd.isnull(row['Discogs_Labels']):
                if 'Discogs_Urls' in row and pd.notnull(row['Discogs_Urls']):
                    search_queue.append((index, row['Discogs_Urls']))
                    # store the index of the row and the Discogs URL


        return search_queue

    def update_release_dataframe_with_release_info(self, release_info_dict):
        """Update the Release DataFrame with release info."""
        if self.Release_Dataframe is None:
            print("DataHandler has no Release Dataframe to update")
            return

    def update_release_info_in_dataframe(self, index, release_content):
        # Update the DataFrame row at the given index with release_content
        for key, value in release_content.items():
            self.Release_Dataframe.at[index, key] = value

    """
    Discogs Master Dataframes
    """
    def update_and_save_master_Discogs_Dataframe(self, dataframe):
        if dataframe is not None:
            if self.master_Discogs_Dataframe_path is not None:
                master_Discogs_Dataframe = self.loadfromCSV(self.master_Discogs_Dataframe_path)
                if master_Discogs_Dataframe is not None:
                    # Add only new rows to the master Spotify DataFrame
                    master_Discogs_Dataframe = pd.concat([master_Discogs_Dataframe, dataframe],
                                                         ignore_index=True).drop_duplicates(
                        subset=['u_id'])
                else:
                    master_Discogs_Dataframe = dataframe


                self.save_dataframe(master_Discogs_Dataframe, self.master_Discogs_Dataframe_path)
                print(f"Master Discogs DataFrame saved to {self.master_Discogs_Dataframe_path}")
            self.order_master_Discogs_Dataframe_by_Discogs_Unique_ID()

    def order_master_Discogs_Dataframe_by_Discogs_Unique_ID(self):
        if self.master_Discogs_Dataframe_path is not None:
            master_Discogs_Dataframe = self.loadfromCSV(self.master_Discogs_Dataframe_path)
            if master_Discogs_Dataframe is not None:
                master_Discogs_Dataframe = master_Discogs_Dataframe.sort_values(
                    by=['u_id'], ascending=False)
                self.save_dataframe(master_Discogs_Dataframe, self.master_Discogs_Dataframe_path)
                print(f"Master Discogs DataFrame saved to {self.master_Discogs_Dataframe_path}")
            else:
                print("No Master Discogs DataFrame to save.")

    def get_master_Discogs_Dataframe_u_ids_list(self):
        if self.master_Discogs_Dataframe['u_id'] is not None:
            return self.master_Discogs_Dataframe['u_id'].tolist()
        else:
            return []

    """
    Master U ID Methods
    """


    def create_new_u_id_dataframe(self, data=None, in_file=False):
        """Create an empty u_id DataFrame."""
        u_id_df = pd.DataFrame(data if data else None,
                               columns=['u_id_track', 'u_id_release', 'u_id_artist', 'u_id_label', 'u_id_genre'])
        if in_file:
            self.save_dataframe(u_id_df, self.master_u_id_Dataframe_path)
        return u_id_df

    """
    Backups
    
    """

    def update_search_dataframe_backup(self, center_releases_content):
        if center_releases_content is not None:
            if isinstance(center_releases_content, list):
                # Convert the list of dictionaries to a DataFrame
                new_df = pd.DataFrame(center_releases_content, columns=["u_id","Discogs_Artists", "Discogs_Titles", "Discogs_Labels", "Discogs_Genres", "Discogs_Styles",
                                                "Discogs_Countries", "Discogs_Years", "Discogs_Search_Filters", "Discogs_Urls",
                                                "Discogs_Formats", "Discogs_Tracklist",  "Discogs_YouTube_Videos"])

            else:
                raise ValueError("new_data must be a list of dictionaries")
        else:
            raise ValueError("self.center_releases_content is None")

        # Concatenate new_df with self.df and drop duplicates
        self.Search_Dataframe = pd.concat([self.Search_Dataframe, new_df], ignore_index=True).drop_duplicates(
            subset=["u_id","Discogs_Artists", "Discogs_Titles", "Discogs_Labels", "Discogs_Genres", "Discogs_Styles",
                                                "Discogs_Countries", "Discogs_Years", "Discogs_Search_Filters", "Discogs_Urls",
                                                "Discogs_Formats", "Discogs_Tracklist",  "Discogs_YouTube_Videos"])
