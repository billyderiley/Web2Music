from DiscogsSearchScraper import DiscogsSearchScraper
from DiscogsReleaseScraper import DiscogsReleaseScraper
from SpotifyScraper import SpotifyScraper
import pandas as pd
import os
from datetime import datetime
import time
import random


class DataHandler:

    @staticmethod
    def save_dataframe(dataframe, save_as_file_name):
        dataframe.to_csv(path_or_buf=save_as_file_name, index=False)

    @staticmethod
    def load_data(csv_file):
        df = pd.read_csv(csv_file)
        print(f"Data loaded from {csv_file}")
        return df

    """@staticmethod
    def list_csv_files():
        print("Available CSV files:")
        csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
        for idx, file in enumerate(csv_files, 1):
            print(f"{idx}. {file}")
        return csv_files
    """
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
        self.Search_Dataframe = Search_Dataframe
        self.Release_Dataframe = Release_Dataframe
        self.Spotify_Dataframe = Spotify_Dataframe
        self.loaded_search_csv_file = loaded_search_csv_file
        self.loaded_spotify_csv_file = loaded_spotify_csv_file

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
            else self.create_new_discogs_dataframe()



    """def display_dataframe(self):
        if self.Search_Dataframe is not None:
            print("Search Dataframe :")
            print(self.Search_Dataframe)
        if self.Release_Dataframe is not None:
            print("Release Dataframe :")
            print(self.Release_Dataframe)"""

    def loadfromCSV(self, csv_file):
        try:
            return pd.read_csv(csv_file)
        except FileNotFoundError:
            return None

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

    def update_spotify_dataframe_with_preview_urls(self, row, preview_urls):
        preview_urls_str = ', '.join(filter(None, preview_urls))  # Concatenate non-None URLs

        if 'Preview_URLs' not in self.Spotify_Dataframe.columns:
            self.Spotify_Dataframe['Preview_URLs'] = None

        # Update the DataFrame with the concatenated preview URLs
        self.Spotify_Dataframe.at[row.name, 'Preview_URLs'] = preview_urls_str

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

    def create_new_discogs_dataframe(self, data=None):
        """Create an empty Discogs DataFrame."""
        discogs_dataframe = pd.DataFrame(data if data else None, columns=['u_id','Discogs_Artists', 'Discogs_Titles', 'Discogs_Labels', 'Discogs_Genres', 'Discogs_Styles',
                                                'Discogs_Countries', 'Discogs_Years', 'Discogs_Search_Filters', 'Discogs_Urls',
                                                'Discogs_Formats', 'Discogs_Tracklist',  'Discogs_YouTube_Videos'])
        return discogs_dataframe

    def display_Search_Dataframedata(self):
        print(self.Search_Dataframe)

    def save_Search_Dataframe(self, path):
        if path.endswith('.csv'):
            pass
        else:
            path = path+'.csv'
        #path = input("Enter a Save name for Dataframe")
        self.Search_Dataframe.to_csv(path_or_buf=path, index=False)

    def update_search_dataframe(self, center_releases_content):
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

    def save_Release_Dataframe(self, path):
        """Save the Release DataFrame to a CSV file."""
        # Ensure the file name ends with .csv
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
            print(f"Release DataFrame saved to {path}")
        else:
            print("No Release DataFrame to save.")

    def get_release_dataframe_from_search_dataframe(self):
        """Create a Release DataFrame from the Search DataFrame."""
        if self.Search_Dataframe is None:
            print("DataHandler has no Search Dataframe to transform")
            return
        DiscogsReleaseScrape_obj = DiscogsReleaseScraper(self.Search_Dataframe)
        DiscogsReleaseScrape_obj.get_release_dataframe_from_search_dataframe()
        self.Release_Dataframe = DiscogsReleaseScrape_obj.Release_Dataframe

        """if self. Release_Dataframe is None:
            print("DataHandler has no Release Dataframe to transform")
            return
        else:
            if center_releases_inner_content is not None:
                if isinstance(center_releases_inner_content, list):
                    # Convert the list of dictionaries to a DataFrame
                    new_df = pd.DataFrame(center_releases_inner_content, columns=['Discogs_Titles','Discogs_Artists', 'Discogs_Tracklist', 'Discogs_Labels', 'Discogs_Genres'
            , 'Discogs_Styles', 'Discogs_Countries', 'Discogs_Years', 'Discogs_Formats','Discogs_Urls', 'Discogs_YouTube_Videos'])
                #self.DiscogsReleaseScrape_obj = DiscogsReleaseScraper()
                for index, row in self.Search_Dataframe.iterrows():
                    url = row['Discogs_Urls']
                    current_url_release_info_dict = self.DiscogsReleaseScrape_obj.get_current_release_url_content(url)
                    self.DiscogsReleaseScrape_obj.add_new_release(current_url_release_info_dict)
        print("Transformed Search_Dataframe to Release_Dataframe")
        print("Release_Dataframe :")
        print(self.DiscogsReleaseScrape_obj.Release_Dataframe)"""

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
    Master u_id Dataframe
    """
    def create_new_u_id_dataframe(self, data=None):
        """Create an empty Spotify DataFrame."""
        spotify_dataframe = pd.DataFrame(data if data else None,
                                         columns=['u_id'])
        return spotify_dataframe

class DataFrameUtility:
    @staticmethod
    def clean_and_split(x):
        """
        Splits the string by comma and strips spaces from each element.

        :param x: The string to be split and cleaned.
        :return: A list of cleaned strings.
        """
        return [val.strip() for val in str(x).split(',')]

    @staticmethod
    def divide_into_batches( dataframe, batch_size):
        # Split the DataFrame into smaller batches
        batches = [dataframe.iloc[i:i + batch_size] for i in range(0, len(dataframe), batch_size)]
        return batches



    """def find_missing_data(self):
        missing_data = self.df.isna()
        return missing_data

    def fill_in_missing_data(self, max_rows_to_update=None):
        missing_data = self.find_missing_data()
        rows_processed = 0

        for index, row in self.df.iterrows():
            if max_rows_to_update is not None and rows_processed >= max_rows_to_update:
                break
            self.discogs_release_content_scraper.get_release_url_content()
            # Check and fetch missing country data
            if missing_data.at[index, 'Discogs_Countries'] and not pd.isna(row['Discogs_Urls']):
                self.df.at[index, 'Discogs_Countries'] = self.fetch_country_data(row['Discogs_Urls'])

            # Check and fetch missing label data
            if missing_data.at[index, 'Discogs_Labels'] and not pd.isna(row['Discogs_Urls']):
                self.df.at[index, 'Discogs_Labels'] = self.fetch_label_data(row['Discogs_Urls'])

            # Check and fetch missing tags data
            if missing_data.at[index, 'Discogs_Tags'] and not pd.isna(row['Discogs_Urls']):
                self.df.at[index, 'Discogs_Tags'] = self.fetch_tags_data(row['Discogs_Urls'])

            rows_processed += 1
            # Additional conditions can be added here for other fields
            # ...

    def fetch_country_data(self, url):
        fetch_request = {'Country' : "blah"}
        self.Soup_From_Url = self.get_Soup_from_url(url)
        # Implement the scraping logic for country data
        return fetch_request

    def fetch_label_data(self, url):
        # Implement the scraping logic for label data
        return "Label from URL"

    def fetch_tags_data(self, url):
        # Implement the scraping logic for tags data
        return "Tags from URL"


    def fill_in_missing_data(self, max_rows_to_update=None):
        rows_processed = 0
        print('here')
        for index, row in self.df.iterrows():
            print('to')
            if max_rows_to_update is not None and rows_processed >= max_rows_to_update:
                break

            discogs_url = row['Discogs_Urls']
            SoupObj = scraper.createSoupObjFromUrl(discogs_url)

            # Update table content
            release_table_content = scraper.get_release_table_content(SoupObj)
            self.update_release_table_content(index, release_table_content)

            # Update tracklist content
            release_tracklist_content = scraper.get_release_tracklist_content(SoupObj)
            self.update_release_tracklist_content(index, release_tracklist_content)

            # Update video links content
            release_video_links_content = scraper.get_release_video_links_content(SoupObj)
            self.update_release_video_links_content(index, release_video_links_content)

            rows_processed += 1
            time.sleep(random.gauss(mu=3, sigma=0.5))"""

