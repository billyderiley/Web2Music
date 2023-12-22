from DiscogsSearchScraper import DiscogsSearchScraper
from DiscogsReleaseScraper import DiscogsReleaseScraper
import pandas as pd
import os
from datetime import datetime
import time
import random


class DataHandler:

    @staticmethod
    def load_data(csv_file):
        df = pd.read_csv(csv_file)
        print(f"Data loaded from {csv_file}")
        return df

    @staticmethod
    def list_csv_files():
        print("Available CSV files:")
        csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
        for idx, file in enumerate(csv_files, 1):
            print(f"{idx}. {file}")
        return csv_files
    def __init__(self, Search_Dataframe = None, Release_Dataframe = None, Spotify_Dataframe = None, loaded_csv_file = None):
        self.Search_Dataframe = Search_Dataframe
        self.Release_Dataframe = Release_Dataframe
        self.Spotify_Dataframe = Spotify_Dataframe
        self.loaded_csv_file = loaded_csv_file

    def set_spotify_dataframe(self, df):
        """Set the Spotify DataFrame."""
        self.Spotify_Dataframe = df

    def set_search_dataframe(self, df):
        """Set the Search DataFrame."""
        self.Search_Dataframe = df

    def set_release_dataframe(self, df):
        """Set the Release DataFrame."""
        self.Release_Dataframe = df

    def set_loaded_csv_file(self, csv_file):
        """Set the loaded CSV file."""
        self.loaded_csv_file = csv_file

    def update_spotify_dataframe_with_artist_metrics(self, artist, artist_metrics):
        """Update the Spotify DataFrame with artist's Spotify popularity and followers."""
        if 'Spotify_Popularity_Followers' not in self.Spotify_Dataframe.columns:
            self.Spotify_Dataframe['Spotify_Popularity_Followers'] = None

        popularity, followers = artist_metrics
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Update the DataFrame with the artist metrics and the current timestamp
        self.Spotify_Dataframe.loc[self.Spotify_Dataframe[
                                       'Discogs_Artists'] == artist, 'Spotify_Popularity_Followers'] = f"{popularity}, {followers}, {current_time}"


    """def display_dataframe(self):
        if self.Search_Dataframe is not None:
            print("Search Dataframe :")
            print(self.Search_Dataframe)
        if self.Release_Dataframe is not None:
            print("Release Dataframe :")
            print(self.Release_Dataframe)"""


    """def save_dataframe(self, save_as_file_name):
        if save_as_file_name is None:
            save_as_file_name = "1_-2test"
        else:
            if save_as_file_name.endswith('.csv'):
                save_as_file_name = save_as_file_name
            else:
                save_as_file_name += '.csv'
        self.df.to_csv(path_or_buf=save_as_file_name)"""

    def loadfromCSV(self, csv_file):
        try:
            return pd.read_csv(csv_file)
        except FileNotFoundError:
            return None


    def save_Search_Dataframe(self, path):
        if path.endswith('.csv'):
            pass
        else:
            path = path+'.csv'
        #path = input("Enter a Save name for Dataframe")
        self.Search_Dataframe.to_csv(path_or_buf=path, index=False)

    def save_Spotify_Dataframe(self, path):
        """Save the Spotify DataFrame to a CSV file."""
        # Ensure the file name ends with .csv
        if not path.endswith('.csv'):
            path += '.csv'

        # Check if the Spotify DataFrame is not empty
        if self.Spotify_Dataframe is not None:
            # Save the DataFrame to the specified path
            self.Spotify_Dataframe.to_csv(path_or_buf=path, index=False)
            print(f"Spotify DataFrame saved to {path}")
        else:
            print("No Spotify DataFrame to save.")

    def display_Search_Dataframedata(self):
        print(self.Search_Dataframe)

    def update_search_dataframe(self, center_releases_content):
        if center_releases_content is not None:
            if isinstance(center_releases_content, list):
                # Convert the list of dictionaries to a DataFrame
                new_df = pd.DataFrame(center_releases_content, columns=["Discogs_Artists", "Discogs_Titles", "Discogs_Labels", "Discogs_Genres", "Discogs_Styles",
                                                "Discogs_Countries", "Discogs_Years", "Discogs_Search_Filters", "Discogs_Urls",
                                                "Discogs_Formats", "Discogs_Tracklist",  "Discogs_YouTube_Videos"])

            else:
                raise ValueError("new_data must be a list of dictionaries")
        else:
            raise ValueError("self.center_releases_content is None")

        # Concatenate new_df with self.df and drop duplicates
        self.Search_Dataframe = pd.concat([self.Search_Dataframe, new_df], ignore_index=True).drop_duplicates(
            subset=["Discogs_Artists", "Discogs_Titles", "Discogs_Labels", "Discogs_Genres", "Discogs_Styles",
                                                "Discogs_Countries", "Discogs_Years", "Discogs_Search_Filters", "Discogs_Urls",
                                                "Discogs_Formats", "Discogs_Tracklist",  "Discogs_YouTube_Videos"])


    def update_release_dataframe(self, center_releases_inner_content):
        if self. Release_Dataframe is None:
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
        print(self.DiscogsReleaseScrape_obj.Release_Dataframe)



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

