from DiscogsSearchScraper import DiscogsSearchScraper
from DiscogsReleaseScraper import DiscogsReleaseScraper
import pandas as pd
import time
import random


class DataHandler:
    def __init__(self, search_df = None, csv_file = None):
        if csv_file is None:
            if search_df is None:
                self.search_df = None
            #else:
            #    self.search_df = search_df
        else:
            self.df = self.loadfromCSV(csv_file=csv_file)
        self.discogs_search_scraper = DiscogsSearchScraper()
        self.discogs_release_content_scraper = DiscogsReleaseScraper()


    def display_dataframe(self):
        print(self.df)

    def save_dataframe(self, save_as_file_name):
        if save_as_file_name is None:
            save_as_file_name = "1_-2test"
        else:
            if save_as_file_name.endswith('.csv'):
                save_as_file_name = save_as_file_name
            else:
                save_as_file_name += '.csv'
        self.df.to_csv(path_or_buf=save_as_file_name)

    def loadfromCSV(self, csv_file):
        try:
           return pd.read_csv(csv_file)
        except FileNotFoundError:
            return None


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

