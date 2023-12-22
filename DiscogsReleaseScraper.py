from BaseScraper import BaseScraper
import pandas as pd
import os


class DiscogsReleaseScraper(BaseScraper):

    def __init__(self, data_handler):
        super().__init__()
        self.current_url_release_info_dict = None
        self.data_handler = data_handler  # DataHandler instance

    def find_rows_needing_update(self):
        # Identify rows that need more information
        search_queue = []
        for index, row in self.data_handler.Release_Dataframe.iterrows():
            if pd.isnull(row['Discogs_Genres']) or pd.isnull(row['Discogs_Styles']) or \
                    pd.isnull(row['Discogs_Countries']) or pd.isnull(row['Discogs_Years']) or \
                    pd.isnull(row['Discogs_Tracklist']) or pd.isnull(row['Discogs_Labels']):
                if 'Discogs_Urls' in row and pd.notnull(row['Discogs_Urls']):
                    search_queue.append((index, row['Discogs_Urls']))
        return search_queue

    def process_search_queue(self, search_queue):
        for index, url in search_queue:
            release_content = self.get_current_release_url_content(url)
            self.update_release_info_in_dataframe(index, release_content)

    def update_release_info_in_dataframe(self, index, release_content):
        # Update the DataFrame row at the given index with release_content
        for key, value in release_content.items():
            self.data_handler.Release_Dataframe.at[index, key] = value

    def get_current_release_url_content(self, url):
        SoupObj = self.get_Soup_from_url(url)
        release_table_content = self.get_release_table_content(SoupObj)
        release_tracklist_content = self.get_release_tracklist_content(SoupObj)
        release_video_links_content = self.get_release_video_links_content(SoupObj)
        current_url_release_info_dict = {
            'Discogs_Urls': url,
            'table_content': release_table_content,
            'tracklist_content': release_tracklist_content,
            'video_links_content': release_video_links_content
        }
        return current_url_release_info_dict

    def update_release_table_content(self, index, release_table_content):
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

            # If a column name is found in the mapping
            if column_name:
                # Update the row_updates dictionary with the new information
                row_updates[column_name] = info

        # Iterate over the items in the row_updates dictionary
        for key, value in row_updates.items():
            # Update the corresponding column of the DataFrame at the specified index with the new value
            self.data_handler.Release_Dataframe.at[index, key] = value

    def update_release_tracklist_content(self, index, release_tracklist_content):
        # Convert each inner list to a string and then join all strings
        tracklist_str = ', '.join([' - '.join(item) for item in release_tracklist_content])
        # Assign the string to the DataFrame
        self.data_handler.Release_Dataframe.at[index, 'Discogs_Tracklist'] = tracklist_str

    def update_release_video_links_content(self, index, release_video_links_content):
        # Convert video links list to a single string format
        video_links_str = ', '.join(release_video_links_content)
        self.data_handler.Release_Dataframe.at[index, 'Discogs_YouTube_Videos'] = video_links_str

    def add_new_release_to_dataframe(self, index, release_content):
        # Check if the release already exists in the DataFrame
        if self.is_duplicate(release_content['Discogs_Urls']):
            return  # Avoid adding duplicate entry

        # Update the DataFrame with the new release content
        self.update_release_table_content(index, release_content['table_content'])
        self.update_release_tracklist_content(index, release_content['tracklist_content'])
        self.update_release_video_links_content(index, release_content['video_links_content'])

    def is_duplicate(self, discogs_url):
        # Assuming 'Discogs_URLS' is a unique identifier for each release
        #url = table_content.get('Discogs_URLS')
        if discogs_url in self.data_handler.Release_Dataframe['Discogs_Urls'].values:
            return True
        return False

    """def main_scraping_method(self, url_list):
        for url in url_list:
            release_content = self.navigate_to_release_url(url)
            self.add_new_release(release_content)"""

    def get_release_tracklist_content(self, SoupObj):
        """
        Extracts the tracklist content from a BeautifulSoup object of a Discogs release page.

        :param SoupObj: A BeautifulSoup object representing the parsed HTML of a Discogs release page.
        :type SoupObj: BeautifulSoup
        """
        release_tracklist_content = []
        # Find the primary artist link; use 'Various' if not found
        artist_link = SoupObj.find('a', class_='link_1ctor link_15cpV')
        primary_artist = artist_link.text if artist_link else 'Various'

        # Find the tracklist table
        table = SoupObj.find('table', class_="tracklist_3QGRS")
        if not table:
            return release_tracklist_content  # Return empty list if tracklist table is not found

        # Iterate through each track row in the table
        for tr in table.find_all('tr'):
            # Find and clean the artist's name, fallback to primary artist if not present
            artist_line = tr.find('td', class_='artist_3zAQD')
            artist = artist_line.text.strip() if artist_line else primary_artist

            # Find and clean the track name, use "Unknown Track" if not present
            track_name_line = tr.find('td', class_='trackTitle_CTKp4')
            track_name = track_name_line.text.strip() if track_name_line else "Unknown Track"

            release_tracklist_content.append([artist, track_name])

        return release_tracklist_content

    """def get_release_tracklist_content_backup(self, SoupObj):
        release_tracklist_content = []
        # Try to get the artist from the top of the page
        artist_link = SoupObj.find('a', class_='link_1ctor link_15cpV')
        #print(artist_link)
        #print('what was that!')
        if artist_link is None:
            artist_link = 'Various'
        #print(f"is this working {artist_link.text}")
        primary_artist = artist_link.text

        table = SoupObj.find('table', class_="tracklist_3QGRS")

        if not table:
            return release_tracklist_content  # Return empty list if table is not found

        for tr in table.find_all('tr'):
            #print(f"heres the artist line")
            artist_line = tr.find('td', class_='artist_3zAQD').text
            #print(artist_line)
            if artist_line:
                artist = artist_line.strip()
            else:
                # Use the primary artist if specific artist info is not found in the track row
                artist = primary_artist

            # Extract track name
            track_name_line = tr.find('td', class_='trackTitle_CTKp4')
            track_name = track_name_line.text.strip() if track_name_line else "Unknown Track"

            release_tracklist_content.append([artist, track_name])

        return release_tracklist_content"""

    def get_release_table_content(self, SoupObj):
        """
        Extracts the release table content (like labels, formats, countries, etc.) from a BeautifulSoup object.

        :param SoupObj: A BeautifulSoup object representing the parsed HTML of a Discogs release page.
        :type SoupObj: BeautifulSoup
        """
        release_table_content = {}
        table = SoupObj.find('table', class_="table_1fWaB")

        # Check if the table is found, if not return an empty dictionary
        if not table:
            return release_table_content

        # Iterate through each row in the table and extract the label and corresponding information
        for tr in table.find_all('tr'):
            info_label = tr.find('th').text.rstrip(":") if tr.find('th') else 'Unknown Label'
            info = tr.find('td').text if tr.find('td') else 'Unknown Info'
            release_table_content[info_label] = info

        return release_table_content

    def get_release_video_links_content(self, SoupObj):
        """
        Extracts video links from a BeautifulSoup object of a Discogs release page.

        :param SoupObj: A BeautifulSoup object representing the parsed HTML of a Discogs release page.
        :type SoupObj: BeautifulSoup
        """
        # Find the video player section
        video_player = SoupObj.find('ul', class_='videos_1xVCN')
        if not video_player:
            return []  # Return empty list if video player is not found

        release_video_links_content = []
        # Iterate through each video item and extract YouTube video URLs
        for video in video_player.find_all('li'):
            img_tag = video.find('img', class_='thumbnail_f0Yrr')
            if img_tag and 'src' in img_tag.attrs:
                youtube_video_id = self.extract_youtube_id(img_tag['src'])
                if youtube_video_id:
                    youtube_video_url = 'https://www.youtube.com/watch?v=' + youtube_video_id
                    release_video_links_content.append(youtube_video_url)

        return release_video_links_content

    def extract_youtube_id(self, url):
        """
        Extracts YouTube video ID from a given image source URL.

        :param url: The URL of the image source which contains the YouTube video ID.
        :type url: str
        """
        # Pattern match for YouTube video ID
        if 'vi/' in url and '/default.jpg' in url:
            start = url.find('vi/') + 3
            end = url.find('/default.jpg', start)
            return url[start:end]
        return None  # Return None if the pattern is not found

    #@staticmethod
    def create_Release_Dataframe(self):
        release_dataframe = pd.DataFrame(columns=['Discogs_Titles','Discogs_Artists', 'Discogs_Tracklist', 'Discogs_Labels', 'Discogs_Genres'
            , 'Discogs_Styles', 'Discogs_Countries', 'Discogs_Years', 'Discogs_Formats','Discogs_Urls', 'Discogs_YouTube_Videos'])
        return release_dataframe

    """def save_release_dataframe(self):
        self.Release_Dataframe.to_csv(path_or_buf='release_dataframe.csv', index=False)

        self.Release_Dataframe = None

    def load_data(self, csv_file):
        self.Release_Dataframe = pd.read_csv(csv_file)
        print(f"Data loaded from {csv_file}")

    def list_csv_files(self):
        print("Available CSV files:")
        csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
        for idx, file in enumerate(csv_files, 1):
            print(f"{idx}. {file}")
        return csv_files"""

    #def load_release_dataframe(self):
   #     self.Release_Dataframe = pd.read_csv('release_dataframe.csv')



    """def update_release_dataframe(self, release_table_content, release_tracklist_content, release_video_links_content):
        #if isinstance(new_data, dict):
            # Convert the dictionary to a DataFrame
            new_df = pd.DataFrame([new_data])
        elif isinstance(new_data, list):
            # Convert the list of dictionaries to a DataFrame
            new_df = pd.DataFrame(new_data)
        else:
            raise ValueError("new_data must be a dictionary or a list of dictionaries")
        self.Release_Dataframe = self.Release_Dataframe.append(release_table_content, ignore_index=True)
        self.Release_Dataframe = self.Release_Dataframe.append(release_tracklist_content, ignore_index=True)
        self.Release_Dataframe = self.Release_Dataframe.append(release_video_links_content, ignore_index=True)
        """



