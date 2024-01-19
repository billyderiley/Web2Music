from BaseScraper import BaseScraper
import pandas as pd
import os


class DiscogsReleaseScraper(BaseScraper):
    @staticmethod
    def create_Release_Dataframe(data=None):
        release_dataframe = pd.DataFrame(data if data else None, columns=['u_id','Discogs_Titles','Discogs_Artists', 'Discogs_Tracklist', 'Discogs_Labels', 'Discogs_Genres'
            , 'Discogs_Styles', 'Discogs_Countries', 'Discogs_Years', 'Discogs_Formats','Discogs_Urls', 'Discogs_YouTube_Videos'])
        return release_dataframe


    def __init__(self, Search_Dataframe=None, Release_Dataframe=None):
        super().__init__()
        self.release_info_dict = None
        #self.data_handler = data_handler  # DataHandler instance
        self.Search_Dataframe = Search_Dataframe
        self.Release_Dataframe = Search_Dataframe

        self.release_table_content = None
        self.release_tracklist_content = None
        self.release_video_links_content= None
        self.current_url = None

    # Updated usage in your get_release_dataframe_from_search_dataframe method:
    """def get_release_dataframe_from_search_dataframe_backup(self):
        search_queue = self.find_search_rows_needing_update()
        release_urls = [row['Discogs_Urls'] for _, row in search_queue]

        # Create a ReleaseBatchSearcher instance and perform batch search
        release_searcher = ReleaseBatchSearcher()
        aggregated_results = release_searcher.batch_search_releases(release_urls)

        # Prepare a dictionary with the index and corresponding release data
        index_release_data_mapping = {index: release_data for index, release_data in enumerate(aggregated_results)}

        for index, row in search_queue:
            # Add each release data to the DataFrame using the corresponding index
            release_data = index_release_data_mapping.get(index)
            if release_data:
                self.add_new_release_to_dataframe(index, release_data)"""

    """def get_release_dataframe_from_search_dataframe_backup(self):
        search_queue = self.find_search_rows_needing_update()
        rows = []
        #discogs_url_to_update = self.Search_Dataframe['Discogs_Urls'][self.Search_Dataframe['Discogs_Genres'].notnull()].tolist()
        for i, r in search_queue:
            rows.append(r)
            #self.navigate_to_release_url(r)
        self.execute_in_batches(rows, action=self.navigate_to_release_url, batch_size=15)"""

    def navigate_to_release_url(self, url):
        print(f"Processing {url}")
        release_info_dict = self.get_current_release_url_content(url)
        self.set_current_content(release_info_dict)
        #self.send_content_to_dataframe(release_info_dict)
        index = self.Search_Dataframe[self.Search_Dataframe['Discogs_Urls'] == url].index[0]
        self.add_new_release_to_dataframe(index, release_info_dict)

    def get_current_release_url_content(self, url):
        SoupObj = self.get_Soup_from_url(url)
        release_table_content = self.get_release_table_content(SoupObj)
        release_tracklist_content = self.get_release_tracklist_content(SoupObj)
        release_video_links_content = self.get_release_video_links_content(SoupObj)
        release_info_dict = {
            'Discogs_Urls': url,
            'table_content': release_table_content,
            'tracklist_content': release_tracklist_content,
            'video_links_content': release_video_links_content
        }
        return release_info_dict

    def process_release_data_to_dict(self, u_id, *args):
        """
        Processes release data into a dictionary of key-value pairs.
        Accepts multiple dictionary inputs like release_data, best_search_release_info, etc.

        :param u_id: Unique identifier for the release.
        :param args: Variable number of dictionaries containing release data.
        :return: Dictionary with structured release data.
        """
        # Define the label mapping
        label_to_column_mapping = {
            'Label': 'Discogs_Labels',
            'Format': 'Discogs_Formats',
            'Country': 'Discogs_Countries',
            'Released': 'Discogs_Years',
            'Genre': 'Discogs_Genres',
            'Style': 'Discogs_Styles'
        }

        # Initialize the dictionary with u_id
        processed_data = {'u_id': u_id}

        for data_dict in args:
            if isinstance(data_dict, dict):
                for key, value in data_dict.items():
                    # Use label_to_column_mapping for release_table_content keys
                    if key == 'table_content':
                        for label, label_value in value.items():
                            column_name = label_to_column_mapping.get(label)
                            if column_name:
                                processed_data[column_name] = label_value
                    elif key == 'tracklist_content':
                        # Special processing for tracklist
                        tracklist_str = ', '.join([' - '.join(item) for item in value])
                        processed_data['Discogs_Tracklist'] = tracklist_str
                    elif key == 'video_links_content':
                        # Special processing for video links
                        video_links_str = ', '.join(value)
                        processed_data['Discogs_YouTube_Videos'] = video_links_str
                    else:
                        # Directly map other keys
                        processed_data[key] = value

        return processed_data
    def process_release_data_to_dict_old(self, u_id, release_data):
        """
        Processes release data into a dictionary of key-value pairs.

        :param u_id: Unique identifier for the release.
        :param release_data: Data of the release.
        :return: Dictionary with structured release data.
        """
        # Define the label mapping
        label_to_column_mapping = {
            'Label': 'Discogs_Labels',
            'Format': 'Discogs_Formats',
            'Country': 'Discogs_Countries',
            'Released': 'Discogs_Years',
            'Genre': 'Discogs_Genres',
            'Style': 'Discogs_Styles'
        }

        # Initialize the dictionary with u_id and Discogs URL
        processed_data = {
            'u_id': u_id,
            'Discogs_Urls': release_data.get('Discogs_Urls', '')
        }

        # Process table content
        for label, value in release_data.get('table_content', {}).items():
            column_name = label_to_column_mapping.get(label)
            if column_name:
                processed_data[column_name] = value

        # Process tracklist content
        tracklist_str = ', '.join([' - '.join(item) for item in release_data.get('tracklist_content', [])])

        processed_data['Discogs_Tracklist'] = tracklist_str

        # Process video links content
        video_links_str = ', '.join(release_data.get('video_links_content', []))
        processed_data['Discogs_YouTube_Videos'] = video_links_str

        # Extracting artist and title from best_search_release_info if available
        processed_data['Discogs_Artists'] = release_data.get('best_search_release_info', {}).get('Discogs_Artists', '')
        processed_data['Discogs_Titles'] = release_data.get('best_search_release_info', {}).get('Discogs_Title', '')

        return processed_data

    def set_current_content(self, release_info_dict):
        self.release_table_content = release_info_dict['table_content']
        self.release_tracklist_content = release_info_dict['tracklist_content']
        self.release_video_links_content = release_info_dict['video_links_content']
        self.current_url = release_info_dict['Discogs_Urls']



    """def process_search_queue(self, search_queue):
        for index, url in search_queue:
            release_content = self.get_current_release_url_content(url)
            self.update_release_info_in_dataframe(index, release_content)"""






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

        # Find the tracklist table (considering both possible class names)
        table = SoupObj.find('table', class_="tracklist_3QGRS") or SoupObj.find('table', class_="tracklist_4KOvL")
        if not table:
            return release_tracklist_content  # Return empty list if tracklist table is not found

        # Iterate through each track row in the table
        counter = 0
        for tr in table.find_all('tr'):
            if tr.class_ == 'heading_24HyI':
                continue
            counter += 1
            # Find and clean the artist's name, fallback to primary artist if not present
            artist_line = tr.find('td', class_='artist_1mUch') or tr.find('td', class_='artist_3zAQD')
            if not artist_line:
                artist = primary_artist
            else:
                artist = artist_line.text.strip()
                if not artist or artist == '' or artist == ' ':
                    artist = primary_artist


            # Find and clean the track name, considering different classes
            track_name_line = tr.find('td', class_='trackTitleWithArtist_3ZfhC') or \
                              tr.find('td', class_='trackTitle_CTKp4') or \
                              tr.find('td', class_='trackTitleNoArtist_ANE8Q')
            track_name = track_name_line.text.strip() if track_name_line else "Unknown Track"

            # Find and clean the duration, use "Unknown Duration" if not present
            duration_line = tr.find('td', class_='duration_25zMZ') or \
                            tr.find('td', class_='duration_2t4qr') or \
                            tr.find('td', class_='duration_2HJZf') or \
                            tr.find('td', class_='duration_3JAzI')
            duration = duration_line.text.strip() if duration_line else ""

            # Find and clean the track position
            track_position_line = tr.find('td', class_='trackPos_2RCje')
            track_position = track_position_line.text.strip() if track_position_line else str(counter)

            # Convert positions like A, B, A1, A2, etc. into the counter value
            if track_position.isdigit():
                track_position = str(track_position)
            else:
                track_position = str(counter)  # Use the current counter value for non-numeric positions

            #remove any starting or trailing - from duration, artist, and track_name
            duration, artist, track_name, track_position = self.clean_strings(duration, artist, track_name, track_position)

            # cd symbol representing home directory in mac
            home = os.path.expanduser('~')

            # Append artist, track name, and duration to the content list
            release_tracklist_content.append([artist, track_name, duration, track_position])

        return release_tracklist_content

    @staticmethod
    def clean_strings(duration, artist, track_name, track_position):
        while any(word.startswith('-') or word.endswith('-') or word.startswith(' ') or word.endswith(' ')
            or word.startswith('–') or word.endswith('–')
                  for word in
                  [duration, artist, track_name, track_position]):
            duration = duration.lstrip('-').rstrip('-').lstrip(" ").rstrip(" ").strip()
            duration = duration.lstrip('–').rstrip('–').lstrip(" ").rstrip(" ").strip().strip("–")
            artist = artist.lstrip('-').rstrip('-').strip().lstrip(" ").rstrip(" ").strip()
            artist = artist.lstrip('–').rstrip('–').strip().lstrip(" ").rstrip(" ").strip().strip("–")
            track_name = track_name.lstrip('-').rstrip('-').strip().lstrip(" ").rstrip(" ").strip()
            track_name = track_name.lstrip('–').rstrip('–').strip().lstrip(" ").rstrip(" ").strip().strip("–")
            track_position = track_position.lstrip('-').rstrip('-').strip().lstrip(" ").rstrip(" ").strip()
            track_position = track_position.lstrip('–').rstrip('–').strip().lstrip(" ").rstrip(" ").strip().strip("–")
        return duration, artist, track_name, track_position

    def get_release_tracklist_content_backup(self, SoupObj):
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

        for tr in table.find_all('tr'):
            th = tr.find('th')
            td = tr.find('td')
            if th:
                info_label = th.text.rstrip(":")
                info = td.text if td else 'Unknown Info'
                release_table_content[info_label] = info
        return release_table_content


    """def get_release_table_content_backup(self, SoupObj):
       Backup Process Logic for Getting Content
       # Iterate through each row in the table and extract the label and corresponding information
        for tr in table.find_all('tr'):
            info_label = tr.find('th').text.rstrip(":") if tr.find('th') else 'Unknown Label'
            info = tr.find('td').text if tr.find('td') else 'Unknown Info'
            release_table_content[info_label] = info

        return release_table_content"""

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
                    youtube_video_title = video.find('div', class_='title_26yzZ') if video.find('div',
                                                                                                 class_='title_26yzZ') else video.find('div',
                                                                                                                                       class_='title_3aPZV')
                    if youtube_video_title:
                        youtube_video_title=youtube_video_title.text.strip()


                    release_video_links_content.append((youtube_video_title, youtube_video_url))

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


