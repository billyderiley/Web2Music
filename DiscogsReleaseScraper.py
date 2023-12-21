from BaseScraper import BaseScraper
import pandas as pd
import os


class DiscogsReleaseScraper(BaseScraper):

    def __init__(self,):
        super().__init__()
        #self.Search_Dataframe = Search_Dataframe
        self.Release_Dataframe = self.create_Release_Dataframe()
        self.current_url_release_info_dict = None
        #if data_handler is not None:
        #    self.data_handler = data_handler
        #else:
        #    self.data_handler = DataHandler()


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


    #@staticmethod
    def create_Release_Dataframe(self):
        release_dataframe = pd.DataFrame(columns=['Discogs_Titles','Discogs_Artists', 'Discogs_Tracklist', 'Discogs_Labels', 'Discogs_Genres'
            , 'Discogs_Styles', 'Discogs_Countries', 'Discogs_Years', 'Discogs_Formats','Discogs_Urls', 'Discogs_YouTube_Videos'])
        return release_dataframe

    def save_release_dataframe(self):
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
        return csv_files

    #def load_release_dataframe(self):
   #     self.Release_Dataframe = pd.read_csv('release_dataframe.csv')

    def display_dataframe(self):
        print(self.Release_Dataframe)


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


    def update_release_table_content(self, index, release_table_content):
        # Define the mapping for table content
        label_to_column_mapping = {
            'Label': 'Discogs_Labels',
            'Format': 'Discogs_Formats',
            'Country': 'Discogs_Countries',
            'Released': 'Discogs_Years',
            'Genre': 'Discogs_Genres',
            'Style': 'Discogs_Styles',
            'Tracklist': 'Discogs_Tracklist',
            # Add more mappings as needed
        }

        row_updates = {}
        for info_label, info in release_table_content.items():
            print("testing123")
            print(info)
            column_name = label_to_column_mapping.get(info_label)
            print(column_name)
            if column_name:
                row_updates[column_name] = info
                #    # Append the new tag to the existing tags
                #    existing_tags = self.df.at[index, column_name]
                #    new_tags = existing_tags + ', ' + info if existing_tags else info
                #    row_updates[column_name] = new_tags
                #else:   # If the column is not 'Discogs_Tags'

        for key, value in row_updates.items():
            self.Release_Dataframe.at[index, key] = value

    def update_release_tracklist_content(self, index, release_tracklist_content):
        # Convert each inner list to a string and then join all strings
        tracklist_str = ', '.join([' - '.join(item) for item in release_tracklist_content])
        # Assign the string to the DataFrame
        self.Release_Dataframe.at[index, 'Discogs_Tracklist'] = tracklist_str

    def update_release_video_links_content(self, index, release_video_links_content):
        # Convert video links list to a single string format
        video_links_str = ', '.join(release_video_links_content)
        self.Release_Dataframe.at[index, 'Discogs_YouTube_Videos'] = video_links_str

    def add_new_release(self, release_content):
        print(f"this should be discogs url {release_content['Discogs_Urls']}")
        print(f"this should be video urls list {release_content['video_links_content']}")
        # Check if the release already exists in the DataFrame
        if self.is_duplicate(release_content['Discogs_Urls']):
            return  # Avoid adding duplicate entry

        # Create a new row in the DataFrame
        new_index = len(self.Release_Dataframe)
        self.Release_Dataframe.at[new_index, 'Discogs_Urls'] = release_content['Discogs_Urls']
        self.update_release_table_content(new_index, release_content['table_content'])
        self.update_release_tracklist_content(new_index, release_content['tracklist_content'])
        self.update_release_video_links_content(new_index, release_content['video_links_content'])

    def is_duplicate(self, discogs_url):
        # Assuming 'Discogs_URLS' is a unique identifier for each release
        #url = table_content.get('Discogs_URLS')
        if discogs_url in self.Release_Dataframe['Discogs_Urls'].values:
            return True
        return False

    def main_scraping_method(self, url_list):
        for url in url_list:
            release_content = self.navigate_to_release_url(url)
            self.add_new_release(release_content)

    def get_release_tracklist_content(self, SoupObj):
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

        return release_tracklist_content

    def get_release_table_content(self, SoupObj):
        release_table_content = {}
        table = SoupObj.find('table', class_="table_1fWaB")
        #tr = table.find_all('tr')
        for tr in table.find_all('tr'):
            info_label = tr.find('th').text.rstrip(":")
            #print(f"check this {info_label}")
            info = tr.find('td').text
            #print(f"check this {info}")
            release_table_content[info_label] = info
            #print(info_label, info)
            #print("whats this")
        return release_table_content

    def get_release_video_links_content(self, SoupObj):
        video_player = SoupObj.find('ul', class_='videos_1xVCN')
        video_player_list_items = video_player.find_all('li')
        release_video_links_content = []
        for i, video in enumerate(video_player_list_items):
            img_tag = video.find('img', class_='thumbnail_f0Yrr')
            if img_tag and 'src' in img_tag.attrs:
                img_src = img_tag['src']
                # Extract the YouTube video ID from the img src
                youtube_video_id = self.extract_youtube_id(img_src)
                youtube_video_url = 'https://www.youtube.com/watch?v=' + youtube_video_id
                release_video_links_content.append(youtube_video_url)

        return release_video_links_content

    def extract_youtube_id(self, url):
        # Extract the part of the URL between 'vi/' and '/default.jpg'
        if 'vi/' in url and '/default.jpg' in url:
            start = url.find('vi/') + 3
            end = url.find('/default.jpg', start)
            return url[start:end]
        return None  # Return None if the pattern is not found
