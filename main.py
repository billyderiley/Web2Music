import time

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import googleapiclient.discovery as youtube_api
class BaseScraper:
    def __init__(self):
        self.df = self.createDF()

    def createDF(self):
        #df = pd.DataFrame(columns=["Release Artists", "Release Titles", "Discogs Url", "Discogs Tags", "SoundCloud Url", "Youtube Url"])
        df = pd.DataFrame(columns=["Discogs", "Youtube", "Spotify", "SoundCloud", "BandCamp"])
        return df

    def createSoupObjFromUrl(self, base_url):
        content = ''
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        try:
            response = requests.get(base_url, headers=headers)
            print(response.status_code)
            response.raise_for_status()  # Raises HTTPError if the HTTP request returned an unsuccessful status code
            content = response.text
        except requests.RequestException as e:
            print(f"Error fetching URL {base_url}: {e}")
        SoupObj = BeautifulSoup(content, 'html.parser')
        return SoupObj


class DiscogsSearchScraper():
    def __init__(self):
        self.search_options_dict = {}

    def get_search_page_content(self, base_url):
        Base_Scraper = BaseScraper()
        SoupObj = Base_Scraper.createSoupObjFromUrl(base_url)
        print(base_url)
        print(f"is this soupy? {SoupObj}")
        aside_navbar_content = self.get_aside_navbar_content(SoupObj)
        center_releases_content = self.get_center_releases_content(SoupObj)
        return aside_navbar_content, center_releases_content


    def get_aside_navbar_content(self, SoupObj):
        #print("testing_aside_navbar_content")
        aside_navbar_content = {}
        left_side_menu_html = SoupObj.find(id="page_aside")
        left_side_menu_html.find_all()
        left_side_facets = left_side_menu_html.find_all('h2', class_="facets_header")
        if len(left_side_facets) == 1:
            print("No additional search options, remove some to proceed.")
            return aside_navbar_content
        applied_filters_bool = len(left_side_facets) == 6
        for i, h2_ in enumerate(left_side_facets):
            print(f"this is important {i, h2_}")
            print(len(left_side_facets))
            if len(left_side_facets) == 6 and i == 0:
                #print('did this ')
                applied_filters_ul = h2_.findNext('ul', class_='explore_filters facets_nav selected_facets')
                applied_filters = [li.text.rstrip(" ").lstrip(" ").strip("\n").strip("\n") for li in applied_filters_ul.find_all('li')]
                print(f"Applied Filters are: {applied_filters}")
                continue
            else:
                #print('did this instead')
                #print(h2_.findNext('div', class_='more_facets_dialog'))
                if h2_.findNext('div', class_='more_facets_dialog') is None:
                    __intermediate_level__ = h2_.findNext('ul', class_='no_vertical facets_nav')
                    facets_nav_uls = [__intermediate_level__]
                    print("more facets is none")

                else:
                    __intermediate_level__ = h2_.findNext('div', class_="more_facets_dialog")
                    facets_nav_uls = __intermediate_level__.find_all('ul')
                    print("more facets is not none")

            header_name = h2_.getText()
            print("checkme123")
            print(__intermediate_level__)
            print(header_name)
            self.search_options_dict[header_name] = {}
            aside_navbar_content[header_name] = {}

            for ul in facets_nav_uls:
                print(ul)
                try:
                    for li in ul.find_all('li'):
                        facet_name = li.find('span', class_='facet_name').text
                        print(facet_name)
                        #print(f"TEST 3 {facet_name}")
                        href = li.find('a')['href']
                        print(href)
                        #print(f"TEST 4 {href}")
                        search_term = href.split('?')[-1]
                        #print(f"TEST 5 {search_term}")
                        #self.search_options_dict[header_name][facet_name] = search_term
                        aside_navbar_content[header_name][facet_name] = search_term
                        #print(aside_navbar_content)
                    #print(self.search_options_dict)
                except AttributeError:
                    pass
        return aside_navbar_content

    def get_center_releases_content(self, SoupObj):
        a_tags = SoupObj.find_all('a', class_='thumbnail_link')
        center_releases_content = []
        """   if type(releases) is not list:
            raise TypeError
        """
        # Extract the 'aria-label' and 'href' attributes from each <a> tag
        for tag in a_tags:
            aria_label = tag.get('aria-label')
            aria_label_parts = aria_label.split(" - ")
            href = tag.get('href')
            if len(aria_label_parts) == 2:
                artist = aria_label_parts[0].strip()
                # Need to clean the artist string
                artist = re.sub(r'\(\d+\)', '', artist)
                title = aria_label_parts[1].strip()

                # Check if the title already exists in the releases
                if not any(release['Release Titles'] == title for release in center_releases_content):
                    release_info = {
                        "Release Artists": artist,
                        "Release Titles": title,
                        "Discogs Url": href
                    }
                    center_releases_content.append(release_info)
        return center_releases_content

""" 
       # Check if the combination of artist and title is already in the DataFrame
        if not ((self.df["Release Artists"] == artist) & (self.df["Release Titles"] == title)).any():
            new_row = pd.DataFrame([{"Release Artists": artist, "Release Titles": title, "Discogs Url": href}])
            self.df = pd.concat([self.df, new_row], ignore_index=True)
            self.df.to_csv(path_or_buf="hello.csv")
"""


class DiscogsReleaseScraper():

    def __init__(self):
        self.Release_Dataframe = DiscogsReleaseScraper.create_Release_Dataframe()


    @staticmethod
    def create_Release_Dataframe():
        release_dataframe = pd.DataFrame(columns=['Release Title','Release Artist', 'Tracklist', 'Label', 'Genre Tags'
            , 'Style Tags', 'Country', 'Year', 'Formats','Discogs_Url', 'Youtube Urls', 'Album Art'])
        return release_dataframe

    def get_release_page_content(self, base_url):
        Base_Scraper = BaseScraper()
        SoupObj = Base_Scraper.createSoupObjFromUrl(base_url)
        release_table_content = self.get_release_table_content(SoupObj)
        release_tracklist_content = self.get_release_tracklist_content(SoupObj)
        # release_video_links_content = self.parse_video_links(youtube_API_Key)

    def get_release_tracklist_content(self, SoupObj):
        release_tracklist_content = []
        #SoupScraper.get_page_content(base_url)
        table = SoupObj.find('table', class_="tracklist_3QGRS")
        tr = table.find_all('tr')
        for tr in table.find_all('tr'):
            #label = tr.find('th').text
            artist_line = tr.find('td', class_='artist_3zAQD')
            artist = artist_line.find('a', class_='link_1ctor link_15cpV').text
            # Need to clean the artist string
            artist = re.sub(r'\(\d+\)', '', artist)
            track_name_line = tr.find('td', class_='trackTitle_CTKp4')
            track_name = track_name_line.find('span', class_='trackTitle_CTKp4').text
            release_tracklist_content.append([artist, track_name])
        return release_tracklist_content

    def get_release_table_content(self, SoupObj):
        release_table_content = {}
        table = SoupObj.find('table', class_="table_1fWaB")
        #tr = table.find_all('tr')
        for tr in table.find_all('tr'):
            info_label = tr.find('th').text
            info = tr.find('td').text
            release_table_content[info_label] = info
        return release_table_content

    def get_release_video_links_content(self,SoupObj, youtube_api_key):
        video_player = SoupObj.find('ul', class_='videos_1xVCN')
        release_video_links_content = []
        YouScrape = YoutubeAPI(youtube_api_key)
        for video in video_player.find_all('li'):
            time.sleep(1)
            video_title = video.find('div', class_='title_26yzZ').text
            # print(video_title)
            video_title, video_url, video_id = YouScrape.search_youtube_video(video_title)
            # print(video_url)
            release_video_links_content.append([video_title, video_url, video_id])
        return release_video_links_content

class YoutubeScraper():
    def __init__(self):
        pass

class YoutubeAPI(YoutubeScraper):
    # my key is : AIzaSyAUCZgYUeP4Xcj-kw88V6X7VfcjQdBPtAg
    def __init__(self, api_key):
        super().__init__()
        self.api_key = api_key
        self.youtube = youtube_api.build('youtube', 'v3', developerKey=self.api_key)

    def search_youtube_video(self, search_term):
        request = self.youtube.search().list(
            q=search_term,
            part='snippet',
            maxResults=1,
            type='video'
        )
        response = request.execute()

        if response['items']:
            first_result = response['items'][0]
            video_title = first_result['snippet']['title']
            video_id = first_result['id']['videoId']
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            return video_title, video_url, video_id
        else:
            return "No results found", None


class YoutubeVideoCommentsBrowser(YoutubeAPI):
    def __init__(self, api_key, data = None, order = None):
        self.api_key = api_key
        if data is None or order is None:
            self.data = {}
            self.order = []
        else:
            self.data = data
            self.order = order
        super().__init__(api_key)

    def get_youtube_video_comments(self, video_id):
        youtube_comments = []
        print(f" printing comments for video id: {video_id}")
        request = self.youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100
        )
        response_json = request.execute()
        # Loop processing response items
        for i, item in enumerate(response_json['items']):
            comment = item['snippet']['topLevelComment']['snippet']
            text = comment['textDisplay'],
            author = comment['authorDisplayName'],
            likes = comment.get('likeCount', 0),
            published_at = comment['publishedAt'],
            updated_at = comment.get('updatedAt', 'Not Updated')
            comment_data = {
                'text': text,
                'author': author,
                'likes': likes,
                'published_at': published_at,
                'updated_at': updated_at
            }
            youtube_comments.append(comment_data)
        return youtube_comments

    def add_entry(self, key, comment_data):
        self.data[key] = comment_data
        self.order.append(key)

    def get_entry_by_key(self, key):
        return self.data.get(key, None)

    def get_entry_by_index(self, index):
        if index < 0 or index >= len(self.order):
            return None
        key = self.order[index]
        return self.data[key]

    def search_partial_text(self, text):
        results = {}
        for key, comment_data in self.data.items():
            if text.lower() in comment_data['text'].lower():
                results[key] = comment_data
        return results


class DiscogsSearch(DiscogsSearchScraper):
    def __init__(self, start_url):
        super().__init__()
        self.base_url = start_url
        self.current_url = start_url
        aside_navbar_content, center_releases_content = self.get_search_page_content(start_url)
        #super(DiscogsSearch, self).get_search_page_content(start_url)
        self.updateSearchOptions(aside_navbar_content=aside_navbar_content)

    def updateCurrentUrl(self, url):
        self.current_url = url

    def updateSearchOptions(self, aside_navbar_content):
        self.search_options_dict = aside_navbar_content

    #def get_search_page_content(self, base_url):
    #    super().get_search_page_content(base_url)
    #    self.updateCurrentUrl(base_url)

    def get_search_options(self):
        #aside_navbar_content, center_releases_content = base.get_search_page_content(base_url)
        #print(self.search_options_dict)
        print("here are the optios you can search")
        for k, nested_dict in self.search_options_dict.items():
            print(f"Key = {k}")
            for nested_key, nested_value in nested_dict.items():
                print(f"    Nested Key: {nested_key}, Nested Value: {nested_value}")

    def search_page_user_interaction(self):
        print(self.current_url)
        print([f"{i}: {genre}" for i, genre in enumerate(self.search_options_dict.keys(), 1)])
        enter_key1 = int(input(""))-1 #-1 to get index number
        #print(self.search_options_dict[enter_key1])
        items = list(self.search_options_dict.items())
        print(f"{items} did items print")
        key, value = items[enter_key1]
        print([f"{i}: {genre}" for i, genre in enumerate(self.search_options_dict[key].keys(), 1)])
        enter_key2 = int(input(""))-1
        items = list(self.search_options_dict[key].items())
        key2, value2 = items[enter_key2]
        print(self.search_options_dict[key][key2])
        new_search_term = self.search_options_dict[key][key2]
        new_discogs_search_url = self.add_discogs_search_term(new_search_term)
        aside_navbar_content, center_releases_content = self.get_search_page_content(new_discogs_search_url)
        self.updateSearchOptions(aside_navbar_content)
        self.updateCurrentUrl(new_discogs_search_url)
        #print(aside_navbar_content)
        #print("who goes there")
        #self.get_page_content(self.base_url)
        #self.parse_search_center_content()
        self.search_page_user_interaction()


    def add_discogs_search_term(self, new_search_term):
        print(f"this is new search term {new_search_term}")
        #Check if discogs url is vanilla or already contains a term
        if self.current_url.endswith("search"):
            new_discogs_search_url = self.current_url +"?"+new_search_term
        else:
            if new_search_term in self.current_url:
                new_discogs_search_url = self.current_url
            else:
                new_discogs_search_url = self.current_url + "&" + new_search_term
        print(f"this is new url {new_discogs_search_url}")
        return new_discogs_search_url

    def remove_discogs_search_term(self, remove_search_term):
        new_discogs_search_url = self.current_url.strip(remove_search_term)
        return new_discogs_search_url





"""

class SpotifyAPI:
    # Methods to interact with Spotify API

class DataHandler:
    # Methods for data storage and manipulation

class MusicScraperApp:
    # Main application logic

# Example usage
app = MusicScraperApp()
app.run_scraper()
app.create_playlist_from_scraped_data()
"""







def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
