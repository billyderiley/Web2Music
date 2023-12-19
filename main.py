import time
import os
import threading
import requests
import urllib.request
from urllib.request import Request, urlopen
class AppURLopener(urllib.request.FancyURLopener):
    version = "Mozilla/5.0"


client_id = '2ea9899462614265a2b26b43c68cf72a'
client_secret = 'fb29534f29134f618623b02cfe8dbc65'
redirect_uri = 'https://open.spotify.com'

from bs4 import BeautifulSoup
from selenium import webdriver


#driver = webdriver.Chrome()  # Specify the correct path to the chromedriver
import time
import pandas as pd
pd.set_option('display.max_columns', None)
import re
import googleapiclient.discovery as youtube_api
import random

import spotipy
from spotipy.oauth2 import SpotifyOAuth





"""
class YoutubeScraper():
    def __init__(self):
        pass

class YoutubeAPI(YoutubeScraper):
    # my key is : AIzaSyAUCZgYUeP4Xcj-kw88V6X7VfcjQdBPtAg
    def __init__(self, api_key='AIzaSyAUCZgYUeP4Xcj-kw88V6X7VfcjQdBPtAg'):
        super().__init__()
        self.api_key = api_key
        self.youtube = youtube_api.build('youtube', 'v3', developerKey=self.api_key)

    def search_youtube_video(self, search_term):
        with requests.Session() as session:
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

"""





# Your code for user interaction here


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
