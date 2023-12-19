from bs4 import BeautifulSoup
import requests
import time
from urllib.request import Request, urlopen
from selenium import webdriver
import pandas as pd

class BaseScraper:
    def __init__(self):
        self.BASE_df = self.createDF()
        self.Soupy_Url_Dict = {}
        self.base_discogs_url = "https://discogs.com"
        self.base_discogs_search_url = "https://discogs.com/search"

    def createDF(self):
        #df = pd.DataFrame(columns=["Release Artists", "Release Titles", "Discogs Url", "Discogs Tags", "SoundCloud Url", "Youtube Url"])
        df = pd.DataFrame(columns=["Discogs", "Youtube", "Spotify", "SoundCloud", "BandCamp"])
        return df

    def createSoupObjFromUrlUrllib(self, url):
        #opener = AppURLopener()
        #response = opener.open(url)
        #webpage = response.read()




        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req, timeout=10).read()
        webpage = webpage.decode('utf-8')
        SoupObj = BeautifulSoup(webpage, 'html.parser')
        return SoupObj

    def createSoupObjFromUrlSelenium(self, url):
        driver = webdriver.Chrome()  # Specify the correct path to the chromedriver
        #driver = webdriver.Chrome()  # Update the path
        driver.get(url)

        # Optionally, wait for some time or until some condition is met
        time.sleep(0.1)  # Waits for 5 seconds

        content = driver.page_source
        driver.quit()

        SoupObj = BeautifulSoup(content, 'html.parser')
        return SoupObj

    def createSoupObjFromUrl(self, url):
        content = ''
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        content = response.text
        status_code = response.status_code
        print(f"status code {status_code}")
        """
        try:
            response = requests.get(url, headers=headers)
            #print(response.status_code)
            status_code = response.status_code # Raises HTTPError if the HTTP request returned an unsuccessful status code
            content = response.text
        #except requests.RequestException as e:
            print(f"Error fetching URL {url}: {e}")
        """
        SoupObj = BeautifulSoup(content, 'html.parser')
        return SoupObj

    def createSoupObjFromUrl_release(self, base_url):
        content = ''
        try:
            user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36'
            # header variable
            headers = {'User-Agent': user_agent}
            response = requests.get(base_url, headers=headers)
            #print(response.status_code)
            response.raise_for_status()  # Raises HTTPError if the HTTP request returned an unsuccessful status code
            content = response.text
        except requests.RequestException as e:
            print(f"Error fetching URL {base_url}: {e}")
        SoupObj = BeautifulSoup(content, 'html.parser')
        return SoupObj


    def get_Soup_from_url(self, url):
        if url in self.Soupy_Url_Dict.keys():
            try:
                SoupObj = self.Soupy_Url_Dict[url]
            except KeyError:
                SoupObj = self.createSoupObjFromUrlSelenium(url)
                #SoupObj = self.createSoupObjFromUrl_release(url)
                self.Soupy_Url_Dict[url] = SoupObj
        else:
            SoupObj = self.createSoupObjFromUrlSelenium(url)
            #SoupObj = self.createSoupObjFromUrl_release(url)
            self.Soupy_Url_Dict[url] = SoupObj
        return SoupObj
