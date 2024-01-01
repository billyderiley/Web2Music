from bs4 import BeautifulSoup
import requests
import time
import random
import datetime
#from urllib.request import Request, urlopen
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
import pandas as pd
import sqlite3
import pickle

from DatabaseManager import DatabaseManager


class BaseScraper(DatabaseManager):
    def __init__(self):
        super().__init__()
        #self.db_path = db_path
        #elf.create_db_table()
        self.BASE_df = self.createDF()
        #self.Soupy_Url_Dict = {}
        #self.load_soup_dict_from_csv('soup_urls.csv')  # Load URLs from CSV on initialization

        self.base_discogs_url = "https://discogs.com"
        self.base_discogs_search_url = "https://discogs.com/search"



    def createDF(self):
        #df = pd.DataFrame(columns=["Release Artists", "Release Titles", "Discogs Url", "Discogs Tags", "SoundCloud Url", "Youtube Url"])
        df = pd.DataFrame(columns=["Discogs", "Youtube", "Spotify", "SoundCloud", "BandCamp"])
        return df

    def create_driver_with_random_user_agent(self):
        # Create a random user agent
        user_agent = UserAgent().random

        # Set Chrome options for headless browsing and user agent
        options = Options()
        options.add_argument(f"user-agent={user_agent}")
        # Set the browser to run headlessly
        options.add_argument("--headless")

        # Obfuscate WebDriver
        options.add_argument("disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # Create the Chrome WebDriver with the specified options
        driver = webdriver.Chrome(options=options)

        # Modify JavaScript properties to prevent WebDriver detection
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        return driver

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
        driver = self.create_driver_with_random_user_agent()
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
        """
        Retrieves or creates a BeautifulSoup object from a given URL.

        If the URL data exists in the database and is less than a day old,
        it returns the stored BeautifulSoup object. Otherwise, it fetches a new
        BeautifulSoup object using Selenium, updates the database, and returns it.

        :param url: The URL to fetch or retrieve the BeautifulSoup object for.
        :return: BeautifulSoup object.
        """
        # Retrieve the stored soup object and its timestamp from the database
        soup_obj, timestamp = self.get_data_from_db(url)

        # Check if a valid soup object is retrieved and if it's a search URL
        if soup_obj:
            if 'search' in url:
                # Determine if the search soup object is older than 1 day
                is_outdated = datetime.datetime.now() - timestamp > datetime.timedelta(days=1)
                if is_outdated:
                    # The data is old, so delete it and fetch a fresh soup object
                    self.delete_and_refresh_soup(url)
                    soup_obj = self.fetch_and_save_new_soup(url)
            return soup_obj

        else:
            # No valid soup object found, fetch a new one
            print(f"No valid soup object found for URL: {url}")
            soup_obj = self.fetch_and_save_new_soup(url)
            return soup_obj

    def delete_and_refresh_soup(self, url):
        """
        Deletes an outdated soup object from the database and fetches a new one.

        :param url: The URL associated with the soup object to refresh.
        :return: The refreshed BeautifulSoup object.
        """
        self.delete_data_from_db(url)
        return self.fetch_and_save_new_soup(url)

    def fetch_and_save_new_soup(self, url):
        """
        Fetches a new BeautifulSoup object using Selenium and saves it to the database.

        :param url: The URL to fetch the BeautifulSoup object for.
        :return: The newly fetched BeautifulSoup object.
        """
        SoupObj = self.createSoupObjFromUrlSelenium(url)
        self.save_data_to_db(url, SoupObj)
        return SoupObj


    def get_Soup_from_url_backup(self, url):
        # Check if URL data exists in the database
        if 'search' in url:
            soup_obj = self.get_data_from_db(url)
        if soup_obj:
            return soup_obj
        else:
            # Create new soup object if not found in database
            SoupObj = self.createSoupObjFromUrlSelenium(url)
            # Save the new soup object to the database
            self.save_data_to_db(url, SoupObj)
            return SoupObj

        """
       if url in self.Soupy_Url_Dict.keys():
            try:
                SoupObj = self.Soupy_Url_Dict[url]
            except KeyError:
                SoupObj = self.createSoupObjFromUrlSelenium(url)
                #SoupObj = self.createSoupObjFromUrl_release(url)
                self.Soupy_Url_Dict[url] = SoupObj
        else:
            SoupObj = self.createSoupObjFromUrlSelenium(url)
    
            self.Soupy_Url_Dict[url] = SoupObj.prettify()  # Store the prettified HTML
            #self.save_soup_dict_to_csv('soup_urls.csv')   # Save each time a new URL is added
            self.save_data_to_db(url, SoupObj.prettify())
    
            #SoupObj = self.createSoupObjFromUrl_release(url)
            self.Soupy_Url_Dict[url] = SoupObj
        return SoupObj.prettify()"""



    """def save_soup_dict_to_csv(self, filepath):
        df = pd.DataFrame(list(self.Soupy_Url_Dict.items()), columns=['Url', 'Soup_obj'])
        df.to_csv(filepath, index=False)

    def load_soup_dict_from_csv(self, filepath):
        try:
            df = pd.read_csv(filepath)
            self.Soupy_Url_Dict = pd.Series(df.Soup_obj.values, index=df.Url).to_dict()
        except FileNotFoundError:
            print("CSV file not found. Starting with an empty dictionary.")"""

    def random_sleep(self):
        current_hour = datetime.datetime.now().hour

        # Define ranges for work hours and off hours
        work_hours = range(9, 17)  # From 9 AM to 5 PM
        min_delay, max_delay = (10, 100) if current_hour in work_hours else (40, 100)

        # Generate a random delay with a higher probability of lower values during work hours
        delay = random.triangular(min_delay, max_delay, min_delay)
        print(f"Sleeping for {delay:.2f} milliseconds")
        time.sleep(delay / 1000)  # Convert milliseconds to seconds

    def reload_webpage(self, url):
        self.random_sleep()
        soup_obj = self.createSoupObjFromUrlSelenium(url)
        self.save_data_to_db(url, soup_obj)
        print(f"Reloaded webpage and updated database for URL: {url}")
        return url

    def reload_all_webpages(self, batch_size=5):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT url FROM soups")
        urls = [url_tuple[0] for url_tuple in cursor.fetchall()]
        conn.close()

        self.execute_in_batches(urls, self.reload_webpage, batch_size)
        print("All webpages reloaded and updated.")



    """def reload_all_webpages(self, batch_size=5):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT url FROM soups")
        urls = [url_tuple[0] for url_tuple in cursor.fetchall()]
        conn.close()

        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            future_to_url = {executor.submit(self.reload_webpage, url): url for url in urls}
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    future.result()
                except Exception as exc:
                    print(f"{url} generated an exception: {exc}")

        print("All webpages reloaded and updated.")"""

    def is_update_needed(self, url):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT timestamp FROM soups WHERE url = ?", (url,))
        data = cursor.fetchone()
        conn.close()

        if data:
            last_updated = datetime.datetime.strptime(data[0], '%Y-%m-%d %H:%M:%S')
            return (datetime.datetime.now() - last_updated) > datetime.timedelta(days=1)
        else:
            return True  # No data means an update is needed

    """def reload_all_webpages(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT url FROM soups")
        urls = cursor.fetchall()
        conn.close()

        for url_tuple in urls:
            url = url_tuple[0]
            # Fetch and update the Soup object
            soup_obj = self.createSoupObjFromUrlSelenium(url)
            self.save_data_to_db(url, soup_obj)
            print(f"Reloaded webpage and updated database for URL: {url}")"""
