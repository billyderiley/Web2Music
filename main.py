import time

import threading
import requests
from bs4 import BeautifulSoup
import pandas as pd
pd.set_option('display.max_columns', None)
import re
import googleapiclient.discovery as youtube_api
import random
class BaseScraper:
    def __init__(self):
        self.df = self.createDF()
        self.Soupy_Url_Dict = {}
        self.base_discogs_url = "https://discogs.com"
        self.base_discogs_search_url = "https://discogs.com/search"

    def createDF(self):
        #df = pd.DataFrame(columns=["Release Artists", "Release Titles", "Discogs Url", "Discogs Tags", "SoundCloud Url", "Youtube Url"])
        df = pd.DataFrame(columns=["Discogs", "Youtube", "Spotify", "SoundCloud", "BandCamp"])
        return df

    def createSoupObjFromUrl(self, url):
        content = ''
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        #session = requests.Session()
        #response = session.get(url, headers=headers)

        try:
            response = requests.get(url, headers=headers)
            #print(response.status_code)
            response.raise_for_status()  # Raises HTTPError if the HTTP request returned an unsuccessful status code
            content = response.text
        except requests.RequestException as e:
            print(f"Error fetching URL {url}: {e}")

        SoupObj = BeautifulSoup(content, 'html.parser')
        return SoupObj

    def createSoupObjFromUrl_release(self, base_url):
        content = ''
        try:
            response = requests.get(base_url)
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
                SoupObj = self.createSoupObjFromUrl(url)
                self.Soupy_Url_Dict[url] = SoupObj
        else:
            print('here')
            SoupObj = self.createSoupObjFromUrl(url)
            self.Soupy_Url_Dict[url] = SoupObj
        return SoupObj


class DiscogsSearchScraper(BaseScraper):
    def __init__(self, start_url, data_handler = None):
        super().__init__()
        self.search_options_dict = {}

        self.DISCOGS_INTERNAL_MAX_SEARCH_PAGES = 200
        self.aside_navbar_content = None
        self.center_releases_content = None
        self.applied_filters = []
        self.base_url = start_url
        self.current_url = start_url
        self.start_url = start_url
        self.next_page = self.get_next_search_page_url(start_url)
        #if data_handler is None:
        #    self.data_handler = DataHandler()
        #else:
        #    self.data_handler = data_handler

    def get_current_search_page_content(self):
        base_url = self.current_url
        #Base_Scraper = BaseScraper()
        SoupObj = self.get_Soup_from_url(base_url)
        aside_navbar_content, applied_filters, new_applied_filters_list = self.get_aside_navbar_content(SoupObj)
        #self.updateAppliedFilters(self.getAppliedFiltersFromUrl(self.current_url))
        center_releases_content = self.get_center_releases_content(SoupObj)
        print("what is this")
        print(applied_filters)
        return aside_navbar_content, center_releases_content, applied_filters, new_applied_filters_list


    def get_aside_navbar_content(self, SoupObj):
        #print("testing_aside_navbar_content")
        aside_navbar_content = {}
        applied_filters = []
        new_applied_filters_list = []
        left_side_menu_html = SoupObj.find(id="page_aside")
        left_side_menu_html.find_all()
        applied_filters_html = left_side_menu_html.find('ul', class_='explore_filters facets_nav selected_facets')
        if applied_filters_html is not None:
            applied_filters = [li.text for li in applied_filters_html.find_all('li')]
            applied_filters = self.clean_applied_filters(applied_filters)
            first_filt_words = [x.split('+')[0] for x in applied_filters]
        else:
            first_filt_words = []
        left_side_facets = left_side_menu_html.find_all('h2', class_="facets_header")
        #applied_filters_bool = len(left_side_facets) == 6
        for i, h2_ in enumerate(left_side_facets):
            if len(left_side_facets) == 1:
                print("No additional search options, remove some to proceed.")
                return aside_navbar_content, applied_filters
            else:
                if 'Applied Filters' in h2_.text:
                    continue
                #print('did this instead')
                #print(h2_.findNext('div', class_='more_facets_dialog'))
                if h2_.findNext('div', class_='more_facets_dialog') is None:
                    __intermediate_level__ = h2_.findNext('ul', class_='no_vertical facets_nav')
                    facets_nav_uls = [__intermediate_level__]

                else:
                    __intermediate_level__ = h2_.findNext('div', class_="more_facets_dialog")
                    facets_nav_uls = __intermediate_level__.find_all('ul')

            header_name = h2_.getText()
            self.search_options_dict[header_name] = {}
            aside_navbar_content[header_name] = {}
            for ul in facets_nav_uls:
                try:
                    for li in ul.find_all('li'):
                        facet_name = li.find('span', class_='facet_name').text

                        #print(facet_name)
                        #facet_name_spaces_replace = facet_name.replace(' ', '+')
                        #print(facet_name_spaces_replace)
                        #print(applied_filters)
                        # check if the facet_name_spaces_replace is in any of the string values in applied_filters
                       # if any(facet_name_spaces_replace in x for x in applied_filters):
                       #     print("yes")

                        #if facet_name_spaces_replace in [x for x in applied_filters].any() is True

                        href = li.find('a')['href']
                        search_term = href.split('?')[-1]
                        #print(search_term)
                        __search_term = search_term.split('=')
                        ____search_term = [term.split('&') for term in __search_term]
                        # code to flatten list ____search_term into a single 1 dimensional list
                        new_applied_filters_list = [item for sublist in ____search_term for item in sublist]
                        # Reverse the list and assign to a new variable
                        # if new_applied_filters_list equal or larger than 4
                        if len(new_applied_filters_list) >= 2:
                            #print(new_applied_filters_list)
                            #slice last two indexes from new_applied_filters_list out of the list
                            new_applied_filters_list = new_applied_filters_list[:-2]
                            new_applied_filters_list = new_applied_filters_list[::-1]



                        aside_navbar_content[header_name][facet_name] = href
                except AttributeError:
                    pass
        return aside_navbar_content, applied_filters, new_applied_filters_list

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
                print(f"whats this {self.applied_filters}")
                # Check if the title already exists in the releases
                if not any(release['Discogs_Titles'] == title for release in center_releases_content):
                    release_info = {
                        "Discogs_Artists": artist,
                        "Discogs_Titles": title,
                        "Discogs_URLS": self.base_discogs_url+href,
                        "Discogs_Search_Terms": self.applied_filters
                    }
                    center_releases_content.append(release_info)
        return center_releases_content


    def getDiscogsUrl(self,href):
        if href.beginswith('/search'):
            full_discogs_url = self.base_discogs_url+href
        else:
            raise ValueError

        return full_discogs_url

    def get_number_of_search_pages(self, base_url):
        SoupObj = self.get_Soup_from_url(base_url)
        total_pagination_text = SoupObj.find('strong', class_='pagination_total').text
        while total_pagination_text.startswith(' ') or total_pagination_text.startswith('\n'):
            total_pagination_text = total_pagination_text.lstrip(' ')
            total_pagination_text = total_pagination_text.lstrip('\n')
        while total_pagination_text.endswith(' ') or total_pagination_text.endswith('\n'):
            total_pagination_text = total_pagination_text.rstrip(' ')
            total_pagination_text = total_pagination_text.rstrip('\n')
        total_number_of_releases = total_pagination_text.split('of ')[-1]
        selected_results_per_page_value = self.get_results_per_search_page(base_url)
        return total_pagination_text

    def get_results_per_search_page(self, base_url):
        SoupObj = self.get_Soup_from_url(base_url)
        select_tag = SoupObj.find('select', id='limit_bottom')
        selected_option = select_tag.find('option', selected=True) if select_tag else None
        selected_results_per_page_value = selected_option['value'] if selected_option else None
        return selected_results_per_page_value

    def get_next_search_page_url(self, base_url):
        SoupObj = self.get_Soup_from_url(base_url)
        try:
            next_page_url = SoupObj.find('a', class_='pagination_next').get('href')
            next_page_url = self.base_discogs_url+next_page_url
        except AttributeError:
            next_page_url = None
        return next_page_url

    def get_current_page_from_url(self, url):
        if "page=" in url:
            current_page = url.split('page=')[-1]
        else:
            current_page = str(1)
        return current_page

    def create_url_from_page_number(self, url, page_number):
        if "page=" in url:
            stripped_page_url, current_page_number = url.split('page=')[0], url.split('page=')[-1]
            new_page_url = stripped_page_url+page_number
        else:
            if url.endswith('/'):
                new_page_url = url+"?page="+page_number
            elif url.endswith('?'):
                new_page_url = url + "page=" + page_number
            else:
                new_page_url = url + "&page=" + page_number
        return new_page_url

    def search_dict_get(self):
        return self.search_options_dict

    def search_dict_get_label_type_keys(self):
        return self.search_options_dict.keys()

    def search_dict_get_label_type_items(self):
        return self.search_options_dict.items()

    def search_dict_get_label_url_items(self, label_type):
        return self.search_options_dict[label_type].items()

    def search_dict_get_label_url_keys(self, label_type):
        return self.search_options_dict[label_type].keys()

    def search_dict_get_search_term(self, label_type, key):
        items = list(self.search_dict_get_label_url_items(label_type))
        search_term, value2 = items[key]
        new_search_term = self.search_dict_get()[label_type][search_term]
        return new_search_term

    def clean_applied_filters(self, applied_filters):
        if type(applied_filters) is not list:
            applied_filters = [applied_filters]
        clean_applied_filters = []
        for filt in applied_filters:
                #applied_filters.remove(filt)
            while filt.startswith(' ') or filt.startswith('\n'):
                filt = filt.lstrip(' ')
                filt = filt.lstrip('\n')
            while filt.endswith(' ') or filt.endswith('\n'):
                filt = filt.rstrip(' ')
                filt = filt.rstrip('\n')
            # code to transform any spaces into +
            filt = filt.replace(' ', '+')
            clean_applied_filters.append(filt)
            #i need the code to get the search term from the applied filter

        return clean_applied_filters

    def addAppliedFilter(self, applied_filter):
        self.applied_filters.append(applied_filter)
    """
    def remove_applied_filter(self, applied_filters, remove_value):
        #print("here")
        for i in range(1, len(applied_filters)):
            #print(applied_filters[i], i)
            if applied_filters[i] == remove_value:
                # Remove the element and the one before it
                del applied_filters[i - 1:i + 1]
                break  # Break after removing the elements to avoid index errors
        return applied_filters
    """
    def updateAppliedFilters(self, applied_filters):
        #print(f'update appledfilters {applied_filters}')
        if type(applied_filters) is list:
            clean_applied_filters = self.clean_applied_filters(applied_filters)
            self.applied_filters = clean_applied_filters
        else:
            unclean_applied_filters = [applied_filters]
            clean_applied_filters = self.clean_applied_filters(unclean_applied_filters)
            self.applied_filters = clean_applied_filters

    def getUrlFromAppliedFilters(self, applied_filters):
        full_terms_list = []
        # Iterate through the list two items at a time
        for i in range(0, len(applied_filters), 2):
            # Access the current item and the next one
            label_type_s_term = applied_filters[i]
            label_info_s_term = applied_filters[i + 1] if i + 1 < len(applied_filters) else None
            full_term = label_info_s_term+"="+label_type_s_term
            full_terms_list.append(full_term)
            # Now you can process the pair of items
        flattened_string = ''.join([f"?{full_terms_list[0]}"] + [f"&{item}" for item in full_terms_list[1:]] if full_terms_list else [])
        url = self.base_discogs_search_url+flattened_string
        #self.getAppliedFiltersFromUrl(url)
        return url

    def flattenAppliedFiltersList(self, applied_filters):
        flattened_string = ''.join(
            [f"?{applied_filters[0]}"] + [f"&{item}" for item in applied_filters[1:]] if applied_filters else [])
        return flattened_string

    def getAppliedFiltersFromUrl(self, url):
        print(url)
        if "search" not in url:
            raise ValueError
        else:
            if 'page' in url:
                #print('need to remove pages from url')
                #print(url)
                # code to use regex to remove page= and any number after it
                url = re.sub(r'page=\d+', '', url)
                #print(url)
            print("IYA")
            if url.endswith('/'):
                applied_filters = []
            else:
                search_term = url.split('/?')[-1]
                print(search_term)
                search_term = search_term.strip('?')
                search_term = search_term.strip('&')
                search_terms = search_term.split('&')
                # Split each item at '=' and extend them into a flat list
                applied_filters = [item for term in search_terms for item in term.split('=')]
                # return flat_list
            return applied_filters


    def get_search_options(self):
        print("here are the options you can search")
        for k, nested_dict in self.search_options_dict.items():
            print(f"Key = {k}")
            for nested_key, nested_value in nested_dict.items():
                print(f"    Nested Key: {nested_key}, Nested Value: {nested_value}")

    def get_page_range(self, new_discogs_search_url, page_number_range):
        try:
            int(page_number_range)
            return [self.create_url_from_page_number(new_discogs_search_url, page_number_range)]
        except ValueError:
            start_number, end_number = int(page_number_range.split(' ')[0]), int(page_number_range.split(' ')[-1])
            if start_number > end_number:
                ___start_number = end_number
                end_number = start_number
                start_number = ___start_number
            end_number = end_number + 1
            #
            pages_term = ['pages', str(start_number), str(end_number)]
            return [self.create_url_from_page_number(new_discogs_search_url, str(page_num)) for page_num in range(start_number, end_number)]





class DiscogsReleaseScraper(BaseScraper):

    def __init__(self, data_handler = None):
        super().__init__()
        self.Release_Dataframe = DiscogsReleaseScraper.create_Release_Dataframe()
        if data_handler is not None:
            self.data_handler = data_handler
        else:
            self.data_handler = DataHandler()

    @staticmethod
    def create_Release_Dataframe():
        release_dataframe = pd.DataFrame(columns=['Discogs_Titles','Discogs_Artists', 'Discogs_Tracklist', 'Discogs_Labels', 'Discogs_Genres'
            , 'Discogs_Styles', 'Discogs_Countries', 'Discogs_Years', 'Discogs_Formats','Discogs_URLS', 'Discogs_YouTube_Videos'])
        return release_dataframe

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
            column_name = label_to_column_mapping.get(info_label)
            if column_name:
                if column_name == 'Discogs_Tags':
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

    def navigate_to_release_url(self, url):
        print('keyy')
        #Base_Scraper = BaseScraper()
        SoupObj = self.get_Soup_from_url(url)
        print(SoupObj.text)
        release_table_content = self.get_release_table_content(SoupObj)
        release_tracklist_content = self.get_release_tracklist_content(SoupObj)
        release_video_links_content = self.get_release_video_links_content(SoupObj)
        return {
            'table_content': release_table_content,
            'tracklist_content': release_tracklist_content,
            'video_links_content': release_video_links_content
        }

    def add_new_release(self, release_content):
        # Check if the release already exists in the DataFrame
        if self.is_duplicate(release_content['table_content']):
            return  # Avoid adding duplicate entry

        # Create a new row in the DataFrame
        new_index = len(self.Release_Dataframe)
        self.update_release_table_content(new_index, release_content['table_content'])
        self.update_release_tracklist_content(new_index, release_content['tracklist_content'])
        self.update_release_video_links_content(new_index, release_content['video_links_content'])

    def is_duplicate(self, table_content):
        # Assuming 'Discogs_URLS' is a unique identifier for each release
        url = table_content.get('Discogs_URLS')
        if url in self.Release_Dataframe['Discogs_URLS'].values:
            return True
        return False

    def main_scraping_method(self, url_list):
        for url in url_list:
            release_content = self.get_release_url_content(url)
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


class DiscogsSearch(DiscogsSearchScraper):
    def __init__(self, start_url, data_handler=None):
        super().__init__(start_url, data_handler)
        self.start_up_search()

        # self.applied_filters = applied_filters
        # super(DiscogsSearch, self).get_search_page_content(start_url)

        # self.update_center_releases_content(center_releases_content)
        # self.search_page_user_interaction()
        if data_handler is not None:
            self.data_handler = data_handler
        else:
            self.data_handler = DataHandler()


    def start_up_search(self):
        self.current_url = self.start_url
        self.navigate_to_search_url(self.current_url)
        # self.updateAppliedFilters(applied_filters=self.getAppliedFiltersFromUrl(self.current_url))
        # self.fetchCurrentSearchPageContent()
        # self.aside_navbar_content, self.center_releases_content, __applied_filters, __new_applied_filters_list \
        #    = self.get_current_search_page_content()
        # self.updateSearchOptions()

    def navigate_to_search_url(self, url):
        self.current_url = url
        self.updateAppliedFilters(applied_filters=self.getAppliedFiltersFromUrl(self.current_url))
        self.aside_navbar_content, self.center_releases_content, __applied_filters, __new_applied_filters_list = self.get_current_search_page_content()
        self.updateSearchOptions(aside_navbar_content=self.aside_navbar_content)
        # self.updateCenterReleasesContent(center_releases_content)
        # self.updateCurrentPageAndNextPage()
        # self.fetchCurrentSearchPageContent()

    def updateSearchOptions(self, aside_navbar_content):
        self.search_options_dict = aside_navbar_content

    def updateDataFrame(self):
        self.data_handler.update_dataframe(self.center_releases_content)

    def user_interaction(self):
        u_i = ''
        switch = {
            '1': self.user_interaction_add_filters,
            '2': self.data_handler.display_dataframe,
            '3': self.user_interaction_fill_in_blanks,
            '4': self.updateDataFrame,
            '5': self.user_interaction_save_dataframe,
            '6': self.user_interaction_select_pages,
            '7': self.user_interaction_view_applied_filters,
            '8': self.user_interaction_remove_filters
        }
        while u_i != "Q":
            print("1: Apply Filters Page \n"
                  "2: Display DataFrame \n"
                  "3: Fill in DataFrame \n"
                  "4: Update DataFrame \n"
                  "5: Save DataFrame to CSV \n"
                  "6: Scrape Pages \n"
                  "7: View Applied Filters \n"
                  "8: Remove Applied Filters \n")
            u_i = input("Enter Q to Quit, or any other key to continue: ")

            # Store functions without calling them

            # Get the function based on the input and call it
            func = switch.get(u_i)
            if func:
                func()
            else:
                print('Choose one of the following operators: +, -, *')

    def user_interaction_add_filters(self):
        print(
            f"Applied filters: {[applied_filter for i, applied_filter in reversed(list(enumerate(self.applied_filters, 1)))]}")
        print([f"{i}: {label_type}" for i, label_type in enumerate(self.search_dict_get_label_type_keys(), 1)])
        if len(self.search_dict_get_label_type_keys()) == 0:
            return
        enter_key1 = int(input("")) - 1  # -1 to get index number
        label_type, values = list(self.search_dict_get_label_type_items())[enter_key1]
        print([f"{i}: {label_data}" for i, label_data in
               reversed(list(enumerate(self.search_dict_get_label_url_keys(label_type), 1)))])
        enter_key2 = int(input("")) - 1
        new_search_term_1 = self.search_dict_get_search_term(label_type, enter_key2)
        new_discogs_search_url = self.base_discogs_url + new_search_term_1
        self.navigate_to_search_url(new_discogs_search_url)

    def user_interaction_remove_filters(self):
        # Display applied filters starting from the second item (index 1)
        if len(self.applied_filters) >= 2:
            # Using dictionary comprehension to display every second item starting from index 1
            displayed_filters = {i // 2 + 1: self.applied_filters[i] for i in
                                 range(0, len(self.applied_filters), 2)}
            print(f"Applied filters: {displayed_filters}")
        else:
            print(f"Applied filters: {self.applied_filters}")

        if len(self.search_dict_get_label_type_keys()) == 0:
            return

        remove_index = int(input("Enter index to remove: ")) - 1
        # Adjust the remove_index to point to the correct element in applied_filters
        adjusted_remove_index = remove_index * 2 + 1

        # Check if adjusted_remove_index is valid
        if 0 <= adjusted_remove_index < len(self.applied_filters):
            # Remove the element and its preceding element
            self.applied_filters = self.remove_applied_filter(self.applied_filters, adjusted_remove_index)
            url = self.getUrlFromAppliedFilters(self.applied_filters)
            self.navigate_to_search_url(url)
        else:
            print("Invalid index")

    # Helper function to remove element and its previous element
    def remove_applied_filter(self, filters, index):
        if index > 0 and index < len(filters):
            del filters[index - 1:index + 1]
        return filters

    def test_function(self, max_rows_to_update=None):
        enter_key1 = '4'
        print(f"page range is {enter_key1}")
        search_pages = self.get_page_range(self.current_url, enter_key1)
        TEST_URL = 'https://www.discogs.com/search/?genre_exact=Electronic&style_exact=Techno&style_exact=Ambient&page=5'
        self.navigate_to_search_url(TEST_URL)
        self.updateDataFrame()
        # max_rows = 3
        # print(" max number of rows is 5")
        self.data_handler.fill_in_missing_data(max_rows_to_update=max_rows_to_update)
        # self.data_handler.display_dataframe()
        # self.saveDataFrameToCSV(save_as_file_name="saved_data.csv")

    def user_interaction_select_pages(self, update_applied_filters=False):
        # self.current_url
        enter_key3 = input("Enter a page number, or range (number seperated by a space)")
        search_pages = self.get_page_range(self.current_url, enter_key3)
        for search_page_url in search_pages:
            time.sleep(0.5)
            print(f"Navigating to : \n {search_page_url}")
            self.navigate_to_search_url(search_page_url)
            self.updateDataFrame()

    # self.addAppliedFilter(pages_filter_term)

    def user_interaction_view_applied_filters(self):
        print(
            f"Applied filters: {[applied_filter for i, applied_filter in reversed(list(enumerate(self.applied_filters, 1)))]}")

    def user_interaction_fill_in_blanks(self):
        max_rows = int(input("Enter the number of rows to fill in: "))
        if max_rows == '':
            max_rows = None
        self.data_handler.fill_in_missing_data(max_rows_to_update=max_rows)

    def user_interaction_save_dataframe(self):
        save_name = input("Enter the name of the file to save as: ")
        self.data_handler.save_dataframe(save_name)

        # self.updateCurrentPageAndNextPage()
        # print(f"now updating applied filters with this {new_applied_filters_list}")
        # self.updateAppliedFilters(new_applied_filters_list)
        # if update_applied_filters is True:
        # self.updateAppliedFilters(new_applied_filters_list)
        # self.getUrlFromAppliedFilters(self.applied_filters)
        # print(self.get_number_of_search_pages(new_discogs_search_url))
        # print(self.get_current_page_from_url(new_discogs_search_url))
        # print(self.get_next_search_page_url(new_discogs_search_url))
        # self.data_handler.update_dataframe(center_releases_content)
        # self.update_center_releases_content(center_releases_content)

    def remove_discogs_search_term(self, remove_search_term):
        new_discogs_search_url = self.current_url.strip(remove_search_term)
        return new_discogs_search_url


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
class DataHandler:
    def __init__(self, df = None, csv_file = None):
        if csv_file is None:
            if df is None:
                self.df = self.create_dataframe()
            else:
                self.df = df
        else:
            self.df = DataHandler.loadfromCSV(csv_file=csv_file)
        #self.discogs_release_content_scraper = DiscogsReleaseScraper()

    def create_dataframe(self):
        df = pd.DataFrame(columns=["Discogs_Artists", "Discogs_Titles", "Discogs_Labels", "Discogs_Tags",
                                                "Discogs_Countries", "Discogs_Years", "Discogs_Search_Filters", "Discogs_URLS",
                                                "Discogs_Formats", "Discogs_Tracklist", "Discogs_YouTube_Videos"])
        return df

    def find_missing_data(self):
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
            if missing_data.at[index, 'Discogs_Countries'] and not pd.isna(row['Discogs_URLS']):
                self.df.at[index, 'Discogs_Countries'] = self.fetch_country_data(row['Discogs_URLS'])

            # Check and fetch missing label data
            if missing_data.at[index, 'Discogs_Labels'] and not pd.isna(row['Discogs_URLS']):
                self.df.at[index, 'Discogs_Labels'] = self.fetch_label_data(row['Discogs_URLS'])

            # Check and fetch missing tags data
            if missing_data.at[index, 'Discogs_Tags'] and not pd.isna(row['Discogs_URLS']):
                self.df.at[index, 'Discogs_Tags'] = self.fetch_tags_data(row['Discogs_URLS'])

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

    def update_dataframe(self, new_data):
        if isinstance(new_data, dict):
            # Convert the dictionary to a DataFrame
            new_df = pd.DataFrame([new_data])
        elif isinstance(new_data, list):
            # Convert the list of dictionaries to a DataFrame
            new_df = pd.DataFrame(new_data)
        else:
            raise ValueError("new_data must be a dictionary or a list of dictionaries")

        # Concatenate new_df with self.df and drop duplicates
        self.df = pd.concat([self.df, new_df], ignore_index=True).drop_duplicates(
            subset=["Discogs_Artists", "Discogs_Titles", "Discogs_Labels", "Discogs_Tags",
                                                "Discogs_Countries", "Discogs_Years", "Discogs_Search_Filters", "Discogs_URLS",
                                                "Discogs_Formats", "Discogs_Tracklist",  "Discogs_YouTube_Videos"])

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


    def fill_in_missing_data(self, max_rows_to_update=None):
        rows_processed = 0
        print('here')
        for index, row in self.df.iterrows():
            print('to')
            if max_rows_to_update is not None and rows_processed >= max_rows_to_update:
                break

            discogs_url = row['Discogs_URLS']
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
            time.sleep(random.gauss(mu=3, sigma=0.5))



class SessionDataManager(DataHandler):
    def __init__(self, csv_file_path):
        super().__init__()
        self.lock = threading.Lock()
        if csv_file_path == '' or csv_file_path is None:
            self.csv_file_path = 'default_df_save.csv'
        else:
            self.csv_file_path = csv_file_path
        if self.loadfromCSV(csv_file_path) is not None:
            self.df = self.loadfromCSV(csv_file_path)
        else:
            self.df = self.create_dataframe()


    def add_new_data(self, new_data):
        with self.lock:
            if isinstance(new_data, dict):
                new_data = [new_data]  # Convert single dictionary to a list of dictionaries
            new_df = pd.DataFrame(new_data)
            if not self.is_duplicate(new_df):
                self.df = pd.concat([self.df, new_df], ignore_index=True)

    def compare_and_update_data(self, data_handler_df):
        def get_more_complete_row(row, cols):
            for col in cols:
                if pd.isna(row[col + '_df1']) and not pd.isna(row[col + '_df2']):
                    row[col + '_df1'] = row[col + '_df2']
                    print(row)
            return row[[col + '_df1' for col in cols]]

        with self.lock:
            # Common columns excluding the ones used for merging
            common_cols = ['Discogs_Labels', 'Discogs_Tags', 'Discogs_Countries', 'Discogs_Years',
                           'Discogs_Search_Filters', 'Discogs_Formats', 'Discogs_Tracklist', 'Discogs_YouTube_Videos']

            # Merge the two DataFrames on the identifying columns
            merged_df = pd.merge(self.df, data_handler_df, on=['Discogs_Artists', 'Discogs_Titles', 'Discogs_URLS'],
                                 how='outer', suffixes=('_df1', '_df2'))

            # Apply the function to get the more complete row for each common column
            more_complete_rows = merged_df.apply(lambda row: get_more_complete_row(row, common_cols), axis=1)

            # Add the more complete rows to the DataFrame
            self.add_new_data_sql_style(more_complete_rows)

    def add_new_data_sql_style(self, new_df):
        with self.lock:
            # Setting an index, for example, a combination of 'Discogs_Artists', 'Discogs_Titles', 'Discogs_URLS'
            self.df.set_index(['Discogs_Artists', 'Discogs_Titles', 'Discogs_URLS'], inplace=True, drop=False)
            new_df.set_index(['Discogs_Artists', 'Discogs_Titles', 'Discogs_URLS'], inplace=True, drop=False)

            # Performing an outer join and keeping only those records which are unique to new_df
            self.df = self.df.join(new_df, how='outer', lsuffix='_old', rsuffix='_new', indicator=True)
            self.df = self.df[self.df['_merge'] == 'right_only']

            # Reset index if needed
            self.df.reset_index(drop=True, inplace=True)

    def is_duplicate(self, new_df):
        # Check for duplicates based on a combination of columns
        # Adjust these columns based on your unique identifier criteria
        columns_to_check = ['Discogs_Artists', 'Discogs_Titles', 'Discogs_URLS']
        existing = self.df[columns_to_check]
        duplicates = new_df[columns_to_check].isin(existing.to_dict(orient='list')).all(axis=1)
        return duplicates.any()  # Returns True if any duplicates found

    def save_data_to_csv(self):
        with self.lock:
            self.df.to_csv(self.csv_file_path, index=False)

    def display_dataframe(self):
        with self.lock:
            print(self.df)

class MusicScraperApp:
    def __init__(self):

        self.session_data_manager = None
        self.search = DiscogsSearch(start_url='https://www.discogs.com/search/?genre_exact=Electronic&style_exact=Techno&style_exact=Ambient')
    # ... existing methods

    def start_app(self):
        self.user_interaction_load_saved_data()
        self.search.user_interaction()

    def user_interaction_load_saved_data(self):
        file_path = input("Enter the file path to load saved data from: ")
        self.load_saved_data(file_path)

    def load_saved_data(self, file_path):
        self.session_data_manager = SessionDataManager(file_path)
        self.session_data_manager.display_dataframe()



    def test_function(self, file_path):
        # Initialize DataHandler and SessionDataManager
        #self.data_handler = DataHandler()
        self.session_data_manager = SessionDataManager(file_path)
        self.search.test_function(max_rows_to_update=5)
        self.data_handler = self.search.data_handler
        # Simulate fetching data - this could be replaced with actual data fetching logic
        simulated_fetched_data = [
            {"Discogs_Artists": "Artist 1", "Discogs_Titles": "Title 1", "Discogs_URLS": "http://example.com/1", "Discogs_Labels": "Label 1"},
            {"Discogs_Artists": "Artist 2", "Discogs_Titles": "Title 2", "Discogs_URLS": "http://example.com/2", "Discogs_Labels": "Label 2"}
        ]

        # Update the DataHandler's DataFrame with the new data
        self.data_handler.update_dataframe(simulated_fetched_data)

        # Display the DataHandler's DataFrame
        print("DataHandler's DataFrame after fetching new data:")
        self.data_handler.display_dataframe()

        # Add the data from DataHandler to SessionDataManager
        for index, row in self.data_handler.df.iterrows():
            self.session_data_manager.add_new_data(row.to_dict())

        # Display the SessionDataManager's DataFrame to show the result
        print("\nSessionDataManager's DataFrame after adding new data:")
        self.session_data_manager.display_dataframe()

        # Optionally, save the SessionDataManager's DataFrame to CSV
        self.session_data_manager.save_data_to_csv('updated_saved_data.csv')




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
