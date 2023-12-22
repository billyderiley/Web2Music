from BaseScraper import BaseScraper
import pandas as pd
import re


class DiscogsSearchScraper(BaseScraper):
    def __init__(self, start_url = None, Search_Dataframe = None):
        super().__init__()
        self.search_options_dict = {}
        self.search_url_content_dict = {}
        self.DISCOGS_INTERNAL_MAX_SEARCH_PAGES = 200
        self.aside_navbar_content = None
        self.center_releases_content = None
        self.applied_filters = []
        self.sort_by = None
        if start_url is None:
            start_url = self.base_discogs_search_url
        else:
            start_url = start_url
        self.base_url = start_url
        self.current_url = start_url
        self.start_url = start_url
        if Search_Dataframe is None:
            self.Search_Dataframe = create_search_dataframe()
        else:
            self.Search_Dataframe = Search_Dataframe

        #self.next_page = self.get_next_search_page_url(start_url)
        #if data_handler is None:
        #    self.data_handler = DataHandler()
        #else:
        #    self.data_handler = data_handler



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

    def get_search_url_content_dict(self, ):
        aside_navbar_content, center_releases_content, applied_filters, new_applied_filters_list, sort_by_dict = self.get_current_search_page_content()
        current_search_url_info_dict = {
            'Discogs_Urls': self.current_url,
            'aside_navbar_content': aside_navbar_content,
            'center_releases_content': center_releases_content,
            'sort_by_dict': sort_by_dict,
        }
        return current_search_url_info_dict


    """def update_search_dataframe(self):
        if self.center_releases_content is not None:
            if isinstance(self.center_releases_content, list):
                # Convert the list of dictionaries to a DataFrame
                new_df = pd.DataFrame(self.center_releases_content, columns=["Discogs_Artists", "Discogs_Titles", "Discogs_Labels", "Discogs_Tags",
                                                "Discogs_Countries", "Discogs_Years", "Discogs_Search_Filters", "Discogs_Urls",
                                                "Discogs_Formats", "Discogs_Tracklist",  "Discogs_YouTube_Videos"])

            else:
                raise ValueError("new_data must be a list of dictionaries")
        else:
            raise ValueError("self.center_releases_content is None")

        # Concatenate new_df with self.df and drop duplicates
        self.Search_Dataframe = pd.concat([self.Search_Dataframe, new_df], ignore_index=True).drop_duplicates(
            subset=["Discogs_Artists", "Discogs_Titles", "Discogs_Labels", "Discogs_Tags",
                                                "Discogs_Countries", "Discogs_Years", "Discogs_Search_Filters", "Discogs_Urls",
                                                "Discogs_Formats", "Discogs_Tracklist",  "Discogs_YouTube_Videos"])"""

    def deep_search_release_info(self):
        pass


    def get_current_search_page_content(self):
        base_url = self.current_url
        #Base_Scraper = BaseScraper()
        SoupObj = self.get_Soup_from_url(base_url)
        aside_navbar_content, applied_filters, new_applied_filters_list = self.get_aside_navbar_content(SoupObj)
        sort_by_dict = self.get_sort_by_dict(SoupObj)
        #self.updateAppliedFilters(self.getAppliedFiltersFromUrl(self.current_url))
        center_releases_content = self.get_center_releases_content(SoupObj)
        # aside_navbar_content
        # center_releases_content
        # applied_filters
        return aside_navbar_content, center_releases_content, applied_filters, new_applied_filters_list, sort_by_dict

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
                #print(f"whats this {self.applied_filters}")
                # Check if the title already exists in the releases
                if not any(release['Discogs_Titles'] == title for release in center_releases_content):
                    #print(self.applied_filters)
                    release_info = {
                        "Discogs_Artists": artist,
                        "Discogs_Titles": title,
                        "Discogs_Urls": self.base_discogs_url+href,
                        "Discogs_Search_Filters": ', '.join(self.applied_filters)
                    }
                    center_releases_content.append(release_info)
        return center_releases_content

    def get_sort_by_dict(self, SoupObj):
        sort_by_dict = {'Selected': {}, 'Options': {}}

        sort_by = SoupObj.find('select', id='sort_top')
        if sort_by is not None:
            options = sort_by.find_all('option')
            for option in options:
                if option.get('selected'):
                    print("no here")
                    sort_by_dict['Selected'][option.text] = option['value']
                else:
                    print('here')
                    sort_by_dict['Options'][option.text] = option['value']
        print(sort_by_dict)
        return sort_by_dict

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
        if "search" not in url:
            raise ValueError
        else:
            if 'page' in url:
                #print('need to remove pages from url')
                #print(url)
                # code to use regex to remove page= and any number after it
                url = re.sub(r'page=\d+', '', url)
                #print(url)
            if 'sort' in url:
                #print(url)
                url = re.sub(r'sort=[^&]+', '', url)
               # print(url)


            if url.endswith('/'):
                applied_filters = []
            else:
                search_term = url.split('/?')[-1]
                search_term = search_term.strip('?')
                search_term = search_term.strip('&')
                search_terms = search_term.split('&')
                # Split each item at '=' and extend them into a flat list
                applied_filters = [item for term in search_terms for item in term.split('=')]
                # return flat_list
            return applied_filters


    def get_sorted_url(self, current_url, new_sort_by):
        print("now here")
        print(new_sort_by)
        # Check if 'sort=' is already in the URL
        if '?sort=' in current_url or '&sort=' in current_url:
            # Replace the existing sort_by term with the new one
            updated_url = re.sub(r'(sort=)[^&]*', r'\1' + new_sort_by, current_url)
            print("now here1")
            print(updated_url)
        else:
            # If 'sort=' is not in the URL, append it after the last '/'
            if current_url.endswith('/'):
                updated_url = current_url + '?sort=' + new_sort_by
                print("now here2")
                print(updated_url)
            else:
                # Check if '?' is already in the URL, if not add '?', else add '&'
                separator = '&' if '?' in current_url else '?'
                updated_url = current_url + separator + 'sort=' + new_sort_by
                print("now here3")
                print(updated_url)

        return updated_url


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


def create_search_dataframe():
    search_df = pd.DataFrame(columns=["Discogs_Artists", "Discogs_Titles", "Discogs_Labels", "Discogs_Tags",
                                            "Discogs_Countries", "Discogs_Years", "Discogs_Search_Filters", "Discogs_Urls",
                                            "Discogs_Formats", "Discogs_Tracklist", "Discogs_YouTube_Videos"])

    return search_df