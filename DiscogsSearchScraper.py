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


    def get_search_url_content_dict(self, ):
        aside_navbar_content, center_releases_content, applied_filters, new_applied_filters_list, sort_by = self.get_current_search_page_content()
        current_search_url_info_dict = {
            'Discogs_Urls': self.current_url,
            'aside_navbar_content': aside_navbar_content,
            'center_releases_content': center_releases_content,
            'sort_by': sort_by,
        }
        return current_search_url_info_dict

    def get_current_search_page_content(self):
        base_url = self.current_url
        SoupObj = self.get_Soup_from_url(base_url)
        aside_navbar_content, applied_filters, new_applied_filters_list = self.get_aside_navbar_content(SoupObj)
        sort_by = self.get_sort_by(SoupObj)
        self.sort_by = sort_by
        center_releases_content = self.get_center_releases_content(SoupObj)
        return aside_navbar_content, center_releases_content, applied_filters, new_applied_filters_list, sort_by

    def get_aside_navbar_content(self, SoupObj):
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
        for i, h2_ in enumerate(left_side_facets):
            if len(left_side_facets) == 1:
                print("No additional search options, remove some to proceed.")
                return aside_navbar_content, applied_filters
            else:
                if 'Applied Filters' in h2_.text:
                    continue
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
                        href = li.find('a')['href']
                        search_term = href.split('?')[-1]
                        __search_term = search_term.split('=')
                        ____search_term = [term.split('&') for term in __search_term]
                        # code to flatten list ____search_term into a single 1 dimensional list
                        new_applied_filters_list = [item for sublist in ____search_term for item in sublist]
                        # Reverse the list and assign to a new variable
                        # if new_applied_filters_list equal or larger than 4
                        if len(new_applied_filters_list) >= 2:
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
                        # add the applied filters seperated by commas, then add the sort by term to the search filters
                        "Discogs_Search_Filters": ','.join(self.applied_filters) + (f",sort={next(iter(self.sort_by['Selected']))}" if self.sort_by['Selected'] else ''),

                    }
                    center_releases_content.append(release_info)
        return center_releases_content

    def get_sort_by(self, SoupObj):
        sort_by_dict = {'Selected': {}, 'Options': {}}
        sort_by = SoupObj.find('select', id='sort_top')
        if sort_by is not None:
            options = sort_by.find_all('option')
            for option in options:
                if option.get('selected'):
                    sort_by_dict['Selected'][option.text] = option['value']
                else:
                    sort_by_dict['Options'][option.text] = option['value']

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
        # Remove 'page=' and any digits following it
        url = re.sub(r'page=\d+', '', url)

        # If the URL ends with a digit, remove all trailing digits
        if re.search(r'\d+$', url):
            url = re.sub(r'\d+$', '', url)

        # Determine the separator based on the existing structure of the URL
        if url.endswith('/'):
            new_page_url = url + "?page=" + str(page_number)
        elif url.endswith('&'):
            new_page_url = url + "page=" + str(page_number)
        elif url.endswith('?'):
            new_page_url = url + "page=" + str(page_number)
        else:
            # Ensure the URL ends with either '?' or '&' before appending the page number
            separator = '&' if '?' in url else '?'
            new_page_url = url + separator + "page=" + str(page_number)

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
            while filt.startswith(' ') or filt.startswith('\n'):
                filt = filt.lstrip(' ')
                filt = filt.lstrip('\n')
            while filt.endswith(' ') or filt.endswith('\n'):
                filt = filt.rstrip(' ')
                filt = filt.rstrip('\n')
            # code to transform any spaces into +
            filt = filt.replace(' ', '+')
            clean_applied_filters.append(filt)

        return clean_applied_filters

    def addAppliedFilter(self, applied_filter):
        self.applied_filters.append(applied_filter)

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
                # code to use regex to remove page= and any number after it
                url = re.sub(r'page=\d+', '', url)
            if 'sort' in url:
                url = re.sub(r'sort=[^&]+', '', url)

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

    def get_page_range(self, new_discogs_search_url, page_number_range, max_number_of_pages):
        try:
            if int(page_number_range) > max_number_of_pages:
                raise ValueError
            return [self.create_url_from_page_number(new_discogs_search_url, page_number_range)]
        except ValueError:
            start_number, end_number = int(page_number_range.split(' ')[0]), int(page_number_range.split(' ')[-1])
            if end_number > max_number_of_pages:
                raise ValueError
            if start_number > end_number:
                ___start_number = end_number
                end_number = start_number
                start_number = ___start_number
            end_number = end_number + 1
            #
            pages_term = ['pages', str(start_number), str(end_number)]
            return [self.create_url_from_page_number(new_discogs_search_url, str(page_num)) for page_num in range(start_number, end_number)]

def create_search_dataframe():
    search_df = pd.DataFrame(columns=["u_id" ,"Discogs_Artists", "Discogs_Titles", "Discogs_Labels", "Discogs_Tags",
                                            "Discogs_Countries", "Discogs_Years", "Discogs_Search_Filters", "Discogs_Urls",
                                            "Discogs_Formats", "Discogs_Tracklist", "Discogs_YouTube_Videos"])

    return search_df
