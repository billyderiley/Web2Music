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
        """
        Parse the sidebar filter navigation from Discogs search page.
        Updated for new Discogs HTML structure (as of 2025).
        
        The new structure uses:
        - <aside> tag instead of id="page_aside"
        - <h2> tags with "font-bold" class for filter categories
        - <button> elements for filter options instead of links
        - Filter URLs are constructed by clicking buttons (JavaScript interaction)
        """
        aside_navbar_content = {}
        applied_filters = []
        new_applied_filters_list = []
        
        # Find the aside element (new structure)
        left_side_menu_html = SoupObj.find('aside')
        
        if left_side_menu_html is None:
            print("Warning: No aside element found on page")
            return aside_navbar_content, applied_filters, new_applied_filters_list
        
        # Find all filter category headers
        # Headers are h2 tags with class containing "font-bold"
        left_side_facets = left_side_menu_html.find_all('h2', class_=lambda x: 'font-bold' in x if x else False)
        
        if not left_side_facets:
            print("Warning: No filter headers found")
            return aside_navbar_content, applied_filters, new_applied_filters_list
        
        for h2_ in left_side_facets:
            header_name = h2_.text.strip().title()
            
            # Handle "Applied Filters" section separately
            if 'applied' in header_name.lower() and 'filter' in header_name.lower():
                # Extract applied filters
                parent_div = h2_.find_parent('div')
                if parent_div:
                    ul = parent_div.find('ul')
                    if ul:
                        # Applied filters are in li tags with a span containing the filter text
                        filter_items = ul.find_all('li', recursive=False)
                        for li in filter_items:
                            # Find the span with the filter value
                            filter_span = li.find('span', class_=lambda x: 'leading-none' in x if x else False)
                            if filter_span:
                                filter_value = filter_span.text.strip()
                                if filter_value:
                                    applied_filters.append(filter_value)
                continue
            
            # Process regular filter categories (Genre, Style, Format, etc.)
            self.search_options_dict[header_name] = {}
            aside_navbar_content[header_name] = {}
            
            # Find the parent div containing this h2
            parent_div = h2_.find_parent('div')
            if parent_div:
                # Find the ul containing filter options
                ul = parent_div.find('ul')
                if ul:
                    # Find all list items with filter buttons
                    list_items = ul.find_all('li', class_=lambda x: 'text-sm' in x if x else False)
                    
                    for li in list_items:
                        # Find the button containing the filter name
                        button = li.find('button', class_=lambda x: 'cursor-pointer' in x if x else False)
                        if button:
                            facet_name = button.text.strip()
                            
                            # Find the count span (if present)
                            count_span = li.find('span', class_=lambda x: 'text-gray-600' in x if x else False)
                            count = count_span.text.strip() if count_span else '0'
                            
                            # Construct the filter URL based on the category and value
                            # The URL pattern is: ?category_exact=Value
                            category_param = header_name.lower().replace(' ', '_')
                            
                            # Special cases for parameter names
                            if category_param == 'decade':
                                filter_param = f"{category_param}={facet_name}"
                            elif category_param == 'year':
                                filter_param = f"year={facet_name}"
                            else:
                                filter_param = f"{category_param}_exact={facet_name.replace(' ', '+')}"
                            
                            # Construct full URL with filter
                            base_url = self.current_url.split('?')[0]
                            current_params = self.current_url.split('?')[1] if '?' in self.current_url else ''
                            
                            # Add the new filter parameter
                            if current_params:
                                href = f"{base_url}?{current_params}&{filter_param}"
                            else:
                                href = f"{base_url}?{filter_param}"
                            
                            # Store the filter option
                            aside_navbar_content[header_name][f"{facet_name} ({count})"] = href
                            
                            # Build the filter list for tracking
                            new_applied_filters_list.append(filter_param)
        
        return aside_navbar_content, applied_filters, new_applied_filters_list

    def get_all_filter_options_with_selenium(self, category_name):
        """
        Use Selenium to click the 'All' button and get all filter options from the expanded dialog.
        This is necessary because Discogs only shows 5 options initially, but has 700+ options in dialogs.
        
        Args:
            category_name: The filter category (e.g., 'Genre', 'Style', 'Format', 'Country', 'Decade')
        
        Returns:
            dict: {option_name: href, ...} with all available options for that category
        """
        from selenium.webdriver.common.by import By
        from bs4 import BeautifulSoup
        import time
        
        all_options = {}
        driver = None
        
        try:
            # Create Selenium driver
            driver = self.create_driver_with_random_user_agent()
            driver.get(self.current_url)
            time.sleep(3)
            
            # Find all "All▾" buttons
            all_buttons = driver.find_elements(By.TAG_NAME, 'button')
            expand_buttons = [btn for btn in all_buttons if 'All' in btn.text and '▾' in btn.text]
            
            # Map category names to button indices
            category_index_map = {
                'genre': 0,
                'style': 1,
                'format': 2,
                'country': 3,
                'decade': 4,
                'year': 4  # Decade and Year are the same
            }
            
            category_lower = category_name.lower()
            if category_lower not in category_index_map:
                print(f"Warning: Unknown category '{category_name}'")
                return all_options
            
            button_index = category_index_map[category_lower]
            
            if button_index >= len(expand_buttons):
                print(f"Warning: Not enough expand buttons found (need {button_index + 1}, have {len(expand_buttons)})")
                return all_options
            
            # Click the appropriate "All" button
            expand_buttons[button_index].click()
            time.sleep(2)
            
            # Parse the dialog
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            dialog = soup.find(attrs={'role': 'dialog'})
            
            if dialog:
                # Find all filter option links in the dialog
                links = dialog.find_all('a', class_=lambda x: 'text-blue-800' in x if x else False)
                
                for link in links:
                    name = link.text.strip()
                    href = link.get('href', '')
                    
                    # Find the count
                    parent = link.find_parent('li')
                    if parent:
                        count_span = parent.find('span', class_=lambda x: 'text-gray-600' in x if x else False)
                        count = count_span.text.strip() if count_span else ''
                        
                        # Store with count in the name
                        option_key = f"{name} ({count})" if count else name
                        
                        # Store just the relative href (it will be properly converted later)
                        # Don't prepend base_discogs_url here - it causes double URL issue
                        all_options[option_key] = href
                
                print(f"✓ Loaded {len(all_options)} {category_name} options from expanded dialog")
            else:
                print(f"Warning: Dialog not found after clicking {category_name} All button")
        
        except Exception as e:
            print(f"Error getting all {category_name} options: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            if driver:
                driver.quit()
        
        return all_options

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
