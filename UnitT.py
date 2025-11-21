
import unittest
from BaseScraper import BaseScraper
from DiscogsSearchScraper import DiscogsSearchScraper
from DiscogsReleaseScraper import DiscogsReleaseScraper
from ScrapeDataHandler import DataHandler
discogs_base_url = "https://www.discogs.com/search"
youtube_api_key = 'AIzaSyAUCZgYUeP4Xcj-kw88V6X7VfcjQdBPtAg'

# Test Class for BaseScraper
class TestBaseScraper(unittest.TestCase):

    def test_setUp(self):
        self.Base_Scraper = BaseScraper()
        test_url = discogs_base_url
        try:
            soup = self.Base_Scraper.createSoupObjFromUrlSelenium(test_url)
            self.assertIsNotNone(soup, "Soup object is None. Page may not have loaded.")
        except Exception as e:
            self.fail(f"Exception occurred: {e}")


# Test Class for DiscogsScraper
class TestDiscogsSearchScraper(unittest.TestCase):

    def test_setUp(self):
        test_url = discogs_base_url
        self.Discogs_Search_Scraper = DiscogsSearchScraper()
        self.Discogs_Search_Scraper.current_url = test_url
        try:
            page_content = None
            try:
                page_content = self.Discogs_Search_Scraper.get_current_search_page_content()
            except AttributeError as e:
                # Check if soup object is None (blocked or empty)
                soup = self.Discogs_Search_Scraper.get_Soup_from_url(test_url)
                if soup is None or not soup.prettify().strip():
                    self.fail("Blocked or empty page: No HTML content returned.")
                else:
                    with open("discogs_debug_output.txt", "w", encoding="utf-8") as f:
                        f.write("--- HTML Content Start ---\n")
                        f.write(soup.prettify()[:2000])
                        f.write("\n--- HTML Content End ---\n")
                        f.write("Present elements:\n")
                        for tag in ['title', 'body', 'div', 'nav', 'aside', 'main', 'section']:
                            found = soup.find_all(tag)
                            f.write(f"<{tag}>: {len(found)} found\n")
                    self.fail("Page structure changed: HTML returned but expected elements missing. See discogs_debug_output.txt for details.")
            self.assertIsNotNone(page_content, "Search page content is None.")
        except Exception as e:
            self.fail(f"Exception occurred: {e}")


class TestDiscogsSearch(unittest.TestCase):

    def test_set_up(self):
        test_url = discogs_base_url
        try:
            self.Discogs_Search = DiscogsSearchScraper(test_url)
            self.assertIsNotNone(self.Discogs_Search, "DiscogsSearchScraper object is None.")
        except Exception as e:
            self.fail(f"Exception occurred: {e}")

class TestDiscogsReleaseScraper(unittest.TestCase):

    def test_set_up(self):
        test_release = "https://www.discogs.com/release/28624954-Jon-Hopkins-LateNightTales"
        try:
            self.Discogs_Release_Scraper = DiscogsReleaseScraper(test_release)
            self.assertIsNotNone(self.Discogs_Release_Scraper, "DiscogsReleaseScraper object is None.")
        except Exception as e:
            self.fail(f"Exception occurred: {e}")

    def test_process_release(self):
        test_release = "https://www.discogs.com/release/28624954-Jon-Hopkins-LateNightTales"
        try:
            self.Discogs_Release_Scraper = DiscogsReleaseScraper()
            content = self.Discogs_Release_Scraper.get_Soup_from_url(test_release)
            self.assertIsNotNone(content, "Release content is None.")
        except Exception as e:
            self.fail(f"Exception occurred: {e}")

class TestDataHandler(unittest.TestCase):
    def test(self):
        test_url = discogs_base_url
        try:
            data_handler = DataHandler()
            discogs_search = DiscogsSearchScraper(test_url)
            self.assertIsNotNone(discogs_search, "DiscogsSearchScraper object is None.")
        except Exception as e:
            self.fail(f"Exception occurred: {e}")

if __name__ == '__main__':
    unittest.main()