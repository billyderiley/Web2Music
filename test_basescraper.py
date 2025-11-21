from BaseScraper import BaseScraper

def main():
    test_url = "https://www.discogs.com/search"
    scraper = BaseScraper()
    soup = scraper.createSoupObjFromUrlSelenium(test_url)

    if soup is None or not soup.prettify().strip():
        print("Blocked or empty page: No HTML content returned.")
    else:
        print("--- HTML Content Start ---")
        print(soup.prettify()[:2000])  # Print first 2000 chars for brevity
        print("--- HTML Content End ---")
        print("Present elements:")
        for tag in ['title', 'body', 'div', 'nav', 'aside', 'main', 'section']:
            found = soup.find_all(tag)
            print(f"<{tag}>: {len(found)} found")

if __name__ == "__main__":
    main()
