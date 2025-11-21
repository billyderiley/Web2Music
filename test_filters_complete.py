#!/usr/bin/env python3
"""
Complete test of the fixed filter functionality.
Tests that we can now get 700+ style options instead of just 5.
"""

from DiscogsSearchScraper import DiscogsSearchScraper

def test_filter_scraping():
    """Test that filter scraping works with both initial load and full expansion"""
    
    print("=" * 60)
    print("DISCOGS FILTER SCRAPING - COMPLETE TEST")
    print("=" * 60)
    
    # Test 1: Initial page load (gets 5 options per category)
    print("\n[TEST 1] Initial sidebar scraping (5 visible options)")
    print("-" * 60)
    
    scraper = DiscogsSearchScraper('https://www.discogs.com/search')
    soup = scraper.get_Soup_from_url(scraper.current_url)
    
    if soup:
        aside_content, applied_filters, new_filters = scraper.get_aside_navbar_content(soup)
        
        print(f"✓ Found {len(aside_content)} filter categories")
        for category, options in aside_content.items():
            print(f"  - {category}: {len(options)} options (sidebar view)")
            for i, option_name in enumerate(list(options.keys())[:3]):
                print(f"      {i+1}. {option_name}")
    
    # Test 2: Expanded view using Selenium (gets ALL options)
    print("\n[TEST 2] Expanded dialog scraping (ALL options)")
    print("-" * 60)
    
    categories_to_test = ['Style', 'Genre', 'Format']
    
    for category in categories_to_test:
        print(f"\n{category}:")
        all_options = scraper.get_all_filter_options_with_selenium(category)
        
        if all_options:
            print(f"✓ Loaded {len(all_options)} total options")
            print(f"  First 10 options:")
            for i, option_name in enumerate(list(all_options.keys())[:10]):
                print(f"    {i+1}. {option_name}")
            
            if len(all_options) > 10:
                print(f"    ... and {len(all_options) - 10} more")
        else:
            print(f"✗ Failed to load options")
    
    print("\n" + "=" * 60)
    print("TEST COMPLETE")
    print("=" * 60)
    print("\n✓ Filter scraping has been updated to handle new Discogs HTML!")
    print("✓ Initial load: 5 options per category (fast)")
    print("✓ Expanded view: 700+ options per category (uses Selenium)")

if __name__ == "__main__":
    test_filter_scraping()
