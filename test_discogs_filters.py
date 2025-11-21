#!/usr/bin/env python3
"""Test script to understand how Discogs filters work in the new interface"""

from BaseScraper import BaseScraper
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def test_filter_interaction():
    """Test clicking on filters and observing URL changes"""
    scraper = BaseScraper()
    
    # Create driver with our working Selenium stealth method
    driver = scraper.create_driver_with_random_user_agent()
    
    try:
        # Navigate to search page
        url = 'https://www.discogs.com/search/?q=techno&type=release'
        print(f"Navigating to: {url}")
        driver.get(url)
        time.sleep(3)  # Wait for page to load
        
        print(f"Current URL: {driver.current_url}")
        
        # Wait for aside to be present
        wait = WebDriverWait(driver, 10)
        aside = wait.until(EC.presence_of_element_located((By.TAG_NAME, "aside")))
        print("✓ Found aside element")
        
        # Find genre buttons
        genre_buttons = driver.find_elements(By.XPATH, "//h2[contains(text(), 'genre')]/following-sibling::div//button")
        print(f"\nFound {len(genre_buttons)} genre filter buttons")
        
        if len(genre_buttons) > 0:
            # Click on "Electronic" genre
            first_button_text = genre_buttons[0].text
            print(f"\nClicking on first genre button: '{first_button_text}'")
            genre_buttons[0].click()
            time.sleep(2)
            
            new_url = driver.current_url
            print(f"URL after click: {new_url}")
            
            # Check if URL changed
            if url != new_url:
                print(f"✓ URL changed! Filter parameter added: {new_url}")
            else:
                print("✗ URL did not change - filters might be client-side only")
        
        # Try clicking a style filter
        style_buttons = driver.find_elements(By.XPATH, "//h2[contains(text(), 'style')]/following-sibling::div//button")
        print(f"\nFound {len(style_buttons)} style filter buttons")
        
        if len(style_buttons) > 0:
            first_style = style_buttons[0].text
            print(f"Clicking on first style button: '{first_style}'")
            style_buttons[0].click()
            time.sleep(2)
            
            final_url = driver.current_url
            print(f"URL after style click: {final_url}")
        
        # Get page source after filters applied
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Check for applied filters indicator
        print("\n=== Checking for applied filters in HTML ===")
        # Look for any elements that might show active filters
        active_elements = soup.find_all(class_=lambda x: x and ('active' in str(x).lower() or 'selected' in str(x).lower()))
        print(f"Found {len(active_elements)} elements with 'active' or 'selected' in class")
        for elem in active_elements[:5]:
            print(f"  {elem.name}: {elem.get('class')} - {elem.text[:50]}")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.quit()

if __name__ == "__main__":
    test_filter_interaction()
