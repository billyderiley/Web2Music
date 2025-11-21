#!/usr/bin/env python3
"""
Test various web scraping methods for Discogs.
This script tests different approaches to bypass bot detection.
"""

import time
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import sys

# Test URL - Discogs search page
TEST_URL = "https://www.discogs.com/search/?q=ambient&type=release"
SIMPLE_URL = "https://www.discogs.com"

def print_result(method_name, success, html_length=0, error=None):
    """Print formatted test result"""
    status = "✓ SUCCESS" if success else "✗ FAILED"
    print(f"\n{'='*60}")
    print(f"{method_name}")
    print(f"Status: {status}")
    if success:
        print(f"HTML Length: {html_length} characters")
        print(f"Has content: {'Yes' if html_length > 1000 else 'No (possibly blocked)'}")
    if error:
        print(f"Error: {error}")
    print(f"{'='*60}")

def check_for_blocking(html):
    """Check if the HTML indicates bot blocking"""
    if not html:
        return True, "No HTML returned"
    
    blocking_indicators = [
        "Access Denied",
        "Blocked",
        "captcha",
        "security check",
        "unusual traffic",
        "Please verify you are a human"
    ]
    
    html_lower = html.lower()
    for indicator in blocking_indicators:
        if indicator.lower() in html_lower:
            return True, f"Blocking indicator found: {indicator}"
    
    # Check if we got meaningful content
    if len(html) < 1000:
        return True, "HTML too short - likely blocked"
    
    return False, "No blocking detected"

# ============================================================================
# METHOD 1: Basic Requests
# ============================================================================
def test_basic_requests():
    """Test 1: Basic requests with no headers"""
    try:
        response = requests.get(SIMPLE_URL, timeout=10)
        html = response.text
        blocked, reason = check_for_blocking(html)
        print_result("Method 1: Basic Requests (No Headers)", 
                    not blocked, len(html), reason if blocked else None)
        return html if not blocked else None
    except Exception as e:
        print_result("Method 1: Basic Requests (No Headers)", False, error=str(e))
        return None

# ============================================================================
# METHOD 2: Requests with User-Agent
# ============================================================================
def test_requests_with_ua():
    """Test 2: Requests with User-Agent header"""
    try:
        ua = UserAgent()
        headers = {'User-Agent': ua.random}
        response = requests.get(TEST_URL, headers=headers, timeout=10)
        html = response.text
        blocked, reason = check_for_blocking(html)
        print_result("Method 2: Requests with Random User-Agent", 
                    not blocked, len(html), reason if blocked else None)
        return html if not blocked else None
    except Exception as e:
        print_result("Method 2: Requests with Random User-Agent", False, error=str(e))
        return None

# ============================================================================
# METHOD 3: Requests with Full Headers
# ============================================================================
def test_requests_full_headers():
    """Test 3: Requests with comprehensive headers"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        response = requests.get(TEST_URL, headers=headers, timeout=10)
        html = response.text
        blocked, reason = check_for_blocking(html)
        print_result("Method 3: Requests with Full Browser Headers", 
                    not blocked, len(html), reason if blocked else None)
        return html if not blocked else None
    except Exception as e:
        print_result("Method 3: Requests with Full Browser Headers", False, error=str(e))
        return None

# ============================================================================
# METHOD 4: Requests with Session
# ============================================================================
def test_requests_with_session():
    """Test 4: Requests with session (maintains cookies)"""
    try:
        session = requests.Session()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        session.headers.update(headers)
        
        # First request to homepage to establish session
        session.get(SIMPLE_URL, timeout=10)
        time.sleep(1)
        
        # Second request to actual page
        response = session.get(TEST_URL, timeout=10)
        html = response.text
        blocked, reason = check_for_blocking(html)
        print_result("Method 4: Requests with Session (Cookie Persistence)", 
                    not blocked, len(html), reason if blocked else None)
        return html if not blocked else None
    except Exception as e:
        print_result("Method 4: Requests with Session (Cookie Persistence)", False, error=str(e))
        return None

# ============================================================================
# METHOD 5: Selenium with Basic Options
# ============================================================================
def test_selenium_basic():
    """Test 5: Selenium with basic ChromeDriver"""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.binary_location = "/usr/bin/chromium"
        
        service = Service("/usr/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(TEST_URL)
        time.sleep(2)
        html = driver.page_source
        driver.quit()
        
        blocked, reason = check_for_blocking(html)
        print_result("Method 5: Selenium Basic ChromeDriver", 
                    not blocked, len(html), reason if blocked else None)
        return html if not blocked else None
    except Exception as e:
        print_result("Method 5: Selenium Basic ChromeDriver", False, error=str(e))
        return None

# ============================================================================
# METHOD 6: Selenium with Stealth Options
# ============================================================================
def test_selenium_stealth():
    """Test 6: Selenium with stealth options"""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from fake_useragent import UserAgent
        
        ua = UserAgent()
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument(f"user-agent={ua.random}")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.binary_location = "/usr/bin/chromium"
        
        service = Service("/usr/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.get(TEST_URL)
        time.sleep(2)
        html = driver.page_source
        driver.quit()
        
        blocked, reason = check_for_blocking(html)
        print_result("Method 6: Selenium with Stealth Options", 
                    not blocked, len(html), reason if blocked else None)
        return html if not blocked else None
    except Exception as e:
        print_result("Method 6: Selenium with Stealth Options", False, error=str(e))
        return None

# ============================================================================
# METHOD 7: Undetected ChromeDriver
# ============================================================================
def test_undetected_chrome():
    """Test 7: Undetected ChromeDriver"""
    try:
        import undetected_chromedriver as uc
        from fake_useragent import UserAgent
        
        ua = UserAgent()
        options = uc.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"user-agent={ua.random}")
        options.binary_location = "/usr/bin/chromium"
        
        driver = uc.Chrome(options=options, use_subprocess=True)
        driver.get(TEST_URL)
        time.sleep(2)
        html = driver.page_source
        driver.quit()
        
        blocked, reason = check_for_blocking(html)
        print_result("Method 7: Undetected ChromeDriver", 
                    not blocked, len(html), reason if blocked else None)
        return html if not blocked else None
    except Exception as e:
        print_result("Method 7: Undetected ChromeDriver", False, error=str(e))
        return None

# ============================================================================
# METHOD 8: Playwright (if available)
# ============================================================================
def test_playwright():
    """Test 8: Playwright with Chromium"""
    try:
        from playwright.sync_api import sync_playwright
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=[
                '--no-sandbox',
                '--disable-dev-shm-usage'
            ])
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = context.new_page()
            page.goto(TEST_URL, wait_until='networkidle')
            html = page.content()
            browser.close()
            
            blocked, reason = check_for_blocking(html)
            print_result("Method 8: Playwright with Chromium", 
                        not blocked, len(html), reason if blocked else None)
            return html if not blocked else None
    except ImportError:
        print_result("Method 8: Playwright with Chromium", False, 
                    error="Playwright not installed (pip install playwright)")
        return None
    except Exception as e:
        print_result("Method 8: Playwright with Chromium", False, error=str(e))
        return None

# ============================================================================
# MAIN EXECUTION
# ============================================================================
def main():
    print("\n" + "="*60)
    print("DISCOGS WEB SCRAPING METHOD TESTING")
    print("="*60)
    print(f"Test URL: {TEST_URL}")
    print(f"Simple URL: {SIMPLE_URL}")
    print("="*60)
    
    results = {}
    
    # Run all tests
    print("\n\n🧪 Running Tests...\n")
    
    results['basic'] = test_basic_requests()
    time.sleep(1)
    
    results['ua'] = test_requests_with_ua()
    time.sleep(1)
    
    results['full_headers'] = test_requests_full_headers()
    time.sleep(1)
    
    results['session'] = test_requests_with_session()
    time.sleep(1)
    
    results['selenium_basic'] = test_selenium_basic()
    time.sleep(1)
    
    results['selenium_stealth'] = test_selenium_stealth()
    time.sleep(1)
    
    results['undetected_chrome'] = test_undetected_chrome()
    time.sleep(1)
    
    results['playwright'] = test_playwright()
    
    # Summary
    print("\n\n" + "="*60)
    print("SUMMARY OF RESULTS")
    print("="*60)
    
    successful_methods = [k for k, v in results.items() if v is not None]
    
    if successful_methods:
        print(f"\n✓ Successful Methods ({len(successful_methods)}):")
        for method in successful_methods:
            print(f"  - {method}")
        print(f"\n🎯 RECOMMENDATION: Use '{successful_methods[0]}' method")
    else:
        print("\n✗ No methods successfully bypassed bot detection")
        print("\n💡 Suggestions:")
        print("  1. Try with a proxy service")
        print("  2. Use a residential proxy")
        print("  3. Consider using Discogs API instead")
        print("  4. Add delays between requests")
        print("  5. Respect robots.txt and rate limits")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    main()
