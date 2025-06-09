from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import subprocess
import logging
import time
import re
import os
import random

# Use existing logger from app.py without reconfiguring
logger = logging.getLogger('scraper')

class YellowPagesScraper:
    def __init__(self):
        self.setup_driver()
        
    def get_chrome_version(self):
        try:
            # Common Chrome installation paths
            possible_paths = [
                r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
                os.path.expanduser(r"~\AppData\Local\Google\Chrome\Application\chrome.exe")
            ]
            chrome_path = next((path for path in possible_paths if os.path.exists(path)), None)
            
            if not chrome_path:
                raise FileNotFoundError("Chrome executable not found.")
            
            logger.info(f"Chrome executable found at: {chrome_path}")

            # PowerShell command to fetch version
            ps_cmd = f'powershell -NoProfile -Command "(Get-Item \\"{chrome_path}\\").VersionInfo.FileVersion"'
            result = subprocess.run(ps_cmd, capture_output=True, text=True, shell=True)
            version = result.stdout.strip()

            if not version:
                raise Exception("Failed to detect Chrome version via PowerShell.")

            major_version = version.split('.')[0]
            logger.info(f"Detected Chrome version: {version} (Major: {major_version})")
            return major_version

        except Exception as e:
            logger.error(f"Error fetching Chrome version: {e}")
            raise

    def setup_driver(self):
        try:
            logger.info("Setting up Chrome WebDriver...")

            chrome_options = Options()
            chrome_options.add_argument('--headless=new')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--ignore-certificate-errors')
            chrome_options.add_argument('--log-level=3')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

            driver_path = r"C:\Users\Bhavya\Downloads\chromedriver-win64\chromedriver.exe"


            self.driver = webdriver.Chrome(
                service=Service(driver_path),
                options=chrome_options
            )
            self.driver.set_page_load_timeout(60)
            self.driver.get('about:blank')
            logger.info("Chrome WebDriver is ready.")

        except Exception as e:
            logger.error(f"WebDriver setup failed: {e}")
            if hasattr(self, 'driver'):
                self.driver.quit()
            raise

    def clean_text(self, text):
        return re.sub(r'\s+', ' ', text.strip()) if text else ""

    def extract_business_details(self, business_element):
        try:
            # Get business name
            name = ""
            try:
                name_elem = business_element.find_element(By.CSS_SELECTOR, '.business-name')
                name = self.clean_text(name_elem.text)
            except NoSuchElementException:
                try:
                    name_elem = business_element.find_element(By.CSS_SELECTOR, 'a.business-name')
                    name = self.clean_text(name_elem.text)
                except NoSuchElementException:
                    logger.warning("Could not find business name")
                    return None

            # Get phone number
            phone = ""
            try:
                phone_elem = business_element.find_element(By.CSS_SELECTOR, '.phones.phone.primary')
                phone = self.clean_text(phone_elem.text)
            except NoSuchElementException:
                try:
                    phone_elem = business_element.find_element(By.CSS_SELECTOR, '.phone')
                    phone = self.clean_text(phone_elem.text)
                except NoSuchElementException:
                    pass

            # Get full address and split into two parts
            address_line1 = ""
            address_line2 = ""
            try:
                street = business_element.find_element(By.CSS_SELECTOR, '.street-address')
                locality = business_element.find_element(By.CSS_SELECTOR, '.locality')
                address_line1 = self.clean_text(street.text)
                address_line2 = self.clean_text(locality.text)
            except NoSuchElementException:
                try:
                    address_elem = business_element.find_element(By.CSS_SELECTOR, '.adr')
                    full_address = self.clean_text(address_elem.text)
                    # Split full address into two parts
                    if ',' in full_address:
                        parts = full_address.split(',', 1)
                        address_line1 = parts[0].strip()
                        address_line2 = parts[1].strip()
                    else:
                        address_line1 = full_address
                        address_line2 = ""
                except NoSuchElementException:
                    pass

            # Construct full address for backward compatibility
            full_address = f"{address_line1}, {address_line2}" if address_line2 else address_line1

            # Get website
            website = ""
            try:
                website_elem = business_element.find_element(By.CSS_SELECTOR, 'a.track-visit-website')
                website = website_elem.get_attribute('href')
            except NoSuchElementException:
                pass

            # Get categories
            categories = ""
            try:
                categories_elem = business_element.find_element(By.CSS_SELECTOR, '.categories')
                categories = self.clean_text(categories_elem.text)
            except NoSuchElementException:
                try:
                    categories_elem = business_element.find_element(By.CSS_SELECTOR, '.links')
                    categories = self.clean_text(categories_elem.text)
                except NoSuchElementException:
                    pass

            # Get business owner name
            owner_name = ""
            try:
                # Try to find owner name in various possible locations
                owner_selectors = [
                    '.owner-name',
                    '.business-owner',
                    '.contact-name',
                    '.sales-info',
                    '.about-business'
                ]
                
                for selector in owner_selectors:
                    try:
                        owner_elem = business_element.find_element(By.CSS_SELECTOR, selector)
                        text = self.clean_text(owner_elem.text)
                        
                        # Look for common owner name patterns
                        owner_patterns = [
                            r'Owner:\s*([^,\n]+)',
                            r'Proprietor:\s*([^,\n]+)',
                            r'Manager:\s*([^,\n]+)',
                            r'Contact:\s*([^,\n]+)',
                            r'Founded by\s*([^,\n]+)',
                            r'Established by\s*([^,\n]+)',
                            r'President:\s*([^,\n]+)',
                            r'CEO:\s*([^,\n]+)'
                        ]
                        
                        for pattern in owner_patterns:
                            match = re.search(pattern, text, re.IGNORECASE)
                            if match:
                                owner_name = match.group(1).strip()
                                break
                        
                        if owner_name:
                            break
                            
                    except NoSuchElementException:
                        continue
                        
                # If we haven't found an owner name, try clicking on "more info" or similar buttons
                if not owner_name:
                    more_info_selectors = [
                        '.more-info',
                        '.view-details',
                        '.business-info',
                        '.show-more'
                    ]
                    
                    for selector in more_info_selectors:
                        try:
                            more_info_btn = business_element.find_element(By.CSS_SELECTOR, selector)
                            more_info_btn.click()
                            time.sleep(1)  # Wait for content to load
                            
                            # Try to find owner information in expanded content
                            expanded_content = business_element.find_element(By.CSS_SELECTOR, '.expanded-info')
                            text = self.clean_text(expanded_content.text)
                            
                            for pattern in owner_patterns:
                                match = re.search(pattern, text, re.IGNORECASE)
                                if match:
                                    owner_name = match.group(1).strip()
                                    break
                                    
                            if owner_name:
                                break
                                
                        except NoSuchElementException:
                            continue
                        except Exception as e:
                            logger.warning(f"Error clicking more info button: {e}")
                            continue
                            
            except Exception as e:
                logger.warning(f"Error extracting owner name: {e}")

            # Only return if we have at least a name
            if name:
                return {
                    'Name': name,
                    'Phone': phone,
                    'Address Line 1': address_line1,
                    'Address Line 2': address_line2,
                    'Website': website,
                    'Categories': categories,
                    'Owner Name': owner_name,  # Add owner name to returned data
                    'Source': 'yellowpages'
                }
            return None

        except Exception as e:
            logger.error(f"Error extracting business details: {e}")
            return None

    def random_delay(self, min_seconds=2, max_seconds=5):
        """Add a random delay between actions"""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)

    def scrape_yellowpages(self, search_query, location, min_results=100):
        results = []
        page = 1
        try:
            formatted_search = search_query.lower().replace(' ', '+')
            formatted_location = location.upper() if len(location) <= 3 else location.title()

            while len(results) < min_results:
                url = f"https://www.yellowpages.com/search?search_terms={formatted_search}&geo_location_terms={formatted_location}&page={page}"
                logger.info(f"Navigating to page {page}: {url}")
                self.driver.get(url)

                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'result'))
                    )
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)

                    business_elements = self.driver.find_elements(By.CLASS_NAME, 'result')
                    if not business_elements:
                        logger.warning(f"No listings found on page {page}")
                        break

                    logger.info(f"Found {len(business_elements)} listings on page {page}")

                    for index, element in enumerate(business_elements, 1):
                        logger.info(f"Scraping business {index}/{len(business_elements)} on page {page}")
                        data = self.extract_business_details(element)
                        if data:
                            results.append(data)

                    # End if fewer than 30 results on a page (YellowPages default)
                    if len(business_elements) < 30:
                        logger.info("Fewer listings found on this page – assuming last page.")
                        break

                except TimeoutException:
                    logger.warning(f"Timeout waiting for results on page {page}")
                    break

                page += 1
                time.sleep(random.uniform(3, 5))

            logger.info(f"Scraping completed with {len(results)} total results across {page} pages.")
            return results

        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            raise

        finally:
            logger.info("Scraping session ended.")
            time.sleep(1)

    def cleanup(self):
        if hasattr(self, 'driver'):
            self.driver.quit()
            logger.info("WebDriver closed.")

