from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import undetected_chromedriver as uc
import subprocess
import logging
import time
import re
import os
import random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# Use existing logger from app.py without reconfiguring
logger = logging.getLogger('scraper')

class YellowPagesScraper:
    def __init__(self):
        self.setup_driver()
        # Dictionary of US state abbreviations
        self.us_states = {
            'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR', 'california': 'CA',
            'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE', 'florida': 'FL', 'georgia': 'GA',
            'hawaii': 'HI', 'idaho': 'ID', 'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA',
            'kansas': 'KS', 'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
            'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS', 'missouri': 'MO',
            'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV', 'new hampshire': 'NH', 'new jersey': 'NJ',
            'new mexico': 'NM', 'new york': 'NY', 'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH',
            'oklahoma': 'OK', 'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
            'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT', 'vermont': 'VT',
            'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY',
            'district of columbia': 'DC'
        }
        # Add reverse mapping (abbreviation to full name)
        self.us_states.update({v: v for v in self.us_states.values()})
        
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
            logger.info("Setting up undetected-chromedriver...")

            options = uc.ChromeOptions()
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-extensions')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--log-level=3')
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

            # Create undetected-chromedriver instance
            self.driver = uc.Chrome(
                options=options,
                headless=False,  # Headless mode often doesn't work well with undetected-chromedriver
                use_subprocess=True,
                version_main=int(self.get_chrome_version())
            )
            
            self.driver.set_page_load_timeout(60)
            self.driver.get('about:blank')
            logger.info("Undetected-chromedriver is ready.")

        except Exception as e:
            logger.error(f"WebDriver setup failed: {e}")
            if hasattr(self, 'driver'):
                self.driver.quit()
            raise

    def clean_text(self, text):
        return re.sub(r'\s+', ' ', text.strip()) if text else ""

    def extract_email_addresses(self, text):
        """Extract email addresses from text using regex."""
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        return list(set(re.findall(email_pattern, text)))

    def extract_phone_numbers(self, text):
        """Extract phone numbers from text using regex."""
        phone_patterns = [
            r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',  # Standard US format
            r'\(\d{3}\)\s*\d{3}[-.]?\d{4}',     # (123) 456-7890
            r'\+1[-.]?\d{3}[-.]?\d{3}[-.]?\d{4}' # +1 format
        ]
        phones = []
        for pattern in phone_patterns:
            phones.extend(re.findall(pattern, text))
        return list(set(phones))

    def extract_license_numbers(self, text):
        """Extract potential license numbers using common patterns."""
        license_patterns = [
            r'License\s*#?\s*(\w+[-\s]?\d+)',
            r'License\s*Number\s*[:.]?\s*(\w+[-\s]?\d+)',
            r'Lic\s*[:.#]?\s*(\w+[-\s]?\d+)',
            r'Registration\s*#?\s*(\w+[-\s]?\d+)',
            r'Cert\s*[:.#]?\s*(\w+[-\s]?\d+)'
        ]
        licenses = []
        for pattern in license_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                licenses.append(match.group(1))
        return list(set(licenses))

    def extract_experience(self, text):
        """Extract experience information using common patterns."""
        experience_patterns = [
            r'(\d+)\+?\s*years?\s*(of\s*)?experience',
            r'(established|founded|serving\s*since)\s*in?\s*(\d{4})',
            r'since\s*(\d{4})',
            r'est\.\s*(\d{4})',
            r'experience\s*[:of]*\s*(\d+)\+?\s*years?'
        ]
        experiences = []
        for pattern in experience_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                if len(match.groups()) > 1 and match.group(2):
                    # For patterns with year
                    year = int(match.group(2))
                    current_year = time.localtime().tm_year
                    years = current_year - year
                    experiences.append(f"{years} years (since {year})")
                else:
                    # For patterns with direct year mention
                    experiences.append(f"{match.group(1)} years")
        return list(set(experiences))

    def extract_description(self, soup):
        """Extract business description from common locations in the webpage."""
        description = ""
        
        # Common description locations
        description_selectors = [
            'meta[name="description"]',
            'meta[property="og:description"]',
            '.about-us',
            '.company-description',
            '.business-description',
            '#about',
            '.about',
            '.overview',
            '.description',
            '[id*="about"]',
            '[class*="about"]',
            '[id*="overview"]',
            '[class*="overview"]'
        ]
        
        # Try meta descriptions first
        for selector in description_selectors[:2]:
            meta = soup.select_one(selector)
            if meta and meta.get('content'):
                description = meta['content'].strip()
                break
        
        # If no meta description, try content sections
        if not description:
            for selector in description_selectors[2:]:
                elements = soup.select(selector)
                for element in elements:
                    text = element.get_text(strip=True)
                    if len(text) > len(description):
                        description = text
        
        return description

    def scrape_business_website(self, url):
        """Scrape additional details from the business website."""
        if not url:
            return {}
        
        try:
            # Add http:// if not present
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url

            logger.info(f"Scraping business website: {url}")
            
            # Use requests with a timeout
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            # Parse the webpage
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Get all text content
            text_content = soup.get_text()
            
            # Extract various details
            emails = self.extract_email_addresses(text_content)
            phones = self.extract_phone_numbers(text_content)
            licenses = self.extract_license_numbers(text_content)
            experiences = self.extract_experience(text_content)
            description = self.extract_description(soup)
            
            # Create result dictionary
            details = {
                'Website Description': description[:500] if description else "",  # Limit description length
                'Additional Emails': ', '.join(emails) if emails else "",
                'Additional Phones': ', '.join(phones) if phones else "",
                'License Numbers': ', '.join(licenses) if licenses else "",
                'Experience': ', '.join(experiences) if experiences else "",
                'Website Status': 'Active'
            }
            
            return details
            
        except requests.RequestException as e:
            logger.warning(f"Error accessing website {url}: {str(e)}")
            return {
                'Website Status': 'Error',
                'Website Error': str(e)
            }
        except Exception as e:
            logger.error(f"Error scraping website {url}: {str(e)}")
            return {
                'Website Status': 'Error',
                'Website Error': str(e)
            }

    def parse_address(self, address_line1, address_line2):
        """
        Parse address components into street, city, state, ZIP, and suburb.
        Returns a dictionary with parsed components.
        """
        try:
            result = {
                'Street Address': '',
                'City': '',
                'State': '',
                'ZIP Code': '',
                'Suburb': ''
            }

            if not address_line1 and not address_line2:
                return result

            # Process address line 1 (usually street address)
            if address_line1:
                result['Street Address'] = address_line1.strip()

            # Process address line 2 (usually city, state, zip)
            if address_line2:
                # Common patterns for address line 2
                # Pattern 1: "City, State ZIP"
                # Pattern 2: "Suburb, City, State ZIP"
                # Pattern 3: "City State ZIP"
                
                # First, try to extract ZIP code
                zip_match = re.search(r'(\d{5}(?:-\d{4})?)', address_line2)
                if zip_match:
                    result['ZIP Code'] = zip_match.group(1)
                    # Remove ZIP from string for further processing
                    address_line2 = address_line2[:zip_match.start()].strip()

                # Look for state abbreviation or full name
                state_pattern = r'(?:^|\W)(' + '|'.join(self.us_states.keys()) + r')(?:\W|$)'
                state_match = re.search(state_pattern, address_line2.lower())
                if state_match:
                    state = state_match.group(1).upper()
                    result['State'] = self.us_states.get(state.lower(), state)
                    # Split the string at state location
                    parts = re.split(state_pattern, address_line2, flags=re.IGNORECASE)
                    
                    # Process parts before and after state
                    before_state = parts[0].strip(' ,')
                    after_state = parts[-1].strip(' ,')
                    
                    # If there's content before state, it could be city or suburb + city
                    if before_state:
                        city_parts = [p.strip() for p in before_state.split(',')]
                        if len(city_parts) > 1:
                            result['Suburb'] = city_parts[0]
                            result['City'] = city_parts[-1]
                        else:
                            result['City'] = city_parts[0]
                else:
                    # If no state found, assume everything is city
                    result['City'] = address_line2.strip(' ,')

            # Clean up results
            for key in result:
                if result[key]:
                    # Remove extra spaces and common punctuation
                    result[key] = re.sub(r'\s+', ' ', result[key].strip(' ,.'))

            return result

        except Exception as e:
            logger.error(f"Error parsing address: {e}")
            return {
                'Street Address': address_line1 or '',
                'City': '',
                'State': '',
                'ZIP Code': '',
                'Suburb': ''
            }

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

            # Get address components
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
                except NoSuchElementException:
                    pass

            # Parse address components
            address_components = self.parse_address(address_line1, address_line2)

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

            # Clean up categories - remove duplicates and redundant text
            if categories:
                # Split categories by common separators
                category_list = [cat.strip() for cat in re.split(r'[,&]', categories)]
                # Remove duplicates while preserving order
                seen = set()
                category_list = [cat for cat in category_list if not (cat in seen or seen.add(cat))]
                # Join back with commas
                categories = ', '.join(filter(None, category_list))

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
                # Format state properly - ensure it's not empty by using the parsed state
                state = address_components['State'].strip()
                if not state and address_components['City']:
                    # Try to extract state from city if it contains a comma
                    city_parts = address_components['City'].split(',')
                    if len(city_parts) > 1:
                        state = city_parts[-1].strip()
                
                details = {
                    'Name': name,
                    'Phone': phone,
                    'Address': f"{address_components['Street Address']}, {address_components['City']}, {state} {address_components['ZIP Code']}".strip(),
                    'Website': website,
                    'Categories': categories,
                    'Owner Name': owner_name,
                    'Source': 'yellowpages'
                }
                
                # If website exists, scrape additional details
                if website:
                    website_details = self.scrape_business_website(website)
                    # Only add non-empty website details
                    for key, value in website_details.items():
                        if value:  # Only add if value is not empty
                            details[key] = value
                
                return details
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
        """Clean up resources by closing the browser."""
        try:
            if hasattr(self, 'driver'):
                self.driver.quit()
                logger.info("WebDriver closed.")
        except Exception as e:
            logger.error(f"Error closing WebDriver: {e}")

