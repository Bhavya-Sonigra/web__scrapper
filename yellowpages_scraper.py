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
            # chrome_options.add_argument('--headless=new')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--ignore-certificate-errors')
            chrome_options.add_argument('--log-level=3')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

            driver_path = r"C:\Users\bhavy\Downloads\chromedriver-win64\chromedriver.exe"

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
            name = self.clean_text(business_element.find_element(By.CLASS_NAME, 'business-name').text)

            phone = ""
            try:
                phone = self.clean_text(business_element.find_element(By.CLASS_NAME, 'phones').text)
            except NoSuchElementException:
                pass

            full_address = ""
            try:
                address = self.clean_text(business_element.find_element(By.CLASS_NAME, 'street-address').text)
                locality = self.clean_text(business_element.find_element(By.CLASS_NAME, 'locality').text)
                full_address = f"{address}, {locality}"
            except NoSuchElementException:
                pass

            website = ""
            try:
                website = business_element.find_element(By.CSS_SELECTOR, 'a.track-visit-website').get_attribute('href')
            except NoSuchElementException:
                pass

            categories = ""
            try:
                categories = self.clean_text(business_element.find_element(By.CLASS_NAME, 'categories').text)
            except NoSuchElementException:
                pass

            return {
                'name': name,
                'phone': phone,
                'address': full_address,
                'website': website,
                'categories': categories,
                'source': 'yellowpages'
            }

        except Exception as e:
            logger.error(f"Error extracting business details: {e}")
            return None

    def scrape_yellowpages(self, search_query, location):
        results = []
        try:
            url = f"https://www.yellowpages.com/search?search_terms={search_query}&geo_location_terms={location}"
            logger.info(f"Navigating to: {url}")

            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'search-results'))
            )

            business_elements = self.driver.find_elements(By.CLASS_NAME, 'result')
            logger.info(f"Found {len(business_elements)} business listings")

            for index, element in enumerate(business_elements, 1):
                logger.info(f"Scraping business {index}/{len(business_elements)}")
                data = self.extract_business_details(element)
                if data:
                    results.append(data)

            logger.info(f"Scraping completed with {len(results)} results.")
            return results

        except TimeoutException:
            logger.error("Timeout: Search results not loaded.")
            with open('debug_page.html', 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            raise

        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            raise

        finally:
            logger.info("Scraping session ended.")
            time.sleep(1)

    def close(self):
        if hasattr(self, 'driver'):
            self.driver.quit()
            logger.info("WebDriver closed.")
