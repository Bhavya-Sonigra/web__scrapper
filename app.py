from flask import Flask, render_template, request, jsonify, send_file, url_for, jsonify
from flask_cors import CORS  # Add CORS import
from bs4 import BeautifulSoup
import pandas as pd
import os
import asyncio
import aiohttp
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote, urlencode
import time
import random
from fake_useragent import UserAgent
from aiohttp_retry import RetryClient, ExponentialRetry
import json
import logging
from logging.handlers import RotatingFileHandler
import sys
import traceback
from functools import wraps
from scrapers.yellowpages_scraper import YellowPagesScraper
from scrapers.sulekha_scraper import SulekhaScraper
from scrapers.justdial_scraper import JustDialScraper
import re


app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Add configuration for timeouts and other settings
app.config.update(
    SEND_FILE_MAX_AGE_DEFAULT=0,
    MAX_CONTENT_LENGTH=16 * 1024 * 1024,  # 16MB max file size
    CORS_HEADERS='Content-Type'
)

# ScraperAPI configuration
SCRAPER_API_KEY = os.getenv('SCRAPER_API_KEY', '')

# Setup logging with more detailed configuration
def setup_logging():
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Create a timestamp for the log file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'logs/scraper_{timestamp}.log'
    
    # Setup formatters
    console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
    file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] - %(message)s')
    
    # Setup file handler with rotation
    file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)  # 10MB per file
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)
    
    # Setup console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)
    
    # Setup logger
    logger = logging.getLogger('scraper')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Log system information
    logger.info("=== Starting new scraping session ===")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Operating System: {sys.platform}")
    logger.info(f"Working Directory: {os.getcwd()}")
    
    return logger

# Initialize logger
logger = setup_logging()

# Error handling decorator
def handle_errors(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            error_msg = f"Error in {func.__name__}: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            return None
        finally:
            duration = time.time() - start_time
            logger.info(f"Function {func.__name__} took {duration:.2f} seconds to execute")
    return wrapper

# Custom exceptions
class ScraperException(Exception):
    """Base exception for scraper errors"""
    pass

class RateLimitException(ScraperException):
    """Raised when rate limit is hit"""
    pass

class ProxyException(ScraperException):
    """Raised when proxy error occurs"""
    pass

class ParsingException(ScraperException):
    """Raised when HTML parsing fails"""
    pass

# Error monitoring and statistics
class ScraperStats:
    def __init__(self):
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.blocked_requests = 0
        self.cache_hits = 0
        self.total_businesses_found = 0
        self.start_time = None
        self.end_time = None
        self.errors = []
    
    def start_session(self):
        self.start_time = datetime.now()
    
    def end_session(self):
        self.end_time = datetime.now()
    
    def add_error(self, error_type, message):
        self.errors.append({
            'type': error_type,
            'message': message,
            'timestamp': datetime.now()
        })
    
    def get_success_rate(self):
        if self.total_requests == 0:
            return 0
        return (self.successful_requests / self.total_requests) * 100
    
    def get_session_duration(self):
        if not self.start_time or not self.end_time:
            return None
        return (self.end_time - self.start_time).total_seconds()
    
    def generate_report(self):
        return {
            'total_requests': self.total_requests,
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'blocked_requests': self.blocked_requests,
            'cache_hits': self.cache_hits,
            'success_rate': f"{self.get_success_rate():.2f}%",
            'total_businesses_found': self.total_businesses_found,
            'session_duration': self.get_session_duration(),
            'errors': self.errors
        }

# Initialize stats
scraper_stats = ScraperStats()

# Load configuration
def load_config():
    default_config = {
        'max_retries': 3,
        'proxy_enabled': False,
        'proxy_list': [],
        'rate_limit': 2,  # seconds between requests
        'max_concurrent_requests': 3,
        'user_agent_rotation': True,
        'save_raw_html': False,
        'cache_duration': 24,  # hours
        'blocked_ip_timeout': 30,  # minutes
    }
    
    try:
        if os.path.exists('config.json'):
            with open('config.json', 'r') as f:
                config = json.load(f)
                return {**default_config, **config}
    except Exception as e:
        print(f"Error loading config: {e}")
    return default_config

# Initialize User Agent rotator
try:
    ua = UserAgent()
except Exception:
    ua = None
    print("Could not initialize UserAgent, falling back to default")

class ScraperUtils:
    def __init__(self):
        self.config = load_config()
        self.blocked_ips = {}
        self.cache = {}
        self.last_request_time = {}
    
    def reset_state(self):
        """Reset all state data between requests"""
        self.blocked_ips = {}
        self.cache = {}
        self.last_request_time = {}
    
    def get_random_user_agent(self):
        if ua and self.config['user_agent_rotation']:
            try:
                return ua.random
            except Exception:
                pass
        return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    
    def get_proxy(self):
        if not self.config['proxy_enabled'] or not self.config['proxy_list']:
            return None
        return random.choice(self.config['proxy_list'])
    
    def is_ip_blocked(self, ip):
        if ip in self.blocked_ips:
            block_time = self.blocked_ips[ip]
            if datetime.now() - block_time < timedelta(minutes=self.config['blocked_ip_timeout']):
                return True
            del self.blocked_ips[ip]
        return False
    
    def mark_ip_blocked(self, ip):
        self.blocked_ips[ip] = datetime.now()
    
    def get_from_cache(self, url):
        if url in self.cache:
            timestamp, data = self.cache[url]
            if datetime.now() - timestamp < timedelta(hours=self.config['cache_duration']):
                return data
            del self.cache[url]
        return None
    
    def save_to_cache(self, url, data):
        self.cache[url] = (datetime.now(), data)
    
    async def make_request(self, session, url, headers):
        # Check cache first
        cached_data = self.get_from_cache(url)
        if cached_data:
            print(f"Using cached data for {url}")
            return cached_data
        
        # Rate limiting
        current_time = datetime.now()
        if url in self.last_request_time:
            time_since_last_request = (current_time - self.last_request_time[url]).total_seconds()
            if time_since_last_request < self.config['rate_limit']:
                await asyncio.sleep(self.config['rate_limit'] - time_since_last_request)
        
        # Update headers with random user agent
        headers['User-Agent'] = self.get_random_user_agent()
        
        # Setup retry client
        retry_options = ExponentialRetry(
            attempts=self.config['max_retries'],
            start_timeout=1,
            max_timeout=10,
            factor=2,
            statuses={500, 502, 503, 504}
        )
        
        # Get proxy
        proxy = self.get_proxy()
        if proxy and not self.is_ip_blocked(proxy):
            try:
                async with RetryClient(
                    client_session=session,
                    retry_options=retry_options,
                    proxy=proxy
                ) as client:
                    async with client.get(url, headers=headers, timeout=30) as response:
                        if response.status == 200:
                            content = await response.text()
                            self.save_to_cache(url, content)
                            self.last_request_time[url] = current_time
                            return content
                        elif response.status == 403:
                            self.mark_ip_blocked(proxy)
                            print(f"Proxy {proxy} has been blocked")
                        return None
            except Exception as e:
                print(f"Error with proxy {proxy}: {str(e)}")
        
        # Fallback to direct connection
        try:
            async with RetryClient(
                client_session=session,
                retry_options=retry_options
            ) as client:
                async with client.get(url, headers=headers, timeout=30) as response:
                    if response.status == 200:
                        content = await response.text()
                        self.save_to_cache(url, content)
                        self.last_request_time[url] = current_time
                        return content
                    return None
        except Exception as e:
            print(f"Error making request to {url}: {str(e)}")
            return None

# Initialize scraper utils
scraper_utils = ScraperUtils()

def clean_search_query(query):
    # Remove extra spaces and common typos
    corrections = {
        'restaurents': 'restaurants',
        'resturants': 'restaurants',
        'restraunts': 'restaurants',
        'appartments': 'apartments',
        'appartment': 'apartment',
        'hotells': 'hotels',
        'accomodation': 'accommodation',
        'acommodation': 'accommodation',
        'buisness': 'business',
        'bussiness': 'business',
        'docter': 'doctor',
        'docteur': 'doctor',
        'enginear': 'engineer',
        'enginer': 'engineer',
        # Add city name corrections
        'vadodra': 'vadodara',
        'bombay': 'mumbai',
        'calcutta': 'kolkata',
        'madras': 'chennai',
        'bangalore': 'bengaluru',
        'poona': 'pune',
        'mysore': 'mysuru',
        'cochin': 'kochi',
        'cuttack': 'katak',
        'trichur': 'thrissur',
        'trivandrum': 'thiruvananthapuram',
        'mangalore': 'mangaluru',
        'simla': 'shimla',
        'gauhati': 'guwahati',
        'hubli': 'hubballi',
        'ahmadabad': 'ahmedabad',
        'allahabad': 'prayagraj',
        'baroda': 'vadodara',
        'benares': 'varanasi',
        'benaras': 'varanasi',
        'vizag': 'visakhapatnam'
    }
    
    # Split into words and correct each word
    words = query.lower().strip().split()
    corrected_words = [corrections.get(word, word) for word in words]
    
    # Join back and capitalize first letter of each word
    return ' '.join(word.capitalize() for word in corrected_words)

def extract_location(query):
    # Common Indian cities and their variations
    cities = {
        'mumbai': ['bombay', 'navi mumbai'],
        'delhi': ['new delhi', 'ncr', 'delhi ncr'],
        'bangalore': ['bengaluru', 'blr', 'blore'],
        'hyderabad': ['hyd', 'secunderabad'],
        'chennai': ['madras'],
        'kolkata': ['calcutta'],
        'pune': ['poona'],
        'ahmedabad': ['amdavad', 'ahd'],
        'surat': ['surat city'],
        'jaipur': ['pink city']
    }
    
    # Flatten city variations
    city_variations = {}
    for main_city, variations in cities.items():
        city_variations[main_city] = main_city
        for var in variations:
            city_variations[var] = main_city
    
    words = query.lower().split()
    location = None
    category = []
    
    # First try to find "in city" pattern
    try:
        loc_index = words.index('in')
        location_words = ' '.join(words[loc_index + 1:])
        category = ' '.join(words[:loc_index])
        
        # Check if the location matches any city variation
        for loc_word in location_words.split():
            if loc_word in city_variations:
                location = city_variations[loc_word]
                break
        if not location:
            location = location_words
    except ValueError:
        # If no "in" found, look for city names directly
        for i, word in enumerate(words):
            if word in city_variations:
                location = city_variations[word]
                category = ' '.join(words[:i] + words[i+1:])
                break
    
    if not location:
        category = query
        # Try to find city name in the entire query
        for city_var in city_variations:
            if city_var in query.lower():
                location = city_variations[city_var]
                category = query.lower().replace(city_var, '').replace('  ', ' ').strip()
                break
    
    return clean_search_query(category), location.title() if location else None

async def scrape_page(session, url, headers):
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            print(f"Attempting to fetch URL (attempt {attempt + 1}): {url}")
            async with session.get(url, headers=headers, timeout=30) as response:
                print(f"Response status: {response.status} for {url}")
                if response.status == 200:
                    content = await response.text()
                    print(f"Successfully fetched content from {url} (length: {len(content)})")
                    # Print first 200 characters to see what we're getting
                    print(f"Content preview: {content[:200]}")
                    return content
                elif response.status == 429:  # Too Many Requests
                    print(f"Rate limited on {url}, waiting {retry_delay * (attempt + 1)} seconds")
                    await asyncio.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    print(f"Error status {response.status} for {url}")
                    return None
        except Exception as e:
            print(f"Error fetching {url} (attempt {attempt + 1}): {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
            else:
                return None
    return None

async def process_listing(listing, platform='justdial'):
    business_data = {
        'Company Name': '',
        'Phone': '',
        'Email': '',
        'Website': '',
        'About': '',
        'Social Links': '',
        'Address': ''
    }
    
    try:
        if platform == 'justdial':
            # Company Name - multiple possible class names
            name_elem = listing.find(['span', 'a', 'h2'], {
                'class': ['jcn', 'store-name', 'lng_cont_name', 'jsx-3349e7cd87e12d75', 'company-name']
            })
            if not name_elem:
                name_elem = listing.find(['a', 'h2'], {'data-test': 'business-name'})
            if name_elem:
                business_data['Company Name'] = name_elem.text.strip()
            
            # Phone - check for multiple patterns
            phone_elem = listing.find(['p', 'span', 'a', 'div'], {
                'class': ['contact-info', 'mobilesv', 'tel', 'jsx-3349e7cd87e12d75 contact', 'phone-number']
            })
            if not phone_elem:
                phone_elem = listing.find(['span', 'div'], {'data-test': 'phone-number'})
            if phone_elem:
                business_data['Phone'] = phone_elem.text.strip()
            
            # Address - multiple possible locations
            addr_elem = listing.find(['p', 'span', 'div'], {
                'class': ['address-info', 'cont_fl_addr', 'jsx-3349e7cd87e12d75 address', 'business-address']
            })
            if not addr_elem:
                addr_elem = listing.find(['span', 'div'], {'data-test': 'business-address'})
            if addr_elem:
                business_data['Address'] = addr_elem.text.strip()
                
        elif platform == 'sulekha':
            # Company Name
            name_elem = listing.find(['h2', 'h3', 'a', 'div'], {
                'class': ['business-name', 'bname', 'vendor-name', 'listing-name']
            })
            if name_elem:
                business_data['Company Name'] = name_elem.text.strip()
            
            # Phone
            phone_elem = listing.find(['p', 'span', 'div'], {
                'class': ['phone', 'contact', 'vendor-phone', 'business-phone']
            })
            if phone_elem:
                business_data['Phone'] = phone_elem.text.strip()
            
            # Address
            addr_elem = listing.find(['p', 'div', 'span'], {
                'class': ['address', 'location', 'vendor-address', 'business-location']
            })
            if addr_elem:
                business_data['Address'] = addr_elem.text.strip()
        
        # Common processing for both platforms
        # Website and email extraction
        links = listing.find_all('a', href=True)
        for link in links:
            href = link.get('href', '').lower()
            if 'mailto:' in href:
                business_data['Email'] = href.replace('mailto:', '').strip()
            elif any(domain in href for domain in ['.com', '.in', '.org', '.net', '.co.in']):
                if not any(excluded in href for excluded in ['justdial', 'sulekha', 'facebook', 'twitter', 'linkedin', 'instagram']):
                    business_data['Website'] = href
        
        # About/Description
        about_elem = listing.find(['p', 'div', 'span'], {
            'class': ['description', 'about', 'desc', 'business-desc', 'vendor-desc']
        })
        if about_elem:
            business_data['About'] = about_elem.text.strip()
            
        # Social Links
        social_links = []
        social_platforms = ['facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com']
        for link in links:
            href = link.get('href', '').lower()
            if any(platform in href for platform in social_platforms):
                social_links.append(href)
        if social_links:
            business_data['Social Links'] = ', '.join(social_links)
            
    except Exception as e:
        print(f"Error processing listing: {str(e)}")
        
    # Print debug info
    if business_data['Company Name']:
        print(f"Found business: {business_data['Company Name']}")
        
    return business_data

@handle_errors
async def scrape_justdial(search_query, location=None):
    # Initialize empty data list
    data = []
    
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://www.justdial.com/',
        'DNT': '1'
    }
    
    try:
        scraper_stats.start_session()
        
        # Format the search query for JustDial's URL structure
        search_query = search_query.replace(' ', '-').lower()
        if location:
            location = location.replace(' ', '-').lower()
            base_url = f"https://www.justdial.com/{location}/{search_query}-in-{location}"
        else:
            base_url = f"https://www.justdial.com/{search_query}"
        
        logger.info(f"Starting scrape for query: {search_query} in {location}")
        
        # Try multiple pages
        max_empty_pages = 3
        empty_page_count = 0
        page = 1
        
        while empty_page_count < max_empty_pages and page <= 10:
            try:
                page_url = base_url if page == 1 else f"{base_url}/page-{page}"
                logger.info(f"Fetching page {page}: {page_url}")
                
                async with aiohttp.ClientSession() as session:
                    content = await scraper_utils.make_request(session, page_url, headers)
                    
                    if content:
                        scraper_stats.successful_requests += 1
                        page_data = []
                        
                        try:
                            soup = BeautifulSoup(content, 'html.parser')
                            listings = []
                            
                            # Look for different types of listing containers
                            possible_containers = [
                                {'class': 'store-details'},
                                {'class': 'jsx-3349e7cd87e12d75'},
                                {'class': 'resultbox_info'},
                                {'class': 'business-listing'},
                                {'class': 'lst_dt'},
                                {'class': 'cntanr'},
                                {'data-href': True},
                                {'itemtype': 'http://schema.org/LocalBusiness'}
                            ]
                            
                            for container in possible_containers:
                                found = soup.find_all(['div', 'li', 'section'], container)
                                if found:
                                    listings.extend(found)
                            
                            logger.info(f"Found {len(listings)} listings on page {page}")
                            
                            # Process each listing
                            for listing in listings:
                                try:
                                    business_data = extract_business_data(listing, 'justdial')
                                    if business_data and business_data['Company Name']:
                                        page_data.append(business_data)
                                        scraper_stats.total_businesses_found += 1
                                except Exception as e:
                                    logger.error(f"Error processing listing: {str(e)}")
                                    scraper_stats.add_error('parsing', str(e))
                                    continue
                            
                            if page_data:
                                data.extend(page_data)
                                empty_page_count = 0
                            else:
                                empty_page_count += 1
                                logger.warning(f"No data found on page {page}")
                            
                        except Exception as e:
                            logger.error(f"Error parsing page {page}: {str(e)}")
                            scraper_stats.add_error('parsing', str(e))
                            empty_page_count += 1
                    else:
                        scraper_stats.failed_requests += 1
                        logger.error(f"Failed to fetch page {page}")
                        empty_page_count += 1
                    
                    await asyncio.sleep(scraper_utils.config['rate_limit'])
                    page += 1
                    
            except Exception as e:
                logger.error(f"Error on page {page}: {str(e)}")
                scraper_stats.add_error('request', str(e))
                empty_page_count += 1
                page += 1
                continue
    
    except Exception as e:
        logger.error(f"Error scraping JustDial: {str(e)}")
        scraper_stats.add_error('scraping', str(e))
        raise ScraperException(f"Scraping failed: {str(e)}")
    
    finally:
        scraper_stats.end_session()
        logger.info("Generating scraping report...")
        report = scraper_stats.generate_report()
        logger.info(f"Scraping Report: {json.dumps(report, indent=2)}")
    
    return data

def decode_phone_number(element):
    """
    Decode phone numbers from various formats including JustDial's protection mechanisms
    """
    if not element:
        return ''
        
    # Dictionary for JustDial's number mapping (commonly used patterns)
    jd_map = {
        # Original mapping
        'icon-dc': '+',
        'icon-fe': '(',
        'icon-hg': ')',
        'icon-ba': '-',
        'icon-yz': '1',
        'icon-wx': '2',
        'icon-vu': '3',
        'icon-ts': '4',
        'icon-rq': '5',
        'icon-po': '6',
        'icon-nm': '7',
        'icon-lk': '8',
        'icon-ji': '9',
        'icon-acb': '0',
        # Additional mappings (JustDial uses multiple patterns)
        'dc': '+',
        'fe': '(',
        'hg': ')',
        'ba': '-',
        'yz': '1',
        'wx': '2',
        'vu': '3',
        'ts': '4',
        'rq': '5',
        'po': '6',
        'nm': '7',
        'lk': '8',
        'ji': '9',
        'acb': '0',
        # Newer format
        'icon-plus': '+',
        'icon-left': '(',
        'icon-right': ')',
        'icon-hyphen': '-',
        'icon-one': '1',
        'icon-two': '2',
        'icon-three': '3',
        'icon-four': '4',
        'icon-five': '5',
        'icon-six': '6',
        'icon-seven': '7',
        'icon-eight': '8',
        'icon-nine': '9',
        'icon-zero': '0',
    }
    
    try:
        # First try to get direct text
        phone = element.text.strip()
        
        # If no direct text, try to decode JustDial format
        if not phone or phone.isspace():
            # Look for spans with special classes
            spans = element.find_all(['span', 'a', 'b'], {'class': True})
            if spans:
                phone = ''
                for span in spans:
                    # Get all classes
                    classes = span.get('class', [])
                    # Try to match against all patterns
                    for cls in classes:
                        # Remove common prefixes if present
                        cls = cls.replace('icon-', '').replace('jd-', '').replace('tel-', '')
                        # Look up in mapping
                        digit = jd_map.get(cls) or jd_map.get(f"icon-{cls}")
                        if digit:
                            phone += digit
                            
            # If no spans found, try to find data attributes
            if not phone:
                # Check various data attributes JustDial uses
                for attr in ['data-href', 'data-phone', 'data-tel', 'data-value']:
                    value = element.get(attr, '')
                    if value and any(c.isdigit() for c in value):
                        phone = value
                        break
                        
        # If still no phone, try to get from 'data-href' attribute (common in mobile versions)
        if not phone or phone.isspace():
            phone = element.get('data-href', '')
            
        # If still no phone, try to get from 'href' attribute (some sites use tel: links)
        if not phone or phone.isspace():
            href = element.get('href', '')
            if href.startswith('tel:'):
                phone = href.replace('tel:', '')
                
        # Clean up the phone number
        if phone:
            # Remove all non-digit characters except +()-
            phone = ''.join(c for c in phone if c.isdigit() or c in '+-() ')
            # Remove any extra spaces
            phone = ' '.join(phone.split())
            # If it's too short or too long, it's probably not a valid phone number
            if len(''.join(c for c in phone if c.isdigit())) < 5:
                return ''
            # Format the number if it looks like an Indian phone number
            digits = ''.join(c for c in phone if c.isdigit())
            if len(digits) == 10:
                return f"+91 {digits[:3]}-{digits[3:6]}-{digits[6:]}"
            elif len(digits) > 10:
                return f"+{digits[:2]} {digits[2:5]}-{digits[5:8]}-{digits[8:]}"
                
        return phone
    except Exception as e:
        print(f"Error decoding phone number: {str(e)}")
        return ''

def extract_complete_address(listing):
    """Extract complete address from listing with multiple fallback methods"""
    address_parts = []
    
    try:
        # Look for structured address containers
        address_containers = []
        
        # Main address containers
        address_containers.extend(listing.find_all(['div', 'p', 'span'], {
            'class': lambda x: x and any(term in str(x).lower() 
                for term in ['address', 'location', 'area', 'full-address', 'map-address'])
        }))
        
        # JustDial specific address containers
        address_containers.extend(listing.find_all(['div', 'p'], {
            'class': lambda x: x and any(term in str(x).lower() 
                for term in ['cont_fl_addr', 'address-info', 'lng_add'])
        }))
        
        # Look for schema.org structured data
        address_containers.extend(listing.find_all(['div', 'span'], {
            'itemprop': 'address'
        }))
        
        # Process each container
        for container in address_containers:
            # Try to find structured address parts
            parts = {}
            
            # Look for specific address components
            for part in ['streetAddress', 'addressLocality', 'addressRegion', 'postalCode']:
                elem = container.find(['span', 'div'], {'itemprop': part})
                if elem:
                    parts[part] = elem.text.strip()
            
            # If we found structured parts, combine them
            if parts:
                structured_addr = ' '.join(filter(None, [
                    parts.get('streetAddress', ''),
                    parts.get('addressLocality', ''),
                    parts.get('addressRegion', ''),
                    parts.get('postalCode', '')
                ]))
                if structured_addr:
                    address_parts.append(structured_addr)
            else:
                # If no structured parts, get the full text
                text = container.text.strip()
                if text:
                    address_parts.append(text)
        
        # Look for address in data attributes
        for elem in listing.find_all(['div', 'span', 'a'], {'data-address': True}):
            addr = elem.get('data-address', '').strip()
            if addr:
                address_parts.append(addr)
        
        # Clean up and combine addresses
        cleaned_addresses = []
        for addr in address_parts:
            # Basic cleanup
            addr = addr.strip()
            addr = ' '.join(addr.split())  # Remove extra whitespace
            addr = addr.replace('\n', ', ').replace('\r', ', ')
            
            # Remove common prefixes
            prefixes = ['address:', 'location:', 'full address:', 'map address:']
            for prefix in prefixes:
                if addr.lower().startswith(prefix):
                    addr = addr[len(prefix):].strip()
            
            # Remove very short or invalid addresses
            if len(addr) > 10 and not addr.isdigit():
                cleaned_addresses.append(addr)
        
        # Remove duplicates while preserving order
        seen = set()
        final_addresses = []
        for addr in cleaned_addresses:
            if addr.lower() not in seen:
                seen.add(addr.lower())
                final_addresses.append(addr)
        
        # Combine all unique addresses
        if final_addresses:
            return ' | '.join(final_addresses)
        
        return ''
        
    except Exception as e:
        print(f"Error extracting address: {str(e)}")
        return ''

def extract_business_data(listing, platform):
    """Helper function to extract business data from a listing."""
    business_data = {
        'Company Name': '',
        'Phone': '',
        'Email': '',
        'Website': '',
        'About': '',
        'Social Links': '',
        'Address': '',
        'Rating': '',
        'Reviews Count': '',
        'Categories': '',
        'Working Hours': '',
        'Features': ''
    }
    
    try:
        # Company Name with multiple fallbacks
        for tag in ['h2', 'h3', 'a', 'span', 'div']:
            for class_pattern in ['name', 'title', 'heading', 'bname']:
                name_elem = listing.find(tag, {'class': lambda x: x and class_pattern in str(x).lower()})
                if name_elem:
                    business_data['Company Name'] = name_elem.text.strip()
                    break
            if business_data['Company Name']:
                break
        
        # Phone number extraction (using existing enhanced code)
        phone_numbers = set()
        phone_containers = []
        
        # Direct phone elements
        phone_containers.extend(listing.find_all(['p', 'span', 'div', 'a', 'b'], {
            'class': lambda x: x and any(term in str(x).lower() for term in ['phone', 'mobile', 'contact', 'tel', 'mob', 'call'])
        }))
        
        # Process each container for phone numbers
        for container in phone_containers:
            phone = decode_phone_number(container)
            if phone:
                phone_numbers.add(phone)
        
        if phone_numbers:
            business_data['Phone'] = ' / '.join(sorted(phone_numbers))
        
        # Enhanced address extraction
        business_data['Address'] = extract_complete_address(listing)
        
        # Website and email extraction
        links = listing.find_all('a', href=True)
        for link in links:
            href = link.get('href', '').lower()
            if 'mailto:' in href:
                business_data['Email'] = href.replace('mailto:', '').strip()
            elif any(domain in href for domain in ['.com', '.in', '.org', '.net', '.co.in']):
                if not any(excluded in href for excluded in ['justdial', 'sulekha', 'facebook', 'twitter', 'linkedin', 'instagram']):
                    business_data['Website'] = href
        
        # Rating extraction
        rating_elem = listing.find(['span', 'div'], {'class': lambda x: x and 'rating' in str(x).lower()})
        if rating_elem:
            rating = rating_elem.text.strip()
            import re
            rating_match = re.search(r'(\d+(\.\d+)?)', rating)
            if rating_match:
                business_data['Rating'] = rating_match.group(1)
        
        # Reviews count extraction
        reviews_elem = listing.find(['span', 'div'], {'class': lambda x: x and 'review' in str(x).lower()})
        if reviews_elem:
            reviews = reviews_elem.text.strip()
            import re
            count_match = re.search(r'(\d+)', reviews)
            if count_match:
                business_data['Reviews Count'] = count_match.group(1)
        
        # Clean up empty fields
        business_data = {k: v for k, v in business_data.items() if v}
        
        return business_data
        
    except Exception as e:
        print(f"Error extracting business data: {str(e)}")
        return None

async def scrape_sulekha(search_query, location=None):
    # Initialize the scraper with the API key
    scraper = SulekhaScraper(SCRAPER_API_KEY)

    # Just call the internal scraper logic and return the data
    data = await scraper.scrape(search_query, location)
    return data

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html', vpn_required=True)

@app.route('/scrape', methods=['POST'])
def scrape():
    try:
        # Reset all states at the start of each request
        scraper_utils.reset_state()
        
        search_query = request.form.get('search_query')
        platform = request.form.get('platform')
        
        logger.info(f"\n=== Starting new scraping request ===")
        logger.info(f"Raw search query: {search_query}")
        logger.info(f"Selected platform: {platform}")
        
        if not search_query or not platform:
            return render_template('index.html', error='Please provide both search query and platform')
        
        # Clean and parse the search query
        category, location = extract_location(search_query)
        
        logger.info(f"Parsed category: {category}")
        logger.info(f"Parsed location: {location}")
        
        if not category:
            return render_template('index.html', 
                                 error='Please provide a business category (e.g., Hotels, Restaurants, Plumbers)')
        
        # Reset stats for new session
        global scraper_stats
        scraper_stats = ScraperStats()
        
        # Create event loop and run async scraping
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        data = []
        try:
            logger.info(f"\nStarting scraping process...")
            if platform == 'justdial':
                logger.info(f"Scraping JustDial for {category} in {location}")
                data = loop.run_until_complete(scrape_justdial(category, location))
            elif platform == 'sulekha':
                logger.info(f"Scraping Sulekha for {category} in {location}")
                data = loop.run_until_complete(scrape_sulekha(category, location))
            elif platform == 'all':
                logger.info(f"Scraping all platforms for {category} in {location}")
                justdial_data, sulekha_data = loop.run_until_complete(asyncio.gather(
                    scrape_justdial(category, location),
                    scrape_sulekha(category, location)
                ))
                justdial_data = justdial_data or []
                sulekha_data = sulekha_data or []
                data = justdial_data + sulekha_data
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
            scraper_stats.add_error('scraping', str(e))
            raise
        finally:
            loop.close()
        
        logger.info(f"\nScraping completed. Found {len(data)} results")
        
        if not data:
            suggestions = [
                f"Try adding a location (e.g., {category} in London)",
                f"Try a different category (e.g., {category}s, {category} Services)",
                "Check for spelling mistakes",
                "Try searching on a single platform instead of all"
            ]
            return render_template('index.html', 
                                 error=f'No data found for "{search_query}" on {platform}.', 
                                 suggestions=suggestions)
        
        # Generate scraping report
        report = scraper_stats.generate_report()
        
        try:
            # Create meaningful filename with timestamp to ensure uniqueness
            safe_category = category.replace(' ', '_').lower()
            safe_location = location.replace(' ', '_').lower() if location else 'all'
            safe_platform = platform.lower()
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            filename = f"{safe_category}_{safe_location}_{safe_platform}_{timestamp}.xlsx"
            
            # Ensure downloads directory exists
            os.makedirs('downloads', exist_ok=True)
            filepath = os.path.join('downloads', filename)
            
            # Clean up old files before creating new one
            try:
                current_time = datetime.now()
                for f in os.listdir('downloads'):
                    if f.endswith('.xlsx'):
                        file_path = os.path.join('downloads', f)
                        # Remove files older than 1 hour
                        if current_time - datetime.fromtimestamp(os.path.getctime(file_path)) > timedelta(hours=1):
                            try:
                                os.remove(file_path)
                                logger.info(f"Removed old file: {f}")
                            except Exception as e:
                                logger.error(f"Error removing old file {f}: {str(e)}")
            except Exception as e:
                logger.error(f"Error during file cleanup: {str(e)}")
            
            # Create DataFrame with all possible columns
            all_columns = set()
            for item in data:
                if isinstance(item, dict):  # Ensure item is a dictionary
                    all_columns.update(item.keys())
            
            # Sort columns in a logical order
            column_order = [
                'Name',
                'Phone',
                'Email',
                'Website',
                'Address Line 1',
                'Address Line 2',
                'Owner Name',
                'Rating',
                'Reviews Count',
                'Category',
                'Categories',  # For YellowPages
                'Description',
                'Company Name',
                'Source'  # For YellowPages
            ]
            
            # Add any additional columns that might exist
            remaining_columns = sorted(list(all_columns - set(column_order)))
            column_order.extend(remaining_columns)
            
            # Create DataFrame with ordered columns, handle empty data case
            if not data:
                df = pd.DataFrame(columns=['Name'])  # Create empty DataFrame with at least Name column
            else:
                df = pd.DataFrame(data)
                
                # Ensure all expected columns exist
                for col in column_order:
                    if col not in df.columns:
                        df[col] = ''
            
            # Reorder columns (only include columns that exist in the data)
            existing_columns = [col for col in column_order if col in df.columns]
            if existing_columns:  # Only reorder if there are columns
                df = df[existing_columns]
            
            # Clean up the data
            for col in df.columns:
                # Replace empty strings and None with NaN
                df[col] = df[col].replace(['', None], pd.NA)
                
                # Clean up text fields
                if df[col].dtype == 'object':
                    df[col] = df[col].str.strip()
                    df[col] = df[col].str.replace('\n', ' ')
                    df[col] = df[col].str.replace('\r', ' ')
                    df[col] = df[col].str.replace('\t', ' ')
                    df[col] = df[col].str.replace('  ', ' ')
            
            # Remove duplicates
            duplicate_subset = ['Name']
            if 'Phone' in df.columns:
                duplicate_subset.append('Phone')
            if 'Address' in df.columns:
                duplicate_subset.append('Address')
            
            df = df.drop_duplicates(subset=duplicate_subset, keep='first')
            
            # Create Excel writer object with xlsxwriter engine
            writer = pd.ExcelWriter(filepath, engine='xlsxwriter')
            
            # Write the dataframe to Excel
            df.to_excel(writer, index=False, sheet_name='Business Data')
            
            # Write the scraping report to a new sheet
            report_df = pd.DataFrame([report])
            report_df.to_excel(writer, index=False, sheet_name='Scraping Report')
            
            # Get the xlsxwriter workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['Business Data']
            
            # Add formats
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#D7E4BC',
                'border': 1
            })
            
            # Write the column headers with the header format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Set column widths
            for i, col in enumerate(df.columns):
                max_length = max(
                    df[col].astype(str).apply(len).max(),
                    len(str(col))
                ) + 2
                worksheet.set_column(i, i, min(max_length, 50))
            
            # Add auto-filter
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
            
            # Save the workbook
            writer.close()
            
            logger.info(f"Created Excel file: {filename} with {len(df)} unique entries")
            
            # Return JSON response with file download URL and stats
            response_data = {
                'success': True,
                'data': data,
                'count': len(df),
                'excel_file': filename,
                'download_url': url_for('download_file', filename=filename),
                'stats': {
                    'total_results': len(df),
                    'platform': platform,
                    'query': search_query
                }
            }
            return jsonify(response_data)
            
        except Exception as e:
            logger.error(f"Error creating Excel file: {str(e)}")
            scraper_stats.add_error('excel', str(e))
            return jsonify({
                'status': 'error',
                'message': f'Error creating Excel file: {str(e)}. Please try again.'
            })
    
    except Exception as e:
        logger.error(f"\nError during scraping: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': f'An error occurred while scraping: {str(e)}. Please try again.'
        })

@app.route('/download/<filename>')
def download_file(filename):
    try:
        # Ensure the file is from the downloads directory and exists
        if not filename.endswith('.xlsx'):
            return jsonify({'error': 'Invalid file type'}), 400
            
        filepath = os.path.join('downloads', filename)
        if not os.path.exists(filepath):
            return jsonify({'error': f'File not found: {filepath}'}), 404
            
        logger.info(f"Sending Excel file: {filepath}")
        return send_file(
            filepath,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        logger.error(f"Error downloading file: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Error downloading file: {str(e)}',
            'filepath': filepath
        }), 500

class ProxyManager:
    def __init__(self):
        self.proxies = []
        self.last_update = None
        self.update_interval = timedelta(minutes=10)

    async def get_proxies(self):
        """Fetch fresh proxies from multiple free proxy APIs"""
        if (self.last_update and datetime.now() - self.last_update < self.update_interval 
            and len(self.proxies) > 0):
            return self.proxies

        self.proxies = []
        try:
            # Try multiple proxy sources
            async with aiohttp.ClientSession() as session:
                # Source 1: ProxyScrape API
                try:
                    url = "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=GB&ssl=all&anonymity=all"
                    async with session.get(url) as response:
                        if response.status == 200:
                            text = await response.text()
                            self.proxies.extend([f"http://{proxy}" for proxy in text.split()])
                except Exception as e:
                    print(f"Error fetching from ProxyScrape: {str(e)}")

                # Source 2: Free-Proxy-List API
                try:
                    url = "https://www.free-proxy-list.net/"
                    async with session.get(url) as response:
                        if response.status == 200:
                            text = await response.text()
                            soup = BeautifulSoup(text, 'html.parser')
                            table = soup.find('table')
                            if table:
                                rows = table.find_all('tr')
                                for row in rows[1:]:  # Skip header row
                                    cols = row.find_all('td')
                                    if len(cols) >= 7:
                                        ip = cols[0].text.strip()
                                        port = cols[1].text.strip()
                                        country = cols[3].text.strip()
                                        if country == 'United Kingdom':
                                            self.proxies.append(f"http://{ip}:{port}")
                except Exception as e:
                    print(f"Error fetching from Free-Proxy-List: {str(e)}")

                # Source 3: GeoNode API
                try:
                    url = "https://proxylist.geonode.com/api/proxy-list?limit=100&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps&country=GB"
                    async with session.get(url) as response:
                        if response.status == 200:
                            data = await response.json()
                            for proxy in data.get('data', []):
                                ip = proxy.get('ip')
                                port = proxy.get('port')
                                if ip and port:
                                    self.proxies.append(f"http://{ip}:{port}")
                except Exception as e:
                    print(f"Error fetching from GeoNode: {str(e)}")

            print(f"Found {len(self.proxies)} proxies")
            self.last_update = datetime.now()
        except Exception as e:
            print(f"Error updating proxies: {str(e)}")

        return self.proxies

    async def get_working_proxy(self, test_url="https://www.justdial.com"):
        """Test proxies and return a working one"""
        if not self.proxies:
            await self.get_proxies()

        headers = {
            'User-Agent': UserAgent().random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }

        for proxy in self.proxies:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        test_url,
                        proxy=proxy,
                        headers=headers,
                        timeout=10,
                        ssl=False
                    ) as response:
                        if response.status == 200:
                            print(f"Found working proxy: {proxy}")
                            return proxy
            except Exception:
                continue

        print("No working proxy found, fetching new proxies...")
        self.proxies = []  # Clear existing proxies
        await self.get_proxies()
        return None

# Initialize the proxy manager
proxy_manager = ProxyManager()

async def check_connection_details():
    """Check current connection details and log information"""
    try:
        logger.info("Checking connection details...")
        async with aiohttp.ClientSession() as session:
            # Try multiple IP checking services
            services = [
                'https://ipapi.co/json/',
                'https://api.ipify.org?format=json',
                'https://ip.seeip.org/json'
            ]
            
            connection_info = {}
            
            for service in services:
                try:
                    print(f"Trying URL: {search_url}")
                    scraper_url = f'http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={quote(search_url)}&render=true'
                    async with session.get(scraper_url, headers=headers, timeout=60) as response:
                        if response.status == 200:
                            data = await response.json()
                            connection_info = {
                                'ip': data.get('ip'),
                                'country': data.get('country_code', '').upper(),
                                'country_name': data.get('country_name', 'Unknown'),
                                'city': data.get('city', 'Unknown'),
                                'region': data.get('region', 'Unknown'),
                                'isp': data.get('org', 'Unknown')
                            }
                            
                            logger.info("Connection Details:")
                            logger.info(f"IP Address: {connection_info['ip']}")
                            logger.info(f"Country: {connection_info['country']} ({connection_info['country_name']})")
                            logger.info(f"City: {connection_info['city']}")
                            logger.info(f"Region: {connection_info['region']}")
                            logger.info(f"ISP: {connection_info['isp']}")
                            
                            # Check if it's likely a VPN connection
                            is_vpn = any(vpn_term.lower() in connection_info['isp'].lower() 
                                       for vpn_term in ['vpn', 'proxy', 'hosting', 'cloud', 'data center'])
                            
                            if is_vpn:
                                logger.info("VPN connection detected")
                            else:
                                logger.warning("No VPN detected - using direct connection")
                            
                            return connection_info
                except Exception as e:
                    logger.error(f"Error with IP service {service}: {str(e)}")
                    continue
            
            logger.error("All IP checking services failed")
            return None
            
    except Exception as e:
        logger.error(f"Error checking connection details: {str(e)}")
        logger.error(traceback.format_exc())
        return None

async def make_request_with_session(session, url, headers):
    """Helper function to make requests with proper error handling and retries"""
    max_retries = 3
    retry_delay = 2
    
    logger.info(f"Making request to: {url}")
    logger.debug(f"Headers: {headers}")
    
    # Check connection details
    connection_info = await check_connection_details()
    if connection_info:
        # Add connection country to headers to help with geolocation
        headers['Accept-Language'] = f"en-{connection_info['country']},en;q=0.9"
        logger.debug(f"Updated headers with country: {connection_info['country']}")
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Request attempt {attempt + 1}/{max_retries}")
            
            # Add jitter to delay
            jitter = random.uniform(0.5, 1.5)
            delay = retry_delay * jitter
            logger.debug(f"Waiting {delay:.2f} seconds before request")
            await asyncio.sleep(delay)
            
            # Update headers with a new random user agent
            headers = headers.copy()
            headers['User-Agent'] = UserAgent().random
            logger.debug(f"Using User-Agent: {headers['User-Agent']}")
            
            # Make the request using system network settings (VPN if connected)
            async with session.get(
                url, 
                headers=headers, 
                timeout=30, 
                allow_redirects=True
            ) as response:
                logger.info(f"Response status: {response.status}")
                logger.debug(f"Response headers: {response.headers}")
                
                if response.status == 200:
                    content = await response.text()
                    content_length = len(content)
                    logger.info(f"Successfully fetched content (length: {content_length})")
                    logger.debug(f"Content preview: {content[:200]}...")
                    return content
                elif response.status == 403:
                    logger.error(f"Access forbidden. Current connection might be blocked.")
                    if connection_info:
                        logger.error(f"Try using a different VPN server or location (Current: {connection_info['country_name']})")
                    return None
                else:
                    logger.error(f"Request failed with status {response.status}")
                    if response.status == 429:
                        logger.error("Rate limit detected - waiting longer before retry")
                        await asyncio.sleep(retry_delay * (attempt + 2))
                    
            await asyncio.sleep(retry_delay * (attempt + 1))
            
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
            logger.error(traceback.format_exc())
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
            else:
                return None
    return None

@app.route('/scrape_yellowpages')
def scrape_yellowpages_route():
    try:
        query = request.args.get('query')
        location = request.args.get('location')
        min_rating = request.args.get('min_rating', '0')  # Default to 0 if not provided
        
        try:
            min_rating = float(min_rating)
            if min_rating < 0 or min_rating > 5:
                min_rating = 0  # Reset to 0 if invalid range
        except ValueError:
            min_rating = 0  # Reset to 0 if invalid format
        
        if not query or not location:
            return jsonify({
                'status': 'error',
                'message': 'Both query and location parameters are required'
            }), 400
        
        logger.info(f"Starting Yellow Pages scraping with minimum rating: {min_rating}...")
        scraper = YellowPagesScraper()
        
        try:
            data = scraper.scrape_yellowpages(query, location)
            
            if not data:
                return jsonify({
                    'status': 'error',
                    'message': 'No results found'
                }), 404

            # Create downloads directory if it doesn't exist
            if not os.path.exists('downloads'):
                os.makedirs('downloads')

            # Generate Excel file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'yellowpages_results_{timestamp}.xlsx'
            filepath = os.path.join('downloads', filename)
            
            # Process the data to split address and handle categories
            processed_data = []
            for item in data:
                if isinstance(item, dict):
                    # Parse the address into components
                    address = item.get('Address', '')
                    address_parts = {
                        'Address Line 1': '',
                        'Address Line 2': '',
                        'City': '',
                        'State': '',
                        'ZIP Code': ''
                    }
                    
                    if address:
                        # Split address by commas
                        parts = [p.strip() for p in address.split(',')]
                        if len(parts) >= 1:
                            # Split street address into two lines if it contains apartment/suite info
                            street = parts[0]
                            addr_split = re.split(r'\s+(?:Ste|Suite|Apt|Unit|#)\s*', street, flags=re.IGNORECASE)
                            if len(addr_split) > 1:
                                address_parts['Address Line 1'] = addr_split[0].strip()
                                address_parts['Address Line 2'] = f"Suite {addr_split[1].strip()}"
                            else:
                                # Also try to split on floor indicators
                                addr_split = re.split(r'\s+(?:Fl|Floor|Level)\s*', street, flags=re.IGNORECASE)
                                if len(addr_split) > 1:
                                    address_parts['Address Line 1'] = addr_split[0].strip()
                                    address_parts['Address Line 2'] = f"Floor {addr_split[1].strip()}"
                                else:
                                    address_parts['Address Line 1'] = street
                        
                        if len(parts) >= 2:
                            # Last part usually contains state and ZIP
                            last_part = parts[-1].strip()
                            # Try to extract ZIP code
                            zip_match = re.search(r'(\d{5}(?:-\d{4})?)', last_part)
                            if zip_match:
                                address_parts['ZIP Code'] = zip_match.group(1)
                                last_part = last_part[:zip_match.start()].strip()
                            
                            # Extract state (assuming it's the last 2 characters before ZIP)
                            state_match = re.search(r'([A-Z]{2})\s*$', last_part)
                            if state_match:
                                address_parts['State'] = state_match.group(1)
                                last_part = last_part[:state_match.start()].strip()
            
                            # If there are parts between street and state/zip, it's the city
                            if len(parts) == 3:
                                address_parts['City'] = parts[1].strip()
                            elif len(parts) == 2:
                                address_parts['City'] = last_part
                    
                    # Get and validate rating
                    rating = item.get('Rating', '')
                    try:
                        rating = float(rating) if rating else 0
                    except (ValueError, TypeError):
                        rating = 0
                    
                    # Skip businesses below minimum rating
                    if rating < min_rating:
                        continue
                    
                    # Create new item with processed data
                    new_item = {
                        'Name': item.get('Name', ''),
                        'Rating': rating,  # Use the converted float rating
                        'Reviews Count': item.get('Reviews Count', ''),
                        'Phone': item.get('Phone', ''),
                        'Email': item.get('Email', ''),
                        'Website': item.get('Website', ''),
                        'Address Line 1': address_parts['Address Line 1'],
                        'Address Line 2': address_parts['Address Line 2'],
                        'City': address_parts['City'],
                        'State': address_parts['State'],
                        'ZIP Code': address_parts['ZIP Code'],
                        'Owner Name': item.get('Owner Name', ''),
                        'Category': item.get('Categories', ''),  # Use Categories as the single category field
                        'Description': item.get('Description', ''),
                        'Source': item.get('Source', 'yellowpages')
                    }
                    
                    # Add any additional fields that weren't explicitly handled
                    for key, value in item.items():
                        if key not in new_item and key not in ['Address', 'Categories', 'Category']:
                            new_item[key] = value
                    
                    processed_data.append(new_item)

            # Create DataFrame with processed data
            df = pd.DataFrame(processed_data)

            # Clean up the data
            for col in df.columns:
                # Replace empty strings and None with NaN
                df[col] = df[col].replace(['', None], pd.NA)
                
                # Clean up text fields
                if df[col].dtype == 'object':
                    df[col] = df[col].str.strip()
                    df[col] = df[col].str.replace('\n', ' ')
                    df[col] = df[col].str.replace('\r', ' ')
                    df[col] = df[col].str.replace('\t', ' ')
                    df[col] = df[col].str.replace('  ', ' ')

            # Remove duplicates
            duplicate_subset = ['Name', 'Phone', 'Address Line 1']
            df = df.drop_duplicates(subset=duplicate_subset, keep='first')

            # Sort by rating (descending) and reviews count (descending)
            df['Reviews Count'] = pd.to_numeric(df['Reviews Count'], errors='coerce').fillna(0)
            df = df.sort_values(by=['Rating', 'Reviews Count'], ascending=[False, False])

            # Create Excel writer with xlsxwriter engine for formatting
            writer = pd.ExcelWriter(filepath, engine='xlsxwriter')
            
            # Write the DataFrame to Excel
            df.to_excel(writer, index=False, sheet_name='Sheet1')
            
            # Get the xlsxwriter workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            
            # Define column widths based on content type
            column_widths = {
                'Name': 30,
                'Rating': 10,
                'Reviews Count': 12,
                'Phone': 15,
                'Email': 25,
                'Website': 35,
                'Address Line 1': 35,
                'Address Line 2': 20,
                'City': 20,
                'State': 8,
                'ZIP Code': 12,
                'Owner Name': 25,
                'Category': 30,
                'Description': 50,
                'Source': 12
            }
            
            # Set column widths and add a bit of padding
            for idx, col in enumerate(df.columns):
                max_length = max(
                    df[col].astype(str).apply(len).max(),  # max length of values
                    len(col)  # length of column name
                ) + 2  # add padding
                
                # Use predefined width if available, otherwise use calculated width
                width = column_widths.get(col, max_length)
                worksheet.set_column(idx, idx, width)
            
            # Add basic formatting
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#D9D9D9',
                'border': 1
            })
            
            # Add number format for Rating column
            rating_format = workbook.add_format({'num_format': '0.0'})
            rating_col = df.columns.get_loc('Rating')
            worksheet.set_column(rating_col, rating_col, 10, rating_format)
            
            # Write the column headers with the header format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Save the Excel file
            writer.close()
            
            logger.info(f"Results saved to Excel file: {filepath}")
            
            # Return success response with file info
            return jsonify({
                'success': True,
                'data': processed_data,
                'count': len(df),
                'excel_file': filename,
                'download_url': url_for('download_file', filename=filename),
                'stats': {
                    'total_results': len(df),
                    'min_rating_filter': min_rating,
                    'platform': 'yellowpages',
                    'query': query,
                    'location': location
                }
            })
        except Exception as e:
            logger.error(f"Error during YellowPages scraping: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f'Error during scraping: {str(e)}'
            }), 500
            
    except Exception as e:
            logger.error(f"Error during YellowPages scraping: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f'Error during scraping: {str(e)}'
            }), 500

    except Exception as e:
        logger.error(f"Error in YellowPages route: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'An error occurred: {str(e)}'
        }), 500

@app.errorhandler(404)
def not_found_error(error):
    return jsonify({'error': 'Not Found', 'message': str(error)}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal Server Error', 'message': str(error)}), 500

@app.errorhandler(Exception)
def handle_exception(error):
    logger.error(f"Unhandled exception: {str(error)}")
    logger.error(traceback.format_exc())
    return jsonify({'error': 'Server Error', 'message': 'An unexpected error occurred'}), 500

if __name__ == '__main__':
    app.run(debug=True)
