import re
import asyncio
import aiohttp
from urllib.parse import quote
import logging
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class SulekhaScraper:
    def __init__(self, scraper_api_key):
        self.scraper_api_key = scraper_api_key
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        }

        self.category_corrections = {
            'college': 'colleges',
            'colleges': 'colleges',
            'university': 'colleges',
            'universities': 'colleges',
            'college & university': 'colleges',
            'colleges & universities': 'colleges',
            'engineering college': 'engineering-colleges',
            'medical college': 'medical-colleges',
            'business school': 'business-schools',
            'mba college': 'business-schools',

            'guitar shop': 'musical-instruments',
            'guitar store': 'musical-instruments',
            'music shop': 'musical-instruments',
            'musical instruments': 'musical-instruments',
            'musical store': 'musical-instruments',
            'guitar class': 'guitar-classes',
            'guitar classes': 'guitar-classes',
            'guitar training': 'guitar-classes',

            'restaurant': 'restaurants',
            'restaurent': 'restaurants',
            'hotel': 'hotels-resorts',
            'hotels': 'hotels-resorts',
            'hospital': 'hospitals',
            'school': 'schools',
            'gym': 'gyms-fitness-centres',
            'fitness': 'gyms-fitness-centres',
            'salon': 'beauty-parlours',
            'beauty parlour': 'beauty-parlours',
            'beauty parlor': 'beauty-parlours',
            'car repair': 'car-repair-services',
            'bike repair': 'bike-repair-services',
            'plumber': 'plumbers',
            'electrician': 'electricians'
        }

        self.location_corrections = {
            'banglore': 'bangalore',
            'bengaluru': 'bangalore',
            'bombay': 'mumbai',
            'calcutta': 'kolkata',
            'madras': 'chennai'
        }

    def _normalize_category(self, search_query):
        search_query_clean = re.sub(r'[^a-z0-9\s&]', '', search_query.lower())
        for key, value in self.category_corrections.items():
            if key in search_query_clean:
                return value
        normalized_category = re.sub(r'[^a-z0-9\s-]', '', search_query_clean)
        return re.sub(r'\s+', '-', normalized_category)

    def _normalize_location(self, location):
        if not location:
            return None
        location = location.lower().strip()
        location_words = location.split()
        corrected_location_words = [self.location_corrections.get(word, word) for word in location_words]
        return '-'.join(corrected_location_words)

    async def scrape(self, search_query, location=None):
        data = []

        if not self.scraper_api_key:
            logger.error("SCRAPER_API_KEY not set in environment variables")
            return data

        try:
            display_location = location
            display_query = search_query

            search_query = search_query.lower().strip()
            normalized_category = self._normalize_category(search_query)
            normalized_location = self._normalize_location(location)

            logger.info(f"Normalized category: {normalized_category}")
            logger.info(f"Normalized location: {normalized_location}")

            # Construct base URL
            if normalized_location:
                base_url = f"https://www.sulekha.com/{normalized_category}/{normalized_location}"
            else:
                base_url = f"https://www.sulekha.com/{normalized_category}"
            
            search_urls = []
            
            # Add URLs for first 5 pages
            for page in range(1, 6):
                if page == 1:
                    search_urls.append(base_url)
                else:
                    # Sulekha uses both formats, try both
                    search_urls.append(f"{base_url}/page-{page}")
                    search_urls.append(f"{base_url}?page={page}")

            logger.info(f"Attempting to scrape Sulekha with category: {display_query} in {display_location if display_location else 'all locations'}")

            async with aiohttp.ClientSession() as session:
                for search_url in search_urls:
                    try:
                        logger.info(f"Trying URL: {search_url}")
                        scraper_url = f'http://api.scraperapi.com?api_key={self.scraper_api_key}&url={quote(search_url)}&render=true'

                        async with session.get(scraper_url, headers=self.headers) as response:
                            if response.status == 404:
                                logger.warning(f"Failed to fetch {search_url}. Status: 404 - Page not found, trying alternative URL pattern")
                                continue

                            if response.status != 200:
                                logger.warning(f"Failed to fetch {search_url}. Status: {response.status}")
                                continue

                            html_content = await response.text()
                            soup = BeautifulSoup(html_content, 'html.parser')

                            # Find all h3 headers that contain business names
                            listings = soup.find_all('h3')
                            for listing in listings:
                                try:
                                    # Get the business name from h3
                                    name = listing.text.strip()
                                    
                                    # Get the parent container for additional info
                                    parent = listing.find_previous('p')
                                    if not parent:
                                        parent = listing.find_next('p')
                                    
                                    # Extract description and other details
                                    description = ''
                                    address = ''
                                    phone = ''
                                    
                                    if parent:
                                        # Try to find address in parent's siblings first
                                        address_div = parent.find_next('div', class_='address')
                                        if address_div:
                                            address = address_div.text.strip()
                                        
                                        # If no address div found, try to parse from text
                                        if not address:
                                            text = parent.text.strip()
                                            lines = text.split('\n')
                                            
                                            # First try to find phone number
                                            for line in lines:
                                                line = line.strip()
                                                if any(char.isdigit() for char in line) and ('+' in line or line.count('-') > 1):
                                                    phone = line
                                                    break
                                            
                                            # Then try to find address using multiple methods
                                            address_keywords = ['street', 'road', 'area', 'near', 'beside', 'opposite', 'mumbai', 'maharashtra',
                                                              'building', 'floor', 'landmark', 'station', 'mall', 'market', 'complex', 'sector',
                                                              'nagar', 'colony', 'highway', 'junction', 'cross', 'main', 'phase', 'industrial',
                                                              'east', 'west', 'north', 'south', 'behind', 'next to', 'above', 'below']
                                            
                                            max_address_score = 0
                                            for line in lines:
                                                line = line.strip()
                                                if not line or line == phone:
                                                    continue
                                                    
                                                # Score the line based on address indicators
                                                score = 0
                                                line_lower = line.lower()
                                                
                                                # Check for address keywords
                                                keyword_count = sum(1 for keyword in address_keywords if keyword in line_lower)
                                                score += keyword_count * 2
                                                
                                                # Check for numbers (like building numbers)
                                                if any(char.isdigit() for char in line):
                                                    score += 2
                                                
                                                # Check for PIN codes
                                                if re.search(r'\b\d{6}\b', line):
                                                    score += 5
                                                
                                                # Check for typical address patterns
                                                if re.search(r'(no|shop|flat|office)\s*[#.:,]?\s*\d+', line_lower):
                                                    score += 3
                                                
                                                # Penalize very short lines
                                                if len(line) < 15:
                                                    score -= 2
                                                
                                                # Bonus for lines with commas (typical in addresses)
                                                score += line.count(',') * 0.5
                                                
                                                if score > max_address_score:
                                                    max_address_score = score
                                                    address = line
                                            
                                            # Find description (usually the longest non-address, non-phone line)
                                            max_length = 0
                                            for line in lines:
                                                line = line.strip()
                                                if line and line != phone and line != address and len(line) > max_length:
                                                    max_length = len(line)
                                                    description = line
                                    
                                    if name and not any(existing.get('Name') == name for existing in data):
                                        business_data = {
                                            'Name': name,
                                            'Phone': phone,
                                            'Address': address,
                                            'Description': description,
                                            'Category': search_query
                                        }
                                        data.append(business_data)
                                        logger.info(f"Added business: {name}")
                                except Exception as e:
                                    logger.warning(f"Failed to parse listing: {e}")

                            page_count = len(data)
                            if page_count > 0:
                                logger.info(f"Found {page_count} businesses on {search_url}")
                            else:
                                logger.info(f"No businesses found on {search_url}")
                                
                            # Don't break after first success, try all pages
                            
                            # Add a delay between pages to avoid rate limiting
                            await asyncio.sleep(3)

                    except Exception as e:
                        logger.error(f"Error fetching {search_url}: {str(e)}")
                        continue

        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")

        return data
