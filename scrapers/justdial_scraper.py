import re
import asyncio
import aiohttp
from urllib.parse import quote
import logging
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class JustDialScraper:
    def __init__(self, scraper_api_key):
        self.scraper_api_key = scraper_api_key
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        }
        
        self.location_corrections = {
            'banglore': 'bangalore',
            'bengaluru': 'bangalore',
            'bombay': 'mumbai',
            'calcutta': 'kolkata',
            'madras': 'chennai'
        }

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
            normalized_location = self._normalize_location(location)

            # Construct JustDial URL
            base_url = "https://www.justdial.com"
            if normalized_location:
                search_url = f"{base_url}/{normalized_location}/{search_query}"
            else:
                search_url = f"{base_url}/search/{search_query}"

            logger.info(f"Attempting to scrape JustDial with query: {display_query} in {display_location if display_location else 'all locations'}")
            logger.info(f"Trying URL: {search_url}")

            # Use ScraperAPI to handle request
            scraper_url = f"http://api.scraperapi.com?api_key={self.scraper_api_key}&url={quote(search_url)}"

            async with aiohttp.ClientSession() as session:
                async with session.get(scraper_url, headers=self.headers) as response:
                    if response.status != 200:
                        logger.warning(f"Failed to fetch {search_url}. Status: {response.status}")
                        return data

                    html_content = await response.text()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    # Find all business listings
                    listings = soup.find_all('li', class_='cntanr')
                    
                    for listing in listings:
                        try:
                            # Extract business name
                            name_elem = listing.find('span', class_='lng_cont_name')
                            if not name_elem:
                                continue
                            name = name_elem.text.strip()
                            
                            # Extract phone number
                            phone = ''
                            phone_elem = listing.find('p', class_='contact-info')
                            if phone_elem:
                                phone = phone_elem.text.strip()
                            
                            # Extract address
                            address = ''
                            # Try multiple possible address selectors
                            address_selectors = [
                                ('p', {'class_': 'address-info'}),
                                ('span', {'class_': 'mrehover'}),
                                ('p', {'class_': 'address-text'}),
                                ('span', {'class_': 'cont_fl_addr'}),
                                ('p', {'class_': 'address'}),
                                ('div', {'class_': 'address'}),
                                ('span', {'class_': 'address'}),
                                ('p', {'class_': 'jrcw'}),  # Another common address class
                                ('div', {'class_': 'rsmap-add'})  # Map address container
                            ]
                            
                            for tag, attrs in address_selectors:
                                address_elem = listing.find(tag, attrs)
                                if address_elem:
                                    address = address_elem.text.strip()
                                    if address:  # If we found a non-empty address
                                        break
                            
                            # If still no address, try looking for any element containing location keywords
                            if not address:
                                location_keywords = ['address', 'location', 'area', 'locality']
                                for elem in listing.find_all(['p', 'span', 'div']):
                                    elem_text = elem.text.strip().lower()
                                    if any(keyword in elem_text for keyword in location_keywords):
                                        address = elem.text.strip()
                                        break
                            
                            # Clean up the address
                            if address:
                                # Remove common prefixes
                                prefixes_to_remove = ['address:', 'location:', 'area:', 'locality:']
                                for prefix in prefixes_to_remove:
                                    if address.lower().startswith(prefix):
                                        address = address[len(prefix):].strip()
                                
                                # Clean up whitespace and special characters
                                address = re.sub(r'\s+', ' ', address)  # Replace multiple spaces with single space
                                address = address.strip('.,')  # Remove trailing dots and commas
                            
                            # Extract rating
                            rating = ''
                            rating_selectors = [
                                ('span', {'class_': 'star_m'}),
                                ('span', {'class_': 'rating'}),
                                ('div', {'class_': 'rating'}),
                                ('span', {'class_': 'green-box'}),
                                ('div', {'class_': 'newrate_n'})
                            ]
                            for tag, attrs in rating_selectors:
                                rating_elem = listing.find(tag, attrs)
                                if rating_elem:
                                    rating_text = rating_elem.text.strip()
                                    # Extract numeric rating
                                    rating_match = re.search(r'(\d+(\.\d+)?)', rating_text)
                                    if rating_match:
                                        rating = rating_match.group(1)
                                        break

                            # Extract reviews count
                            votes = ''
                            votes_selectors = [
                                ('span', {'class_': 'rt_count'}),
                                ('span', {'class_': 'review_count'}),
                                ('span', {'class_': 'votes'}),
                                ('div', {'class_': 'votes'}),
                                ('span', {'class_': 'review'})
                            ]
                            for tag, attrs in votes_selectors:
                                votes_elem = listing.find(tag, attrs)
                                if votes_elem:
                                    votes_text = votes_elem.text.strip()
                                    # Extract numeric vote count
                                    votes_match = re.search(r'(\d+)', votes_text)
                                    if votes_match:
                                        votes = votes_match.group(1)
                                        break
                            
                            # If no direct votes found, try looking for elements containing review keywords
                            if not votes:
                                review_keywords = ['reviews', 'votes', 'ratings']
                                for elem in listing.find_all(['span', 'div']):
                                    elem_text = elem.text.strip().lower()
                                    if any(keyword in elem_text for keyword in review_keywords):
                                        votes_match = re.search(r'(\d+)', elem_text)
                                        if votes_match:
                                            votes = votes_match.group(1)
                                            break
                            
                            # Extract categories
                            categories = ''
                            cat_elem = listing.find('span', class_='category')
                            if cat_elem:
                                categories = cat_elem.text.strip()
                            
                            if name and not any(existing.get('Company Name') == name for existing in data):
                                business_data = {
                                    'Company Name': name or '',
                                    'Name': name or '',
                                    'Phone': phone or '',
                                    'Address': address or '',
                                    'Rating': rating or '',
                                    'Reviews Count': votes or '',
                                    'Category': categories or '',
                                    'Email': '',
                                    'Website': '',
                                    'Description': ''
                                }
                                data.append(business_data)
                                logger.info(f"Added business: {name}")
                        
                        except Exception as e:
                            logger.warning(f"Failed to parse listing: {e}")
                    
                    if data:
                        logger.info(f"Found {len(data)} businesses on {search_url}")
                    else:
                        logger.info(f"No businesses found on {search_url}")

        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")

        return data
