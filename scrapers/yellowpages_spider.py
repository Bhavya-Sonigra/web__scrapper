import scrapy
from scrapy.spiders import CrawlSpider
from scrapy.exceptions import CloseSpider
import re
import logging
from urllib.parse import urlencode
import random
import time

logger = logging.getLogger('yellowpages_spider')

class YellowPagesSpider(CrawlSpider):
    name = 'yellowpages'
    allowed_domains = ['yellowpages.com']
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,  # YellowPages blocks based on robots.txt
        'CONCURRENT_REQUESTS': 1,  # Reduce concurrent requests
        'DOWNLOAD_DELAY': random.uniform(5, 10),  # Random delay between requests
        'COOKIES_ENABLED': True,  # Enable cookies
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 500,
            'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 750,
        },
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [403, 429, 500, 502, 503, 504],
        'HTTPCACHE_ENABLED': False  # Disable cache to avoid stale responses
    }

    def __init__(self, search_query=None, location=None, min_results=100, *args, **kwargs):
        super(YellowPagesSpider, self).__init__(*args, **kwargs)
        self.search_query = search_query
        self.location = location
        self.min_results = int(min_results)
        self.results_count = 0
        self.start_urls = [self.get_search_url(1)]

    def get_search_url(self, page):
        """Generate search URL for YellowPages"""
        params = {
            'search_terms': self.search_query,
            'geo_location_terms': self.location,
            'page': str(page)
        }
        return f'https://www.yellowpages.com/search?{urlencode(params)}'

    def start_requests(self):
        """Override start_requests to add headers"""
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Sec-Ch-Ua': '"Not.A/Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://www.google.com/'
        }
        
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                headers=headers,
                callback=self.parse,
                dont_filter=True,
                meta={'dont_retry': False}
            )

    def parse(self, response):
        """Parse the search results page"""
        # Check if we got a 403 error
        if response.status == 403:
            logger.warning("Received 403 error, retrying with delay...")
            time.sleep(random.uniform(10, 15))  # Add longer delay
            yield scrapy.Request(
                response.url,
                callback=self.parse,
                dont_filter=True,
                meta={'dont_retry': False}
            )
            return

        businesses = response.css('.result')
        
        if not businesses:
            logger.warning("No business listings found on page")
            return

        for business in businesses:
            if self.results_count >= self.min_results:
                raise CloseSpider('Reached minimum required results')

            item = {
                'name': self.extract_text(business, '.business-name'),
                'phone': self.extract_text(business, '.phones.phone.primary'),
                'website': self.extract_attr(business, 'a.track-visit-website::attr(href)'),
                'categories': self.extract_categories(business),
                'rating': self.extract_text(business, '.rating'),
                'review_count': self.extract_review_count(business),
                'years_in_business': self.extract_years_in_business(business),
                **self.extract_address(business),
                'source': 'yellowpages'
            }

            # Clean up empty values
            item = {k: v for k, v in item.items() if v}
            
            if item.get('name'):  # Only yield if we have at least a business name
                self.results_count += 1
                
                # If website exists, follow it to get more details
                if item.get('website'):
                    yield scrapy.Request(
                        item['website'],
                        callback=self.parse_business_website,
                        meta={
                            'item': item,
                            'dont_retry': False,
                            'download_timeout': 30
                        },
                        errback=self.handle_website_error,
                        dont_filter=True,
                        headers=response.request.headers  # Reuse the same headers
                    )
                else:
                    yield item

            # Add random delay between business processing
            time.sleep(random.uniform(1, 3))

        # Follow pagination if we need more results
        if self.results_count < self.min_results:
            next_page = response.css('a.next::attr(href)').get()
            if next_page:
                # Add random delay before next page
                time.sleep(random.uniform(5, 8))
                yield response.follow(
                    next_page,
                    callback=self.parse,
                    headers=response.request.headers,  # Reuse the same headers
                    meta={'dont_retry': False}
                )

    def parse_business_website(self, response):
        """Extract additional information from business website"""
        item = response.meta['item']
        
        # Extract email addresses
        emails = self.extract_emails(response.text)
        if emails:
            item['additional_emails'] = emails

        # Extract additional phone numbers
        phones = self.extract_phones(response.text)
        if phones:
            item['additional_phones'] = phones

        # Extract business description
        description = self.extract_description(response)
        if description:
            item['website_description'] = description

        # Extract social media links
        social_links = self.extract_social_links(response)
        if social_links:
            item['social_media'] = social_links

        yield item

    def handle_website_error(self, failure):
        """Handle errors when scraping business websites"""
        item = failure.request.meta['item']
        item['website_status'] = 'error'
        item['website_error'] = str(failure.value)
        return item

    def extract_text(self, selector, css_path):
        """Extract and clean text using CSS selector"""
        return selector.css(f'{css_path}::text').get('').strip()

    def extract_attr(self, selector, css_path):
        """Extract attribute using CSS selector"""
        return selector.css(css_path).get('')

    def extract_categories(self, business):
        """Extract and clean business categories"""
        categories = business.css('.categories::text').getall()
        if not categories:
            categories = business.css('.links::text').getall()
        
        if categories:
            # Clean and deduplicate categories
            categories = [cat.strip() for cat in ' '.join(categories).split(',')]
            return ', '.join(filter(None, dict.fromkeys(categories)))
        return ''

    def extract_address(self, business):
        """Extract and parse address components"""
        street = self.extract_text(business, '.street-address')
        locality = self.extract_text(business, '.locality')
        
        address_components = {
            'street_address': street,
            'city': '',
            'state': '',
            'zip_code': '',
            'suburb': ''
        }
        
        if locality:
            # Parse locality which contains city, state, and ZIP
            match = re.match(r'^([^,]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)', locality)
            if match:
                address_components.update({
                    'city': match.group(1).strip(),
                    'state': match.group(2),
                    'zip_code': match.group(3)
                })
        
        return address_components

    def extract_review_count(self, business):
        """Extract number of reviews"""
        review_text = self.extract_text(business, '.review-count')
        if review_text:
            match = re.search(r'(\d+)', review_text)
            if match:
                return int(match.group(1))
        return None

    def extract_years_in_business(self, business):
        """Extract years in business"""
        years_text = self.extract_text(business, '.years-in-business')
        if years_text:
            match = re.search(r'(\d+)', years_text)
            if match:
                return int(match.group(1))
        return None

    def extract_emails(self, text):
        """Extract email addresses from text"""
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        return list(set(re.findall(email_pattern, text)))

    def extract_phones(self, text):
        """Extract phone numbers from text"""
        phone_patterns = [
            r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
            r'\(\d{3}\)\s*\d{3}[-.]?\d{4}',
            r'\+1[-.]?\d{3}[-.]?\d{3}[-.]?\d{4}'
        ]
        phones = []
        for pattern in phone_patterns:
            phones.extend(re.findall(pattern, text))
        return list(set(phones))

    def extract_description(self, response):
        """Extract business description from website"""
        # Try meta descriptions first
        for meta in response.css('meta[name="description"], meta[property="og:description"]'):
            content = meta.attrib.get('content', '').strip()
            if content:
                return content

        # Try common description containers
        selectors = [
            '.about-us', '.company-description', '.business-description',
            '#about', '.about', '.overview', '.description',
            '[id*="about"]', '[class*="about"]', '[id*="overview"]',
            '[class*="overview"]'
        ]
        
        descriptions = []
        for selector in selectors:
            elements = response.css(f'{selector}::text').getall()
            if elements:
                text = ' '.join(e.strip() for e in elements)
                descriptions.append(text)
        
        if descriptions:
            # Return the longest description
            return max(descriptions, key=len)
        return ''

    def extract_social_links(self, response):
        """Extract social media links"""
        social_patterns = {
            'facebook': r'facebook\.com/[^/\s"\']+',
            'twitter': r'twitter\.com/[^/\s"\']+',
            'linkedin': r'linkedin\.com/(?:company|in)/[^/\s"\']+',
            'instagram': r'instagram\.com/[^/\s"\']+',
            'youtube': r'youtube\.com/(?:user|channel)/[^/\s"\']+',
        }
        
        social_links = {}
        for platform, pattern in social_patterns.items():
            matches = re.findall(pattern, response.text)
            if matches:
                social_links[platform] = f'https://www.{matches[0]}'
        
        return social_links if social_links else None 