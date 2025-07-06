import os
import json
import logging
import tempfile
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy import signals
from scrapy.signalmanager import dispatcher
from .yellowpages_spider import YellowPagesSpider

logger = logging.getLogger('yellowpages_scraper')

class YellowPagesScraper:
    def __init__(self):
        """Initialize the YellowPages scraper with Scrapy settings"""
        # Set up Scrapy settings
        settings_module_path = 'scrapers.scrapy_settings'
        os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_module_path)
        
        self.settings = get_project_settings()
        self.results = []
        
        # Create a temporary directory for HTTP cache
        self.temp_dir = tempfile.mkdtemp()
        self.settings.set('HTTPCACHE_DIR', os.path.join(self.temp_dir, 'httpcache'))
        
        # Initialize the crawler process
        self.process = CrawlerProcess(self.settings)
    
    def collect_item(self, item, response, spider):
        """Callback method to collect scraped items"""
        self.results.append(dict(item))
    
    def scrape_yellowpages(self, search_query, location, min_results=100):
        """
        Scrape YellowPages using Scrapy spider
        
        Args:
            search_query (str): The search term to look for
            location (str): The location to search in
            min_results (int): Minimum number of results to gather
            
        Returns:
            list: List of dictionaries containing business information
        """
        try:
            logger.info(f"Starting YellowPages scraping for '{search_query}' in '{location}'")
            
            # Reset results for new search
            self.results = []
            
            # Connect the item scraped signal
            dispatcher.connect(self.collect_item, signal=signals.item_scraped)
            
            # Run the spider with parameters
            self.process.crawl(
                YellowPagesSpider,
                search_query=search_query,
                location=location,
                min_results=min_results
            )
            self.process.start()
            
            logger.info(f"Scraping completed. Found {len(self.results)} results")
            
            return self.results
            
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
            raise
        
        finally:
            # Clean up temporary directory
            try:
                import shutil
                if os.path.exists(self.temp_dir):
                    shutil.rmtree(self.temp_dir)
            except Exception as e:
                logger.warning(f"Error cleaning up temporary directory: {e}")
            
            # Disconnect the signal
            try:
                dispatcher.disconnect(self.collect_item, signal=signals.item_scraped)
            except Exception as e:
                logger.warning(f"Error disconnecting signal: {e}")
    
    def cleanup(self):
        """Clean up resources"""
        try:
            # Clean up temporary directory
            import shutil
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
                logger.info("Cleanup completed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}") 