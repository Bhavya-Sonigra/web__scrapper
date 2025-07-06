import logging
from scrapers import YellowPagesScraper
import json
from datetime import datetime
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

def main():
    scraper = None
    try:
        # Initialize scraper
        logger.info("Initializing YellowPages scraper...")
        scraper = YellowPagesScraper()
        
        # Test parameters
        search_query = "restaurants"
        location = "Miami, FL"
        min_results = 10  # Starting with a small number for testing
        
        logger.info(f"\nStarting YellowPages scraper test:")
        logger.info(f"Search Query: {search_query}")
        logger.info(f"Location: {location}")
        logger.info(f"Minimum Results: {min_results}\n")
        
        # Run the scraper
        results = scraper.scrape_yellowpages(search_query, location, min_results)
        
        # Save results to a JSON file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"yellowpages_results_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\nScraping completed successfully!")
        logger.info(f"Total results found: {len(results)}")
        logger.info(f"Results saved to: {output_file}")
        
        # Print first result as a sample
        if results:
            logger.info("\nSample result (first entry):")
            print(json.dumps(results[0], indent=2))
            
    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}")
        logger.error("Traceback:")
        logger.error(traceback.format_exc())
    finally:
        if scraper:
            try:
                scraper.cleanup()
                logger.info("Cleanup completed successfully")
            except Exception as e:
                logger.error(f"Error during cleanup: {str(e)}")

if __name__ == "__main__":
    main() 