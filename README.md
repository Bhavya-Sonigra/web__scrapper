# Business Data Scraper

A web application that scrapes business information from JustDial and Sulekha websites and exports the data to Excel format.

## Features

- Scrapes business information including:
  - Company name
  - Email
  - Phone
  - About us
  - Website
  - Social links
  - Address
- Supports multiple platforms (JustDial, Sulekha)
- Exports data to Excel spreadsheet
- Clean and user-friendly interface

## Setup

1. Install Python 3.7 or higher
2. Install required packages:
   ```
   pip install -r requirements.txt
   ```
3. Run the application:
   ```
   python app.py
   ```
4. Open your browser and go to `http://localhost:5000`

## Usage

1. Enter your search query (e.g., "restaurants in Mumbai")
2. Select the platform(s) to scrape from
3. Click "Start Scraping"
4. Wait for the Excel file to download

## Note

Please be mindful of the websites' terms of service and implement appropriate delays between requests to avoid overwhelming their servers.
