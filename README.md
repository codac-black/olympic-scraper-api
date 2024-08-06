# Olympic Scraper API

A Flask-based API for scraping and managing Olympic medal data. This API scrapes real-time data from the official Olympics website, stores it in a SQLite database, and provides endpoints for accessing the data in JSON and CSV formats.

## Features

- **Data Scraping**: Automatically fetches medal data from the Olympics website.
- **Data Storage**: Stores data in a SQLite database.
- **Rate Limiting**: Limits the number of API calls to prevent abuse.
- **Error Handling**: Handles various errors gracefully.
- **File-based Caching**: Caches responses for faster access and reduced load.
- **Swagger Documentation**: Provides API documentation for easy exploration.
- **Prometheus Metrics**: Tracks API usage and performance metrics.

## Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/codac-black/olympic-scraper-api.git
   cd olympic-scraper-api

2. **Create and activate a virtual environment:**
```bash
  python -m venv venv
  # linux/mac
  source venv/bin/activate
  # windows
  venv\Scripts\activate
```
3. **Install dependencies:**
   ```bash
     pip install -r requirements.txt
   ```

4. **Set up configuration:**
   Create a `config.ini` file based on config.ini.example and set up necessary configurations.

5. **Initialize the database:**
   ```bash
     flask db init
     flask db migrate
     flask db upgrade

## Usage

1. **Run the aplication:**
   ```bash
     python app.py
2. **Access the API:**
    - Scrape data: POST /scrape
    - Get medal data: GET /medals
    - Download medal data as CSV: GET /medals/csv

3. **Swagger Documentation:**
   Access the Swagger UI for API documentation and testing at:
   ```bash
     http://localhost:5000/apidocs/

## Testing

1. **Run tests:**
   ```bash
     python -m unittest discover   
The tests are located in test_app.py and include checks for various API functionalities.




