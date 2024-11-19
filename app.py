import bs4
import json
import urllib3
import csv
import time
import logging
import os
import hashlib
from urllib3.util import Retry
from datetime import datetime, timedelta
from flask import Flask, jsonify, send_file, request, abort
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_httpauth import HTTPTokenAuth
from apscheduler.schedulers.background import BackgroundScheduler
from flasgger import Swagger, swag_from
from functools import wraps
import configparser
import sqlite3
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, DateTime
from prometheus_flask_exporter import PrometheusMetrics

app = Flask(__name__)
limiter = Limiter(key_func=get_remote_address)
limiter.init_app(app)
auth = HTTPTokenAuth(scheme='Bearer')
swagger = Swagger(app)
metrics = PrometheusMetrics(app)

# Load configuration
config = configparser.ConfigParser()
config.read('config.ini')

# Configure SQLAlchemy
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///olympic_medals.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Configure logging
logging.basicConfig(filename='olympic_scraper.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Configure retry strategy
retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])

# Create a PoolManager instance with retry strategy
http = urllib3.PoolManager(retries=retries)

# Initialize token authentication
auth = HTTPTokenAuth(scheme='Bearer')

# Simple token authentication
tokens = {
    config['API']['TOKEN']: "admin"
}

@auth.verify_token
def verify_token(token):
    if token in tokens:
        expires_at = datetime.strptime(config['API']['expires_at'], '%Y-%m-%d %H:%M:%S')
        if datetime.now() < expires_at:
            return tokens[token]
        else:
            logging.error("Token has expired")
            return None
    return None

# Database models
class MedalData(db.Model):
    id = Column(Integer, primary_key=True)
    version = Column(Integer, nullable=False)
    country = Column(String(100), nullable=False)
    gold = Column(Integer, nullable=False)
    silver = Column(Integer, nullable=False)
    bronze = Column(Integer, nullable=False)
    total = Column(Integer, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)

# File-based caching functions
def get_cache_filename(url):
    return f"cache_{hashlib.md5(url.encode()).hexdigest()}.json"

def get_cached_data(url):
    filename = get_cache_filename(url)
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            data = json.load(f)
        if datetime.now() < datetime.fromisoformat(data['expiry']):
            logging.info("Data retrieved from file cache")
            return data['content']
    return None

def save_to_cache(url, content, expiry_hours=1):
    filename = get_cache_filename(url)
    expiry = (datetime.now() + timedelta(hours=expiry_hours)).isoformat()
    with open(filename, 'w') as f:
        json.dump({'content': content, 'expiry': expiry}, f)
    logging.info("Data saved to file cache")

# Function to fetch data with rate limiting and file-based caching
def fetch_data(url, headers):
    cached_data = get_cached_data(url)
    if cached_data:
        return cached_data

    logging.info(f"Fetching data from {url}")
    time.sleep(1)  # Wait for 1 second between requests
    response = http.request("GET", url, headers=headers)
    
    if response.status == 200:
        content = response.data.decode('utf-8')
        save_to_cache(url, content)
        logging.info("Data fetched and cached successfully")
        return content
    else:
        logging.error(f"Failed to fetch data. Status code: {response.status}")
        return None

# Function to scrape and process Olympic medal data
def scrape_olympic_data():
    """
    Scrape Olympic medal data from the official website and save it to the database.
    """
    with app.app_context():  # Ensure app context is present
        url = "https://olympics.com/en/paris-2024/medals"
        headers = {"User-Agent": "Mozilla/5.0"}

        try:
            html = fetch_data(url, headers)
            if not html:
                return None

            # Parse HTML
            soup = bs4.BeautifulSoup(html, "html.parser")
            data = json.loads(soup.find("script", id="__NEXT_DATA__").string)
            table = data['props']['pageProps']['initialMedals']['medalStandings']['medalsTable']

            # Get the latest version number
            latest_version = db.session.query(db.func.max(MedalData.version)).scalar() or 0
            new_version = latest_version + 1

            # Prepare and save data
            for row in table:
                country = row["description"]
                medals = {m["type"]: m for m in row["medalsNumber"]}
                total_medals = medals["Total"]
                new_data = MedalData(
                    version=new_version,
                    country=country,
                    gold=total_medals["gold"],
                    silver=total_medals["silver"],
                    bronze=total_medals["bronze"],
                    total=total_medals["total"]
                )
                db.session.add(new_data)  # Add the new data to the session

            db.session.commit()  # Commit the session
            logging.info("Data saved to database with version %d", new_version)

        except (urllib3.exceptions.HTTPError, json.JSONDecodeError, KeyError) as e:
            logging.error("An error occurred: %s", str(e))
            db.session.rollback()  # Rollback the session in case of an error

        except Exception as e:
            db.session.rollback()  # Rollback the session in case of an error
            logging.error(f"An error occurred: {str(e)}")
            return None

        finally:
            db.session.remove()  # Ensure the session is removed after the operation

# Error handling decorator
def handle_errors(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
            return jsonify({"error": "An internal error occurred"}), 500
    return decorated_function

# API routes
@app.route('/scrape', methods=['POST'])
@auth.login_required
@limiter.limit("1 per minute")
@handle_errors
@metrics.counter('scrape_count', 'Number of scrape requests')
@swag_from({
    'tags': ['scraping'],
    'security': [{'Bearer': []}],
    'responses': {
        '202': {'description': 'Scraping process started'},
        '429': {'description': 'Rate limit exceeded'}
    }
})
def scrape():
    """
    Trigger Olympic medal data scraping
    ---
    tags:
      - scraping
    security:
      - Bearer: []
    responses:
      202:
        description: Scraping process started
      429:
        description: Rate limit exceeded
    """
    scheduler = BackgroundScheduler()
    scheduler.add_job(scrape_olympic_data, 'date', run_date=datetime.now() + timedelta(seconds=1))
    scheduler.start()
    return jsonify({"message": "Scraping process scheduled"}), 202

@app.route('/medals', methods=['GET'])
@auth.login_required
@limiter.limit("10 per minute")
@handle_errors
@metrics.counter('medals_request_count', 'Number of medal data requests')
@swag_from({
    'tags': ['medals'],
    'parameters': [
        {
            'name': 'version',
            'in': 'query',
            'type': 'integer',
            'required': False,
            'description': 'The version of the medal data to retrieve'
        }
    ],
    'security': [{'Bearer': []}],
    'responses': {
        '200': {'description': 'A list of medal data'},
        '404': {'description': 'Data not available. Try scraping first.'},
        '429': {'description': 'Rate limit exceeded'}
    }
})
def get_medals():
    """
    Get Olympic medal data
    ---
    tags:
      - medals
    security:
      - Bearer: []
    parameters:
      - name: version
        in: query
        type: integer
        required: false
        description: Data version to retrieve (optional)
    responses:
      200:
        description: A list of medal data by country
      404:
        description: Data not available
      429:
        description: Rate limit exceeded
    """
    version = request.args.get('version', type=int)
    if version:
        data = MedalData.query.filter_by(version=version).all()
    else:
        latest_version = db.session.query(db.func.max(MedalData.version)).scalar()
        data = MedalData.query.filter_by(version=latest_version).all()

    if not data:
        abort(404, description="Data not available. Try scraping first.")

    return jsonify([{
        'country': item.country,
        'gold': item.gold,
        'silver': item.silver,
        'bronze': item.bronze,
        'total': item.total,
        'version': item.version,
        'timestamp': item.timestamp.isoformat()
    } for item in data])

@app.route('/medals/csv', methods=['GET'])
@auth.login_required
@limiter.limit("5 per minute")
@handle_errors
@metrics.counter('csv_download_count', 'Number of CSV downloads')
def get_medals_csv():
    """
    Download Olympic medal data as CSV
    ---
    tags:
      - medals
    security:
      - Bearer: []
    parameters:
      - name: version
        in: query
        type: integer
        required: false
        description: Data version to retrieve (optional)
    responses:
      200:
        description: CSV file
      404:
        description: CSV file not available
      429:
        description: Rate limit exceeded
    """
    version = request.args.get('version', type=int)
    if version:
        data = MedalData.query.filter_by(version=version).all()
    else:
        latest_version = db.session.query(db.func.max(MedalData.version)).scalar()
        data = MedalData.query.filter_by(version=latest_version).all()

    if not data:
        abort(404, description="Data not available. Try scraping first.")

    csv_file = 'olympic_medals_temp.csv'
    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Country', 'Gold', 'Silver', 'Bronze', 'Total', 'Version', 'Timestamp'])
        for item in data:
            writer.writerow([item.country, item.gold, item.silver, item.bronze, item.total, item.version, item.timestamp])

    return send_file(csv_file, as_attachment=True, download_name='olympic_medals.csv')

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=config['APP'].getboolean('DEBUG'), port=config['APP'].getint('PORT'))
