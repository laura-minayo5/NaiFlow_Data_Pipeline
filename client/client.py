"""
client.py  —  Nairobi Rental Pipeline  |  Airflow Task 1
=========================================================
 
WHAT THIS FILE DOES:
---------------------
  • Called by Airflow (Task 1) on a schedule
  • Runs the Apify scraper for Nairobi rentals
  • Enriches each listing: neighbourhood → lat/lon → distance to Archives
  • Classifies value zone (Premium / Sweet Spot / Commuter)
  • POSTs each record as JSON to AWS API Gateway
    → triggers Lambda → Kinesis Data Stream (Pipeline 1 in your diagram)
"""
# --- Standard Library (Built-in) ---
import os
import sys
import json
import time
import logging
import re
import requests
import hashlib
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from typing import Optional
from logging.handlers import RotatingFileHandler

# --- Third-Party (Needs pip install) ---
import requests
from geopy.distance import geodesic
from rapidfuzz import process, fuzz
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# Adds the parent directory (NaiFlow) to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.paths import LOCATION_CACHE, PIPELINE_LOG

# load environment variables from .env file
# this reads the .env file and makes the variables available via os.getenv()
load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
# create logger for this specific module only
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Set the "Master" level to the lowest (DEBUG)

# 1. formatter
# Simple format for console logs (timestamp, level, message)
console_fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Detailed format for the file (includes timestamp and line number)
file_fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - [%(lineno)d] - %(message)s")


# 2. StreamHandler (Terminal) and 
# StreamHandler = send logs to a "stream" by default that stream is your terminal (stdout)
console_job = logging.StreamHandler()
console_job.setLevel(logging.INFO) 
console_job.setFormatter(console_fmt) # Apply the short formatter to the console handler


# Extract the folder path from your log filename configuration
log_dir = os.path.dirname(PIPELINE_LOG)
if log_dir:
    # This automatically builds the folder if it's missing!
    os.makedirs(log_dir, exist_ok=True)

# 2. RotatingFileHandler (File) — saves logs to a file and rotates it when it reaches a certain size
# RotatingFileHandler = saves logs to a file and rotates it when it reaches a certain size
# backupCount=3 means it keeps 3 old files before deleting the oldest
# maxBytes=1,000,000 is ~1MB
file_job = RotatingFileHandler(
    PIPELINE_LOG,
    maxBytes=10**6, 
    backupCount=3
)
file_job.setLevel(logging.DEBUG)
file_job.setFormatter(file_fmt) # Apply the detailed format

# 3. Add handlers to the logger
logger.addHandler(console_job) # logs INFO and above to console
logger.addHandler(file_job)    # logs DEBUG and above to file (which includes everything)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# APIFY_TOKEN        = os.environ["APIFY_TOKEN"]           # Apify API token needed to authenticate and run the actor.
# APIFY_ACTOR_ID     = "ashersam01~property24-africa-scraper" # Apify Actor ID for the Property24 Africa scraper. This is the specific scraper we set up on Apify to extract rental listings from Property24.
API_GATEWAY_URL    = os.environ["API_GATEWAY_URL"]       # the endpoint URL for your AWS API Gateway where the clean data will be POSTed. This is the entry point to your AWS pipeline, so it should match the URL of the API Gateway you set up in AWS.
API_GATEWAY_KEY    = os.getenv("API_GATEWAY_KEY", "")   # optional x-api-key header

# Reference Point: Kenya National Archives, Nairobi CBD
ARCHIVES_LAT = -1.2850
ARCHIVES_LON =  36.8258

# INITIALIZE CACHE & API
CACHE_FILE = str(LOCATION_CACHE)  # This is the CSV file where we will store previously resolved location strings and their corresponding coordinates. It acts as a "memory" to speed up future lookups.
# Initialize the Geocoder (Nominatim is free, uses OpenStreetMap)
geolocator = Nominatim(user_agent="nairobi_rental_project")
# RateLimiter prevents you from getting blocked by adding a 1-second delay between requests
geocode_service = RateLimiter(geolocator.geocode, min_delay_seconds=1.5)

# Expert Dictionary of Nairobi Neighbourhoods with their approximate latitudes and longitudes.
NEIGHBORHOOD_COORDS: dict[str, dict[str, float]] = {
    # --- LIMURU ROAD & NORTHERN BYPASS (High-End & Diplomatic) ---
    "Gigiri":          {"lat": -1.2333, "lon": 36.8000},
    "Runda":           {"lat": -1.2167, "lon": 36.8167},
    "Runda Mimosa":    {"lat": -1.2210, "lon": 36.8050},
    "Runda Evergreen": {"lat": -1.2100, "lon": 36.8300},
    "Old Muthaiga":    {"lat": -1.2533, "lon": 36.8233},
    "New Muthaiga":    {"lat": -1.2417, "lon": 36.8117},
    "Muthaiga":        {"lat": -1.2500, "lon": 36.8167},
    "Muthaiga North":  {"lat": -1.2309, "lon": 36.8078},
    "Rosslyn":         {"lat": -1.2256, "lon": 36.7903},
    "Rosslyn Heights": {"lat": -1.2280, "lon": 36.7850},
    "Nyari":           {"lat": -1.2355, "lon": 36.7725},
    "Kitisuru":        {"lat": -1.2396, "lon": 36.7840},
    "Hillview Estate": {"lat": -1.2450, "lon": 36.7900},
    "Ruaka":           {"lat": -1.2083, "lon": 36.7750},
    "Thindigua":       {"lat": -1.1967, "lon": 36.8417},
    "Ndenderu":        {"lat": -1.2000, "lon": 36.7450},
    "Wangige":         {"lat": -1.2400, "lon": 36.7150},

    # --- WAIYAKI WAY & WESTERN BYPASS (Corporate & Residential Hubs) ---
    "Westlands":       {"lat": -1.2633, "lon": 36.8037},
    "Brookside":       {"lat": -1.2647, "lon": 36.7939},
    "General Mathenge":{"lat": -1.2558, "lon": 36.7992},
    "Spring Valley":   {"lat": -1.2581, "lon": 36.7884},
    "Loresho":         {"lat": -1.2483, "lon": 36.7567},
    "Kyuna":           {"lat": -1.2567, "lon": 36.7767},
    "Lower Kabete":    {"lat": -1.2450, "lon": 36.7633},
    "Mountain View":   {"lat": -1.2683, "lon": 36.7417},
    "Kangemi":         {"lat": -1.2633, "lon": 36.7500},
    "Kikuyu":          {"lat": -1.2500, "lon": 36.6667},
    "Kinoo":           {"lat": -1.2667, "lon": 36.6833},
    "Uthiru":          {"lat": -1.2650, "lon": 36.7167},
    "Sigona":          {"lat": -1.2417, "lon": 36.6333},

    # --- NGONG ROAD & SOUTHERN BYPASS (Modern Apartment Hotspots) ---
    "Kilimani":        {"lat": -1.2874, "lon": 36.7845},
    "Hurlingham":      {"lat": -1.2950, "lon": 36.7967},
    "Adams Arcade":    {"lat": -1.3009, "lon": 36.7808},
    "Riara":           {"lat": -1.2986, "lon": 36.7694},
    "Lavington":       {"lat": -1.2833, "lon": 36.7750},
    "Kileleshwa":      {"lat": -1.2723, "lon": 36.7997},
    "Riverside":       {"lat": -1.2721, "lon": 36.7936},
    "Karen":           {"lat": -1.3333, "lon": 36.7000},
    "Jamhuri":         {"lat": -1.3033, "lon": 36.7667},
    "Woodley":         {"lat": -1.3033, "lon": 36.7750},
    "Dagoretti":       {"lat": -1.2917, "lon": 36.7417},
    "Kawangware":      {"lat": -1.2833, "lon": 36.7417},
    "Ngong Town":      {"lat": -1.3667, "lon": 36.6333},
    "Bulbul":          {"lat": -1.3550, "lon": 36.6650},
    "Kerarapon":       {"lat": -1.3284, "lon": 36.6673},
    "Ongata Rongai":   {"lat": -1.3917, "lon": 36.7417},

    # --- THIKA ROAD & NORTHERN BYPASS (High Density & Middle Income) ---
    "Garden Estate":   {"lat": -1.2167, "lon": 36.8667},
    "Ridgeways":       {"lat": -1.2217, "lon": 36.8400},
    "Thome":           {"lat": -1.2144, "lon": 36.8525},
    "WillMary":        {"lat": -1.2144, "lon": 36.8525},
    "Windsor":         {"lat": -1.2386, "lon": 36.8544},
    "Roysambu":        {"lat": -1.2185, "lon": 36.8856},
    "Kasarani":        {"lat": -1.2217, "lon": 36.8967},
    "Mirema":          {"lat": -1.2050, "lon": 36.8900},
    "Zimmerman":       {"lat": -1.2050, "lon": 36.8950},
    "Kahawa Sukari":   {"lat": -1.1850, "lon": 36.9350},
    "Kahawa Wendani":  {"lat": -1.2000, "lon": 36.9200},
    "Githurai 44":     {"lat": -1.2017, "lon": 36.9100},
    "Githurai 45":     {"lat": -1.1983, "lon": 36.9250},
    "Safari Park":     {"lat": -1.2250, "lon": 36.8833},
    "Clay City":       {"lat": -1.2234, "lon": 36.9173},
    "Lucky Summer":    {"lat": -1.2333, "lon": 36.9000},
    "Babadogo":        {"lat": -1.2400, "lon": 36.8800},

    # --- MOMBASA ROAD & EASTERN BYPASS (Industrial & Airport Hubs) ---
    "South B":         {"lat": -1.3122, "lon": 36.8456},
    "South C":         {"lat": -1.3200, "lon": 36.8300},
    "Upper Hill":      {"lat": -1.2967, "lon": 36.8167},
    "Imara Daima":     {"lat": -1.3250, "lon": 36.8750},
    "Syokimau":        {"lat": -1.3600, "lon": 36.9380},
    "Mlolongo":        {"lat": -1.3930, "lon": 36.9420},
    "Athi River":      {"lat": -1.4500, "lon": 36.9833},
    "Kitengela":       {"lat": -1.4833, "lon": 36.9667},
    "Katani":          {"lat": -1.3450, "lon": 36.9950},
    "Utawala":         {"lat": -1.2833, "lon": 36.9667},
    "Ruai":            {"lat": -1.2750, "lon": 37.0167},
    "Nyayo Estate":    {"lat": -1.3150, "lon": 36.9050},
    "Pipeline":        {"lat": -1.3117, "lon": 36.9017},

    # --- JOGOO ROAD & OUTER RING (Historical & High Density) ---
    "Donholm":         {"lat": -1.3000, "lon": 36.8833},
    "Buruburu":        {"lat": -1.2983, "lon": 36.8700},
    "Umoja":           {"lat": -1.2833, "lon": 36.8833},
    "Fedha":           {"lat": -1.3100, "lon": 36.9100},
    "Eastleigh":       {"lat": -1.2717, "lon": 36.8467},
    "Pangani":         {"lat": -1.2682, "lon": 36.8401},
    "Ngara":           {"lat": -1.2741, "lon": 36.8245},
    "Dandora":         {"lat": -1.2467, "lon": 36.9033},
    "Kayole":          {"lat": -1.2660, "lon": 36.9169},
    "Kariobangi":      {"lat": -1.2500, "lon": 36.8833},
    "Huruma":          {"lat": -1.2583, "lon": 36.8667},
    "Mathare":         {"lat": -1.2583, "lon": 36.8500},
    "Kibera":          {"lat": -1.3133, "lon": 36.7883},
}
# --- Utility Functions ---
# Hybrid Extraction Logic: Bedrooms from text, Bathrooms from tags
# ---------------------------------------------------------------------------
def get_bedrooms_from_text(title: str, description: str = "") -> Optional[int]:
    """
    Extracts bedroom count from title or description strings using Regex.
    Specifically handles 'Studio' as 0 bedrooms.
    """
    combined_text = f"{title} {description}".lower()
    
    if 'studio' in combined_text:
        return 0
    
    # Matches patterns like '2 bed', '3 bedroom', '1 bd'
    match = re.search(r'(\d+)\s*(?:bed|bd|bedroom)', combined_text)
    return int(match.group(1)) if match else None

def get_bathrooms_from_soup(card_soup: BeautifulSoup) -> Optional[int]:
    """
    Extracts bathroom count from the structured feature-labels/icons in the HTML.
    """
    # Look for spans that typically contain 'Bathroom' text near an icon
    features = card_soup.find_all("span", class_=re.compile("feature|label|text-sm"))
    for f in features:
        text = f.get_text().lower()
        if 'bathroom' in text or 'bath' in text:
            match = re.search(r'\d+', text)
            return int(match.group()) if match else None
    return None
# ---------------------------------------------------------------------------
# Distance calculation using GeoPy 
# Calculates geodesic distance in kilometers between two lat/lon points
# This gives us the distance from each property to the Nairobi Archives, which is our reference point for "centrality"
# ---------------------------------------------------------------------------
def distance_to_archives(lat: float, lon: float) -> float:
    """
    Calculates the geodesic distance between a property and the Nairobi Archives.
    """
    archives_coords = (ARCHIVES_LAT, ARCHIVES_LON)
    property_coords = (lat, lon)
    
    # geodesic() is more accurate than Haversine for Earth distances
    dist = geodesic(archives_coords, property_coords).km
    return round(dist, 3)

# ---------------------------------------------------------------------------
# Cache helper function
# ---------------------------------------------------------------------------
def save_to_cache(raw: str, matched: str, lat: float, lon: float):
    """
    This is the helper function that actually handles the CSV writing.
    It takes the data and appends it as a new row to your cache file.
    """
    new_row = pd.DataFrame([{
        'raw_string': raw,
        'matched_name': matched,
        'lat': lat,
        'lon': lon
    }])
    
    # Check if file exists so we know whether to write the header (column names)
    file_exists = os.path.exists(CACHE_FILE)
    
    # Append to CSV (header=False if file already exists)
    # mode='a' means APPEND (add to the bottom, don't overwrite)
    new_row.to_csv(CACHE_FILE, mode='a', index=False, header=not file_exists)


# ---------------------------------------------------------------------------
# Neighbourhood resolver using fuzzy matching Levenshtein Distance approach, with a CSV cache and API fallback.

# 1. Levenshtein distance approach
# Levenshtein distance measures how many single-character edits (insertions, deletions, substitutions) are needed to change one string into another
# input location_str: takes raw text scraped from Property24 (e.g. "Ngara", "Ngara Estate", "Ngara Rd") and tries to match it to our known list of neighbourhoods
# output: returns (matched_name, lat, lon) if a match is found, otherwise (None, None, None)
# If the raw location string contains (or is contained in) any of our known neighborhood names, we consider it a match and return the corresponding lat/lon

# 2. CSV Cache: Before doing any fuzzy matching or API calls, we check if we've already resolved this exact raw string before and saved it in our cache. This speeds up processing for repeated strings.
# The cache is a simple CSV file with columns: raw_string, matched_name, lat, lon. When we find a new match (either through fuzzy matching or API), we save it to the cache for future quick lookups.
# This way, if we encounter the same raw location string again in another listing, we can quickly return the cached coordinates without doing fuzzy matching or API calls again.


# 3. API Fallback: If we can't find a good match through fuzzy matching, we use the Nominatim geocoding service to try to resolve the location. We append "Nairobi, Kenya" to the search query to keep it focused on our area of interest. If the API returns a result, we take the first part of the address as the matched name and save it to the cache for future reference.
# This multi-step approach (cache → fuzzy match → API) allows us to efficiently resolve location strings while minimizing API calls and handling variations in how locations are written in the raw data.

# ---------------------------------------------------------------------------
def resolve_neighbourhood(location_str: str) -> tuple[Optional[str], Optional[float], Optional[float]]:
    """
    This is a helper function that resolves a raw location string to a neighbourhood name and coordinates.
    It checks the CSV cache first, then performs fuzzy matching, and finally falls back to the Nominatim API.
    """
    # Checks CSV Cache -> Checks Expert Dictionary -> Calls API Fallback
    # If the location string is empty or None, return None for all fields
    if not location_str or pd.isna(location_str):
        return None, None, None

    # STEP 1: CHECK CSV CACHE (The "Memory")
    if os.path.exists(CACHE_FILE):
        cache_df = pd.read_csv(CACHE_FILE)
        # Check for exact string match
        match_in_cache = cache_df[cache_df['raw_string'].str.lower() == location_str.lower()]
        # If a match is found in the cache, return the cached coordinates
        if not match_in_cache.empty:
            row = match_in_cache.iloc[0] # Get the first matching row (there should ideally be only one)
            return row['matched_name'], row['lat'], row['lon']

    # STEP 2: FUZZY MATCH DICTIONARY (The "Expert Logic")
    # extract the 'best' match from our list of neighborhood names
    # names = ['Ngara', 'Pangani', 'South B', ...]
    names = list(NEIGHBORHOOD_COORDS.keys())
    
    # process.extractOne finds the closest match based on Levenshtein distance
    # location_str: raw text from property listings(Contestant), names: List of known neighborhoods(Judges), scorer: how to measure similarity, score_cutoff: minimum similarity threshold
    # score_cutoff=80 means "only return a match if it's 80% similar or better"
    match = process.extractOne(location_str, names, scorer=fuzz.WRatio, score_cutoff=80)
    
    # when extractOne finds a winner(match), it returns a tuple: (matched_name, score, index)
    if match:
        matched_name, score, index = match
        coords = NEIGHBORHOOD_COORDS[matched_name]  # lookup lat/lon for the matched neighborhood in our dictionary
        lat, lon = coords["lat"], coords["lon"]
        # Save this new finding to cache
        save_to_cache(location_str, matched_name, lat, lon) # Save the raw string, matched name, and coordinates to the cache for future quick lookups
        return matched_name, lat, lon

    # --- STEP 3: API FALLBACK (The "Internet Search") ---
    try:
        # Append Nairobi, Kenya to keep the API focused
        search_query = f"{location_str}, Nairobi, Kenya"
        location = geocode_service(search_query)
        
        if location:
            # Get the first part of the returned address as the name
            api_name = location.address.split(',')[0]
            lat, lon = location.latitude, location.longitude
            save_to_cache(location_str, api_name, lat, lon)
            return api_name, lat, lon
    except Exception as e:
        print(f"API Error for {location_str}: {e}")

    return None, None, None

# ---------------------------------------------------------------------------
# Price parser  (handles "KSh 25,000 per month", "25000", etc.)
# ---------------------------------------------------------------------------
def parse_price_ksh(raw_price) -> Optional[int]:
    """
    Cleans currency strings into integers.
    Handles: "KSh 25,000", "25000.00", "Price: 30k", etc.
    """
    if raw_price is None or raw_price == "":
        return None
    
    # 1. Convert to string (e.g., "KSh 45,000.00")
    price_str = str(raw_price).replace(",", "")
    
    # 2. Regex: Find the first sequence of digits and optional decimals
    # \d+ means "one or more digits"
    # (\.\d+)? means "an optional dot followed by more digits"
    match = re.search(r"(\d+(\.\d+)?)", price_str)
    
    if match:
        try:
            # convert to float first to handle the ".00", then to int
            return int(float(match.group(1)))
        except (ValueError, TypeError):
            return None


# ---------------------------------------------------------------------------
# BuyRentKenya Scraper
# ---------------------------------------------------------------------------
    
def scrape_buyrentkenya(pages=100) -> list[dict]:
    """
    Scrapes property listings from buyrentkenya.com.
    This is a simple example of how to scrape a different site with a different structure.
    In production, you would set up a separate Apify Actor for this site and call it similarly to the Property24 scraper.
    """
    listings = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    for page in range(1, pages + 1):
        # Page 1 is the base URL; Page 2+ needs the query parameter
        if page == 1:
            url = "https://www.buyrentkenya.com/flats-apartments-for-rent/nairobi"
        else:
            url = f"https://www.buyrentkenya.com/flats-apartments-for-rent/nairobi?page={page}"
        
        logger.info(f"Fetching page {page}: {url}")
        try:
            # 1. Send GET request to the website to fetch the HTML content
            resp = requests.get(url, headers=headers, timeout=15)
            
            # If a page returns 404, we don't want to crash the whole script
            if resp.status_code == 404:
                logger.warning(f"Page {page} not found (404). Moving to next...")
                continue
             
            resp.raise_for_status()

            # 2. Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(resp.text, "html.parser")
            # if page == 1:
            #     # We save the first page only so we don't overwrite it constantly
            #     with open("debug_buyrent_structure.html", "w", encoding="utf-8") as f:
            #         f.write(soup.prettify())
            #     logger.info("Saved 'debug_buyrent_structure.html'. Open this file to inspect tags!")

            logger.info(f"Finished page {page}. Sleeping for 2 seconds...")
            time.sleep(2) # Be polite and avoid hitting the server too hard

            # 3. Extracting the data  
            # Select listing cards — the exact HTML structure may vary, so we look for common patterns.
            # Regular listings might just be <div class="listing-card">
            # Premium/Promoted listings might be <div class="relative shadowed-box">
            # re.compile allows for fuzzy searching of class names that contain certain keywords, which is useful if the website uses multiple classes or changes them slightly.
            cards = soup.find_all("div", class_=re.compile("listing-card|relative"))
            
            for card in cards:
                # use .find() to look inside each card
                # Extract title, price, location using a combination of tag names and class patterns
                title_elem = card.find("h2") or card.find("a", class_=re.compile("title"))
                price_elem = card.find(text=re.compile("KSh"))
                loc_elem = card.find("div", class_=re.compile("location|text-sm"))
                # 1. Try to find the link specifically
                link_elem = card.find("a", href=True)
                property_url = ""

                if link_elem:
                    # Ensure it's an absolute URL
                    href = link_elem['href']
                    property_url = href if href.startswith("http") else "https://www.buyrentkenya.com" + href

                if title_elem and price_elem:
                    # We pass the 'card' (soup object) into the raw dictionary 
                    # so transform_listing can extract bathrooms from classes
                    listings.append({
                        "title": title_elem.get_text(strip=True),
                        "price": price_elem.strip(),
                        "location": loc_elem.get_text(strip=True) if loc_elem else "Nairobi",   
                        "url": property_url,
                        "card_soup": card # Added for hybrid extraction
                    })

            # We finished processing the data, now we rest before the NEXT page request.
            logger.info(f"Finished page {page}. Sleeping for 2 seconds...")
            time.sleep(2) # Be polite and avoid hitting the server too hard
        except Exception as e:
            logger.error(f"Error on page {page}: {e}")
            
    return listings

# ---------------------------------------------------------------------------
# Transforms a raw dictionary (from BeautifulSoup or Apify) into our standardized 'Canonical Schema'.
# ---------------------------------------------------------------------------

def transform_listing(raw: dict) -> dict:
    """
    Transforms a raw dictionary (from BeautifulSoup or Apify) into our 
    standardized 'Canonical Schema'.
    """
    
    # 1. Resolve Location & Coordinates
    # We look for any key that might hold the address
    location_raw = raw.get("location") or raw.get("suburb") or raw.get("area") or ""
    neighbourhood, lat, lon = resolve_neighbourhood(location_str=location_raw)

    # 2. Calculate Distance to Archives
    dist_km = distance_to_archives(lat, lon) if lat and lon else None

    # 3. Clean and Parse Price
    # We prioritize 'price' but check 'rentPrice' just in case
    price_raw = raw.get("price") or raw.get("rentPrice") or ""
    price_ksh = parse_price_ksh(price_raw)
    
    # 4. Create a unique "Fingerprint" of the URL
    url_string = raw.get("url") or ""
    url_hash = hashlib.md5(url_string.encode()).hexdigest()[:12] 

    # 5. HYBRID EXTRACTION: Bedrooms (Text) & Bathrooms (Tags)
    title = raw.get("title") or ""
    description = raw.get("description") or ""
    
    # Bedrooms from Title/Description (more reliable for Studios/Missing tags)
    bedrooms = get_bedrooms_from_text(title, description)
    
    # Bathrooms from CSS/Tags (if card_soup was passed from scraper)
    bathrooms = None
    if "card_soup" in raw:
        bathrooms = get_bathrooms_from_soup(raw["card_soup"])
    
    # Fallback to raw keys if soup extraction isn't available
    if bedrooms is None: bedrooms = raw.get("bedrooms") or raw.get("beds")
    if bathrooms is None: bathrooms = raw.get("bathrooms") or raw.get("baths")

    # 5. Build the Final Standardized Dictionary
    return {
        # Identity & Metadata
        "property_id":   raw.get("id") or raw.get("propertyId") or f"brk-{url_hash}",
        "scraped_at":    datetime.now(timezone.utc).isoformat(),
        "source_url":    raw.get("url") or raw.get("link") or "",
        "title":         raw.get("title") or raw.get("name") or "Unnamed Property",

        # Geospatial Data
        "location_raw":        location_raw,
        "neighbourhood":       neighbourhood or "Unknown",
        "latitude":            lat,
        "longitude":           lon,
        "dist_to_archives_km": dist_km,

        # Financials
        "price_raw":     str(price_raw),
        "price_ksh":     price_ksh,

        # Property Specs (If available in the raw data)
        "bedrooms":      bedrooms,
        "bathrooms":     bathrooms,
        "property_type": raw.get("propertyType") or raw.get("type") or "Apartment",
        # "size_sqm":      raw.get("sizeSqm") or raw.get("floorSize"),
    }


# ---------------------------------------------------------------------------
# POST to AWS API Gateway
# Data Loader: sends clean dictionary to AWS API Gateway.
# ---------------------------------------------------------------------------
def post_to_api(payload: dict, retries: int = 3) -> bool:
    headers = {"Content-Type": "application/json"}
    if API_GATEWAY_KEY:
        headers["x-api-key"] = API_GATEWAY_KEY # Add API key to headers if it's set in the environment variables

    for attempt in range(1, retries + 1):
        try:
            # We remove the card_soup before posting to keep payload light
            post_payload = {k: v for k, v in payload.items() if k != "card_soup"}
            resp = requests.post(API_GATEWAY_URL, json=post_payload, headers=headers, timeout=10)
            if resp.status_code in (200, 201):
                return True
            logger.warning(f"API returned {resp.status_code}: {resp.text[:200]}")
        except requests.RequestException as exc:
            logger.warning(f"Attempt {attempt} failed: {exc}")
        time.sleep(2 ** attempt)   # exponential back-off  2 s, 4 s, 8 s
    return False


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def run_pipeline(use_local_json=None, max_items=10000) -> dict:
    logger.info("=== Starting Nairobi Rental Pipeline ===")
    if use_local_json:
        with open(use_local_json, 'r') as f:
            raw_data = json.load(f)
    else:
        raw_data = scrape_buyrentkenya(pages=100) # Scrape the first 100 pages of BuyRentKenya. Adjust as needed.
    
    total_scraped = len(raw_data)

    logger.info(f"Processing {total_scraped} listings...")

    success_count = 0
    fail_count    = 0
    skip_count    = 0
    
    for i, raw in enumerate(raw_data, start=1):
        if i > max_items: break
        try:
            # Transform the raw listing into our canonical schema
            clean_record = transform_listing(raw)
            if not clean_record["location_raw"]:
                skip_count += 1
                logger.debug(f"Skipping record {i}/{total_scraped}: no location data")
                continue

            # POST the clean record to the API Gateway
            ok = post_to_api(clean_record)
            # ok = True # For testing without actually hitting the API Gateway, set ok to True. In production, use the line above to post to the API.

            # Extract the data into variables first to keep the f-string tidy
            # 1. Extract data for the 'Pretty Table'
            status_icon = "✔" if ok else "✘"
            total_listings = total_scraped
            neighbourhood = clean_record.get("neighbourhood") or "Unknown"
            property_type = clean_record.get("property_type", "Unknown")
            price_ksh = clean_record.get("price_ksh")
            price_fmt = f"{price_ksh:,}" if price_ksh else "N/A"
            dist = clean_record.get("dist_to_archives_km") or 0.0
            beds = clean_record.get("bedrooms", "?")
            baths = clean_record.get("bathrooms", "?")

            # 2. Format the Human-Readable Table Row
            table_row = (
                f"{status_icon} "
                f"[{i}/{total_listings}] "
                f"{neighbourhood:<15} | {property_type:<15} | "
                f"{beds}BR/{baths}BA | "
                f"KSh {price_fmt:<8} | {dist:>4.1f} km to Archives"
                )
            logger.info(table_row)
    
            if ok:
                success_count += 1
                logger.info(f"Successfully posted property_id={clean_record['property_id']} to API Gateway, ")
            else:
                fail_count += 1
                logger.error(f"Failed to POST property_id={clean_record['property_id']}")

        except Exception as exc:
            fail_count += 1
            logger.error(f"Error processing listing {i}: {exc}")
    summary = {
        "total_scraped": total_scraped,
        "posted_ok":     success_count,
        "failed":        fail_count,
        "skipped":       skip_count,
        "run_at":        datetime.now(timezone.utc).isoformat(),
    }
    logger.info(f"=== Pipeline complete: {summary} ===")
    return summary


#  Local test entry point
if __name__ == "__main__":
    # Quick local test 
    result = run_pipeline(max_items=10000) # Set max_items to limit how many listings we process during testing

    print("\n--- TEST RUN SUMMARY ---")
    print(json.dumps(result, indent=2))  # Print the summary result in a nice format