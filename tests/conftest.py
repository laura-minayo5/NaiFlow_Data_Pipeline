# tests/conftest.py
# ===========================================================================
# Global Configuration and Shared Fixtures for Pytest
# ===========================================================================
# This file is automatically discovered and loaded by pytest before any test
# modules run. It configures the runtime environment, mocks system-level
# configurations, overrides file paths, and provides shared test data.

import os
import sys
import shutil
import tempfile
import pytest
import pandas as pd
from bs4 import BeautifulSoup

# ===========================================================================
# 1. RUNTIME ISOLATION LAYER (CRITICAL FOR PATHS.PY)
# ===========================================================================

# STEP 1.1: Generate a secure, unique, and isolated temporary directory path.
# This prevents our test suites from modifying folders on your real computer.
TEST_SANDBOX_DIR = tempfile.mkdtemp()

# STEP 1.2: Force paths.py to use this temporary sandbox folder as its BASE_DIR.
# When paths.py calls .mkdir(), it will create the 'data/' and 'logs/' folders
# safely inside this sandbox instead of polluting your actual Git project root.
os.environ["PROJECT_DIR"] = TEST_SANDBOX_DIR

# STEP 1.3: Set dummy environment variables to satisfy module-level lookups.
# client.py reads these values when imported. Setting them here ensures that
# the script initializes successfully without throwing errors or requiring real keys.
os.environ.setdefault("API_GATEWAY_URL", "https://test.execute-api.us-east-1.amazonaws.com/test")
os.environ.setdefault("API_GATEWAY_KEY", "mock_key_for_testing_purposes")
os.environ.setdefault("APIFY_TOKEN",     "apify_api_test_token")
os.environ.setdefault("DBT_PROJECT_HOST_PATH", "/mock/host/path/nairobi_dbt")

# ===========================================================================
# 2. PYTHON SYSTEM PATH HANDLING
# ===========================================================================

# STEP 2.1: Locate and resolve absolute directory targets for your application layers.
# This ensures that standard Python import statements can resolve correctly.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CLIENT_DIR   = os.path.join(PROJECT_ROOT, "client")
CONFIG_DIR   = os.path.join(PROJECT_ROOT, "config")

# STEP 2.2: Loop through targets and insert them directly into Python's sys.path lists.
# This makes modules like 'import client' and 'from config import paths' importable.
for path in [PROJECT_ROOT, CLIENT_DIR, CONFIG_DIR]:
    if path not in sys.path:
        sys.path.insert(0, path)

# ===========================================================================
# 3. GLOBAL CLEANUP AUTOMATION (SESSION FIXTURE)
# ===========================================================================

@pytest.fixture(scope="session", autouse=True)
def cleanup_test_sandbox():
    """
    Automates post-test garbage collection across the entire test session.
    Yields control to execute tests, then securely wipes out the scratchpad.
    """
    # The 'yield' keyword tells pytest: 'Pause here, run all the unit tests first'
    yield
    
    # After every single test function finishes, execution drops down here.
    # We completely erase the sandbox directory and its automatically generated log files.
    if os.path.exists(TEST_SANDBOX_DIR):
        shutil.rmtree(TEST_SANDBOX_DIR)


# ===========================================================================
# 4. GEOSPATIAL & FILE CACHE FIXTURES (FUNCTION SCOPE)
# ===========================================================================

@pytest.fixture
def temp_cache_file(tmp_path, monkeypatch):
    """
    Creates an empty, temporary CSV cache file dedicated to a single test function.
    Uses monkeypatch to redirect client.client.LOCATION_CACHE dynamically.
    """
    # Create a safe, unique path utilizing pytest's built-in tmp_path engine
    cache_path = tmp_path / "test_location_cache.csv"
    
    # Lazily import client after path overrides are safely locked down
    import client.client
    
    # Dynamically point client.client.LOCATION_CACHE to our temporary test file
    monkeypatch.setattr(client.client, "LOCATION_CACHE", cache_path)
    
    return cache_path

@pytest.fixture
def sample_cache_file(temp_cache_file):
    """
    Generates a pre-populated cache file loaded with predictable location data.
    Mainly used to verify cache HIT loops inside resolve_neighbourhood routines.
    """
    # Construct a structured pandas DataFrame representing cached location data
    df = pd.DataFrame([
        {
            "raw_string":   "Westlands CBD",
            "matched_name": "Westlands",
            "lat":          -1.2633,
            "lon":          36.8037
        },
        {
            "raw_string":   "Kilimani Area",
            "matched_name": "Kilimani",
            "lat":          -1.2874,
            "lon":          36.7845
        }
    ])
    
    # Export data cleanly into our isolated cache target
    df.to_csv(temp_cache_file, index=False)
    
    return temp_cache_file


# ===========================================================================
# 5. WEBSCRAPING PARSER & HTML FIXTURES
# ===========================================================================

@pytest.fixture
def mock_card_soup():
    """
    Generates a pre-built BeautifulSoup tree structure.
    Used to test BeautifulSoup tag extractions like get_bathrooms_from_soup.
    """
    html_content = """
    <div class="listing-card">
        <h2>Executive Apartment</h2>
        <span class="feature-label">2 Bathrooms</span>
        <span class="text-sm">3 Beds</span>
    </div>
    """
    return BeautifulSoup(html_content, "html.parser")


# ===========================================================================
# 6. PROPERTY SCHEMA DATA FIXTURES
# ===========================================================================

@pytest.fixture
def raw_listing_full(mock_card_soup):
    """
    Provides a dictionary mimicking a raw listing object from BuyRentKenya.
    Includes an attached BeautifulSoup element to test hybrid parser pipelines.
    """
    return {
        "id":           "brk-abc123",
        "title":        "Spacious 2 Bedroom Apartment in Westlands",
        "price":        "KSh 65,000 per month",
        "location":     "Westlands, Nairobi",
        "url":          "https://www.buyrentkenya.com/listings/abc123",
        "propertyType": "Apartment",
        "bedrooms":     2,
        "bathrooms":    2,
        "card_soup":    mock_card_soup
    }


@pytest.fixture
def raw_listing_minimal():
    """Provides a minimal property structure containing only mandatory fields."""
    return {
        "title":    "Studio in Kilimani",
        "price":    "KSh 25000",
        "location": "Kilimani",
    }


@pytest.fixture
def raw_listing_missing_price():
    """Provides a data payload where price is missing (evaluates to None)."""
    return {
        "title":    "Apartment in Karen",
        "location": "Karen",
        "price":    None,
    }


@pytest.fixture
def raw_listing_missing_location():
    """Provides a data payload where location is completely omitted from keys."""
    return {
        "title": "Mystery Apartment",
        "price": "KSh 45,000",
    }
