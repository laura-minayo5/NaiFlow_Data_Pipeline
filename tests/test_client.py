# tests/test_scraper.py
# ===========================================================================
# Core Unit Tests for the Nairobi Rental Scraper Pipeline
# ===========================================================================
# This file leverages the global fixtures established in tests/conftest.py 
# to perform fast, offline, repeatable validations of your extraction logic.
# Every test utilizes mocking to isolate logic from external network calls.

import json
import pytest
from bs4 import BeautifulSoup

# Import pipeline components directly from your client source package
from client.client import (
    get_bedrooms_from_text,
    get_bathrooms_from_soup,
    distance_to_archives,
    parse_price_ksh,
    transform_listing,
    run_pipeline,
    resolve_neighbourhood
)

# ===========================================================================
# 1. PARSING & TEXT MINING TESTS (Pure Functions)
# ===========================================================================

@pytest.mark.utility
@pytest.mark.parametrize("title, description, expected", [
    ("Stunning 2 bedroom apartment in Kilimani", "Spacious lounge...", 2),
    ("Executive 1 bd studio flat", "Fully furnished, pool access", 0),  # Studio/bedsitter detection
    ("Beautiful house along Waiyaki Way", "Features 4 beds with SQ", 4),
    ("Modern rental property", "No mention of bedrooms here", None)   # Missing data fallback
])
def test_bedroom_extraction_regex(title, description, expected):
    """
    Validates that regex safely maps variations of structural text 
    into numerical counts or flags studios as 0.
    """
    assert get_bedrooms_from_text(title, description) == expected


@pytest.mark.utility
def test_bathroom_extraction_soup(mock_card_soup):
    """
    Tests HTML tree extraction. Instead of recreating raw HTML code here,
    we pass the 'mock_card_soup' fixture automatically built by conftest.py.
    """
    assert get_bathrooms_from_soup(mock_card_soup) == 2


# ===========================================================================
# 2. TESTING GEOSPATIAL MATH(Location metrics) & CACHING (File Interactions)
# ===========================================================================

@pytest.mark.geospatial
def test_distance_to_archives():
    """
    Verifies that spatial distance evaluations relative to the Nairobi CBD core hub 
    yield reasonable float numbers for close areas like Ngara.
    """
    # Coordinates mapping close to the National Archives hub
    ngara_dist = distance_to_archives(-1.2741, 36.8245)
    
    assert isinstance(ngara_dist, float)
    assert 1.0 <= ngara_dist <= 2.5


@pytest.mark.geospatial
def test_resolve_neighbourhood_cache_hit(mocker, sample_cache_file):
    """
    Tests a Cache HIT scenario. It passes 'sample_cache_file' (which already 
    has Westlands inside it) to confirm that the scraper reads from the local 
    CSV and skips making a live API call to Geopy/Google Maps entirely.
    """
    # We patch the live geocoding service so if the scraper attempts to call 
    # the live internet, the test will intercept it and throw a failure.
    mock_geo = mocker.patch("client.client.geocode_service")

    # Act: Search for a term pre-populated in your sample cache ("Westlands CBD")
    name, lat, lon = resolve_neighbourhood("Westlands CBD")
    
    # Assertions
    assert name == "Westlands"
    assert lat == -1.2633
    assert lon == 36.8037
    mock_geo.assert_not_called()  # Proves the internet wasn't used!

# ===========================================================================
# 3. PRICE FIELD TRIMMING (Data Normalisation)
# ===========================================================================

@pytest.mark.utility
@pytest.mark.parametrize("raw_input, expected", [
    ("KSh 45,000 per month", 45000),
    ("85000.00", 85000),
    ("Price on call", None),
    (None, None)
])
def test_currency_regex_parser(raw_input, expected):
    """
    Validates currency cleanup tracking. Ensures non-numeric characters, 
    commas, and text headers drop cleanly into database-ready integers.
    """
    assert parse_price_ksh(raw_input) == expected


# ===========================================================================
# 4. CANONICAL TRANSFORMATIONS (Schema Pipeline Mapping)
# ===========================================================================

@pytest.mark.utility
def test_transform_listing_layout(mocker, raw_listing_full):
    """
    Tests converting raw unorganized data maps into our unified Canonical Schema.
    We pass 'raw_listing_full' from conftest.py directly to keep our code clean.
    """
    # Stub geospatial functions to prevent live network lookups during test runs
    mock_resolve = mocker.patch("client.client.resolve_neighbourhood", return_value=("Westlands", -1.2633, 36.8037))
    mock_dist = mocker.patch("client.client.distance_to_archives", return_value=4.56)
    
    # Process our mock raw data dictionary
    result = transform_listing(raw_listing_full)
    
    # Assert structural targets match perfectly
    assert result["property_id"] == "brk-abc123"
    assert result["price_ksh"] == 65000
    assert result["neighbourhood"] == "Westlands"
    
    # CRITICAL SCRAPER TEST: Memory Bloat Verification
    # Beautiful Soup objects consume heavy RAM. They must be stripped out 
    # of final transformation dictionaries before passing down batch arrays.
    assert "card_soup" not in result


# ===========================================================================
# 5. BATCH ORCHESTRATION PIPELINE (Mocked Integration Run)
# ===========================================================================

@pytest.mark.network
def test_run_pipeline_orchestration_loop(mocker):
    """
    Mocks file execution pipelines. Ensures your scraper reads from storage,
    transforms payloads sequentially, maps uploads, and generates an output summary.
    """
    # 1. Arrange: Define deterministic data responses for our pipeline operations
    mock_post = mocker.patch("client.client.post_to_api", return_value=True)
    mock_transform = mocker.patch("client.client.transform_listing", return_value={
        "property_id": "brk-mock123",
        "location_raw": "Ngara Estate",
        "neighbourhood": "Ngara",
        "property_type": "Apartment",
        "price_ksh": 30000,
        "dist_to_archives_km": 1.7,
        "bedrooms": 1,
        "bathrooms": 1
    })
    
    # Create simulated file contents tracking raw scraped items
    fake_scraped_data = [{"title": "1 BR Ngara", "location": "Ngara Estate", "price": "KSh 30,000"}]
    mock_file_stream = json.dumps(fake_scraped_data)
    
    # 2. Act: Intercept python's standard 'open()' file read function safely 
    mocker.patch("builtins.open", mocker.mock_open(read_data=mock_file_stream))
    summary = run_pipeline(use_local_json="mock_backup.json", max_items=5)
        
    # 3. Assert: Confirm accounting trackers accurately trace pipeline outcomes
    assert summary["total_scraped"] == 1
    assert summary["posted_ok"] == 1
    assert summary["failed"] == 0

    # CRITICAL SECURITY GUARD: Verification of execution
    # Ensures the code didn't just count numbers, but actually tried to upload payloads.
    mock_post.assert_called_once()  # Proves our pipeline attempted to post data to the API
