# tests/test_pipeline.py
# ===========================================================================
# Pipeline Integration and Configuration Infrastructure Tests
# ===========================================================================
# This file validates that core infrastructural layers—such as filesystem configurations,
# Docker setup parameters, and Airflow orchestration variables—initialize properly.

import os
from pathlib import Path
import pytest

# ===========================================================================
# 1. FILESYSTEM INFRASTRUCTURE INTEGRATION
# ===========================================================================

@pytest.mark.geospatial
def test_config_paths_module():
    """
    Verifies that the paths management system resolves core pipeline components 
    and automatically builds target folders on the disk.
    """
    from config.paths import BASE_DIR, DATA_DIR, LOG_DIR, LOCATION_CACHE, PIPELINE_LOG
    
    # Assert structural type definitions are standard Path engines
    assert isinstance(BASE_DIR, Path)
    assert isinstance(DATA_DIR, Path)
    
    # Name validation targets
    assert DATA_DIR.name == "data"
    assert LOG_DIR.name == "logs"
    assert LOCATION_CACHE.name == "location_cache.csv"
    assert PIPELINE_LOG.name == "nairobi_pipeline.log"
    
    # CRITICAL VERIFICATION: Filesystem Generation
    # Confirms paths.py executes '.mkdir(parents=True, exist_ok=True)' instantly on load.
    # Because conftest.py intercepts the environment, this proves directory creation 
    # loops work successfully inside our secure sandbox storage.
    assert DATA_DIR.exists() is True
    assert LOG_DIR.exists() is True


# ===========================================================================
# 2. AIRFLOW & DOCKER ORCHESTRATION LAYER VALIDATION
# ===========================================================================

@pytest.mark.network
def test_pipeline_config_initialization():
    """
    Validates that system parameters read configuration variables, 
    map integers accurately, and construct clean Docker structures for Airflow.
    """
    # ── FORCE DOCKER/AIRFLOW DIRECT FILE PATH IMPORT ──
    # This manually looks up the file exactly where it lives inside your Airflow layout,
    # completely bypassing pytest environment path bugs.
    import sys
    import importlib.util
    from pathlib import Path

    # Target the exact path: NaiFlow/airflow/dags/pipeline_config.py
    target_path = Path(__file__).resolve().parent.parent / "airflow" / "dags" / "pipeline_config.py"

    # Force Python to load the exact file directly from disk
    spec = importlib.util.spec_from_file_location("pipeline_config", target_path)
    pipeline_config = importlib.util.module_from_spec(spec)
    sys.modules["pipeline_config"] = pipeline_config
    spec.loader.exec_module(pipeline_config)
    # ──────────────────────────────────────────────────
    
    # Check execution parsing bounds
    assert pipeline_config.MAX_ITEMS == 10000
    assert pipeline_config.FIREHOSE_WAIT_SECS == 600  # 10 minutes * 60 seconds
    
    # Verify the API Gateway points to the global endpoint mock configured in conftest.py
    assert "execute-api" in pipeline_config.API_GATEWAY_URL
    
    # Check Redshift Warehouse credentials parsing
    assert pipeline_config.REDSHIFT_DB == "dev"
    assert pipeline_config.REDSHIFT_PORT == 5439
    
    # Validate Docker Kwargs configurations for Airflow operators
    kwargs = pipeline_config.DOCKER_OPERATOR_KWARGS
    assert kwargs["image"] == "dbt-redshift:1.9.0"
    assert kwargs["force_pull"] == False
    assert kwargs["network_mode"] == "bridge"
    assert "DBT_REDSHIFT_HOST" in kwargs["environment"]
    
    # Verify dbt volume structural constraints match deployment schema
    assert len(kwargs["mounts"]) == 1
    
    # Correctly unpack list index 0 before querying configuration targets
    assert kwargs["mounts"][0]["Target"] == "/dbt"
    assert kwargs["mounts"][0]["Source"].endswith("nairobi_dbt")
