# ===========================================================================
# 2. AIRFLOW & DOCKER ORCHESTRATION LAYER VALIDATION
# ===========================================================================

@pytest.mark.network
def test_pipeline_config_initialization():
    """
    Validates that system parameters read configuration variables, 
    map integers accurately, and set up correct Docker mount sources.
    """
    # Import the configuration module inside the test to leverage conftest environment variables
    import pipeline_config
    
    # 1. Verify basic default arguments and types
    assert isinstance(pipeline_config.DEFAULT_ARGS, dict)
    assert pipeline_config.DEFAULT_ARGS["owner"] == "nairobi-pipeline"
    assert pipeline_config.DEFAULT_ARGS["retries"] == 1
    
    # 2. Verify parsed integer computations
    assert isinstance(pipeline_config.MAX_ITEMS, int)
    assert isinstance(pipeline_config.FIREHOSE_WAIT_SECS, int)
    assert pipeline_config.FIREHOSE_WAIT_SECS == 600  # 10 minutes * 60 seconds
    
    # 3. Extract and isolate Docker operator configurations
    kwargs = pipeline_config.DOCKER_OPERATOR_KWARGS
    assert isinstance(kwargs, dict)
    
    # 4. Deep structural validation of the Docker settings
    assert kwargs["image"] == "dbt-redshift:1.9.0"
    assert kwargs["docker_url"] == "unix://var/run/docker.sock"
    assert kwargs["auto_remove"] == "success"
    assert kwargs["force_pull"] is False
    assert kwargs["mount_tmp_dir"] is False
    assert kwargs["network_mode"] == "bridge"
    
    # 5. Validate Environment Variable propagation mapping
    env = kwargs["environment"]
    assert isinstance(env, dict)
    assert "DBT_REDSHIFT_HOST" in env
    assert "DBT_REDSHIFT_DB" in env
    
    # 6. CRITICAL VERIFICATION: Mount Source Infrastructure
    mounts = kwargs["mounts"]
    assert isinstance(mounts, list)
    assert len(mounts) == 1
    
    # Extract structural attributes from the Docker Type Mount engine object
    dbt_mount = mounts[0]
    assert dbt_mount["Target"] == "/dbt"
    assert dbt_mount["Type"] == "bind"
    
    # This assertion passes seamlessly thanks to the conftest.py fallback injection
    assert dbt_mount["Source"].endswith("nairobi_dbt")
