#!/bin/bash
# docker/entrypoint.sh
# ====================
# this script is the entrypoint for the dbt container — it's what runs when DockerOperator spins up a new container to run a dbt command.
#This script does three things in order:
# 1. Writes /dbt/profiles.yml from env vars injected by DockerOperator.
# 2. Installs dbt packages if packages.yml exists and dbt_packages/ is missing.
# After it finishes, it passes the steering wheel to dbt to run the command Airflow asked for.
# 3. exec "$@"  → runs the dbt command Airflow passed (dbt run / dbt test etc.)
# When dbt finishes, the container exits and everything in it disappears, including profiles.yml and any secrets.

# set -euo pipefail → fail fast if any command errors or if any variable is missing.
set -euo pipefail

# ── Guard: required env vars must exist ──────────────────────────────────────
: "${DBT_REDSHIFT_HOST:?DBT_REDSHIFT_HOST is not set}"
: "${DBT_REDSHIFT_DB:?DBT_REDSHIFT_DB is not set}"
: "${DBT_REDSHIFT_USER:?DBT_REDSHIFT_USER is not set}"
: "${DBT_REDSHIFT_PASSWORD:?DBT_REDSHIFT_PASSWORD is not set}"

# ── Write profiles.yml ────────────────────────────────────────────────────────
# profiles.yml must live in the project dir (/dbt) so dbt finds it automatically.
# Every single time the DockerOperator spins up a container, this script runs and "hallucinates" a fresh profiles.yml using the live environment variables.
# When the container finishes its dbt run and exits, this file disappears along with the container
# We write it fresh every container start so secrets are never baked into the image.
cat > /dbt/profiles.yml << EOF
nairobi_dbt:
  target: prod
  outputs:
    prod:
      type:             redshift
      host:             ${DBT_REDSHIFT_HOST}
      port:             ${DBT_REDSHIFT_PORT:-5439}
      dbname:           ${DBT_REDSHIFT_DB}
      user:             ${DBT_REDSHIFT_USER}
      password:         ${DBT_REDSHIFT_PASSWORD}
      schema:           silver
      threads:          4
      connect_timeout:  30
      sslmode:          require
EOF

echo "[entrypoint] profiles.yml written → host=${DBT_REDSHIFT_HOST} db=${DBT_REDSHIFT_DB}"

# ── Install dbt packages (dbt deps) if not already present ───────────────────
# dbt_packages/ is written into the mounted volume so it persists across runs.
# We only reinstall if the folder is missing (first run) or empty.
if [ -f /dbt/packages.yml ] && [ ! -d /dbt/dbt_packages ]; then
    echo "[entrypoint] Running dbt deps..."
    dbt deps --project-dir /dbt --profiles-dir /dbt
    echo "[entrypoint] dbt deps complete."
fi

# ── Hand off to the dbt command Airflow passed ────────────────────────────────
echo "[entrypoint] Executing: $*"
exec "$@"