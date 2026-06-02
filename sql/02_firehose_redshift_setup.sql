-- PURPOSE: Redshift-side setup so Kinesis Firehose can COPY into bronze.raw_rentals

-- Create a Redshift user that Firehose will authenticate as
CREATE USER firehose_user PASSWORD 'NairobiPipeline2026!';


-- For the Scraper/Firehose
GRANT INSERT ON bronze.raw_rentals TO firehose_user;

-- 1. DATABASE LEVEL: Ensure the user can create the new schemas
-- Grant permission to connect and create new schemas (like gold/silver) in the database
-- Grant permission to connect to the database and create new schemas
GRANT CREATE, TEMP ON DATABASE dev TO firehose_user;

-- 2. SCHEMA LEVEL: Grant access to the three core layers
GRANT USAGE, CREATE ON SCHEMA bronze TO firehose_user;
GRANT USAGE, CREATE ON SCHEMA silver TO firehose_user;
GRANT USAGE, CREATE ON SCHEMA gold TO firehose_user;

-- 3. TABLE LEVEL: Grant full control for dbt to build/drop models
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO firehose_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO firehose_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO firehose_user;

-- 4. FUTURE PROOFING: Ensure future models inherit these permissions
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL PRIVILEGES ON TABLES TO firehose_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL PRIVILEGES ON TABLES TO firehose_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL PRIVILEGES ON TABLES TO firehose_user;

-- 5. This is the specific command Redshift requires for system log access
ALTER USER firehose_user SYSLOG ACCESS UNRESTRICTED;

-- 6. Grant the S3 copy role specifically to firehose_user
-- This role has AmazonS3ReadOnlyAccess which allows Redshift to read
-- the NDJSON files that Firehose wrote to the staging S3 bucket.
-- Without this grant, firehose_user can INSERT into the table but cannot
-- read from S3 during COPY — causing the COPY to submit successfully
-- but load zero rows silently.
GRANT ASSUMEROLE ON 'arn:aws:iam::305291767541:role/naiflow-redshift-s3-copy-role'
TO firehose_user
FOR COPY;


-- Create the analyst user
CREATE USER analyst WITH PASSWORD 'Analyst@2026!';

-- Grant them usage on the schemas so they can actually see the data
GRANT USAGE ON SCHEMA bronze TO analyst;
GRANT USAGE ON SCHEMA silver TO analyst;
GRANT USAGE ON SCHEMA gold TO analyst;

-- Ensure they can read data from tables created in the future (Crucial for dbt)
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT ON TABLES TO analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT ON TABLES TO analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO analyst;


-- Allow your user to see the system catalog
GRANT SELECT ON TABLE svv_table_info TO firehose_user;
GRANT SELECT ON TABLE svv_column_info TO firehose_user;

-- Additionally, dbt often needs access to these for full documentation
GRANT SELECT ON TABLE stl_explain TO firehose_user;
GRANT SELECT ON TABLE stv_tbl_perm TO firehose_user;

-- Verify it was created
SELECT usename FROM pg_user WHERE usename = 'firehose_user';
-- =============================================================================
-- Step 1: Revoke blanket IAM role access from all users
-- =============================================================================
-- By default, Redshift grants ALL users (PUBLIC) permission to assume
-- any IAM role attached to the cluster/workgroup. This is a security risk
-- because any database user could use any IAM role for COPY or UNLOAD.
--
-- This statement removes that blanket permission so we can then grant
-- role access on a per-user basis (principle of least privilege).
--
-- NOTE: This does NOT break any existing COPY jobs that run as admin/superuser.
-- It only affects non-superuser accounts like firehose_user.
-- =============================================================================
REVOKE ASSUMEROLE ON ALL FROM PUBLIC FOR ALL;


-- =============================================================================
-- Step 2: Grant the S3 copy role specifically to firehose_user
-- =============================================================================
-- Now that blanket access is revoked, we explicitly grant firehose_user
-- permission to assume the naiflow-redshift-s3-copy-role IAM role —
-- but ONLY for COPY operations (not UNLOAD or other operations).
--
-- This role has AmazonS3ReadOnlyAccess which allows Redshift to read
-- the NDJSON files that Firehose wrote to the staging S3 bucket.
--
-- Without this grant, firehose_user can INSERT into the table but cannot
-- read from S3 during COPY — causing the COPY to submit successfully
-- but load zero rows silently.
-- =============================================================================
GRANT ASSUMEROLE ON 'arn:aws:iam::305291767541:role/naiflow-redshift-s3-copy-role'
TO firehose_user
FOR COPY;


-- =============================================================================
-- Step 3: Verify the grant was applied correctly
-- =============================================================================
-- Checks svv_role_grants to confirm firehose_user now has
-- the IAM role permission. Should return one row showing
-- firehose_user → naiflow-redshift-s3-copy-role.
--
-- If this returns no rows, the grant did not apply and
-- the COPY will still load zero rows.
-- =============================================================================
-- SELECT
--     identity_name,
--     identity_type,
--     role_name
-- FROM svv_role_grants
-- WHERE identity_name = 'firehose_user';

-- Allow the IAM role to assume this user (Redshift Serverless IAM auth)
-- Replace the ARN below with your actual Firehose IAM role ARN
-- CREATE IDENTITY PROVIDER firehose_idp
--     TYPE iam
--     NAMESPACE 'firehose';


-- ---------------------------------------------------------------------------
-- Verify the table is ready to receive data
-- Run this after creating the Firehose delivery stream and sending test data
-- ---------------------------------------------------------------------------
-- SELECT
--     COUNT(*)                              AS total_rows,
--     COUNT(DISTINCT neighbourhood)         AS distinct_neighbourhoods,
--     MIN(firehose_arrival_at)              AS earliest_record,
--     MAX(firehose_arrival_at)              AS latest_record,
--     SUM(CASE WHEN price_ksh IS NULL THEN 1 ELSE 0 END) AS null_prices
-- FROM bronze.raw_rentals;