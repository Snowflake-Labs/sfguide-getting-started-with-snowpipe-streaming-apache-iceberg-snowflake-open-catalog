-- ====================
-- SETUP
-- ====================

-- TODO: Update STORAGE_BASE_URL and STORAGE_AWS_ROLE_ARN with your values
CREATE OR REPLACE EXTERNAL VOLUME aws_s3_ext_volume_snowflake
  STORAGE_LOCATIONS =
      (
        (
            NAME = 'aws_s3_ext_volume_snowflake'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://<bucketname>/<optional-folder-name>/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::84935######:role/dash-snowflake-access-role'
        )
      );

-- desc external volume aws_s3_ext_volume_snowflake; 

-- TODO: Update CATALOG_URI, OAUTH_CLIENT_ID and OAUTH_CLIENT_SECRET with your values
CREATE OR REPLACE CATALOG INTEGRATION polaris_external_cat_integration 
    CATALOG_SOURCE=POLARIS 
    TABLE_FORMAT=ICEBERG 
    CATALOG_NAMESPACE='raw' 
    REST_CONFIG = (
        CATALOG_URI ='https://<orgname>-<my-snowflake-open-catalog-account-name>.snowflakecomputing.com/polaris/api/catalog' 
        WAREHOUSE = 'snowflake_catalog'
    )
    REST_AUTHENTICATION = (
        TYPE=OAUTH 
        OAUTH_CLIENT_ID='t954zgl8HjmpIMXXXXXXXXXXXXX' 
        OAUTH_CLIENT_SECRET='fE44DlmXKonEx41Ks9kYlsprXXXXXXXXXXXXX' 
        OAUTH_ALLOWED_SCOPES=('PRINCIPAL_ROLE:ALL') 
    ) 
ENABLED=true;
