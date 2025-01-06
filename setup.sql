use role ACCOUNTADMIN;

create database if not exists DASH_DB;
create schema if not exists DASH_SCHEMA;
create warehouse if not exists DASH_WH_S WAREHOUSE_SIZE=SMALL;

use database DASH_DB;
use schema DASH_SCHEMA;
use warehouse DASH_WH_S;

create compute pool if not exists CPU_X64_XS
  MIN_NODES = 1
  MAX_NODES = 2
  INSTANCE_FAMILY = CPU_X64_XS;

create role if not exists DASH_CONTAINER_RUNTIME_ROLE;
grant role DASH_CONTAINER_RUNTIME_ROLE to role ACCOUNTADMIN;
grant usage on database DASH_DB to role DASH_CONTAINER_RUNTIME_ROLE;
grant all on schema DASH_SCHEMA to role DASH_CONTAINER_RUNTIME_ROLE;
grant usage on warehouse DASH_WH_S to role DASH_CONTAINER_RUNTIME_ROLE;
grant all on compute pool CPU_X64_XS to role DASH_CONTAINER_RUNTIME_ROLE;

create network rule if not exists allow_all_rule
  TYPE = 'HOST_PORT'
  MODE= 'EGRESS'
  VALUE_LIST = ('0.0.0.0:443','0.0.0.0:80');

create external access integration if not exists allow_all_access_integration
  ALLOWED_NETWORK_RULES = (allow_all_rule)
  ENABLED = true;

grant usage on integration allow_all_access_integration to role DASH_CONTAINER_RUNTIME_ROLE;