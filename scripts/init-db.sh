#!/bin/bash
set -e

# This script is executed by PostgreSQL Docker container on first startup.
# It initializes both o4x and o4x_test databases using the generated schema.
#
# The schema is generated from schema/schema.go via:
#   make schema-gen
#
# IMPORTANT: If you modify schema/schema.go, regenerate scripts/schema.sql:
#   make schema-gen

SCHEMA_FILE="/opt/o4x/schema.sql"

# Create o4x_test database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE o4x_test;
EOSQL

# Initialize o4x database schema
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "o4x" < "$SCHEMA_FILE"

# Initialize o4x_test database schema (same schema)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "o4x_test" < "$SCHEMA_FILE"

echo "Initialized o4x and o4x_test databases"
