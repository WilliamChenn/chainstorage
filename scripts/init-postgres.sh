#!/bin/bash
set -e

echo "Initializing chainstorage metadata postgres database..."  


psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
     -- Create user for chainstorage application
    CREATE USER chainstorage WITH PASSWORD 'chainstorage';
    
    -- Create dedicated database for chainstorage metadata
    CREATE DATABASE chainstorage_metadata;

    GRANT ALL PRIVILEGES ON DATABASE chainstorage_metadata TO chainstorage;

    -- Connect to the new database and grant permissions
    \connect chainstorage_metadata;
    GRANT ALL ON SCHEMA public TO chainstorage;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO chainstorage;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO chainstorage;

EOSQL
