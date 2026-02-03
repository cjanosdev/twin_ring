-- Create keyspace for cache
CREATE KEYSPACE IF NOT EXISTS kvstore
WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE kvstore;

-- Basic key-value table
CREATE TABLE IF NOT EXISTS kv (
    key text PRIMARY KEY,
    value text
);
