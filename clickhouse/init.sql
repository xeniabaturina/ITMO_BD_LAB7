-- Initialize ClickHouse database
CREATE DATABASE IF NOT EXISTS food_clustering;

-- Use the database
USE food_clustering;

-- Processed food data table
CREATE TABLE IF NOT EXISTS processed_food_data (
    id UInt64,
    product_name String,
    brands String,
    categories String,
    energy_100g Float64,
    proteins_100g Float64,
    carbohydrates_100g Float64,
    sugars_100g Float64,
    fat_100g Float64,
    saturated_fat_100g Float64,
    fiber_100g Float64,
    salt_100g Float64,
    sodium_100g Float64,
    processed_at DateTime
) ENGINE = MergeTree()
ORDER BY (id, processed_at)
SETTINGS index_granularity = 8192;

-- Feature vectors table for ML model
CREATE TABLE IF NOT EXISTS feature_vectors (
    id UInt64,
    features Array(Float64),
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY (id, created_at)
SETTINGS index_granularity = 8192;

-- Model runs table
CREATE TABLE IF NOT EXISTS model_runs (
    run_id UInt64,
    run_name String,
    silhouette_score Float64,
    status String,
    started_at DateTime,
    completed_at Nullable(DateTime)
) ENGINE = MergeTree()
ORDER BY (run_id, started_at)
SETTINGS index_granularity = 8192;

-- Clustering results table
CREATE TABLE IF NOT EXISTS clustering_results (
    id UInt64,
    model_run_id UInt64,
    processed_data_id UInt64,
    cluster_id UInt32,
    distance_to_center Float64,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY (model_run_id, cluster_id, id)
SETTINGS index_granularity = 8192;

-- Cluster centers table
CREATE TABLE IF NOT EXISTS cluster_centers (
    id UInt64,
    model_run_id UInt64,
    cluster_id UInt32,
    energy_100g Float64,
    proteins_100g Float64,
    carbohydrates_100g Float64,
    sugars_100g Float64,
    fat_100g Float64,
    saturated_fat_100g Float64,
    fiber_100g Float64,
    salt_100g Float64,
    sodium_100g Float64,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY (model_run_id, cluster_id)
SETTINGS index_granularity = 8192;

-- Create materialized view for cluster analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS cluster_analysis_mv
ENGINE = AggregatingMergeTree()
ORDER BY (model_run_id, cluster_id)
AS SELECT
    model_run_id,
    cluster_id,
    count() as product_count,
    avg(energy_100g) as avg_energy,
    avg(proteins_100g) as avg_proteins,
    avg(carbohydrates_100g) as avg_carbohydrates,
    avg(fat_100g) as avg_fat,
    min(energy_100g) as min_energy,
    max(energy_100g) as max_energy
FROM processed_food_data pfd
JOIN clustering_results cr ON pfd.id = cr.processed_data_id
GROUP BY model_run_id, cluster_id;
