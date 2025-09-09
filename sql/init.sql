-- Initialize database
USE food_clustering;

-- Table for storing processed/cleaned food data
CREATE TABLE IF NOT EXISTS processed_food_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(1000),
    brands VARCHAR(500),
    categories VARCHAR(1000),
    energy_100g DECIMAL(10,2),
    proteins_100g DECIMAL(10,2),
    carbohydrates_100g DECIMAL(10,2),
    sugars_100g DECIMAL(10,2),
    fat_100g DECIMAL(10,2),
    saturated_fat_100g DECIMAL(10,2),
    fiber_100g DECIMAL(10,2),
    salt_100g DECIMAL(10,2),
    sodium_100g DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_created_at (created_at)
);

-- Table for storing model runs and configurations
CREATE TABLE IF NOT EXISTS model_runs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    run_name VARCHAR(255),
    k_clusters INT,
    max_iter INT,
    seed INT,
    silhouette_score DECIMAL(15,10),
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    status ENUM('running', 'completed', 'failed') DEFAULT 'running',
    INDEX idx_started_at (started_at),
    INDEX idx_status (status)
);

-- Table for storing clustering results
CREATE TABLE IF NOT EXISTS clustering_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    model_run_id INT,
    processed_data_id INT,
    cluster_id INT,
    distance_to_center DECIMAL(15,10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (model_run_id) REFERENCES model_runs(id),
    FOREIGN KEY (processed_data_id) REFERENCES processed_food_data(id),
    INDEX idx_model_run_id (model_run_id),
    INDEX idx_cluster_id (cluster_id),
    INDEX idx_processed_data_id (processed_data_id)
);

-- Table for storing cluster centers
CREATE TABLE IF NOT EXISTS cluster_centers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    model_run_id INT,
    cluster_id INT,
    energy_100g DECIMAL(15,10),
    proteins_100g DECIMAL(15,10),
    carbohydrates_100g DECIMAL(15,10),
    sugars_100g DECIMAL(15,10),
    fat_100g DECIMAL(15,10),
    saturated_fat_100g DECIMAL(15,10),
    fiber_100g DECIMAL(15,10),
    salt_100g DECIMAL(15,10),
    sodium_100g DECIMAL(15,10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (model_run_id) REFERENCES model_runs(id),
    UNIQUE KEY unique_model_cluster (model_run_id, cluster_id),
    INDEX idx_model_run_id (model_run_id),
    INDEX idx_cluster_id (cluster_id)
);

-- Table for storing model metadata and files
CREATE TABLE IF NOT EXISTS model_metadata (
    id INT AUTO_INCREMENT PRIMARY KEY,
    model_run_id INT,
    model_path VARCHAR(500),
    model_size_bytes BIGINT,
    feature_columns JSON,
    preprocessing_params JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (model_run_id) REFERENCES model_runs(id),
    INDEX idx_model_run_id (model_run_id)
);

-- Create a view for easy access to latest clustering results
CREATE OR REPLACE VIEW latest_clustering_results AS
SELECT 
    mr.id as model_run_id,
    mr.run_name,
    mr.silhouette_score,
    mr.completed_at,
    cr.cluster_id,
    COUNT(cr.id) as products_in_cluster,
    AVG(pfd.energy_100g) as avg_energy,
    AVG(pfd.proteins_100g) as avg_proteins,
    AVG(pfd.carbohydrates_100g) as avg_carbohydrates,
    AVG(pfd.fat_100g) as avg_fat
FROM model_runs mr
JOIN clustering_results cr ON mr.id = cr.model_run_id
JOIN processed_food_data pfd ON cr.processed_data_id = pfd.id
WHERE mr.status = 'completed'
    AND mr.completed_at = (
        SELECT MAX(completed_at) 
        FROM model_runs 
        WHERE status = 'completed'
    )
GROUP BY mr.id, mr.run_name, mr.silhouette_score, mr.completed_at, cr.cluster_id
ORDER BY cr.cluster_id;
