import logging
import os
from datetime import datetime

# Logging configuration
def clean_logs():
    """Clean log files before starting a new run"""
    log_file = os.path.join(LOG_DIR, 'food_clustering_lab7.log')
    if os.path.exists(log_file):
        try:
            # Clear the log file
            with open(log_file, 'w') as f:
                f.write('')
            print(f"Cleaned log file: {log_file}")
        except Exception as e:
            print(f"Warning: Could not clean log file {log_file}: {e}")

def get_logger(name):
    """Get configured logger with both file and console output"""
    logger = logging.getLogger(name)
    if not logger.handlers:
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)-15s - %(levelname)-8s - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler
        log_file = os.path.join(LOG_DIR, 'food_clustering_lab7.log')
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        logger.setLevel(logging.INFO)
    return logger

# Spark configuration
SPARK_CONFIG = {
    "master": "local[*]",
    "driver_memory": "4g",
    "executor_memory": "4g",
    "log_level": "WARN",
    "app_name": "FoodClusteringWithDataMart",
    # Fix BindException
    "driver_bind_address": "127.0.0.1",
    "driver_host": "127.0.0.1",
    "spark_driver_bindAddress": "127.0.0.1",
    "spark_driver_host": "127.0.0.1",
    "spark_driver_port": "0",
    "spark_blockManager_port": "0",
    "spark_broadcast_port": "0",
    "spark_fileserver_port": "0",
    "spark_replClassServer_port": "0",
    "spark_ui_port": "0"
}

# K-means clustering configuration
KMEANS_CONFIG = {
    "k": 5,
    "max_iter": 200,
    "seed": 42,
    "max_records": 50000,
    "features": [
        "energy_100g",
        "proteins_100g", 
        "carbohydrates_100g",
        "sugars_100g",
        "fat_100g",
        "saturated_fat_100g",
        "fiber_100g",
        "salt_100g",
        "sodium_100g"
    ]
}

# DataMart configuration
DATAMART_CONFIG = {
    "base_url": "http://localhost:8081",
    "timeout": 300,
    "max_wait_seconds": 120
}

# Paths and directories
LOG_DIR = "logs"
MODELS_DIR = "models"
REPORTS_DIR = "reports"

# Ensure directories exist
for directory in [LOG_DIR, MODELS_DIR, REPORTS_DIR]:
    os.makedirs(directory, exist_ok=True)
