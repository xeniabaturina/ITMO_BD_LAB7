import os
import sys
import time
import findspark
findspark.init()

from datetime import datetime
import numpy as np

# Add the python_client directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'python_client'))

from datamart_client import DataMartClient, create_spark_dataframe_from_features

# Import Lab 7 configuration
from config_lab7 import get_logger, clean_logs, KMEANS_CONFIG, SPARK_CONFIG

clean_logs()

logger = get_logger(__name__)

def create_spark_session(app_name="FoodClusteringWithDataMart"):
    """Create and configure Spark session"""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(SPARK_CONFIG["master"]) \
        .config("spark.driver.memory", SPARK_CONFIG["driver_memory"]) \
        .config("spark.executor.memory", SPARK_CONFIG["executor_memory"]) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.bindAddress", SPARK_CONFIG["driver_bind_address"]) \
        .config("spark.driver.host", SPARK_CONFIG["driver_host"]) \
        .config("spark.driver.port", SPARK_CONFIG["spark_driver_port"]) \
        .config("spark.blockManager.port", SPARK_CONFIG["spark_blockManager_port"]) \
        .config("spark.broadcast.port", SPARK_CONFIG["spark_broadcast_port"]) \
        .config("spark.fileserver.port", SPARK_CONFIG["spark_fileserver_port"]) \
        .config("spark.replClassServer.port", SPARK_CONFIG["spark_replClassServer_port"]) \
        .config("spark.ui.port", SPARK_CONFIG["spark_ui_port"]) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel(SPARK_CONFIG["log_level"])
    return spark

def run_kmeans_clustering_with_datamart(spark, feature_df, k=5, max_iter=200, seed=42):
    """
    Run K-means clustering using preprocessed data from DataMart
    
    Args:
        spark: Spark session
        feature_df: Spark DataFrame with preprocessed features
        k: Number of clusters
        max_iter: Maximum iterations
        seed: Random seed
    
    Returns:
        Tuple of (model, predictions_df, silhouette_score, cluster_centers)
    """
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    
    logger.info(f"Starting K-means clustering with k={k}, max_iter={max_iter}, seed={seed}")
    
    # Initialize K-means
    kmeans = KMeans(
        k=k,
        maxIter=max_iter,
        seed=seed,
        featuresCol="features",
        predictionCol="prediction"
    )
    
    # Fit the model
    logger.info("Training K-means model...")
    start_time = time.time()
    model = kmeans.fit(feature_df)
    training_time = time.time() - start_time
    
    logger.info(f"Model training completed in {training_time:.2f} seconds")
    
    # Make predictions
    logger.info("Making predictions...")
    predictions = model.transform(feature_df)
    
    # Evaluate using silhouette score
    logger.info("Evaluating model...")
    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="prediction",
        metricName="silhouette"
    )
    
    silhouette_score = evaluator.evaluate(predictions)
    logger.info(f"Silhouette Score: {silhouette_score:.4f}")
    
    # Get cluster centers
    cluster_centers = model.clusterCenters()
    logger.info(f"Found {len(cluster_centers)} cluster centers")
    
    # Log cluster centers with proper formatting
    feature_names = ["Energy", "Protein", "Carbs", "Sugar", "Fat", "SatFat", "Fiber", "Salt", "Sodium"]
    for i, center in enumerate(cluster_centers):
        # Format center values to 4 decimal places
        center_str = "[" + ", ".join([f"{val:.4f}" for val in center]) + "]"
        logger.info(f"Cluster {i} center: {center_str}")
    
    # Also log a readable interpretation
    logger.info("Cluster centers interpretation:")
    for i, center in enumerate(cluster_centers):
        dominant_features = []
        for j, (val, name) in enumerate(zip(center, feature_names)):
            if val > 0.5:  # Significant values
                dominant_features.append(f"{name}={val:.3f}")
        
        if dominant_features:
            logger.info(f"  Cluster {i}: High in {', '.join(dominant_features)}")
        else:
            logger.info(f"  Cluster {i}: Balanced nutritional profile")
    
    return model, predictions, silhouette_score, cluster_centers

def main():
    """Main function integrating with ClickHouse DataMart"""
    logger.info("=" * 80)
    logger.info("ITMO Big Data Lab 7: PySpark K-Means with ClickHouse DataMart Integration")
    logger.info("=" * 80)
    
    # Initialize DataMart client
    logger.info("Initializing DataMart client...")
    datamart_client = DataMartClient()
    
    # Wait for DataMart to be available
    if not datamart_client.wait_for_datamart(max_wait_seconds=120):
        logger.error("DataMart is not available. Please ensure the DataMart service is running.")
        return
    
    # Check DataMart status
    try:
        status = datamart_client.get_status()
        logger.info("DataMart Status:")
        logger.info(f"  MySQL Connected: {status.get('mysqlConnected', False)}")
        logger.info(f"  ClickHouse Connected: {status.get('clickhouseConnected', False)}")
        logger.info(f"  MySQL Records: {status.get('mysqlRecordCount', 0)}")
        logger.info(f"  ClickHouse Records: {status.get('clickhouseRecordCount', 0)}")
        
        # Sync data if ClickHouse is empty
        if status.get('clickhouseRecordCount', 0) == 0:
            logger.info("ClickHouse appears to be empty. Triggering data sync...")
            sync_result = datamart_client.sync_data()
            logger.info(f"Data sync completed: {sync_result}")
            
    except Exception as e:
        logger.error(f"Failed to get DataMart status: {e}")
        return
    
    # Get preprocessed data from DataMart
    logger.info("Retrieving preprocessed data from DataMart...")
    try:
        feature_matrix, metadata = datamart_client.get_processed_data(
            max_records=KMEANS_CONFIG.get("max_records", 50000)
        )
        
        if feature_matrix.size == 0:
            logger.error("No data received from DataMart")
            return
            
        logger.info(f"Retrieved feature matrix with shape: {feature_matrix.shape}")
        logger.info(f"Processing stats: {metadata['stats']}")
        
    except Exception as e:
        logger.error(f"Failed to get processed data from DataMart: {e}")
        return
    
    # Create Spark session
    logger.info("Creating Spark session...")
    spark = create_spark_session()
    
    try:
        # Convert numpy array to Spark DataFrame
        logger.info("Converting feature matrix to Spark DataFrame...")
        feature_df = create_spark_dataframe_from_features(
            spark, feature_matrix, metadata['ids']
        )
        
        logger.info(f"Created Spark DataFrame with {feature_df.count()} rows")
        
        # Run clustering
        logger.info("Running K-means clustering...")
        model, predictions, silhouette_score, cluster_centers = run_kmeans_clustering_with_datamart(
            spark,
            feature_df,
            k=KMEANS_CONFIG["k"],
            max_iter=KMEANS_CONFIG["max_iter"],
            seed=KMEANS_CONFIG["seed"]
        )
        
        # Collect predictions for storing in DataMart
        logger.info("Collecting predictions...")
        predictions_collected = predictions.select("id", "prediction").collect()
        
        # Convert to numpy arrays for DataMart storage
        data_ids = [row.id for row in predictions_collected]
        cluster_predictions = np.array([row.prediction for row in predictions_collected])
        cluster_centers_array = np.array(cluster_centers)
        
        # Generate run ID
        run_id = int(datetime.now().timestamp())
        
        # Store results in DataMart
        logger.info(f"Storing results in DataMart with run_id={run_id}...")
        try:
            result = datamart_client.store_model_results(
                run_id=run_id,
                predictions=cluster_predictions,
                cluster_centers=cluster_centers_array,
                data_ids=data_ids,
                silhouette_score=silhouette_score
            )
            logger.info(f"Results stored successfully: {result}")
            
        except Exception as e:
            logger.error(f"Failed to store results in DataMart: {e}")
        
        # Print summary
        logger.info("=" * 80)
        logger.info("Clustering completed successfully!")
        logger.info("=" * 80)
        logger.info("Results summary:")
        logger.info(f"  Run ID: {run_id}")
        logger.info(f"  Records processed: {len(data_ids)}")
        logger.info(f"  Number of clusters: {KMEANS_CONFIG['k']}")
        logger.info(f"  Silhouette score: {silhouette_score:.4f}")
        logger.info(f"  Data source: ClickHouse DataMart")
        logger.info(f"  Preprocessing: Handled by DataMart")
        
        # Cluster distribution
        unique, counts = np.unique(cluster_predictions, return_counts=True)
        logger.info("Cluster distribution:")
        for cluster_id, count in zip(unique, counts):
            percentage = (count / len(cluster_predictions)) * 100
            logger.info(f"  Cluster {cluster_id}: {count} records ({percentage:.1f}%)")
        
        return silhouette_score
        
    except Exception as e:
        logger.error(f"Clustering failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0.0
        
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    start_time = time.time()
    
    try:
        silhouette_score = main()
        end_time = time.time()
        
        logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")
        
        if silhouette_score and silhouette_score > 0:
            logger.info("Lab 7 completed successfully!")
            logger.info("Architecture: Model -> DataMart -> ClickHouse + MySQL")
        else:
            logger.error("Lab 7 failed")
            
    except Exception as e:
        logger.error(f"Application failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
