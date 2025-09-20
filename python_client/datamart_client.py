import requests
import json
import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Tuple
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DataMartClient:
    """Client for interacting with the ClickHouse Data Mart API"""
    
    def __init__(self, base_url: str = None):
        """
        Initialize the DataMart client
        
        Args:
            base_url: Base URL of the DataMart API server
        """
        import os
        if base_url is None:
            base_url = os.getenv('DATAMART_BASE_URL', 'http://localhost:8081')
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def health_check(self) -> bool:
        """
        Check if the DataMart API is healthy
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def get_status(self) -> Dict:
        """
        Get the DataMart status
        
        Returns:
            Dictionary containing status information
        """
        try:
            response = self.session.get(f"{self.base_url}/status", timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get status: {e}")
            raise
    
    def sync_data(self) -> Dict:
        """
        Trigger data synchronization from MySQL to ClickHouse
        
        Returns:
            Dictionary containing sync results and statistics
        """
        try:
            response = self.session.post(f"{self.base_url}/sync", timeout=300)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to sync data: {e}")
            raise
    
    def get_processed_data(self, max_records: Optional[int] = None, 
                          filters: Optional[Dict[str, str]] = None) -> Tuple[np.ndarray, Dict]:
        """
        Get processed data for ML model training
        
        Args:
            max_records: Maximum number of records to retrieve
            filters: Optional filters to apply
            
        Returns:
            Tuple of (feature_matrix, metadata)
        """
        try:
            request_data = {
                "maxRecords": max_records,
                "filters": filters
            }
            
            response = self.session.post(
                f"{self.base_url}/data",
                json=request_data,
                timeout=60
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Extract feature vectors
            feature_vectors = data.get('data', [])
            stats = data.get('stats', {})
            
            if not feature_vectors:
                logger.warning("No feature vectors received from DataMart")
                return np.array([]), stats
            
            # Convert to numpy array
            features = []
            ids = []
            
            for vector in feature_vectors:
                ids.append(vector['id'])
                features.append(vector['features'])
            
            feature_matrix = np.array(features)
            
            logger.info(f"Retrieved {len(feature_vectors)} feature vectors from DataMart")
            logger.info(f"Feature matrix shape: {feature_matrix.shape}")
            
            metadata = {
                'ids': ids,
                'stats': stats,
                'shape': feature_matrix.shape
            }
            
            return feature_matrix, metadata
            
        except Exception as e:
            logger.error(f"Failed to get processed data: {e}")
            raise
    
    def store_model_results(self, run_id: int, predictions: np.ndarray, 
                           cluster_centers: np.ndarray, data_ids: List[int],
                           silhouette_score: float = 0.0) -> Dict:
        """
        Store ML model results back to the DataMart
        
        Args:
            run_id: Model run ID
            predictions: Cluster predictions for each data point
            cluster_centers: Cluster center coordinates
            data_ids: IDs corresponding to the predictions
            silhouette_score: Model silhouette score
            
        Returns:
            Dictionary containing storage results
        """
        try:
            # Prepare clustering results
            clustering_results = []
            for i, (data_id, cluster_id) in enumerate(zip(data_ids, predictions)):
                clustering_results.append({
                    "id": i + 1,  # Sequential ID for clustering result
                    "modelRunId": run_id,
                    "processedDataId": int(data_id),
                    "clusterId": int(cluster_id),
                    "distanceToCenter": 0.0  # Would need to calculate actual distance
                })
            
            # Prepare cluster centers
            cluster_center_records = []
            for cluster_id, center in enumerate(cluster_centers):
                cluster_center_records.append({
                    "id": cluster_id + 1,
                    "modelRunId": run_id,
                    "clusterId": cluster_id,
                    "energy100g": float(center[0]),
                    "proteins100g": float(center[1]),
                    "carbohydrates100g": float(center[2]),
                    "sugars100g": float(center[3]),
                    "fat100g": float(center[4]),
                    "saturatedFat100g": float(center[5]),
                    "fiber100g": float(center[6]),
                    "salt100g": float(center[7]),
                    "sodium100g": float(center[8])
                })
            
            request_data = {
                "modelRunId": run_id,
                "results": clustering_results,
                "clusterCenters": cluster_center_records
            }
            
            response = self.session.post(
                f"{self.base_url}/results",
                json=request_data,
                timeout=60
            )
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"Stored {len(clustering_results)} clustering results and {len(cluster_center_records)} cluster centers")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to store model results: {e}")
            raise
    
    def wait_for_datamart(self, max_wait_seconds: int = 120) -> bool:
        """
        Wait for DataMart to become available
        
        Args:
            max_wait_seconds: Maximum time to wait
            
        Returns:
            True if DataMart becomes available, False if timeout
        """
        import time
        
        start_time = time.time()
        while time.time() - start_time < max_wait_seconds:
            if self.health_check():
                logger.info("DataMart is available")
                return True
            
            logger.info("Waiting for DataMart to become available...")
            time.sleep(5)
        
        logger.error(f"DataMart did not become available within {max_wait_seconds} seconds")
        return False


def create_spark_dataframe_from_features(spark, feature_matrix: np.ndarray, 
                                       ids: List[int]) -> 'DataFrame':
    """
    Create a Spark DataFrame from feature matrix for compatibility with existing code
    
    Args:
        spark: Spark session
        feature_matrix: NumPy feature matrix
        ids: List of data IDs
        
    Returns:
        Spark DataFrame with features
    """
    try:
        from pyspark.sql import Row
        from pyspark.ml.linalg import Vectors
        
        # Create rows with features
        rows = []
        for i, (data_id, features) in enumerate(zip(ids, feature_matrix)):
            row = Row(
                id=int(data_id),
                features=Vectors.dense(features.tolist())
            )
            rows.append(row)
        
        # Create DataFrame
        df = spark.createDataFrame(rows)
        
        logger.info(f"Created Spark DataFrame with {df.count()} rows")
        return df
        
    except Exception as e:
        logger.error(f"Failed to create Spark DataFrame: {e}")
        raise


# Example usage for integration with existing model
def integrate_with_existing_model():
    """
    Example of how to integrate the DataMart client with the existing PySpark model
    """
    
    # Initialize client
    client = DataMartClient()
    
    # Wait for DataMart to be available
    if not client.wait_for_datamart():
        raise Exception("DataMart is not available")
    
    # Check status
    status = client.get_status()
    print(f"DataMart Status: {status}")
    
    # Sync data if needed
    if status.get('clickhouseRecordCount', 0) == 0:
        print("Syncing data from MySQL...")
        sync_result = client.sync_data()
        print(f"Sync completed: {sync_result}")
    
    # Get processed data
    feature_matrix, metadata = client.get_processed_data(max_records=10000)
    
    print(f"Retrieved feature matrix with shape: {feature_matrix.shape}")
    print(f"Metadata: {metadata}")
    
    return feature_matrix, metadata


if __name__ == "__main__":
    # Test the client
    logging.basicConfig(level=logging.INFO)
    
    try:
        feature_matrix, metadata = integrate_with_existing_model()
        print("DataMart client integration test successful!")
    except Exception as e:
        print(f"DataMart client integration test failed: {e}")
