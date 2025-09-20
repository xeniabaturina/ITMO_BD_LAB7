import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
import sys
from tqdm import tqdm

def get_mysql_connection():
    """Create MySQL connection"""
    import os
    
    config = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'port': int(os.getenv('MYSQL_PORT', '3306')),
        'database': os.getenv('MYSQL_DATABASE', 'food_clustering'),
        'user': os.getenv('MYSQL_USER'),
        'password': os.getenv('MYSQL_PASSWORD'),
        'charset': 'utf8mb4',
        'autocommit': True
    }
    
    # Validate required credentials
    if not config['user'] or not config['password']:
        raise ValueError("MYSQL_USER and MYSQL_PASSWORD environment variables must be set")
    
    try:
        connection = mysql.connector.connect(**config)
        print(f"Connected to MySQL database: {config['database']}")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        raise

def load_processed_data(parquet_path, max_records=50000):
    """Load processed data from parquet file into MySQL"""
    
    # Check if data file exists
    if not os.path.exists(parquet_path):
        print(f"Data file not found: {parquet_path}")
        print("Please ensure you have processed data from Lab 5/6")
        return False
    
    print(f"Loading data from: {parquet_path}")
    
    try:
        # Read parquet file
        df = pd.read_parquet(parquet_path)
        print(f"Loaded {len(df)} records from parquet file")
        
        # Limit records for testing
        if len(df) > max_records:
            df = df.head(max_records)
            print(f"Limited to {max_records} records for testing")
        
        # Display basic info
        print(f"Data shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        # Check for required columns
        required_columns = [
            'energy_100g', 'proteins_100g', 'carbohydrates_100g',
            'sugars_100g', 'fat_100g', 'saturated-fat_100g',
            'fiber_100g', 'salt_100g', 'sodium_100g'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Missing columns: {missing_columns}")
        
        # Get MySQL connection
        connection = get_mysql_connection()
        cursor = connection.cursor()
        
        # Check if data already exists
        cursor.execute("SELECT COUNT(*) FROM processed_food_data")
        existing_count = cursor.fetchone()[0]
        
        if existing_count > 0:
            print(f"Database already contains {existing_count} records")
            print("Clearing existing data for fresh load...")
            cursor.execute("DELETE FROM processed_food_data")
            connection.commit()
            print("Existing data cleared")
        
        # Prepare data for insertion
        df_clean = df.copy()
        
        # Handle column name mapping (saturated-fat vs saturated_fat)
        if 'saturated-fat_100g' in df_clean.columns:
            df_clean['saturated_fat_100g'] = df_clean['saturated-fat_100g']
        
        # Fill missing values
        df_clean = df_clean.fillna('')
        
        # Prepare insert query
        insert_query = """
        INSERT INTO processed_food_data (
            product_name, brands, categories, energy_100g, proteins_100g,
            carbohydrates_100g, sugars_100g, fat_100g, saturated_fat_100g,
            fiber_100g, salt_100g, sodium_100g
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Insert data in batches
        batch_size = 1000
        total_batches = (len(df_clean) + batch_size - 1) // batch_size
        
        print(f"Inserting data in {total_batches} batches of {batch_size}...")
        
        for i in tqdm(range(0, len(df_clean), batch_size), desc="Inserting batches"):
            batch = df_clean.iloc[i:i + batch_size]
            
            batch_data = []
            for _, row in batch.iterrows():
                record = (
                    str(row.get('product_name', ''))[:1000],  # Limit length
                    str(row.get('brands', ''))[:500],
                    str(row.get('categories', ''))[:1000],
                    float(row.get('energy_100g', 0)) if pd.notna(row.get('energy_100g')) else None,
                    float(row.get('proteins_100g', 0)) if pd.notna(row.get('proteins_100g')) else None,
                    float(row.get('carbohydrates_100g', 0)) if pd.notna(row.get('carbohydrates_100g')) else None,
                    float(row.get('sugars_100g', 0)) if pd.notna(row.get('sugars_100g')) else None,
                    float(row.get('fat_100g', 0)) if pd.notna(row.get('fat_100g')) else None,
                    float(row.get('saturated_fat_100g', 0)) if pd.notna(row.get('saturated_fat_100g')) else None,
                    float(row.get('fiber_100g', 0)) if pd.notna(row.get('fiber_100g')) else None,
                    float(row.get('salt_100g', 0)) if pd.notna(row.get('salt_100g')) else None,
                    float(row.get('sodium_100g', 0)) if pd.notna(row.get('sodium_100g')) else None,
                )
                batch_data.append(record)
            
            try:
                cursor.executemany(insert_query, batch_data)
                connection.commit()
            except Error as e:
                print(f"Error inserting batch {i//batch_size + 1}: {e}")
                connection.rollback()
                raise
        
        # Verify insertion
        cursor.execute("SELECT COUNT(*) FROM processed_food_data")
        final_count = cursor.fetchone()[0]
        
        print(f"Loaded {final_count} records into MySQL")
        
        # Show sample data
        cursor.execute("""
            SELECT id, product_name, energy_100g, proteins_100g, fat_100g 
            FROM processed_food_data 
            LIMIT 5
        """)
        
        print("\nSample data in MySQL:")
        for row in cursor.fetchall():
            print(f"  ID: {row[0]}, Name: {row[1][:50]}..., Energy: {row[2]}, Protein: {row[3]}, Fat: {row[4]}")
        
        cursor.close()
        connection.close()
        
        return True
        
    except Exception as e:
        print(f"Error loading data: {e}")
        return False

def main():
    """Main function"""
    print("=" * 80)
    print("ITMO Big Data Lab 7: Data Loader")
    print("=" * 80)

    data_path = "/data/processed_food_data.parquet"
    
    if len(sys.argv) > 1:
        data_path = sys.argv[1]
    
    print(f"Loading processed food data for Lab 7 DataMart")
    print(f"Data source: {data_path}")
    
    success = load_processed_data(data_path)
    
    if success:
        print("\nData loading completed successfully!")
    else:
        print("\nData loading failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
