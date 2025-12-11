from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
import os
import tempfile

def setup_spark():
    """Setup Spark with proper configuration for Windows"""
    # Create a temporary directory for Hadoop if it doesn't exist
    hadoop_home = os.path.join(tempfile.gettempdir(), "hadoop")
    if not os.path.exists(hadoop_home):
        os.makedirs(hadoop_home)
    
    # Set environment variables
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['PYSPARK_PYTHON'] = 'python'
    
    return hadoop_home

def count_names_with_letter(names_list, target_letter):
    """
    Count names containing a specific letter using PySpark
    """
    # Setup Spark environment
    hadoop_home = setup_spark()
    print(f"Using HADOOP_HOME: {hadoop_home}")
    
    # Initialize Spark session with proper configuration for Windows
    spark = SparkSession.builder \
        .appName("NameLetterCounter") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    
    # Reduce logging verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("Spark session created successfully!")
        
        # Create DataFrame from names list
        names_df = spark.createDataFrame([(name,) for name in names_list], ["name"])
        
        # Convert to lowercase for case-insensitive matching
        names_df = names_df.withColumn("name_lower", lower(col("name")))
        
        # Filter names containing the target letter
        filtered_names = names_df.filter(
            col("name_lower").contains(target_letter.lower())
        )
        
        # Collect results to driver for display
        matching_names = filtered_names.collect()
        
        # Print the names containing the letter
        print(f"Names containing '{target_letter}':")
        for row in matching_names:
            print(f"  {row['name']}")
        
        # Count the names
        count = filtered_names.count()
        
        return count
    except Exception as e:
        print(f"Error occurred: {e}")
        raise
    finally:
        # Stop the Spark session
        spark.stop()
        print("Spark session stopped.")

# Example usage
if __name__ == "__main__":
    # Sample data
    sample_names = [
        "Alice", "Bob", "Charlie", "David", "Eve", 
        "Frank", "Grace", "Henry", "Ivy", "Jack",
        "Kate", "Liam", "Mia", "Noah", "Olivia"
    ]
    
    target = "a"
    
    print("=== Basic Count ===")
    try:
        count = count_names_with_letter(sample_names, target)
        print(f"Number of names containing '{target}': {count}")
    except Exception as e:
        print(f"Failed to run the script: {e}")