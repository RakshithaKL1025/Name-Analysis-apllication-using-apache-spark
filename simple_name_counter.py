import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

def count_names_with_letter(names_list, target_letter):
    """
    Count names containing a specific letter using PySpark
    """
    # Set environment variable to handle Windows temp directory issue
    os.environ['HADOOP_HOME'] = 'C:\\hadoop'  # This can point to any directory
    
    # Initialize Spark session with proper configuration for Windows
    spark = SparkSession.builder \
        .appName("NameLetterCounter") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .getOrCreate()
    
    # Reduce logging verbosity
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
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
    finally:
        # Stop the Spark session
        spark.stop()

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
    count = count_names_with_letter(sample_names, target)
    print(f"Number of names containing '{target}': {count}")