from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, sum as spark_sum
import datetime

def count_names_with_letter(names_list, target_letter):
    """
    Count names containing a specific letter using PySpark
    
    Args:
        names_list (list): List of names to analyze
        target_letter (str): Letter to search for in names
    
    Returns:
        int: Count of names containing the target letter
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("NameLetterCounter") \
        .getOrCreate()
    
    try:
        # Create DataFrame from names list
        names_df = spark.createDataFrame([(name,) for name in names_list], ["name"])
        
        # Convert to lowercase for case-insensitive matching
        names_df = names_df.withColumn("name_lower", lower(col("name")))
        
        # Filter names containing the target letter
        filtered_names = names_df.filter(
            col("name_lower").contains(target_letter.lower())
        )
        
        # Count the names
        count = filtered_names.count()
        
        # Show the names containing the letter
        print(f"Names containing '{target_letter}':")
        filtered_names.select("name").show(truncate=False)
        
        return count
    finally:
        # Stop the Spark session
        spark.stop()

def count_names_with_letter_advanced(names_list, target_letter):
    """
    Advanced version that counts occurrences of the letter in each name
    
    Args:
        names_list (list): List of names to analyze
        target_letter (str): Letter to search for in names
    
    Returns:
        tuple: (count of names with letter, total occurrences of letter)
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("AdvancedNameLetterCounter") \
        .getOrCreate()
    
    try:
        # Create DataFrame
        names_df = spark.createDataFrame([(name,) for name in names_list], ["name"])
        
        # Convert to lowercase
        names_df = names_df.withColumn("name_lower", lower(col("name")))
        
        # Filter names containing the target letter
        filtered_names = names_df.filter(
            col("name_lower").contains(target_letter.lower())
        )
        
        # Count names with the letter
        name_count = filtered_names.count()
        
        # Count total occurrences of the letter across all names
        # This uses a more complex approach with RDD transformations
        names_rdd = names_df.rdd.map(lambda row: row.name_lower)
        letter_occurrences = names_rdd.flatMap(
            lambda name: [1 for char in name if char == target_letter.lower()]
        ).count()
        
        print(f"Names containing '{target_letter}':")
        filtered_names.select("name").show(truncate=False)
        print(f"Total occurrences of '{target_letter}' across all names: {letter_occurrences}")
        
        return name_count, letter_occurrences
    finally:
        spark.stop()

def save_spark_results_to_file(names_list, target_letter, filename="spark_name_count_results.csv"):
    """
    Save Spark results to a CSV file
    
    Args:
        names_list (list): List of names to analyze
        target_letter (str): Letter to search for in names
        filename (str): Name of CSV file to save results to
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SaveNameLetterCounterResults") \
        .getOrCreate()
    
    try:
        # Create DataFrame from names list
        names_df = spark.createDataFrame([(name,) for name in names_list], ["name"])
        
        # Convert to lowercase for case-insensitive matching
        names_df = names_df.withColumn("name_lower", lower(col("name")))
        
        # Add a column indicating if name contains the target letter
        from pyspark.sql.functions import when
        result_df = names_df.withColumn(
            "contains_letter",
            when(col("name_lower").contains(target_letter.lower()), True).otherwise(False)
        )
        
        # Add a column with count of occurrences
        from pyspark.sql.functions import udf
        from pyspark.sql.types import IntegerType
        
        def count_occurrences(name, letter):
            return name.lower().count(letter.lower())
        
        count_udf = udf(count_occurrences, IntegerType())
        result_df = result_df.withColumn(
            "letter_count",
            count_udf(col("name"), spark.sparkContext.broadcast(target_letter).value)
        )
        
        # Save to CSV file
        result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(filename)
        
        print(f"Spark results saved to {filename} directory")
        return filename
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
    
    print("\n=== Advanced Count ===")
    name_count, letter_count = count_names_with_letter_advanced(sample_names, target)
    print(f"Names with '{target}': {name_count}")
    print(f"Total occurrences of '{target}': {letter_count}")
    
    print("\n=== Saving Spark Results to File ===")
    filename = save_spark_results_to_file(sample_names, target)
    print(f"Spark output has been saved to: {filename}")