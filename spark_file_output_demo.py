"""
Demonstration of how Spark results can be saved to files
"""

def explain_spark_file_output():
    """Explain how Spark can save results to files"""
    
    print("=" * 60)
    print("SPARK FILE OUTPUT DEMONSTRATION")
    print("=" * 60)
    
    print("""
In Spark, there are several ways to save output to persistent storage:

1. DATAFRAME WRITE OPERATIONS:
   # Save as CSV
   df.coalesce(1).write.mode("overwrite").option("header", "true").csv("output.csv")
   
   # Save as Parquet (more efficient for big data)
   df.write.mode("overwrite").parquet("output.parquet")
   
   # Save as JSON
   df.write.mode("overwrite").json("output.json")

2. TEXT FILE OUTPUT:
   # Save as text file
   rdd.saveAsTextFile("output_txt_directory")

3. CUSTOM FORMATTING:
   # Format and save results
   results_df = df.select(
       col("name"),
       col("contains_letter"),
       col("letter_count")
   )
   results_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("detailed_results")

DIRECTORY STRUCTURE:
When saving Spark DataFrames, the output is typically saved as a directory
containing part files. For example:
- spark_output/
  - part-00000.csv
  - part-00001.csv
  - _SUCCESS
  
To get a single file, use coalesce(1) before writing.

EXAMPLE CODE SNIPPET:
----------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when

spark = SparkSession.builder.appName("FileOutput").getOrCreate()

# Your data processing here...
# df = spark.createDataFrame(...)

# Save results
df.coalesce(1).write.mode("overwrite").option("header", "true").csv("results_directory")

spark.stop()
----------------------------------------

FILE LOCATIONS:
- Relative path: Results saved in the current working directory
- Absolute path: Results saved at specified location (e.g., "C:/data/results")
- Network path: Results can be saved to shared/network drives if accessible

BEST PRACTICES:
1. Use Parquet format for large datasets (columnar storage, compression)
2. Use coalesce(1) sparingly as it can be a bottleneck for large data
3. Consider partitioning for very large datasets
4. Always specify appropriate write modes (overwrite, append, errorIfExists, ignore)
""")


if __name__ == "__main__":
    explain_spark_file_output()
    
    print("\n" + "=" * 60)
    print("RELATED FILES IN YOUR PROJECT:")
    print("=" * 60)
    print("1. name_count_results.txt - Local Python results (plain text)")
    print("2. spark_name_count_results.csv - Directory with Spark CSV output (when Spark runs successfully)")
    print("3. All other .py files contain functions that can be modified to save results")
    
    print("\nTo save results in your own Spark applications:")
    print("- Modify the write operations in the existing Spark code")
    print("- Specify your desired output path and format")
    print("- Consider the size of your data when choosing output strategies")