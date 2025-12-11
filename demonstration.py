"""
Spark Big Data Name Counter - Demonstration Script

This script demonstrates two approaches for counting names with a specific letter:
1. Local processing (fast, no dependencies)
2. Spark processing (scalable for big data)

For the Spark version to work properly, you need:
- Java JDK installed
- Spark installed and configured
- Proper environment variables set

The local version works immediately and demonstrates the same logic.
"""

def demonstrate_approaches():
    """Demonstrate both local and Spark approaches"""
    
    # Sample data
    sample_names = [
        "Alice", "Bob", "Charlie", "David", "Eve", 
        "Frank", "Grace", "Henry", "Ivy", "Jack",
        "Kate", "Liam", "Mia", "Noah", "Olivia"
    ]
    
    target = "a"
    
    print("=" * 50)
    print("SPARK BIG DATA NAME COUNTER DEMONSTRATION")
    print("=" * 50)
    
    print("\nSample Data:")
    print(f"Names: {sample_names}")
    print(f"Target letter: '{target}'")
    
    # Local approach (always works)
    print("\n" + "-" * 30)
    print("LOCAL APPROACH (Immediate Results)")
    print("-" * 30)
    
    from local_name_counter import count_names_with_letter, count_letter_occurrences
    
    count = count_names_with_letter(sample_names, target)
    print(f"Result: {count} names contain the letter '{target}'")
    
    total_occurrences = count_letter_occurrences(sample_names, target)
    print(f"Total occurrences: {total_occurrences}")
    
    # Explanation of Spark approach
    print("\n" + "-" * 30)
    print("SPARK APPROACH (For Big Data)")
    print("-" * 30)
    
    print("""
For processing large datasets with Spark, you would:

1. Use the Spark versions (name_counter.py or NameCounter.scala)
2. Ensure Spark is properly installed and configured
3. Submit the job to a Spark cluster

Example Spark commands:
   # Python
   spark-submit name_counter.py
   
   # Scala
   spark-submit --class NameCounter NameCounter.jar

Benefits of Spark approach:
- Scales to handle millions/billions of names
- Distributes processing across cluster nodes
- Handles fault tolerance automatically
- Optimizes execution plans

For both approaches, results can be saved to persistent storage:
- Local approach: Direct file I/O operations
- Spark approach: DataFrame write operations (CSV, Parquet, etc.)
""")
    
    print("=" * 50)
    print("DEMONSTRATION COMPLETE")
    print("=" * 50)

if __name__ == "__main__":
    demonstrate_approaches()