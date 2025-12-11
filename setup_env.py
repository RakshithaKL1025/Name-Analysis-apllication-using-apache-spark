import os
import sys

# Set environment variables for PySpark on Windows
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk-11.0.2'
os.environ['SPARK_HOME'] = 'C:\\spark'

# Add Spark to Python path
sys.path.append('C:\\spark\\python')
sys.path.append('C:\\spark\\python\\lib\\py4j-0.10.9-src.zip')

print("Environment variables set:")
print(f"JAVA_HOME: {os.environ.get('JAVA_HOME', 'Not set')}")
print(f"SPARK_HOME: {os.environ.get('SPARK_HOME', 'Not set')}")

# Check if required directories exist
if os.path.exists(os.environ.get('JAVA_HOME', '')):
    print("Java installation found")
else:
    print("Warning: Java installation not found at JAVA_HOME path")

if os.path.exists(os.environ.get('SPARK_HOME', '')):
    print("Spark installation found")
else:
    print("Warning: Spark installation not found at SPARK_HOME path")