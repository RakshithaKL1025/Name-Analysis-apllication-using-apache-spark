# Spark Name Letter Counter

This project demonstrates how to count names containing a specific letter using Apache Spark with both Python (PySpark) and Scala implementations, plus a web interface for easy interaction.

## Implementations

### Python Implementation (PySpark)
- File: `name_counter.py`
- Provides two functions:
  1. `count_names_with_letter()` - Counts how many names contain a specific letter
  2. `count_names_with_letter_advanced()` - Counts both names with the letter and total occurrences

### Scala Implementation
- File: `NameCounter.scala`
- Provides two functions:
  1. `countNamesWithLetter()` - Counts how many names contain a specific letter
  2. `countNamesWithLetterAdvanced()` - Counts both names with the letter and total occurrences

### Web Interface
- File: `web_app.py`
- Provides a user-friendly web interface for interacting with the name counting functionality
- Runs on http://localhost:5000

## How to Run

### Command Line Versions

#### Python Version
```bash
# Make sure you have PySpark installed
pip install pyspark

# Run the script
python name_counter.py
```

#### Scala Version
```bash
# Compile and package the Scala code (requires sbt)
sbt package

# Submit to Spark (adjust paths as needed)
spark-submit --class NameCounter target/scala-2.12/name-counter_2.12-1.0.jar
```

### Web Interface

#### Windows
Double-click `start_web_app.bat` or run from command prompt:
```cmd
start_web_app.bat
```

#### Other Platforms
```bash
# Install dependencies
pip install -r requirements.txt

# Run the web application
python web_app.py
```

Then open your browser to http://localhost:5000

## Features

- Case-insensitive matching
- Shows which names contain the target letter
- Handles both basic counting and advanced occurrence counting
- Properly manages Spark sessions
- Clean shutdown of Spark resources
- Web interface with intuitive UI
- Results can be saved to files
- Downloadable result files

## Sample Output

The programs will output something like:
```
=== Basic Count ===
Names containing 'a':
+-------+
|name   |
+-------+
|Charlie|
|David  |
|Kate   |
|Liam   |
|Olivia |
+-------+

Number of names containing 'a': 5
```

## Customization

To count names with a different letter, simply change the `target` variable in the main section of either implementation.

For processing your own data, replace the `sample_names` list/array with your actual data source.

For the web interface, you can enter your own names and target letter directly in the browser.