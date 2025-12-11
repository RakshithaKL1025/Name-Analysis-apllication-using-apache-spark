import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NameCounter {
  
  def countNamesWithLetter(namesList: List[String], targetLetter: String): Long = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("NameLetterCounter")
      .master("local[*]") // Use local mode for simplicity
      .getOrCreate()
    
    try {
      import spark.implicits._
      
      // Create DataFrame from names list
      val namesDF = namesList.toDF("name")
      
      // Convert to lowercase for case-insensitive matching
      val namesWithLower = namesDF.withColumn("name_lower", lower($"name"))
      
      // Filter names containing the target letter
      val filteredNames = namesWithLower.filter($"name_lower".contains(targetLetter.toLowerCase))
      
      // Show the names containing the letter
      println(s"Names containing '$targetLetter':")
      filteredNames.select("name").show(false)
      
      // Count and return the names
      val count = filteredNames.count()
      count
    } finally {
      // Stop the Spark session
      spark.stop()
    }
  }
  
  def countNamesWithLetterAdvanced(namesList: List[String], targetLetter: String): (Long, Long) = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("AdvancedNameLetterCounter")
      .master("local[*]")
      .getOrCreate()
    
    try {
      import spark.implicits._
      
      // Create DataFrame
      val namesDF = namesList.toDF("name")
      
      // Convert to lowercase
      val namesWithLower = namesDF.withColumn("name_lower", lower($"name"))
      
      // Filter names containing the target letter
      val filteredNames = namesWithLower.filter($"name_lower".contains(targetLetter.toLowerCase))
      
      // Count names with the letter
      val nameCount = filteredNames.count()
      
      // Count total occurrences of the letter across all names
      // Using RDD transformations for more complex counting
      val namesRDD = namesDF.rdd.map(row => row.getAs[String]("name").toLowerCase)
      val letterOccurrences = namesRDD.flatMap(name => 
        name.toCharArray.map(_.toString)
      ).filter(_ == targetLetter.toLowerCase).count()
      
      println(s"Names containing '$targetLetter':")
      filteredNames.select("name").show(false)
      println(s"Total occurrences of '$targetLetter' across all names: $letterOccurrences")
      
      (nameCount, letterOccurrences)
    } finally {
      spark.stop()
    }
  }
  
  def main(args: Array[String]): Unit = {
    // Sample data
    val sampleNames = List(
      "Alice", "Bob", "Charlie", "David", "Eve",
      "Frank", "Grace", "Henry", "Ivy", "Jack",
      "Kate", "Liam", "Mia", "Noah", "Olivia"
    )
    
    val target = "a"
    
    println("=== Basic Count ===")
    val count = countNamesWithLetter(sampleNames, target)
    println(s"Number of names containing '$target': $count")
    
    println("\n=== Advanced Count ===")
    val (nameCount, letterCount) = countNamesWithLetterAdvanced(sampleNames, target)
    println(s"Names with '$target': $nameCount")
    println(s"Total occurrences of '$target': $letterCount")
  }
}