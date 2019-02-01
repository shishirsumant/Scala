package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

/** Find the minimum temperature by weather station */
object MaxPrecip {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val precip = fields(3)
    (stationID, entryType, precip)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxPrecip")
    
    // Read each line of input data
    val lines = sc.textFile("../1800.csv")
    
    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)
    
    // Filter out all but TMIN entries
    val Maxprcp = parsedLines.filter(x => x._2 == "PRCP")
    
    // Convert to (stationID, temperature)
    val stationprcp = Maxprcp.map(x => (x._1, x._3.toFloat))
    
    // Reduce by stationID retaining the minimum temperature found
    val maxprcpByStation = stationprcp.reduceByKey( (x,y) => max(x,y))
    
    // Collect, format, and print the results
    val results = maxprcpByStation.collect()
    
    for (result <- results.sorted) {
       val station = result._1
       val prcp = result._2
       val formattedprcp = f"$prcp%.2f cm"
       println(s"$station maximum precipitation: $formattedprcp") 
    }
      
  }
}