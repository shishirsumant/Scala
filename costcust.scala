package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object costcust {
  
    def parseLine(line:String)= {
    val fields = line.split(",")
    val custID = fields(0)
    val cost = fields(2)
    (custID.toInt, cost.toFloat)
  }
  
   def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "costcust")
    
    val input = sc.textFile("../customer-orders.csv")
    val parsedLines = input.map(parseLine)
    val totalsByCust = parsedLines.reduceByKey( (x,y) => x + y)

    val results = totalsByCust.collect()
    
    // Sort and print the final results.
    results.sorted.foreach(println)
        
   }
}