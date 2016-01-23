package com.capitalone.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka._

 //@author xoc201

object readwrite {
  def main(args: Array[String]){
    
  // Create new configuration
   val conf = new SparkConf().setAppName("ReadWrite Stream"))  
  
  // Create new streaming context object
  val ssc = new StreamingContext(conf, Seconds(60))
  
  // Port for Kafka broker and topic name here
  val kafkaParams = Map("broker" -> "localhost:9092")
 
  val topics = Set("sometopic")
 
  // If Key Value Pairs: Should return a stream of tuples formed from each Kafka message’s key and value
  val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
  
  //A Filter Use Case
  val filterLines = stream.filter{case (key, value) => value.length < 20}
    
  // Print the stream
  filterLines.print()
  
  //TO DO: LAND ON HDFS OR SEND BACK TO KAFKA
  }
  
}
