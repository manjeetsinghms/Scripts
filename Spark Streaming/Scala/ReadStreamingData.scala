package com.practice.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

/** The following SparkStreaming script accepts Kafka broker IP's and topic names as runtime arguments 
 *  in order to consume and print messages streaming from Kafka topics. 
 */
object ReadStreamingData {
   
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(s"""
        | Usage: Print StreamingMessages <brokers> <topics>
        |   <brokers> is a list of one or more Kafka brokers
        |   <topics> is a list of one or more kafka topics to consume from
      """.stripMargin)
      System.exit(1)
    }
    
    // Kafka Broker IP's and topic names here
    val Array(brokers, topics) = args
    
    // New spark context
    val sparkConf = new SparkConf().setAppName("StreamingApp")
    
    // New streaming context
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    
    // Accept multiple topics and brokers
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    
    // Api call to connect to kafka topics
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    
    // Print the DStream messages 
    messages.print()
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  
  }

}
