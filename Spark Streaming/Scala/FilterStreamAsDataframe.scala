package com.practice.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.json

/** The following SparkStreaming script accepts Kafka broker IP's and topic names as runtime arguments 
 *  in order to consume messages streaming from Kafka topics. The streaming JSON data is converted to 
 *  DataFrame and messages are filtered in realtime based on filter logic applied on DataFrame column.
 */
object FilterStreamAsDataframe {
   
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
    val sparkConf = new SparkConf().setAppName("StreamingDataFrameApp")
    
    // New streaming context
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    
    // Accept multiple topics and brokers
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    
    // Api call to connect to kafka topics
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    
    // DATAFRAME
    messages.foreachRDD {rdd =>
       
      // If data is present, continue
      If (rdd.count() > 0){
        
        // Create SQLContext and parse JSON
        val sqlContext = new SQLContext(sc)
        
        // Read data
        val messageTracker = sqlContext.jsonRDD(rdd.values)
        
        // Filter message stream based on logic applied on DataFrame column1
        messageTracker.filter(messageTracker("column1") > 6).show()
        
      }
      
    }
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  
  }

}
