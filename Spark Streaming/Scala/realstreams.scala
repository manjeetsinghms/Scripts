package com.capitalone.sparkstreaming

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.streaming.{ Time, StreamingContext, Seconds, Duration }
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.json
import org.json4s._
import org.json4s.native.JsonParser
import org.json4s.native.JsonMethods._
import org.apache.spark.sql.SQLContext

/* The following SparkStreaming script accepts Kafka broker IP's and topic names as runtime arguments in order to consume messages streaming from Kafka topics. The streaming JSON data is check-pointed on HDFS and converted to DataFrame. Messages are filtered in realtime based on business logic applied on DataFrame column.
*/

object realstreams {

  // 30 minute window and 1 sec slide
  //val WINDOW_LENGTH = new Duration(Seconds(2))

  //val SLIDE_INTERVAL = new Duration(100 * 1000)

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println(s"""
        | Usage: StreamingApp <brokers> <topics>
        |   <brokers> is a list of one or more Kafka brokers
        |   <topics> is a list of one or more kafka topics to consume from
      """.stripMargin)

      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("StreamingApp")

    // New spark context
    val sc = new SparkContext(sparkConf)

    // New sql context
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    
   
    // Brokers IP's and topic names

    val Array(brokers, topics) = args     //, checkpointDir) = args

    val topicsSet = topics.split(",").toSet

     // <checkpoint-directory> directory to HDFS-compatible file system 
    val checkpointDir: String = "hdfs://nameservice1/rwa/card/trsn/DATESETDIR"
    
    // Output directory to HDFS-compatible file system 
    val outputHdfs: String = "hdfs://nameservice1/src/ts2/trxn/DATASETDIR"
    
    
    // Function to create and setup a new StreamingContext
   def functionToCreateContext(): StreamingContext = {

      // New streaming context
      val ssc = new StreamingContext(sc, Seconds(1))
           
      //ssc.checkpoint("hdfs://nameservice1/devl/rwa/card/fdcm/caselog_npi_keyd_stg")   
      ssc.checkpoint(checkpointDir)

      ssc
    }
        
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")

    // Get StreamingContext from checkpoint data or create a new one
    val context = StreamingContext.getOrCreate(checkpointDir, functionToCreateContext _)
     
    //val ssc = new StreamingContext(sc, Seconds(1))
   
    // Read messages
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](context, kafkaParams, topicsSet)
         
     //val stream_values = messages.map{ case (key,value) => value}
    
    messages.foreachRDD { rdd =>
    val stream_values = rdd.map{ case (key,value) => value}
    stream_values.saveAsTextFile(outputHdfs)
    
    }
  
  val json_keyDefinition = stream_values.transform(rdd => {
          rdd.map( record => record.split(",")).map(fields => (fields(5), fields(22), fields(41), fields(61), fields(75)))
                   }).print()

    val stream_values = messages.map{ case (key,value) => value}.filter( value => value.contains("merchantName"))
       
        stream_values.print()
    
   }
    
    messages.foreachRDD { rdd =>

      //If data is present, continue
      if (rdd.count() > 0) {

        //Create SQLContect and parse JSON
        val sqlContext = new SQLContext(sc)
        
        //Requirement 1: Read card present transaction
        val trackingEvents = sqlContext.jsonRDD(rdd.values)
        
        //Requirement 2: Transaction filter > 5 
        //trackingEvents.show()
        
        trackingEvents.rdd.saveAsTextFile(outputHdfs)
        }
                
        trackingEvents.show()
      }
   
     // Apply window and collect messages
       val windowDStream = messages.window(WINDOW_LENGTH, SLIDE_INTERVAL)
    
    
    // Pick messages in the window and convert to RDD and to DataFrame
    windowDStream.foreachRDD { (rdd, time: Time) =>

      // If data is present, continue
      if (rdd.count() > 0) {

        // Create SQLContect and parse JSON
        val sqlContext = new SQLContext(sc)

        // Requirement 1: Read card present transaction
        val trackingEvents = sqlContext.jsonRDD(rdd.values)

        trackingEvents.registerTempTable("Records")
        
        trackingEvents.show()//rdd.saveAsTextFile("hdfs://devl/src/ts2/trxn/ts2_authzn_post_apprl")
        
         // val passedFields = sqlContext.sql(
         "SELECT city, name FROM Records Where currencyCode IN (100002) GROUP BY city, name")

        
        
        // CREATE WINDOW IF VISA CARD IS PRESENT
        val passedFields = sqlContext.sql(
          "SELECT CARDNUMBER, MERCHANTNAME, MERCHANTCITY, count(*) as count FROM Records Where contains(Source, 'B OR P') AND CRDHLDRVERIMETHD IN (1,2,3) AND contains(CNPIND, 'N') GROUP BY CARDNUMBER, MERCHANTNAME, MERCHANTCITY")

        passedFields.filter(passedFields("count") > 1).show() //This should go to KAFKA TOPIC       
             
      }
    }


    context.start()
    context.awaitTermination()

  }
}
    
