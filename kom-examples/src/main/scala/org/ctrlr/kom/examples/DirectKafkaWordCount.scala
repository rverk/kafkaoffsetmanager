package org.ctrlr.kom.examples

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{OffsetRange, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.ctrlr.kom.core.KafkaOffsetManager
import org.ctrlr.kom.daoimplementation.Hbase1OffsetStore

import collection.JavaConversions._
import scala.collection.JavaConverters._


/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: DirectKafkaWordCount <brokers> <topics>
  *   <brokers> is a list of one or more Kafka brokers
  *   <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
  *    topic1,topic2
  */
object DirectKafkaWordCount {
   def main(args: Array[String]) {
     if (args.length < 2) {
       System.err.println(s"""
         |Usage: DirectKafkaWordCount <brokers> <topics>
         |  <brokers> is a list of one or more Kafka brokers
         |  <topics> is a list of one or more kafka topics to consume from
         |
         """.stripMargin)
       System.exit(1)
     }

     val Array(brokers, topics) = args

     /** Setup Hbase configuration. 1.4 will support SPARK-6918 */
     val hbaseConf = HBaseConfiguration.create()

     val dao = new Hbase1OffsetStore.Builder().setHbaseConfiguration(hbaseConf)
       .setOffsetTable("kafkaoffsettable").build()

     val osm = new KafkaOffsetManager.Builder()
       .setOffsetManager(dao)
       .setKafkaBrokerList("localhost:9092")
       .setGroupID("testGroupID")
       .setTopic("kafkaTestTopic").build()

     /** Get offsets or start at beginning. getLatestOffsets is also an option. */
     var offsets = osm.getOffsets
     if (offsets.isEmpty) { offsets = osm.getEarliestOffsets }
     val offsetMap2 = scala.collection.mutable.Map[TopicAndPartition, Long]()
     offsets.foreach(kv => offsetMap2.put(kv._1, kv._2))

     // Create context with 2 second batch interval
     val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
     val ssc = new StreamingContext(sparkConf, Seconds(2))

     // Create direct kafka stream with brokers and topics
     val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

     // Hold a reference to the current offset ranges, so it can be used downstream
     var offsetRanges = Array[OffsetRange]()

     val lines = KafkaUtils.createDirectStream[ Array[Byte] , Array[Byte] , DefaultDecoder , DefaultDecoder, Array[Byte]] (ssc,
       kafkaParams, offsetMap2.toMap, (mmd: MessageAndMetadata[Array[Byte],Array[Byte]]) => mmd.message())

     lines.transform { rdd =>
       offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
       rdd
     }
     .flatMap(new String(_).split(" "))
     .map(x => (x, 1L)).reduceByKey(_ + _)
     .foreachRDD { rdd =>

       for (o <- offsetRanges) {
         if (o.fromOffset < o.untilOffset) {
           val offsets: Map[TopicAndPartition, Long] = Map()
           offsets.put(new TopicAndPartition(o.topic, o.partition), o.untilOffset)
           //osm.setOffsets(offsets.asJava)
         }
       }
       rdd
     }


     // Start the computation
     ssc.start()
     ssc.awaitTermination()
   }
 }
