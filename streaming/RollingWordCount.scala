/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.streaming
import org.apache.spark.SparkContext._

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 *
 * Usage: NetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.NetworkWordCount localhost 9999`
 */
object RollingWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: RollingWordCount <hostname> <port>")
      System.exit(1)
    }

   StreamingExamples.setStreamingLogLevels()

    
    val ssc = new StreamingContext("local[2]","RollingWordCount", Seconds(3))
    // ssc.checkpoint("/Users/prsachde/Desktop/ckpt")
    
    val rawlines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)

  
    rawlines.foreachRDD(rdd => println("\n Total processed %d".format(rdd.count())))
    val lines = rawlines.filter(_.split("\\|").length > 1)
    // val batchedlines = lines.window(Seconds(3),Seconds(1))
    // val refmap = batchedlines.map(line => (line.split("\\|")(0),1))     
    // val count = refmap.reduceByKey(_ + _)
    // val revcount = count.map{case (topic,count) => (count,topic)}
    // val sortedcount = revcount.transform(rdd => rdd.sortByKey(false))
    
    val refmap = lines.map(line => (line.split("\\|")(0),1))     


    val sortedcount = refmap.reduceByKeyAndWindow(_ + _, Seconds(9))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))

    
    
    
    
    sortedcount.foreachRDD(rdd => {
      val topList = rdd.take(5)
      println("\nTrending referers in last second (%s total):".format(rdd.count()))
      topList.foreach{case (count, ref) => println("%s (%s counts)".format(ref, count))}
    })
     
    val slidemap = lines.map(line => (line.split("\\|")(1),1))     
    val sortedcountslide = slidemap.reduceByKeyAndWindow(_ + _, Seconds(9))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))

    
    
    
    
    sortedcountslide.foreachRDD(rdd => {
      val topList = rdd.take(5)
      println("\nTrending slides in last second (%s total):".format(rdd.count()))
      topList.foreach{case (count, ref) => println("%s (%s counts)".format(ref, count))}
    })
    

    ssc.start()
//    wordCounts.print()
    ssc.awaitTermination()
  }
}
