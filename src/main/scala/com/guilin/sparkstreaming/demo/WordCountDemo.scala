package com.guilin.sparkstreaming.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

/**
 * Created by hadoop on 2016/2/21.
 */
object WordCountDemo {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("WordCountDemo").setMaster("local[2]")
    val sc = new StreamingContext(sparkConf, Seconds(20))
    val dir = WordCountDemo.getClass.getClassLoader.getResource("").getPath
    println("dir:" + dir)
    val lines = sc.textFileStream(dir)
    val words = lines.flatMap(_.split(" +"))
    val wordCount = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCount.print()
    sc.start()
    sc.awaitTermination()
  }

}
