package com.knoldus

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by akash on 19/7/16.
  */
object GlobalData {

  val sparkConf = new SparkConf().setMaster("local").setAppName("Apache Spark")
  val sparkContext = new SparkContext(sparkConf)
}

class PageCount {

  import GlobalData._

  def getRDD: RDD[String] = {

    sparkContext.textFile("/home/knoldus/Desktop/pagecount")
  }

  def getTenRecords(dataRDD: RDD[String]): List[String] = {

    dataRDD.take(10).toList
  }

  def getTotalCount(dataRDD: RDD[String]): Long = {

    dataRDD.count()
  }

  def getEnglishPagesRDD(dataRDD: RDD[String]): RDD[String] = {

    dataRDD.filter(line => line.split(" ")(0).contains("en"))
  }

  def getEnglishPageCount(dataRDD: RDD[String]): Long = {

    dataRDD.count()
  }

  def getPageRequested(dataRDD: RDD[String]): Long = {

    dataRDD.filter(line => line.split(" ")(2).toLong > 200000L).count()
  }

}

object PageCount extends App {

  val obj = new PageCount
  val dataRDD = obj.getRDD
  val englishRDD = obj.getEnglishPagesRDD(dataRDD)

  println("Ten Records are " + obj.getTenRecords(dataRDD))
  println("Total Counts are " + obj.getTotalCount(dataRDD))
  println("English Pages are " + englishRDD.take(10).toList)
  println("English Page Count " + obj.getEnglishPageCount(englishRDD))
  println("page Requested More than 200000 times " + obj.getPageRequested(dataRDD))
}
