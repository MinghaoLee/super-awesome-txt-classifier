package org.snakesinthebox.LR

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.snakesinthebox.preprocessing.Preprocessor

/**
  * Created by Brent on 8/30/2016.
  */
object LogisticReg {


  val conf = ConfigFactory.load()

  val sparkConf = new SparkConf().setAppName(conf.getString("spark.appName"))
  val sc = new SparkContext(sparkConf)

  val trainData = sc.textFile(conf.getString("data.train.doc.path"))
  val testData = sc.textFile(conf.getString("data.test.doc.path"))
  val categories = sc.textFile(conf.getString("data.train.cat.path"))
  val stopWords = sc.textFile(conf.getString("data.stopwords.path"))

  val stopWordsSet = stopWords.collect.toSet
  val stopWordsBC = sc.broadcast(stopWordsSet)

  val catData = categories.zip(trainData)

  val cDocs = catData
    .filter({ case (key, value) => key.contains("CCAT") })
  val cData = cDocs
    .values
    .flatMap(word => word.split(" "))
    .filter(Preprocessor.removeNumbers)
    .map(Preprocessor.removeSpecials)
    .map(Preprocessor.removeForwardSlash)
    .map(Preprocessor.removePunctuation)
    .map(word => word.toLowerCase())

  val gDocs = catData
    .filter({ case (key, value) => key.contains("GCAT") })
  val gData = gDocs
    .values
    .flatMap(word => word.split(" "))
    .filter(Preprocessor.removeNumbers)
    .map(Preprocessor.removeSpecials)
    .map(Preprocessor.removeForwardSlash)
    .map(Preprocessor.removePunctuation)
    .map(word => word.toLowerCase())

  val mDocs = catData
    .filter({ case (key, value) => key.contains("MCAT") })
  val mData = mDocs
    .values
    .flatMap(word => word.split(" "))
    .filter(Preprocessor.removeNumbers)
    .map(Preprocessor.removeSpecials)
    .map(Preprocessor.removeForwardSlash)
    .map(Preprocessor.removePunctuation)
    .map(word => word.toLowerCase())

  val eDocs = catData
    .filter({ case (key, value) => key.contains("ECAT") })
  val eData = eDocs
    .values
    .flatMap(word => word.split(" "))
    .filter(Preprocessor.removeNumbers)
    .map(Preprocessor.removeSpecials)
    .map(Preprocessor.removeForwardSlash)
    .map(Preprocessor.removePunctuation)
    .map(word => word.toLowerCase())

  val cClean = cData.mapPartitions {
    partition =>
      val stopWordsSet = stopWordsBC.value
      partition.filter(word => !stopWordsSet.contains(word))
  }
  val cWordCount = cClean
    .map(word => (word, 1.0))
    .reduceByKey(_ + _)

  val cWeights = cWordCount.mapValues((value) => 1.0)

  val gClean = gData.mapPartitions {
    partition =>
      val stopWordsSet = stopWordsBC.value
      partition.filter(word => !stopWordsSet.contains(word))
  }
  val gWordCount = gClean
    .map(word => (word, 1.0))
    .reduceByKey(_ + _)

  val gWeights = cWordCount.mapValues((value) => 1.0)

  val mClean = mData.mapPartitions {
    partition =>
      val stopWordsSet = stopWordsBC.value
      partition.filter(word => !stopWordsSet.contains(word))
  }
  val mWordCount = mClean
    .map(word => (word, 1.0))
    .reduceByKey(_ + _)

  val mWeights = cWordCount.mapValues((value) => 1.0)

  val eClean = eData.mapPartitions {
    partition =>
      val stopWordsSet = stopWordsBC.value
      partition.filter(word => !stopWordsSet.contains(word))
  }
  val eWordCount = eClean
    .map(word => (word, 1.0))
    .reduceByKey(_ + _)

  val eWeights = cWordCount.mapValues((value) => 1.0)



}
