package com.snakesinthebox.preprocessing

/**
  * @author Brad Bazemore
  *
  *         =Overview=
  *         Will take two text files, the training data and the stop words.
  *         The stop words have to be converted to sets and then distributed out to the nodes to
  *         prevent redundant shuffling of the data.
  *
  * 1. Convert doc into one RDD with each word as an element
  * 2. Remove all numbers and words with numbers in them
  * 3. Remove the odd special words such as &quote;
  * 4. Remove forward slashes and replace with a space
  * 5. Remove punctuations
  * 6. Convert all words to lowercase
  * 7. Remove stop words
  */

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Driver object
  */
object Main {

  /**
    * Driver method
    *
    * @note This is for testing the preprocessing and will need to be moved elsewhere on deploy
    * @param args commandline argument to driver
    */
  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load()

    val sparkConf = new SparkConf().setAppName(conf.getString("spark.appName"))
    val sc = new SparkContext(sparkConf)

    val trainData = sc.textFile(conf.getString("data.train.path"))
    val stopWords = sc.textFile(conf.getString("data.stopwords.path"))

    val stopWordsSet = stopWords.collect.toSet
    val stopWordsBC = sc.broadcast(stopWordsSet)

    val processedTrainData = trainData
      .flatMap(word => word.split(" "))
      .filter(Preprocessor.removeNumbers)
      .map(Preprocessor.removeSpecials)
      .map(Preprocessor.removeForwardSlash)
      .map(Preprocessor.removePunctuation)
      .map(word => word.toLowerCase())

    val cleanTrainData = processedTrainData.mapPartitions {
      partition =>
        val stopWordsSet = stopWordsBC.value
        partition.filter(word => !stopWordsSet.contains(word))
    }

    cleanTrainData.take(100).foreach(println)
  }
}
