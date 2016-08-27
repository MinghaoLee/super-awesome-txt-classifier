/**
  * Created by Brad Bazemore on 8/19/16.
  */

import org.apache.spark._

/**
  * Driver object
  */
object Main {

  /**
    * Driver method
    *
    * @param args commandline argument to driver
    */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("preprocessor")
    val sc = new SparkContext(conf)

    val trainData = sc.textFile("/home/brad/Documents/InteliJProjects/super-awesome-txt-classifier/X_train_vsmall.txt")
    val stopWords = sc.textFile("/home/brad/Documents/InteliJProjects/super-awesome-txt-classifier/stopwords.txt")

    val stopWordsSet = stopWords.collect.toSet
    val stopWordsBC = sc.broadcast(stopWordsSet)

    val processedTrainData = trainData
      .flatMap(word => word.split(" "))
      .filter(PreprocessFunctions.removeNumbers)
      .map(PreprocessFunctions.removeSpecials)
      .map(PreprocessFunctions.removeForwardSlash)
      .map(PreprocessFunctions.removePeriod)
      .map(word=>word.toLowerCase())

    val cleanTrainData = processedTrainData.mapPartitions {
      partition =>
        val stopWordsSet = stopWordsBC.value
        partition.filter(word => !stopWordsSet.contains(word))
    }

    cleanTrainData.take(100).foreach(println)
  }
}

/**
  * All functions related to preprocessing of data
  */
object PreprocessFunctions {

  /**
    * Function to be used on filters to remove all numbers and
    * strings with numbers
    *
    * @param word instance of string from filter
    * @return
    */
  def removeNumbers(word: String): Boolean = {
    word.matches("[^0-9]*")
  }

  /**
    * Removes the substring of &[a-z-A-Z]*;
    *
    * @param word instance of string from map
    * @return
    */
  def removeSpecials(word: String): String = {
    word.replaceAll("&[a-zA-Z]*;", "")
  }

  /**
    * Removes any forward slash and replaces it with a space
    *
    * @param word instance of string from map
    * @return
    */
  def removeForwardSlash(word: String): String = {
    word.replaceAll("\\/", " ")
  }

  /**
    * Removes punctuation from string
    *
    * @param word instance of string from map
    * @return
    */
  def removePeriod(word: String): String = {
    word.replaceAll("\\p{Punct}", "")
  }

}