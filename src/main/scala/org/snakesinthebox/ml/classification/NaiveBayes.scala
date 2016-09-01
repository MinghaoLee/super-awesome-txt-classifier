package org.snakesinthebox.ml.classification

import org.apache.spark.broadcast.Broadcast
import org.snakesinthebox.preprocessing.Preprocessor
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created by brad on 8/30/16.
  */

trait NBData{
  var stopWordsBC: Broadcast[Set[String]]

  var catData:ListBuffer[RDD[String]]

  var
}

object NaiveBayes extends NBData with Serializable {

  def train(trainData: RDD[(String, String)], categories: Array[String]):Unit ={
    categorize(trainData,categories)
    for(cat<-categories){
      removeStopWords()
    }
  }

  def setStopWords(stopWords:Broadcast[Set[String]]): Unit ={
    stopWordsBC=stopWords
  }

  def categorize(trainData: RDD[(String, String)], categories: Array[String]):Unit={
    for(cat<-categories) {
      val temp = trainData
        .filter({ case (key, value) => key.contains(cat) })
      val tempData = temp
        .values
        .flatMap(word => word.split(" "))
        .filter(Preprocessor.removeNumbers)
        .map(Preprocessor.removeSpecials)
        .map(Preprocessor.removeForwardSlash)
        .map(Preprocessor.removePunctuation)
        .map(word => word.toLowerCase())
      catData.append(tempData)
    }
  }

  def removeStopWords(filteredData:RDD[String]): RDD[String] ={
    filteredData.mapPartitions {
      partition =>
        val stopWordsSet = stopWordsBC.value
        partition.filter(word => !stopWordsSet.contains(word))
    }
  }

  def wordCount(words: RDD[String]): RDD[(String,Double)] ={
    words
      .map(word => (word, 1.0))
      .reduceByKey(_ + _)
  }

  override var catData: ListBuffer[RDD[String]] = _
  override var stopWordsBC: Broadcast[Set[String]] = _
}
