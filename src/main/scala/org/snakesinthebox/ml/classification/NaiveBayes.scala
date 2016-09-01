package org.snakesinthebox.ml.classification

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.snakesinthebox.preprocessing.Preprocessor
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created by brad on 8/30/16.
  */

trait NBData {

  var stopWordsBC: Broadcast[Set[String]]

  var totalWords: RDD[(String, Double)]

  var normalizer: RDD[(String, Double)]

  var catDocs: ListBuffer[RDD[(String, Double)]]

  var catDocsSmooth: ListBuffer[RDD[(String, Double)]]

  var totalDocsCount: Double

  var catFractions: ListBuffer[RDD[(String, Double)]]

  var priors: ListBuffer[Double]

  var categories: Array[String]
}

object NaiveBayes extends NBData with Serializable {

  /**
    *
    * @param trainData
    * @param catData
    * @param stopWords
    * @param a
    */
  def train(trainData: RDD[String], catData: RDD[String], stopWords: Broadcast[Set[String]], a: Array[String]): Unit = {
    stopWordsBC = stopWords
    categories = a
    val KVData = zip(catData, trainData)
    val catDocCount:ListBuffer[Double]=ListBuffer()
    for (cat <- categories) {
      val catData = categorize(KVData, cat)
      catDocCount.append(catData.count())
      totalDocsCount = totalDocsCount + catData.count()
      val cleanData = clean(catData.values)
      val cleanData2 = removeStopWords(cleanData)
      catDocs.append(wordCount(cleanData2))
    }
    totalWords = catDocs.reduce(_ ++ _)
    normalizer = totalWords.map(k => (k._1, 1.0))
    catDocsSmooth = catDocs.map(c => smooth(c, normalizer))
    val totalSmooth = smooth(totalWords, normalizer)
    for (cat <- catDocsSmooth) {
      catFractions.append(trainFractions(cat, totalSmooth))
    }
    for (count<-catDocCount){
      priors.append(prior(count,totalDocsCount))
    }
    priors.foreach(println)
  }

  /**
    *
    * @param testData
    * @param sc
    */
  def test(testData: RDD[String], sc: SparkContext) = {
    val stuffs: Array[String] = testData.collect()
    for (doc <- stuffs) {
      val docRDD = sc.parallelize(List(doc))
      println(coolNP(docRDD))
    }
  }

  /**
    *
    * @param doc
    * @return
    */
  def coolNP(doc: RDD[String]): String = {
    val tData = doc
      .flatMap(word => word.toString.split(" "))
      .filter(Preprocessor.removeNumbers)
      .map(Preprocessor.removeSpecials)
      .map(Preprocessor.removeForwardSlash)
      .map(Preprocessor.removePunctuation)
      .map(word => word.toLowerCase())
    val tClean = tData.mapPartitions {
      partition =>
        val stopWordsSet = stopWordsBC.value
        partition.filter(word => !stopWordsSet.contains(word))
    }
    val tWordCount = tClean
      .map(word => (word, 1.0))
      .reduceByKey(_ + _)
    val testDataSmooth = smooth(tWordCount, normalizer)

    val results: ListBuffer[Double] = ListBuffer()

    var i = 0
    for (cat <- catFractions) {
      val found = testDataSmooth.join(cat).reduceByKey((c: (Double, Double), t: (Double, Double)) => (c._1 + c._2, t._1 * t._2))
      results.append(found.first._2._2)
      i = i + 1
    }

    println("####################")
    results.foreach(println)
    println("####################")
    return ""

  }

  /**
    *
    * @param A
    * @param B
    * @return
    */
  def zip(A: RDD[String], B: RDD[String]): RDD[(String, String)] = {
    val catPrime = A.zipWithIndex().map(_.swap)
    val trainPrime = B.zipWithIndex().map(_.swap)
    catPrime.join(trainPrime).values
  }

  /**
    *
    * @param trainData
    * @param cat
    * @return
    */
  def categorize(trainData: RDD[(String, String)], cat: String): RDD[(String, String)] = {
    trainData
      .filter({ case (key, value) => key.contains(cat) })
  }

  /**
    * pass in the key value pars values
    *
    * @param data
    * @return
    */
  def clean(data: RDD[String]): RDD[String] = {
    data
      .flatMap(word => word.split(" "))
      .filter(Preprocessor.removeNumbers)
      .map(Preprocessor.removeSpecials)
      .map(Preprocessor.removeForwardSlash)
      .map(Preprocessor.removePunctuation)
      .map(word => word.toLowerCase())
  }

  /**
    *
    * @param filteredData
    * @return
    */
  def removeStopWords(filteredData: RDD[String]): RDD[String] = {
    filteredData.mapPartitions {
      partition =>
        val stopWordsSet = stopWordsBC.value
        partition.filter(word => !stopWordsSet.contains(word))
    }
  }

  /**
    *
    * @param words
    * @return
    */
  def wordCount(words: RDD[String]): RDD[(String, Double)] = {
    words
      .map(word => (word, 1.0))
      .reduceByKey(_ + _)
  }

  /**
    *
    * @param i
    * @param n
    * @return
    */
  def smooth(i: RDD[(String, Double)], n: RDD[(String, Double)]): RDD[(String, Double)] = {
    (i ++ n).reduceByKey(_ + _)
  }

  /**
    *
    * @param catSmooth
    * @param totalSmooth
    * @return
    */
  def trainFractions(catSmooth: RDD[(String, Double)], totalSmooth: RDD[(String, Double)]): RDD[(String, Double)] = {
    catSmooth.foreach(println)
    System.exit(0)
    catSmooth.join(totalSmooth).mapValues((t: (Double, Double)) => t._1 / t._2)
  }

  /**
    *
    * @param cat
    * @param total
    * @return
    */
  def prior(cat: Double, total: Double): Double = {
    cat / total
  }

  override var stopWordsBC: Broadcast[Set[String]] = _
  override var totalWords: RDD[(String, Double)] = _
  override var normalizer: RDD[(String, Double)] = _
  override var catDocs: ListBuffer[RDD[(String, Double)]] = ListBuffer()
  override var catDocsSmooth: ListBuffer[RDD[(String, Double)]] = ListBuffer()
  override var totalDocsCount: Double = _
  override var catFractions: ListBuffer[RDD[(String, Double)]] = ListBuffer()
  override var priors: ListBuffer[Double] = ListBuffer()
  override var categories: Array[String] = Array()
}
