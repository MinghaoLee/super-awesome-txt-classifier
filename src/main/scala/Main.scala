import com.typesafe.config.ConfigFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.snakesinthebox.ml.classification.NaiveBayes
import org.snakesinthebox.preprocessing.Preprocessor

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

    val trainData = sc.textFile(conf.getString("data.train.doc.path"))
    val testData = sc.textFile(conf.getString("data.test.doc.path"))
    val categories = sc.textFile(conf.getString("data.train.cat.path"))
    val stopWords = sc.textFile(conf.getString("data.stopwords.path"))

    val stopWordsSet = stopWords.collect.toSet
    val stopWordsBC: Broadcast[Set[String]] = sc.broadcast(stopWordsSet)

    val catPrime = categories.zipWithIndex().map(_.swap)
    val trainPrime = trainData.zipWithIndex().map(_.swap)

    val catData = catPrime.join(trainPrime).values

    val cDocs = catData
      .filter({ case (key, value) => key.contains("GCAT") })
    val cData: RDD[String] = cDocs
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


    val gClean = gData.mapPartitions {
      partition =>
        val stopWordsSet = stopWordsBC.value
        partition.filter(word => !stopWordsSet.contains(word))
    }
    val gWordCount = gClean
      .map(word => (word, 1.0))
      .reduceByKey(_ + _)


    val mClean = mData.mapPartitions {
      partition =>
        val stopWordsSet = stopWordsBC.value
        partition.filter(word => !stopWordsSet.contains(word))
    }
    val mWordCount = mClean
      .map(word => (word, 1.0))
      .reduceByKey(_ + _)

    val eClean = eData.mapPartitions {
      partition =>
        val stopWordsSet = stopWordsBC.value
        partition.filter(word => !stopWordsSet.contains(word))
    }
    val eWordCount = eClean
      .map(word => (word, 1.0))
      .reduceByKey(_ + _)

    val docTotal = (cWordCount ++ gWordCount ++ mWordCount ++ eWordCount)
      .reduceByKey(_ + _)

    val cFraction = cWordCount.join(docTotal).mapValues((t:(Double,Double))=>t._1/t._2)
    val gFraction = gWordCount.join(docTotal).mapValues((t:(Double,Double))=>t._1/t._2)
    val mFraction = mWordCount.join(docTotal).mapValues((t:(Double,Double))=>t._1/t._2)
    val eFraction = eWordCount.join(docTotal).mapValues((t:(Double,Double))=>t._1/t._2)

    val totalDocs:Double = cDocs.count()+gDocs.count()+mDocs.count()+eDocs.count()

    val stuffs:Array[String] = testData.collect()

    for(doc<-stuffs){
      println(coolNP(doc))
    }

    def coolNP(doc:String):String={
      val docRDD = sc.parallelize(List(doc))
      val tData = docRDD
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


      val cPrior = cDocs.count()/totalDocs
      val cFound = tWordCount.join(cFraction).reduceByKey((c:(Double,Double),t:(Double,Double))=>(c._1+c._2,t._1*t._2))
      val cConf = cFound.first._2._2*cPrior

      val gPrior = gDocs.count()/totalDocs
      val gFound = tWordCount.join(gFraction).reduceByKey((c:(Double,Double),t:(Double,Double))=>(c._1+c._2,t._1*t._2))
      val gConf = gFound.first._2._2*gPrior

      val mPrior = mDocs.count()/totalDocs
      val mFound = tWordCount.join(mFraction).reduceByKey((c:(Double,Double),t:(Double,Double))=>(c._1+c._2,t._1*t._2))
      val mConf = mFound.first._2._2*mPrior

      val ePrior = eDocs.count()/totalDocs
      val eFound = tWordCount.join(eFraction).reduceByKey((c:(Double,Double),t:(Double,Double))=>(c._1+c._2,t._1*t._2))
      val eConf = eFound.first._2._2*ePrior

/*      println("###################################")
      println(s"CCAT: $cConf")
      println(s"GCAT: $gConf")
      println(s"MCAT: $mConf")
      println(s"ECAT: $eConf")
      println("###################################")*/

      val confs:Map[Double, String] = Map(cConf->"CCAT",gConf->"GCAT",mConf->"MCAT",eConf->"ECAT")

      confs.maxBy(_._1)._2

    }
  }
}
