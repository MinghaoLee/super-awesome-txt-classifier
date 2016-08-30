import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  *
  */
object Main {

  val conf = new SparkConf().setAppName("preprocessor")
  val sc = new SparkContext(conf)

  val xTrainFile = sc.textFile("super-awesome-txt-classifier/Examples/Data/X_train_vsmall.txt")
  val yTrainFile = sc.textFile("super-awesome-txt-classifier/Examples/Data/Y_train_vsmall.txt")

  //Place holders, will be replaced by preprocessing
  val dict1 = xTrainFile.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
  val dictTotal1 = dict1.count
  val dict2 = xTrainFile.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
  val dictTotal2 = dict1.count
  val dictProp1 = dictTotal1.toDouble / (dictTotal1 + dictTotal2)
  val dictProp2 = dictTotal2.toDouble / (dictTotal1 + dictTotal2)

  def wordProbability (dic: RDD[Pair[String, Int]], word: String, dicTotal:Long) : Double = {
    val filtered = dic.filter( (p:Pair[String,Int]) => word == p._1).first()
    return filtered._1.toDouble / dicTotal
  }

  def classProbability(classDic: RDD[Pair[String,Int]], classDicTotal:Long, classProportion:Double, targetDic: RDD[Pair[String,Int]]) : Double = {
    var probability = 0.0
    targetDic.foreach( pair=>probability = probability + ( pair._2 * wordProbability(classDic, pair._1, classDicTotal)) )
    return probability * classProportion
  }

  def classify(targetDic: RDD[Pair[String,Int]]) : String = {
    val probability1 = classProbability(dict1, dictTotal1, dictProp1, targetDic)
    val probability2 = classProbability(dict2, dictTotal2, dictProp2, targetDic)
    if (math.max(probability1, probability2) == probability1) return "Class1"
    else return "Class2"
  }

  def main(args: Array[String]): Unit ={

  }
}

