import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  *
  */
object Main {

  val conf = new SparkConf().setAppName("preprocessor")
  val sc = new SparkContext(conf)

  val XFile = sc.textFile("super-awesome-txt-classifier/Examples/Data/X_train_vsmall.txt")
  val YFile = sc.textFile("super-awesome-txt-classifier/Examples/Data/Y_train_vsmall.txt")

  //Place holders, will be replaced by preprocessing
  val Dict = XFile.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
  val DictTotal = Dict.count

  def WordProbability (Dic: RDD[Pair[String, Int]], word: String, DicTotal:Long) : Double = {
    var filtered = Dic.filter( (p:Pair[String,Int]) => word == p._1).first()
    return filtered._1.toDouble / DicTotal
  }

  def ClassProbability(ClassDic: RDD[Pair[String,Int]], ClassDicTotal:Long, TargetDic: RDD[Pair[String,Int]]) : Double = {
    var probability = 0.0


  }

  def main(args: Array[String]): Unit ={

    println(WordProbability(Dict, "the", DictTotal))

  }
}

