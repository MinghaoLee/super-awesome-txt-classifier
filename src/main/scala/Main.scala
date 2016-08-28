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
  val Dict1 = XFile.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
  val DictTotal1 = Dict1.count
  val Dict2 = XFile.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
  val DictTotal2 = Dict1.count
  val DictProp1 = DictTotal1.toDouble / (DictTotal1 + DictTotal2)
  val DictProp2 = DictTotal2.toDouble / (DictTotal1 + DictTotal2)

  def WordProbability (Dic: RDD[Pair[String, Int]], word: String, DicTotal:Long) : Double = {
    val filtered = Dic.filter( (p:Pair[String,Int]) => word == p._1).first()
    return filtered._1.toDouble / DicTotal
  }

  def ClassProbability(ClassDic: RDD[Pair[String,Int]], ClassDicTotal:Long, ClassProportion:Double, TargetDic: RDD[Pair[String,Int]]) : Double = {
    var probability = 0.0
    TargetDic.foreach( pair=>probability = probability + ( pair._2 * WordProbability(ClassDic, pair._1, ClassDicTotal)) )
    return probability * ClassProportion
  }

  def Classify(TargetDic: RDD[Pair[String,Int]]) : String = {
    val probability1 = ClassProbability(Dict1, DictTotal1, DictProp1, TargetDic)
    val probability2 = ClassProbability(Dict2, DictTotal2, DictProp2, TargetDic)
    if (math.max(probability1, probability2) == probability1) return "Class1"
    else return "Class2"
  }

  def main(args: Array[String]): Unit ={

  }
}

