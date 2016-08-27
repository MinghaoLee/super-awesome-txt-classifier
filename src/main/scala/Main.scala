import org.apache.spark.rdd.RDD

/**
  *
  */
object Main {

  val XFile = sc.textFile("Examples/Data/X_train_vsmall.txt")
  val YFile = sc.textFile("Examples/Data/Y_train_vsmall.txt")

  //Place holders, will be replaced by preprocessing
  val Dict = XFile.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
  val DictTotal = Dict.count

  def WordProbability (Dic: RDD[Pair[String, Int]], word: String) : Double = {
    var filtered = Dic.filter( (p:Pair[String,Int]) => word == p._1).first()
    return (filtered._1).toDouble / DictTotal
  }

  def main(args: Array[String]): Unit ={
  println(WordProbability(Dict, "the"))



  }
}

