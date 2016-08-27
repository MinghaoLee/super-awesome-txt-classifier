import org.apache.spark.rdd.RDD

/**
  * Created by bradford_bazemore on 8/19/16.
  */
val XFile = sc.textFile("Examples/Data/X_train_vsmall.txt")
val YFile = sc.textFile("Examples/Data/Y_train_vsmall.txt")

//Place holders, will be replaced by preprocessing
val Dict = XFile.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
val DictTotal = Dict.count

def WordProbabiliy (Dic: RDD[Pair[String, Int]], word: String) : Double {
  var filtered = Dic.filter( (w: String, _) => word == w).first()
  return (Double)filtered(1)/(Double)DictTotal
}



object Main {
  def main(args: Array[String]): Unit ={


  }
}

