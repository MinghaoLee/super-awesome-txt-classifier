/**
  * Created by bradford_bazemore on 8/19/16.
  */
object Main {
  def main(args: Array[String]): Unit ={

  }
}
val XFile = sc.textFile("X_train_vsmall.txt")
val YFile = sc.textFile("Y_train_vsmall.txt")

val Dict = XFile.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)

