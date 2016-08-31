package org.snakesinthebox.ml.classification

import org.apache.spark.rdd.RDD

/**
  * Created by brad on 8/30/16.
  */
object NaiveBayes extends Model with Serializable{

  override def predict(testData: RDD[String]): RDD[(String, Double)] = ???

  override def predict(testData: String): (String, Double) = ???
}
