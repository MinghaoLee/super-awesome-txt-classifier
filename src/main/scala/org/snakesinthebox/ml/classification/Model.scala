package org.snakesinthebox.ml.classification

import org.apache.spark.rdd.RDD

/**
  * Created by brad on 8/30/16.
  */
trait Model extends Serializable{

  def predict(testData: RDD[String]):RDD[(String,Double)]

  def predict(testData: String):(String,Double)

}
