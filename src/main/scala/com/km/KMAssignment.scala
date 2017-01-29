package com.km

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by sdhandapani on 1/28/2017.
  */
class KMAssignment extends Serializable {
  val numOfIterations = 5

  /** Method to generate the KMeans model based on the supplied input RDD and
    * supplied random centroids matching the number of expected clusters.
    *
    * @param inputRdd
    * @param inputCentroids
    * @return
    */
  def generateModel(inputRdd: RDD[Int], inputCentroids: Array[Double]): Model = {
    val model = new Model()
    val clusterSize = inputCentroids.length
    var newActiveCentroids = inputCentroids
    inputRdd.cache()

    for (i <- 1 to numOfIterations) {
      val currentCentroids: Broadcast[Array[Double]] = SparkSingletons.sc.broadcast(newActiveCentroids)
      val centroidWithTotalAvg: Array[(Double, Double)] = KMAssignment.getCentroidTotalAndCount(inputRdd, currentCentroids).values.map { a => (a._1 / a._2, a._2) }.collect
      newActiveCentroids = centroidWithTotalAvg.map(a => a._1)
      currentCentroids.unpersist()
      println(s"Iteration: $i")
      centroidWithTotalAvg.foreach(println)
      if (numOfIterations == i) {
        model.clusterSize = clusterSize
        model.centroidInfo = centroidWithTotalAvg.toMap
      }
    }
    model
  }
}

object KMAssignment extends Serializable {
  val seqOp = (a: (Double, Double), b: Int) => (a._1 + b, a._2 + 1)
  val combOp = (a: (Double, Double), b: (Double, Double)) => (a._1 + b._1, a._2 + b._2)

  /**
    * Given the input RDD and current centroids, this method will identify centroid closer to input elements and
    * produces total and count of elements that belong to each of the supplied centroids
    *
    * @param inputRdd
    * @param currentCentroids
    * @return
    */

  def getCentroidTotalAndCount(inputRdd: RDD[Int], currentCentroids: Broadcast[Array[Double]]) =
    inputRdd.map { ele =>
      (KMAssignment.getNearestCentroid(ele, currentCentroids.value), ele)
    }.aggregateByKey((0.0, 0.0))(seqOp, combOp)

  /**
    * Get nearest centroid for the supplied element, given array of centroids
    *
    * @param element
    * @param currentCentroids
    * @return
    */
  def getNearestCentroid(element: Int, currentCentroids: Array[Double]): Double = {
    var assignedCentroid = 0.0
    var distance = Double.MaxValue
    currentCentroids.foreach { centroid =>
      val currentDistance = Math.abs(element - centroid)
      if (currentDistance < distance) {
        assignedCentroid = centroid
        distance = currentDistance
      }
    }
    assignedCentroid
  }
}
