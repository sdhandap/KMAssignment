package com.km

/**
  * Created by sdhandapani on 1/28/2017.
  */
object AssignmentHandler {

  // Main entry point for the Spark Job
  def main(args: Array[String]) = {

    val sc = SparkSingletons.sc
    // Parallelize list of Integers
    val l1 = List(7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 14, 16, 18, 20)
    val rdd1 = sc.parallelize(l1, 4)
    // Assign Random centroids to initialize
    val randomCentroids = Array(1.0, 2.0, 8.0)
    rdd1.cache()
    // Generate KMeans Model
    val m: Model = new KMAssignment().generateModel(rdd1, randomCentroids)

    println("Centroids based on batch data-")
    m.centroidInfo.foreach(println)

    /* In case of new list of elements, find its closest centroid and increment its total and count.
     * Update the model at the end.
     */
    val newList = List(22, 25, 10)
    val rdd2 = sc.parallelize(newList)

    val existingCentroids = sc.broadcast(m.centroidInfo.keys.toArray)
    val newRddCentroidTotalCountMap = KMAssignment.getCentroidTotalAndCount(rdd2, existingCentroids).collect.toMap

    // Using the existing model centroid info, compute the latest centroid and count based on the new incoming data.
    val updatedStats =
      m.centroidInfo.map { case (centroid: Double, centroidCount: Double) =>
        util.Try(newRddCentroidTotalCountMap(centroid)) match {
          case util.Success(s) =>
            val newTotal = (centroid * centroidCount) + s._1
            val newCount = centroidCount + s._2
            val newCentroid = newTotal / newCount
            (newCentroid, newCount)
          case util.Failure(err) =>
            (centroid, centroidCount)
        }
      }

    // Update the model with updated centroind and count
    m.centroidInfo = updatedStats

    println("Centroids after new incoming data- ")
    m.centroidInfo.foreach(println)

  }
}
