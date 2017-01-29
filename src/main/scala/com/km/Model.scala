package com.km

/**
  * KMeans Model class
  *
  * Created by sdhandapani on 1/28/2017.
  */
class Model extends Serializable {
  var clusterSize: Int = 0
  //Map to capture centroid and count
  var centroidInfo: Map[Double, Double] = Map()

  def getNearestCentroid(element: Int): Double = {
    val centroidKeys = centroidInfo.keys.toArray
    KMAssignment.getNearestCentroid(element, centroidKeys)
  }
}
