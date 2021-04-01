/**
  * Created by itim on 17.02.2018.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.graphframes._
// To make some of the examples work we will also need RDD
import org.apache.spark.sql.SparkSession

object Body {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()
    val stations_txt = "data/station_data.csv"
    //Read stations
    val stations = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv(stations_txt)
    val trips_txt = "data/trip_data.csv"
    //Read trips
    val trips = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv(trips_txt)

    val stationVertices = stations.withColumnRenamed("station_id", "id").distinct()
    val tripEdges = trips
      .withColumnRenamed("Start Terminal", "src")
      .withColumnRenamed("End Terminal", "dst")

    val stationGraph = GraphFrame(stationVertices, tripEdges)
    stationGraph.cache()

    val ranks = stationGraph.pageRank.resetProbability(0.15).maxIter(10).run()
    ranks.vertices.orderBy(desc("pagerank")).show()
    val topTrips = stationGraph
      .edges
      .groupBy("src", "dst")
      .count()
      .orderBy(desc("count"))
      .limit(10).show()
    val inDeg = stationGraph.inDegrees
    inDeg.orderBy(desc("inDegree")).limit(5).show()

    val outDeg = stationGraph.outDegrees
    outDeg.orderBy(desc("outDegree")).limit(5).show()
  }
}
