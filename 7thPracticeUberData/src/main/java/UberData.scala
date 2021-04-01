/**
  * Created by itim on 02.12.2017.
  */

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeans

object UberData {

  case class Uber(dt: String, lat: Double, lon: Double, base: String) extends Serializable

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkSession = SparkSession
      .builder()
      .appName("spark-uber-analysis")
      .master("local[*]")
      .getOrCreate();
    import sparkSession.implicits._
    val uberDataDir = "src\\data\\uber.csv"

    val schema = StructType(Array(
      StructField("dt", TimestampType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("base", StringType, true)
    ))

    val uberData = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "false")
      .schema(schema)
      .csv(uberDataDir)
      .as[Uber]
    uberData.createOrReplaceTempView("Uber");

    //Most trips
    //uberData.sqlContext.sql("Select Count(*) as count,base from Uber group by base order by count desc").show()


    //Most by date
    //uberData.sqlContext.sql("Select Count(*) as count,date(dt) from Uber group by date(dt) order by count desc").show()

    val featureCols = Array("lat", "lon")
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val uberFeatures = assembler.transform(uberData)
    val Array(trainingData, testData) = uberFeatures.randomSplit(Array(0.7, 0.3), 5043)

    val kmeans = new KMeans()
      .setK(20)
      .setFeaturesCol("features")
      .setMaxIter(5)

    val model = kmeans.fit(trainingData)

    println("Final Centers: ")
    model.clusterCenters.foreach(println)

    //Get Predictions
    val predictions = model.transform(testData)
    predictions.createOrReplaceTempView("Uber")
    //predictions.show

    //Time-spot
    //    predictions.select(hour($"dt").alias("hour"), $"prediction")
    //      .groupBy("hour", "prediction").agg(count("prediction")
    //      .alias("count"))
    //      .orderBy(desc("count"))
    //      .show
    //uberData.show()
    //uberData.printSchema()
    predictions.groupBy("prediction").count().show()

    //    val res = sparkSession.sql("select dt, lat, lon, base, prediction as cid FROM Uber where prediction = 1")
    //    res.coalesce(1).write.format("json").save("./data/uber.json")
    val k = model.clusterCenters.map(x => x.toArray)


    import java.io._

    val file = "./data/center.js"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))

    writer.write("var markers=[\n")
    for (x <- k) {
      writer.write("{\"lat\":" + x(0) + ",\"lon\":" + x(1) + "}," + "\n") // however you want to format it
    }
    writer.write("\b\n]")
    writer.close()

  }
}
