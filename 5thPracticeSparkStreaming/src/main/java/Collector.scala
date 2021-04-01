import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors


/**
  * Created by itim on 28.10.2017.
  */

object Collector {

  def featurize(s: String) = {
    val numFeatures = 1000
    val tf = new HashingTF(numFeatures)
    tf.transform(s.sliding(2).toSeq)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val jsonFiles = "data/[0-9]* ms/part-00000"
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()
    val Tweets = sparkSession.read.json(jsonFiles)

    val TweetFrame = Tweets.createOrReplaceTempView("TweetSql")
    val TweetBodies = sparkSession.sql("Select text as body from TweetSql")
    TweetBodies.show()
    val tweets = TweetBodies.rdd.map(r => r(0).toString)

    val features = tweets.map(s => featurize(s))

    val numClusters = 10
    val numIterations = 40

    // Train KMenas model and save it to file
//    val model: KMeansModel = KMeans.train(features, numClusters, numIterations)
//    model.save(sparkSession.sparkContext, "data/Model")

    val model = KMeansModel.load(sparkSession.sparkContext, "data/Model")
    val langNumber = 3
    val filtered = tweets.filter(t => model.predict(featurize(t)) == langNumber)
   filtered.foreach(x=>println(x))
    //val data = sc.textFile("data/[0-9]* ms/part-00000")

  }
}
