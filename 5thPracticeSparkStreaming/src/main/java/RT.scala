/**
  * Created by itim on 28.10.2017.
  */

import com.google.gson.Gson
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}

object RT {

  def featurize(s: String) = {
    val numFeatures = 1000
    val tf = new HashingTF(numFeatures)
    tf.transform(s.sliding(2).toSeq)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
    conf.setAppName("spark-sreaming")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))

    // Configure your Twitter credentials
    val apiKey = "dMGh0fKJe5DVkS0Kiugn3u6Mj"
    val apiSecret = "vnWjZMl7RrXqzxrP8esP9VIvWG3PYjfGhvfe5KkgkrlBDQEuuC"
    val accessToken = "924164071187992576-SWBNzLabsrKDE3WRfcRlIaOxPcF52hf"
    val accessTokenSecret = "31Pg6Xzk9NNPAmqDCTBpEzXB2q4ROASykAIxLc7BSUzFy"

    System.setProperty("twitter4j.oauth.consumerKey", apiKey)
    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    System.setProperty("hadoop.home.dir", "C:\\HADOOP\\")
    println("Initializing the KMeans model...")
    val model = KMeansModel.load(sc, "data/Model")
    val langNumber = 1
    val tweets = TwitterUtils
      .createStream(ssc, None)

    val text = tweets.map(t => t.getText)

    text.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.coalesce(1)
        //outputRDD.foreach(x=>println(x))

        val filtered = outputRDD.filter(t => model.predict(featurize(t)) == langNumber)
        filtered.foreach(x => println(x))
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
