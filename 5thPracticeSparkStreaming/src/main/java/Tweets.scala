/**
  * Created by itim on 28.10.2017.
  */
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import com.google.gson.Gson
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
object Tweets {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

    def main(args: Array[String]): Unit = {
      val outputDirectory = "data"
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
      // Create Twitter Stream
      //val stream = TwitterUtils.createStream(ssc, None)
      val tweets = TwitterUtils
        .createStream(ssc, None)
        .map(new Gson().toJson(_))
      //stream.map(t=>t.)
      //tweets.print()
      val numTweetsCollect = 10000L
      var numTweetsCollected = 0L

      tweets.foreachRDD((rdd, time) => {
        val count = rdd.count()
        if (count > 0) {
          val outputRDD = rdd.coalesce(1)
          outputRDD.saveAsTextFile(outputDirectory+"/"+time)
          numTweetsCollected += count
          if (numTweetsCollected > numTweetsCollect) {
            System.exit(0)
          }
        }
      })

      ssc.start()
      ssc.awaitTermination()

  }
}
