/**
  * Created by itim on 30.09.2017.
  */

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object Second {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val inputFile = "data/sampletweets.json";
    //val inputFile = "data/sampletweets.json"

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-dataset-basic")
      .master("local[*]")
      .getOrCreate();

    //Read json file to DF
    val dataFrame = sparkSession.read.json(inputFile)
    println("Scheme")

    //      dataFrame.show(100)
    dataFrame.printSchema()
    //dataFrame.select("object").select("body").show(10)
    val per = dataFrame.createOrReplaceTempView("tweetTable")
    //    println("Tweets")
    //    sparkSession.sql("Select body from tweetTable").show(10, false)
    //    println("Languages")
    //    sparkSession.sql("Select COUNT(actor.languages), actor.languages from tweetTable group by actor.languages order by COUNT(actor.languages) DESC ").show(10, false)
//    println("Time")
//
//    sparkSession.sql("Select object.postedTime from tweetTable where postedTime is not null order by postedTime asc").show(1)
//    sparkSession.sql("Select object.postedTime from tweetTable where postedTime is not null order by postedTime desc").show(1)
//    println("Device")
//    sparkSession.sql("Select COUNT(generator.displayName), generator.displayName " +
//      "from tweetTable where generator.displayName is not null " +
//      "group by generator.displayName order by COUNT(generator.displayName) DESC").show(10,false)

//    println("Tweets by user")
//    sparkSession.sql("Select actor.displayName, body from tweetTable group by actor.displayName,body order by actor.displayName ").show(100, false)
//    println("Tweets amount by user")
//    sparkSession.sql("Select actor.displayName, COUNT(actor.displayName) from tweetTable group by actor.displayName order by COUNT(actor.displayName) desc").show(100, false)
      val af= sparkSession.sql("select body from tweetTable").rdd
      af.filter(x=>x.toString().contains("@")).foreach(println)
    //
  }


  //Read json file to DF
  //    val passengers = sparkSession.read
  //      .option("header", "true")
  //      .option("delimiter", "\t")
  //      .option("nullValue", "")
  //      .option("treatEmptyValuesAsNulls", "true")
  //      .option("inferSchema", "true")
  //      .csv(inputFile)


  //    passengers.show(100)

}
