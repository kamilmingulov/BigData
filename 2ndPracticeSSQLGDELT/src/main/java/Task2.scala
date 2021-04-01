import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{FloatType, _}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.SparkSession

/**
  * Created by itim on 30.09.2017.
  */
object Task2 {

  import org.apache.spark.sql.SparkSession

  import org.apache.log4j.{Level, Logger}

  var gdeltSchema = StructType(Array(
    StructField("GLOBALEVENTID", IntegerType, true),
    StructField("SQLDATE", IntegerType, true),
    StructField("MonthYear", IntegerType, true),
    StructField("Year", IntegerType, true),
    StructField("FractionDate", DoubleType, true),
    StructField("Actor1Code", StringType, true),
    StructField("Actor1Name", StringType, true),
    StructField("Actor1CountryCode", StringType, true),
    StructField("Actor1KnownGroupCode", StringType, true),
    StructField("Actor1EthnicCode", StringType, true),
    StructField("Actor1Religion1Code", StringType, true),
    StructField("Actor1Religion2Code", StringType, true),
    StructField("Actor1Type1Code", StringType, true),
    StructField("Actor1Type2Code", StringType, true),
    StructField("Actor1Type3Code", StringType, true),
    StructField("Actor2Code", StringType, true),
    StructField("Actor2Name", StringType, true),
    StructField("Actor2CountryCode", StringType, true),
    StructField("Actor2KnownGroupCode", StringType, true),
    StructField("Actor2EthnicCode", StringType, true),
    StructField("Actor2Religion1Code", StringType, true),
    StructField("Actor2Religion2Code", StringType, true),
    StructField("Actor2Type1Code", StringType, true),
    StructField("Actor2Type2Code", StringType, true),
    StructField("Actor2Type3Code", StringType, true),
    StructField("IsRootEvent", IntegerType, true),
    StructField("EventCode", StringType, true),
    StructField("EventBaseCode", StringType, true),
    StructField("EventRootCode", StringType, true),
    StructField("QuadClass", IntegerType, true),
    StructField("GoldsteinScale", DoubleType, true),
    StructField("NumMentions", IntegerType, true),
    StructField("NumSources", IntegerType, true),
    StructField("NumArticles", IntegerType, true),
    StructField("AvgTone", DoubleType, true),
    StructField("Actor1Geo_Type", IntegerType, true),
    StructField("Actor1Geo_FullName", StringType, true),
    StructField("Actor1Geo_CountryCode", IntegerType, true),
    StructField("Actor1Geo_ADM1Code", StringType, true),
    StructField("Actor1Geo_Lat", FloatType, true),
    StructField("Actor1Geo_Long", FloatType, true),
    StructField("Actor1Geo_FeatureID", IntegerType, true),
    StructField("Actor2Geo_Type", IntegerType, true),
    StructField("Actor2Geo_FullName", StringType, true),
    StructField("Actor2Geo_CountryCode", StringType, true),
    StructField("Actor2Geo_ADM1Code", StringType, true),
    StructField("Actor2Geo_Lat", FloatType, true),
    StructField("Actor2Geo_Long", FloatType, true),
    StructField("Actor2Geo_FeatureID", IntegerType, true),
    StructField("ActionGeo_Type", IntegerType, true),
    StructField("ActionGeo_FullName", StringType, true),
    StructField("ActionGeo_CountryCode", StringType, true),
    StructField("ActionGeo_ADM1Code", StringType, true),
    StructField("ActionGeo_Lat", FloatType, true),
    StructField("ActionGeo_Long", FloatType, true),
    StructField("ActionGeo_FeatureID", IntegerType, true),
    StructField("DATEADDED", IntegerType, true),
    StructField("SOURCEURL", StringType, true)))


  val inputFile = "data/sampletweets.json";

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val inputFile = "data/gdelt.csv"
    val cameoFile = "data/CAMEO_event_codes.csv"
    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-dataset-basic")
      .master("local[*]")
      .getOrCreate();

    //Read json file to DF
    val dataFrame = sparkSession.read.option("header", false)
      .option("delimiter", "\t")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", true)
      .schema(gdeltSchema).csv(inputFile)
    val cameoFrame = sparkSession.read.option("header", true)
      .option("delimiter", "\t")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", true)
      .csv(cameoFile)
    val CSV = dataFrame.createOrReplaceTempView("GDELT");
    val cam = cameoFrame.createOrReplaceTempView("Cameo")

    println("Events in Russia and USA")
    sparkSession.sql("select GLOBALEVENTID, EventCode, ActionGeo_CountryCode from GDELT where" +
      " ActionGeo_CountryCode='US' OR" +
      " ActionGeo_CountryCode='RS' ").show(false)

    println("Most mentioned actors")
    sparkSession.sql("select Actor1Name,COUNT(Actor1Name) from GDELT group by Actor1Name order by COUNT(Actor1Name) DESC").show(10)
    sparkSession.sql("select Actor2Name,COUNT(Actor1Name) from GDELT group by Actor2Name order by COUNT(Actor2Name) DESC").show(10)
    //sparkSession.sql("select Actor1Name,COUNT(Actor1Name) from (select Actor1Name from GDELT UNION select Actor2Name from GDELT) group by Actor1Name order by COUNT(Actor1Name) DESC").show()
    //sparkSession.sql("select  from GDELT group by Actor2Name order by COUNT(Actor2Name) DESC").show(10)
    println("Descriptions")
    sparkSession.sql("select GLOBALEVENTID,EventDescription from GDELT,Cameo where EventCode=CAMEOcode ").show(10,false)
    println("10 most mentioned events with description")
    sparkSession.sql("select GLOBALEVENTID,NumMentions,EventDescription from GDELT,Cameo where EventCode=CAMEOcode order by NumMentions DESC ").show(10,false)

  }
}

