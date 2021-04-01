import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("Spark Hello World")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val datum = sc.textFile("products.csv").flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((a,n)=>a+n)
    //val datum = sc.textFile("products.csv")//.map(x=>x.sp)
    datum.foreach(println)
    //println(sc)
    }
}
