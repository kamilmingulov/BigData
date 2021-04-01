import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
object TestScala2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("Spark Hello World")
    conf.setMaster("local[2]")
    var array=Array("s","a")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.OFF)
    val bas = sc.textFile("numbers.txt")
    //val basi = bas.map(x=>x.split(' '))
//    val rTotal = basi.map(x=>x.map(y=>y.toInt).reduce((a,b)=>a+b))
    val rTotal = bas.map(a=>a.split(' ')).map(b=>b.map(c=>c.toInt).reduce((x,y)=>x+y))
    val rMultiplesOfFive=bas.map(a=>a.split(' ')).map(b=>b.map(c=>c.toInt).filter((x=>x%5==0)).reduce((x,y)=>x+y))
    val RMax = bas.map(a=>a.split(' ')).map(b=>b.map(c=>c.toInt).max)
    val RMin = bas.map(a=>a.split(' ')).map(b=>b.map(c=>c.toInt).min)
    val RDistinct = bas.map(a=>a.split(' ').distinct.reduce((a,b)=>a+' '+b))

    val Total = sc.textFile("numbers.txt").flatMap(x => x.split(' ')).map(x=>x.toInt).reduce((a,b)=>a+b)
    val MultiplesOfFive = sc.textFile("numbers.txt").flatMap(x => x.split(' ')).map(x=>x.toInt).filter(x=>(x%5==0)).reduce((a, b)=>a+b)
    val Max = sc.textFile("numbers.txt").flatMap(x => x.split(' ')).map(x=>x.toInt).max()
    val Min = sc.textFile("numbers.txt").flatMap(x => x.split(' ')).map(x=>x.toInt).min()
    val Distinct = sc.textFile("numbers.txt").flatMap(x => x.split(' ')).distinct.reduce((a,b)=>a+' '+b)
    //distinct.foreach(println)
    //sc.textFile().re
    //  println(basi)
    println("Row Totals")
    rTotal.foreach(println)
    println("Row TotalsOfMult5")
    rMultiplesOfFive.foreach(println)
    println("Row Maxs")
    RMax.foreach(println)
    println("Row Mins")
    RMin.foreach(println)
    println("Row Distincts")
    RDistinct.foreach(println)
    println("File Total")
    println(Total)
    println("File TotalOfMult5")
    println(MultiplesOfFive)
    println("File Max")
    println(Max)
    println("File Min")
    println(Min)
    println("File Distincts")
    Distinct.foreach(print)
    //println(rTotal)
    //basi.take(0).foreach(println)
  }
}

