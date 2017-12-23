import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by mgawali1 on 12/23/2017.
  */
object Fuctionally_Test {

  def main(args: Array[String]): Unit = {
    val t0 = System.nanoTime()
    val conf=new SparkConf().setAppName("SEAL_Data_Preprocess")
    val sc=new SparkContext(conf)
    println(sc.applicationId)
    val ip=sc.textFile("Input_Path")
    def getHttps_records( line:String ) : Boolean = {
        if(line.contains("https"))
        {
          return true

        }
        else {
          return false
        }
      }
    def getUrl(line:String):List[String]= {
      var a1 = line.split("\"")

      var b1=List[String]()
      for (i <- 0 until a1.length) {
        if (a1(i).contains("https")){
              b1=b1:+a1(i)
        }
      }
          return b1
    }
    val with_urls=ip.filter(line => getHttps_records(line)).map(line => getUrl(line)).collect().foreach(println)
    val t1 = System.nanoTime()
    println("Elapsed time: " + ((t1 - t0) /1e9d) + " sec")
  }
}
