import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf


object RDDCreation {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf();
    conf.setAppName("Creating RDD").setMaster("local")
    val sparkSession = SparkSession.builder().appName("Creating RDD").config(conf).getOrCreate()
    
    val list = List(1,2,3,4,5,6);
    val mappedData = list.map(e=>e*2)
    mappedData.foreach(println)
  }
}