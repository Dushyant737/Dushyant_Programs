package exercises

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object _01 {
  def main(args: Array[String]): Unit = {
     Logger.getLogger("org").setLevel(Level.OFF);
    val sparkSession = SparkSession.builder().appName("Demo").master("local").getOrCreate();
    val userData = sparkSession.sparkContext.textFile("./datasets/u.user")

    val filteredRecords = userData.filter(record => {
      val fields = record.split("\\|");
      fields(3).contains("technician");
    })
    val selectedData = filteredRecords.map(record=>{
      val fields = record.split("\\|");
      val str = fields(3)+","+fields(4);
      str
    })
    
    selectedData.top(5).foreach(println)
  }
}