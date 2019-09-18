import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object FilterTransformationsDemo01 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    val sparkSession = SparkSession.builder().appName("App1").master("local").getOrCreate();

    val userData = sparkSession.sparkContext.textFile("./datasets/u.user");
    val str = "1|24|M|technician|85711"
    val fields = str.split("\\|");
//    val filteredRecords = userData.filter(record=>{val fields = record.split("\\|");fields(3).contains("technician")})
    val filteredRecords = userData.filter(record=>parseString(record));
//     val filteredRecords = userData.filter(parseString);
    filteredRecords.collect().foreach(println)
  }
  
  def parseString(record:String):Boolean={
    val fields = record.split("\\|");
    return fields(3)=="technician"
  }
}