import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level


object MapTranformationsDemo01 {
  def main(args: Array[String]): Unit = {
    
    // Disabling Logs
    Logger.getLogger("org").setLevel(Level.OFF);
    val sparkSession = SparkSession.builder().appName("App1").master("local").getOrCreate();
    
    val numbersData=sparkSession.sparkContext.textFile("./datasets/numbers.txt");
    val mappedData = numbersData.map(record=>record.toInt*2);
    mappedData.collect().foreach(println)
  }
}