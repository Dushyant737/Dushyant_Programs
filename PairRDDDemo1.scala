import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object PairRDDDemo1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    val sparkSession = SparkSession.builder().appName("Demo").master("local").getOrCreate();
    val userData = sparkSession.sparkContext.textFile("./datasets/groupby-dataset.txt")
    val pairRDD = userData.map(record => { parseRecord(record) })

    pairRDD.foreach(println)

  }

  def parseRecord(record: String): (String, String) = {
    val fields = record.split(",");
    (fields(0), fields(1));
  }
}