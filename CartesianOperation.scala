import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object CartesianOperation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    val spark = SparkSession.builder().appName("App1").master("local").getOrCreate();

    val months = spark.sparkContext.parallelize(List("Jan", "Feb", "Mar", "Apr"));

    val years = spark.sparkContext.parallelize(List("2016", "2017", "2018"));

    val cartesianProduct = months.cartesian(years);

    cartesianProduct.collect().foreach(println)

  }
}