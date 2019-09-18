import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object DistinctTransformations {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    val sparkSession = SparkSession.builder().appName("App1").master("local").getOrCreate();

    val movieList = sparkSession.sparkContext.textFile("./datasets/random_movie_names.txt");
    movieList.distinct().collect().foreach(println)
  }
}