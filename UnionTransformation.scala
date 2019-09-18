import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object UnionTransformation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    val sparkSession = SparkSession.builder().appName("App1").master("local").getOrCreate();

    val julyFirstLogs = sparkSession.sparkContext.textFile("in/nasa_19950701.tsv")
    val augustFirstLogs = sparkSession.sparkContext.textFile("in/nasa_19950801.tsv")

    val aggregatedLogLines = julyFirstLogs.union(augustFirstLogs)

    val cleanLogLines = aggregatedLogLines.filter(line => isNotHeader(line))

    val sample = cleanLogLines.sample(withReplacement = true, fraction = 0.1)

  }

  def isNotHeader(line: String): Boolean = {
    !(line.startsWith("host") && line.contains("bytes"))
  }
}