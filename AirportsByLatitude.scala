package use_cases.airport_data_analysis

import org.apache.spark.sql.SparkSession

object AirportsByLatitude {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("AirportsInUSA").master("local").getOrCreate();

    val airportDataAsRDD = sparkSession.sparkContext.textFile("./datasets/airports.txt");

    val airportsInUSA = airportDataAsRDD.filter(line => line.split(AirportConstants.COMMA_DELIMITER)(6).toFloat > 40)

    val airportsNameAndCityNames = airportsInUSA.map(line => {
      val splits = line.split(AirportConstants.COMMA_DELIMITER)
      splits(1) + ", " + splits(6)
    })
  }
}