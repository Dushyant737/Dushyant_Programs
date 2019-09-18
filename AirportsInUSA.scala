package use_cases.airport_data_analysis

import org.apache.spark.sql.SparkSession

object AirportsInUSA {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("AirportsInUSA").master("local").getOrCreate();

    val airportDataAsRDD = sparkSession.sparkContext.textFile("./datasets/airports.txt");
    val airportsInUSA = airportDataAsRDD.filter(record => {
      val fields = record.split(AirportConstants.COMMA_DELIMITER);
      fields(3) == "\"United States\"";
    });
    
    val airportNameAndCityNames = airportsInUSA.map(record=>{
      val fields = record.split(AirportConstants.COMMA_DELIMITER)
      fields(1)+","+fields(2);
    })
    airportNameAndCityNames.foreach(println)
  }
}