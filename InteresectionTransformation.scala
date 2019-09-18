import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger


object InteresectionTransformation {
  def main(args: Array[String]): Unit = {
     Logger.getLogger("org").setLevel(Level.OFF);
    val spark = SparkSession.builder().appName("App1").master("local").getOrCreate();
    
    val javaSkill = List("Tom Mahoney","Alicia Whitekar","Paul Jones","Rodney Marsh");
    val javaSkillRDD = spark.sparkContext.parallelize(javaSkill);
    
    val dbSkill = List("James Kent", "Paul Jones", "Tom Mahoney", "Adam Waugh");
    val dbSkillRDD = spark.sparkContext.parallelize(dbSkill);

    val javaDBSkilledCandidates = javaSkillRDD.intersection(dbSkillRDD);
    
    javaDBSkilledCandidates.collect().foreach(println)
  }
}