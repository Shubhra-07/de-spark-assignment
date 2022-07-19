package sparkcodes.excersise
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Shubhra {

//created a main function
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("InClassTasks")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )

    //task 01: Create DataFrame from CSV data. Show your resultant data.

    //task 1
    println("---------- TASK 1 -----------")
    val DF: DataFrame = sparkSession.read.option("header","true").csv("data/input/spark_assignment.csv")
    DF.show()

    //task 02: Create a new column "hostname" derived from "url" column. Show your resultant data.
    println("----------TASK 2-------------")
    val newDF : DataFrame = DF.withColumn("hostname", functions.split(col("url"), "/").getItem(2))
    newDF.show()

    //task 03: Filter "job_title" by any manager. Show your resultant data.
    //task 3
    println("----------TASK 3-------------")
    DF.filter(DF("job_title").contains("Manager")).show()

    //task 04: Highest yearly salary of each gender. Show your resultant data.
    println("----------TASK 4--------------")
    DF.groupBy("gender").agg(max("salary")).show()





  }

}
