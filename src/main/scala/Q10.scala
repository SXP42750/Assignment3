import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window

object Q10 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "karthik")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()
    import spark.implicits._

    val flight_delays = List(
      ("F001", "Delta", "2023-11-01 08:00", "2023-11-01 10:00", 40, "New York"),
      ("F002", "United", "2023-11-01 09:00", "2023-11-01 11:30", 20, "New Orleans"),
      ("F003", "American", "2023-11-02 07:30", "2023-11-02 09:00", 60, "New York"),
      ("F004", "Delta", "2023-11-02 10:00", "2023-11-02 12:15", 30, "Chicago"),
      ("F005", "United", "2023-11-03 08:45", "2023-11-03 11:00", 50, "New York"),
      ("F006", "Delta", "2023-11-04 07:30", "2023-11-04 09:45", 70, "New York"),
      ("F007", "American", "2023-11-05 08:00", "2023-11-05 10:00", 45, "New Orleans"),
      ("F008", "United", "2023-11-06 06:00", "2023-11-06 08:30", 90, "New York"),
      ("F009", "Delta", "2023-11-07 09:30", "2023-11-07 12:00", 35, "New York"),
      ("F010", "American", "2023-11-08 08:15", "2023-11-08 10:00", 25, "New Orleans")
    ).toDF("flight_id", "airline", "departure_time", "arrival_time", "delay", "destination")

    val df =flight_delays.withColumn("delay_minutes",date_format(col("arrival_time"),"mm") - date_format(col("departure_time"),"mm"))

    val filter_df = df.filter(col("delay_minutes") > 30 && col("destination").startsWith("New"))

    filter_df.show()

    val group_df = df.groupBy(col("airline"),col("delay"))
      .agg(
        sum(col("delay_minutes")).as("Total"),
        max(col("delay")).as("Maximum"),
        avg(col("delay")).as("Average")
      )

    group_df.show()

    val window = Window.partitionBy("airline").orderBy("delay")

    val lag_df = flight_delays.withColumn("delay_trend",lag(col("delay"),1).over(window))

    lag_df.show()




  }
}
