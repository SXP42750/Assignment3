import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window

object Q1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)
    //   val spark=SparkSession.builder()
    //     .appName("karthik")
    //     .master("local[*]")
    //     .getOrCreate()

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "karthik")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val employee = List(
      ("E001", "Sales", 85, "2024-02-10", "Sales Manager"),
      ("E002", "HR", 78, "2024-03-15", "HR Assistant"),
      ("E003", "IT", 92, "2024-01-22", "IT Manager"),
      ("E004", "Sales", 88, "2024-02-18", "Sales Rep"),
      ("E005", "HR", 95, "2024-03-20", "HR Manager"),
      ("E006", "Finance", 81, "2024-02-25", "Finance Manager"),
      ("E007", "IT", 83, "2024-01-30", "IT Support"),
      ("E008", "Sales", 91, "2024-04-05", "Sales Manager"),
      ("E009", "Marketing", 93, "2024-02-11", "Marketing Manager"),
      ("E010", "IT", 88, "2024-03-22", "IT Manager"),
      ("E011", "HR", 89, "2024-03-05", "HR Manager"),
      ("E012", "Sales", 82, "2024-01-18", "Sales Rep"),
      ("E013", "Finance", 76, "2024-01-30", "Finance Assistant"),
      ("E014", "Marketing", 86, "2024-03-10", "Marketing Analyst"),
      ("E015", "Sales", 94, "2024-02-20", "Sales Manager"),
      ("E016", "IT", 90, "2024-04-02", "IT Manager"),
      ("E017", "HR", 77, "2024-01-18", "HR Assistant"),
      ("E018", "Sales", 83, "2024-03-14", "Sales Manager"),
      ("E019", "Finance", 87, "2024-02-22", "Finance Manager"),
      ("E020", "Marketing", 91, "2024-01-30", "Marketing Manager")
    ).toDF("employee_id", "department", "performance_score", "review_date", "position")

//    val df = employee.withColumn("review_month", month(col("review_date")))
//
//    employee.filter(col("position").endsWith("Manager") && col("performance_score") > 80).show()
//
//    df.groupBy(col("review_month"), col("department"))
//      .agg(
//        avg(col("performance_score")).as("Average"),
//        count(when(col("performance_score")>90, "True")
//          .otherwise("False")).as("Count")
//
//      ).show()
//
//    val window =Window.orderBy("review_date")
//
//    val df1 = employee.withColumn("previous_record",lag(col("performance_score"),1).over(window))
//
//    val lag_df = df1.withColumn("performance_review", when(col("performance_score") > col("previous_record"),"Performance Improved")
//      .when(col("performance_score") < col("previous_record"),"Performance Declined")
//      .otherwise("Date available to its previous")
//
//    )
//
//    lag_df.show()

    employee.createOrReplaceTempView("Employee")

    spark.sql(
      """
      SELECT *, month(review_date) as review_month
      FROM Employee
       """).show()

    spark.sql(
      """
      SELECT
         *
      FROM Employee where position LIKE '%Manager' and performance_score > 80
        """).show()

    spark.sql(
      """
      SELECT department , month(review_date) as review_month,
       avg(performance_score) as average_performance,
       count(
            CASE
             WHEN performance_score > 90 then 1
             ELSE 0
            END
            ) as count
      FROM Employee group by department ,  month(review_date)
        """).show()

    spark.sql(
      """
      SELECT employee_id , department , performance_score ,
       lag(performance_score,1) over(partition by employee_id  order by month(review_date)) as previous_performance,
        CASE
         WHEN performance_score > lag(performance_score,1) over(partition by employee_id  order by month(review_date)) THEN 'Improvement'
         WHEN performance_score < lag(performance_score,1) over(partition by employee_id  order by month(review_date)) THEN 'Decline'
         ELSE 'No Change'
        END AS change_performance
       FROM Employee
       """).show()


  }
}

