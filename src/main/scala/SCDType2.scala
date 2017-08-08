import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import java.text.SimpleDateFormat
import scala.io.Source

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.util.parsing.json.JSON


object SCDType2 {

  def main(args: Array[String]) = {

    // Start the Spark context
    val conf = new SparkConf().setAppName("SCDType2")

    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)
//    sqlContext.refreshTable("phila_schools.employee_d")

    val run_date = args(0)
    val as_of_date = new SimpleDateFormat("MM/dd/yyy").parse(run_date)
    val as_of_date_str = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(as_of_date) //"2014-11-26 00:00:00"

    println(Source.fromFile("/Users/dloftus/IdeaProjects/SCDExample/src/main/resources/tableMetadata.json").getLines().mkString)
    val tableMetadata = JSON.parseFull(Source.fromFile("/Users/dloftus/IdeaProjects/SCDExample/src/main/resources/tableMetadata.json").getLines().mkString)

    println(tableMetadata.mkString)
  }
}
