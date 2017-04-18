import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import java.text.SimpleDateFormat
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


object SCDExample {

  def main(args: Array[String]) = {

    // Start the Spark context
    val conf = new SparkConf().setAppName("SCDExample")

    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)
    //sqlContext.refreshTable("phila_schools.employee_d")

    val run_date = args(0)
    val as_of_date = new SimpleDateFormat("yyyyMMdd").parse(run_date)
    val as_of_date_str = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(as_of_date) //"2014-11-26 00:00:00"

    // Read some example file to a test RDD
    // val df = sqlContext.sql("select run_date, count(*) as cnt from phila_schools.school_employees group by run_date order by cnt desc")

    val staged_data = sqlContext.sql("select login, premium_user, address, phone, first_name, surname," +
                                      " year_of_birth, city, credit_score, score_type, cust_ssn_tin_number," +
                                      " branch_number, officer_number, " +
                                      " from_unixtime(unix_timestamp(dl_as_of_dt, 'yyyyMMdd')) as as_of_date" +
                                      " from big_dim_test.staging where dl_as_of_dt='" + run_date + "'")

    val dim_data = sqlContext.sql("select * from big_dim_test.users_d")

    var max_key = dim_data.agg(Map("key" -> "max")).collect()(0).getInt(0)
    println("max_key = " + max_key)

    val dim_open_records = dim_data.filter(dim_data("end_date").isNull) //open records
    val dim_closed_records = dim_data.filter(dim_data("end_date").isNotNull) //closed records

    // process most recent dim records and new incoming records
    val columns = dim_open_records.columns.map(a => a+"_d")


    val renamed_dim_open_records = dim_open_records.toDF(columns :_*)

    val dimSchema = dim_open_records.schema

//    val joined = renamed_employee_d.join(employee_stg,
//                                    renamed_employee_d("last_name_d")===employee_stg("last_name") &&
//                                    renamed_employee_d("first_name_d")===employee_stg("first_name") &&
//                                    renamed_employee_d("home_organization_d")===employee_stg("home_organization"),
//                                 "outer")

    val df1KeyArray: Array[String] = Array("login_d")
    val df2KeyArray: Array[String] = Array("login")

    val joinExprs = df1KeyArray
      .zip(df2KeyArray)
      .map{case (c1, c2) => renamed_dim_open_records(c1) === staged_data(c2)}
      .reduce(_ && _)

    val joined = renamed_dim_open_records
      .join(staged_data, joinExprs, "outer")
      .flatMap((r => upsert(r, as_of_date_str)))

    val new_dim = sqlContext.createDataFrame(joined, dimSchema)

    //val dim_inserts = new_dim.filter(new_dim("key") === null).repartition(1)
    val dim_inserts = new_dim.filter("key is null")
      .repartition(1)
      .mapPartitions(iterator => {
      //val indexed = iterator.zipWithIndex.toList
      iterator.zipWithIndex.map(r =>
        Row(
          r._2 + max_key + 1,
          r._1.getAs("login"),
          r._1.getAs("premium_user"),
          r._1.getAs("address"),
          r._1.getAs("phone"),
          r._1.getAs("first_name"),
          r._1.getAs("surname"),
          r._1.getAs("year_of_birth"),
          r._1.getAs("city"),
          r._1.getAs("credit_score"),
          r._1.getAs("score_type"),
          r._1.getAs("cust_ssn_tin_number"),
          r._1.getAs("branch_number"),
          r._1.getAs("officer_number"),
          r._1.getAs("most_recent"),
          r._1.getAs("version"),
          r._1.getAs("begin_date"),
          r._1.getAs("end_date")
        )
      )
    })

    val dim_non_inserts = new_dim.filter("key is not null")

    val unioned = sqlContext.createDataFrame(dim_inserts, dimSchema)
      .unionAll(dim_non_inserts)
      .unionAll(dim_closed_records)
      .repartition(5)

    //dim_inserts_new_df.show(50)

    //dim_inserts_new_df.write.mode("overwrite").saveAsTable("phila_schools.temp_table_inserts")
    //new_dim.write.mode("overwrite").saveAsTable("phila_schools.temp_table")
    unioned.write.mode("overwrite").saveAsTable("big_dim_test.temp_table_union")
    //sqlContext.sql("ALTER TABLE big_dim_test.users_d RENAME TO big_dim_test.users_d_pre_" + run_date.replaceAll("/", "_"))
    sqlContext.sql("ALTER TABLE big_dim_test.users_d RENAME TO big_dim_test.users_d_pre_" + run_date)
    sqlContext.sql("ALTER TABLE big_dim_test.temp_table_union RENAME TO big_dim_test.users_d")
    sqlContext.sql("ALTER TABLE big_dim_test.users_d SET SERDEPROPERTIES ('path' = 'hdfs://ip-10-0-0-172.ec2.internal:8020/user/hive/warehouse/big_dim_test.db/users_d')")

    // val df2 = df.filter(!df("last_name").contains("LAST_NAME")).groupBy("run_date").count()
    // df2.orderBy(desc("count")).show()
  }

  def upsert(joinedRow: Row, as_of_date_str: String) :Array[Row] = {

    // println(joinedRow.schema.printTreeString())

    var r = Row.empty
    var new_r = Row.empty

    if (joinedRow.getAs("login_d") == null) {
      // record didn't exist in the dim
      r = Row(null,
        joinedRow.getAs("login"),
        joinedRow.getAs("premium_user"),
        joinedRow.getAs("address"),
        joinedRow.getAs("phone"),
        joinedRow.getAs("first_name"),
        joinedRow.getAs("surname"),
        joinedRow.getAs("year_of_birth"),
        joinedRow.getAs("city"),
        joinedRow.getAs("credit_score"),
        joinedRow.getAs("score_type"),
        joinedRow.getAs("cust_ssn_tin_number"),
        joinedRow.getAs("branch_number"),
        joinedRow.getAs("officer_number"),
        "Y",
        1,
        joinedRow.getAs("as_of_date"),
        null
      )

      return Array(r)

    } else if (joinedRow.getAs("login") == null) {
      // record doesn't exist in the incoming data
      // close the record out

      r = Row(
        joinedRow.getAs("key_d"),
        joinedRow.getAs("login_d"),
        joinedRow.getAs("premium_user_d"),
        joinedRow.getAs("address_d"),
        joinedRow.getAs("phone_d"),
        joinedRow.getAs("first_name_d"),
        joinedRow.getAs("surname_d"),
        joinedRow.getAs("year_of_birth_d"),
        joinedRow.getAs("city_d"),
        joinedRow.getAs("credit_score_d"),
        joinedRow.getAs("score_type_d"),
        joinedRow.getAs("cust_ssn_tin_number_d"),
        joinedRow.getAs("branch_number_d"),
        joinedRow.getAs("officer_number_d"),
        "Y",
        joinedRow.getAs("version_d"),
        joinedRow.getAs("begin_date_d"),
        as_of_date_str) //"2014-11-26 00:00:00"

      return Array(r)

    } else {
      // there was a matching recording in the incoming data
      // compare field by field to see if there is an update
      // if there is we have to close out the existing record
      // and create a new one

      if (joinedRow.getAs("premium_user_d").equals(joinedRow.getAs("premium_user")) &&
          joinedRow.getAs("address_d").equals(joinedRow.getAs("address")) &&
          joinedRow.getAs("phone_d").equals(joinedRow.getAs("phone")) &&
          joinedRow.getAs("first_name_d").equals(joinedRow.getAs("first_name")) &&
          joinedRow.getAs("surname_d").equals(joinedRow.getAs("surname")) &&
          joinedRow.getAs("year_of_birth_d").equals(joinedRow.getAs("year_of_birth")) &&
          joinedRow.getAs("city_d").equals(joinedRow.getAs("city")) &&
          joinedRow.getAs("credit_score_d").equals(joinedRow.getAs("credit_score")) &&
          joinedRow.getAs("score_type_d").equals(joinedRow.getAs("score_type")) &&
          joinedRow.getAs("cust_ssn_tin_number_d").equals(joinedRow.getAs("cust_ssn_tin_number")) &&
          joinedRow.getAs("branch_number_d").equals(joinedRow.getAs("branch_number")) &&
          joinedRow.getAs("officer_number_d").equals(joinedRow.getAs("officer_number"))
      ) {
        // all the fields were the same return the existing _d recording

        r = Row(
          joinedRow.getAs("key_d"),
          joinedRow.getAs("login_d"),
          joinedRow.getAs("premium_user_d"),
          joinedRow.getAs("address_d"),
          joinedRow.getAs("phone_d"),
          joinedRow.getAs("first_name_d"),
          joinedRow.getAs("surname_d"),
          joinedRow.getAs("year_of_birth_d"),
          joinedRow.getAs("city_d"),
          joinedRow.getAs("credit_score_d"),
          joinedRow.getAs("score_type_d"),
          joinedRow.getAs("cust_ssn_tin_number_d"),
          joinedRow.getAs("branch_number_d"),
          joinedRow.getAs("officer_number_d"),
          "Y",
          joinedRow.getAs("version_d"),
          joinedRow.getAs("begin_date_d"),
          null)  //this is the only difference from the existing dim record, probably have to think about how to handle this


        return Array(r)

      } else {
        // something was different close the _d record and create a new one

        r = Row(
          joinedRow.getAs("key_d"),
          joinedRow.getAs("login_d"),
          joinedRow.getAs("premium_user_d"),
          joinedRow.getAs("address_d"),
          joinedRow.getAs("phone_d"),
          joinedRow.getAs("first_name_d"),
          joinedRow.getAs("surname_d"),
          joinedRow.getAs("year_of_birth_d"),
          joinedRow.getAs("city_d"),
          joinedRow.getAs("credit_score_d"),
          joinedRow.getAs("score_type_d"),
          joinedRow.getAs("cust_ssn_tin_number_d"),
          joinedRow.getAs("branch_number_d"),
          joinedRow.getAs("officer_number_d"),
          "N",
          joinedRow.getAs("version_d"),
          joinedRow.getAs("begin_date_d"),
          as_of_date_str)

        new_r = Row(
          null,
          joinedRow.getAs("login"),
          joinedRow.getAs("premium_user"),
          joinedRow.getAs("address"),
          joinedRow.getAs("phone"),
          joinedRow.getAs("first_name"),
          joinedRow.getAs("surname"),
          joinedRow.getAs("year_of_birth"),
          joinedRow.getAs("city"),
          joinedRow.getAs("credit_score"),
          joinedRow.getAs("score_type"),
          joinedRow.getAs("cust_ssn_tin_number"),
          joinedRow.getAs("branch_number"),
          joinedRow.getAs("officer_number"),
          "Y",
          joinedRow.getAs("version_d").asInstanceOf[Int] + 1,
          joinedRow.getAs("begin_date_d"),
          null
        )

        return Array(r, new_r)
      }
    }
  }
}

/*
key                 	int
  last_name           	string
  first_name          	string
  pay_rate_type       	string
  pay_rate            	string
  title_description   	string
  home_organization   	string
  home_organization_description	string
  organization_level  	string
  type_of_representation	string
  gender              	string
  version             	int
  begin_date          	string
  end_date            	string
  most_recent         	string
*/