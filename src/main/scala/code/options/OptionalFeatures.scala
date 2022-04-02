package code.options

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import scala.io.Source
import java.io.{File, PrintWriter}
import java.sql.Timestamp
import java.util.Date

/**
 * Получения дополнительной информации по сигналу для формирования
 * записей в таблицы RRA, RRA_ATTR
 * @param spark
 */
class OptionalFeatures(spark: SparkSession) extends Serializable {
    def getValueTemplate(dictTable: String, numberFKR: String): String = {
        val value_template = spark.sql(
            s"""select value_template from $dictTable
       where number_fcr = ${numberFKR}
       """).collectAsList().toString.replace("[", "").replace("]", "")
        value_template
    }

    def getRaPk(dictTable: String, numberFKR: String):String = {
        spark.sql(s"from $dictTable").
            select("ra_pk").where(s"number_fcr = '${numberFKR}'").
            collectAsList().toString.replace("[", "").replace("]", "")
    }

    def getDatabaseLocation(db: String):Any = {
        val database_location = spark.sql(s"describe database $db").
            filter(col("database_description_item") === "Location").
            select(col("database_description_value")).first().get(0)
        database_location
    }

    def getAttrKey(raAttrTable: String, ra_pk: String, key: String):String = {
        spark.sql(s"from $raAttrTable").
            filter(col("ra_id") === ra_pk).
            filter(col("attr_key").like(s"$key%")).
            collect().map(_.getString(0)).toList(0)
    }

    def check_date(fcr: String, dt: String, pw: PrintWriter, logDTTM:String): Unit = {
        if (dt != new Timestamp((new Date).getTime()).toString.split(' ')(0)) {
            val msg = (s"Start date = $dt\n Current date = ${new Timestamp((new Date).getTime()).toString.split(' ')(0)}\n")
            println(msg)
            println(s"\n $fcr canceled\n")
            pw.write(s"""$logDTTM;WARN;Job canceled: $msg;$fcr\n""")
            System.exit(0)
        }
    }

    def logger(ap: String, clsName: String, message: String) = {
        println(s"ApplicationId: ${ap.toUpperCase}; CLASS: ${clsName.toUpperCase}\nMESSAGE: $message")
    }

    def logTableWriter(dictTable: String, ra_id: String, replLogTable: String) = {
        val job_name = spark.sql(s"from $dictTable").
            select("job_name").
            where(s"ra_pk = '${ra_id}'").
            collectAsList().toString.replace("[", "").replace("]", "")

        import spark.implicits._
        spark.sparkContext.parallelize(Seq(
            (s"${job_name}", 1, "PROCESSING",
                (new Timestamp((new Date).getTime())).toString.split('.')(0),
                (new Timestamp((new Date).getTime())).toString.split("=")(0)
            ))).toDF("OBJECT_NAME", "STATUS", "COMMENT", "CREATE_DT", "CREATE_DT_DATE").
            write.mode(SaveMode.Append).insertInto(s"$replLogTable")
    }

    def logFileWriter(database_location: Any, temp_log_file: String, sparkCalcLog: String) = {

        val spark_log = new org.apache.hadoop.fs.Path(s"${database_location}/$sparkCalcLog")

        val conf = spark.sparkContext.hadoopConfiguration
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
        val exists = fs.exists(spark_log)

        if (exists) {
            val fileOutputStream = fs.append(spark_log)
            val bufferedSource = Source.fromFile(temp_log_file)

            for (line <- bufferedSource.getLines) {
                fileOutputStream.write(s"$line\n".getBytes("UTF-8"))
            }
            bufferedSource.close()
            fileOutputStream.close()

        } else {
            val fileOutputStream = fs.create(spark_log)
            val bufferedSource = Source.fromFile(temp_log_file)

            for (line <- bufferedSource.getLines) {
                fileOutputStream.write(s"$line\n".getBytes("UTF-8"))
            }
            bufferedSource.close()
            fileOutputStream.close()
        }

        new File(temp_log_file).delete()
    }
}
