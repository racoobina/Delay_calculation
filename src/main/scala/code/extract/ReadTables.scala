package code.extract

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import java.io.File

object ReadTables {

    private def getTablesNames(dir: String = "data"): Map[String, String] = {
        val file = new File(dir)
        file.listFiles.filter(_.isFile)
            .map(f => f.getName.replaceAll(".csv", "") -> f.getPath).toMap
    }

    private val tables: Map[String, String] = getTablesNames()

    private def createTable(name: String, structType: StructType, path: String, delimiter: String = ";")
                           (implicit spark: SparkSession): Unit = {
        spark.read
            .format("com.databricks.spark.csv")
            .options(
                Map(
                    "delimiter" -> delimiter,
                    "nullValue" -> "\\N",
                    "header" -> "true"
                )
            )
            .schema(structType)
            .load(path)
            .createOrReplaceTempView(name)
    }

    def initTables()(implicit spark: SparkSession): Unit = {
        tables.foreach{
            case (key, value) => {
                println(s"$key", s"$value")
                createTable(
                    key,
                    StructType(TablesShemas.SOURCE_TABLES_SCHEMA(key)),
                    value,
                    ";"
                )
            }
        }
    }
}