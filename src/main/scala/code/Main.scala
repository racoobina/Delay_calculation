package code

import code.processing.ResultTable109
import code.processing.ResultTable109.createResultTable109
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends App {

    val parameters = Parameters.instance(args)
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
        .builder()
        .master("local[*]")
        .appName("base_line")
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

        val result: DataFrame = createResultTable109()(spark, parameters)

//    result.show()
    spark.stop()
}

