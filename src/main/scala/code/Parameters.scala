package code
import java.text.SimpleDateFormat
import java.io.File
import java.sql.Timestamp
import java.time.LocalDateTime

object Parameters {
    def instance(args: Array[String]) = new Parameters(args)
}

class Parameters(args: Array[String]) extends Serializable {
    private val paramMap: Map[String, String] = args
        .map(param => {
            val pair = param.split("=", -1)
            (pair(0), pair(1))
        }).toMap

    private def getOldDateThreshold: String = {
        new SimpleDateFormat("dd-MM-yyyy").format(Timestamp.valueOf(LocalDateTime.now().minusMonths(24)))
    }

    private def getCurrentDate: String = {
        new SimpleDateFormat("dd-MM-yyyy").format(Timestamp.valueOf(LocalDateTime.now()))
    }

    val LOAD_PATH: String = new File("src/main/scala/test_task/load/result_table").getAbsolutePath
    val DATA_BASE: String = "TEST_DB"
    val SOURCE_TABLE: String = paramMap.getOrElse("SOURCE_TABLE", "source_table")

    val DATE: String = getCurrentDate
    val OLDEST_ALLOWED_DATE: String = getOldDateThreshold

    val PARTITION_COLUMN: String = "DATE_TIME"
    val PARQUET_PATH: String = "../PARQUET_PATH"

    val STAGE: Int = paramMap.getOrElse("STAGE", "1").toInt
}
