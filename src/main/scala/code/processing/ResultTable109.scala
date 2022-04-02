package code.processing

import code.Parameters
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import code.extract.ReadTables
import org.apache.spark.sql.expressions.Window
import code.UDFFunction.Udf
import code.options.OptionalFeatures

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Date

object ResultTable109 extends App {
  def createResultTable109()(implicit spark: SparkSession, parameters: Parameters): DataFrame = {

    val rt = ReadTables
    rt.initTables()(spark)

    val clientsTable = "clients_h"
    val ctlTable = "ctl_task_code"
    val sourceBshTable = "source_pravo_bankrupt_stages_history"
    val sourceCaTable = "source_pravo_case_arbitr"
    val sourceCsTable = "source_pravo_case_side"
    val targetBshTable = "target_pravo_bankrupt_stg_hist"
    val targetCaTable = "target_pravo_case_arbitr"
    val targetCsTable = "target_pravo_case_side_h"

    val UDFFunction = new Udf
    val opt = new OptionalFeatures(spark)

    spark.sparkContext.setLogLevel("WARN")
    try {
      val clients_h = spark.table(clientsTable)
        .select(
          col("inn"),
          col("client_pk").as("client_id")
        )
        .dropDuplicates("client_id")

      println("Clients records are generated")
      clients_h.show()

      val ctl_task_code = spark.table(ctlTable)
        .filter(col("task_code_type") === "KANTOR" and col("task_code") === "VTB_INN")
        .select(
          col("task_key")
        )
        .dropDuplicates("task_key")
      ctl_task_code.show()


      val target_pravo_case_arbitr = spark.table(targetCaTable)
        .select(
          col("case_number"),
          col("case_id"),
          col("inn"),
          col("client_id"),
          col("sysdate_dttm")
        )
        .withColumn("min_target_sysdate", min(col("sysdate_dttm")).over(Window.partitionBy("client_id", "inn", "case_id", "case_number")))
        .drop("sysdate_dttm")
        .cache()
      target_pravo_case_arbitr.show()

      val source_pravo_case_arbitr = spark.table(sourceCaTable)
        .filter((col("case_type_mcode").isin('Б') and col("inn") === col("bancruptcy_debtor_inn")))
        .select(
          col("inn"),
          col("case_number"),
          col("case_id"),
          col("effective_date"),
          col("claim_sum")
        )
        .withColumn("min_eff_date_ca", min(col("effective_date")).over(Window.partitionBy("case_id")))
        .drop("effective_date")
        .cache()
      source_pravo_case_arbitr.show()


      val target_pravo_case_side_h = spark.table(targetCsTable)
        .select(
          col("side_inn"),
          col("case_id"),
          col("inn"),
          col("client_id"),
          col("side_type")
        )
        .cache()
      target_pravo_case_side_h.show()

      val source_pravo_case_side = spark.table(sourceCsTable)
        .select(
          col("side_inn"),
          col("case_id"),
          col("inn"),
          col("side_type"),
          col("effective_date")
        )
        .cache()
      source_pravo_case_side.show()

      val dcs_min_date = source_pravo_case_side
        .filter(col("inn") === col("side_inn") and col("side_type") === "1")
        .select(
          col("case_id"),
          col("effective_date")
        )
        .withColumn("min_eff_date_cs", min(col("effective_date")).over(Window.partitionBy("case_id")))
        .drop("effective_date")
        .cache()
      dcs_min_date.show()

      val dcs_debt_min_date = source_pravo_case_side
        .filter(col("inn") === col("side_inn") and col("side_type") === "0")
        .select(
          col("inn"),
          col("case_id"),
          col("side_type"),
          col("effective_date")
        )
        .withColumn("min_eff_date_dcs_debt", min(col("effective_date")).over(Window.partitionBy("case_id")))
        .drop("effective_date")
        .join(target_pravo_case_side_h, Seq("case_id", "inn", "side_type"), "inner")
        .cache()
      dcs_debt_min_date.show()

      val dcs_plaint_min_date = source_pravo_case_side
        .filter(col("inn") =!= col("side_inn") and col("side_type") === "0")
        .join(ctl_task_code, col("task_key") === col("side_inn"), "inner")
        .select(
          col("case_id"),
          col("side_inn"),
          col("side_type"),
          col("effective_date")
        )
        .withColumn("min_eff_date_dcs_plaint", min(source_pravo_case_side.col("effective_date")).over(Window.partitionBy("case_id", "side_inn")))
        .drop("effective_date")
        .join(target_pravo_case_side_h, Seq("case_id", "side_inn", "side_type"), "inner")
        .cache()
      dcs_plaint_min_date.show()

      val target_pravo_bankrupt_stg_hist = spark.table(targetBshTable)
        .filter(col("inn") =!= col("bancruptcy_debtor_ogrn"))
        .select(
          col("client_id"),
          col("case_id"),
          col("inn"),
          col("bankrupt_stage_id"),
          col("start_date")
        )
        .cache()
      target_pravo_bankrupt_stg_hist.show()

      val source_pravo_bankrupt_stages_history = spark.table(sourceBshTable)
        .select(
          col("case_id"),
          col("bankrupt_stage_id"),
          col("start_date"),
          col("effective_date")
        )
        .withColumn("min_eff_date_bsh", min(col("effective_date")).over(Window.partitionBy("case_id", "bankrupt_stage_id", "start_date")))
        .drop("effective_date")
        .cache()
      source_pravo_bankrupt_stages_history.show()

      val all_data = target_pravo_case_arbitr.join(source_pravo_case_arbitr, Seq("case_id", "inn", "case_number"), "inner")
        .join(target_pravo_case_side_h, Seq("case_id"), "inner")
        .join(dcs_min_date, Seq("case_id"), "inner")
        .join(target_pravo_bankrupt_stg_hist, Seq("case_id"), "inner")
        .join(source_pravo_bankrupt_stages_history, Seq("case_id", "bankrupt_stage_id", "start_date"), "inner")
        .join(target_pravo_case_side_h, Seq("case_id"), "inner")
        .join(target_pravo_case_side_h, Seq("case_id"), "inner")
        .join(dcs_debt_min_date, Seq("case_id"), "left")
        .join(dcs_plaint_min_date, Seq("case_id"), "left")
        .cache()
      all_data.show()

      val date_delay = all_data
        .distinct()
        .withColumn("latest_date",
          when(col("bankrupt_stage_id") === "6", greatest("min_eff_date_ca", "min_eff_date_cs", "min_eff_date_bsh", "min_eff_date_dcs_plaint"))
            .when(col("bankrupt_stage_id") === "12", greatest("min_eff_date_ca", "min_eff_date_cs", "min_eff_date_bsh", "min_eff_date_dcs_debt"))
            .otherwise(greatest("min_eff_date_ca", "min_eff_date_cs", "min_eff_date_bsh"))
        )
        .withColumn("date_delay", datediff(to_date(col("min_target_sysdate"), "dd.MM.yyyy"), to_date(col("latest_date"), "yyyy-MM-dd")))
        .select(
          target_pravo_case_arbitr("inn"),
          target_pravo_case_arbitr("client_id"),
          target_pravo_case_arbitr("case_number"),
          target_pravo_case_arbitr("case_id"),
          col("min_target_sysdate"),
          col("latest_date"),
          col("date_delay")

        )
        .distinct()
      date_delay.show()


      //  date_delay.filter(col("date_delay").cast("int") > 4)
      //  date_delay.show()

      date_delay
    }
  }
}
