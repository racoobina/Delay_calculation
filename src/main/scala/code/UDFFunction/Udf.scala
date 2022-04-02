package code.UDFFunction

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class Udf {
    val value_container109: UserDefinedFunction = udf((value_template: String,
                                                       inn_atrib: String,
                                                       fio_atrib: String,
                                                       actualdate_atrib:String,
                                                       inn: Any,
                                                       fio: Any,
                                                       actualdate: Any) => {
        s"""${
            value_template.
                replace("{$" + s"${inn_atrib}" + "$}", String.valueOf(inn).replaceAll("^null$", "")).
                replace("{$" + s"${fio_atrib}" + "$}", String.valueOf(fio).replaceAll("^null$", "")).
                replace("{$" + s"${actualdate_atrib}" + "$}", String.valueOf(actualdate).replaceAll("^null$", ""))
        }
      """.stripMargin
    })
}
