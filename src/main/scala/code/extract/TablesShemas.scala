package code.extract

import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructField}

object TablesShemas {
    val SOURCE_TABLES_SCHEMA = Map(
        "clients_h" ->
            List(
                StructField("client_pk", StringType),
                StructField("inn", StringType)
            ),
        "source_pravo_bankrupt_stages_history" ->
            List(
                  StructField("bankrupt_stage_id", StringType),
                  StructField("start_date", StringType),
                  StructField("end_date", StringType),
                  StructField("document_id", StringType),
                  StructField("document_date", StringType),
                  StructField("document_content_type", StringType),
                  StructField("inn", StringType),
                  StructField("case_id", StringType),
                  StructField("effective_date", StringType),
                  StructField("expiration_date", StringType),
                  StructField("комметнарий", StringType)

  ),

        "source_pravo_case_arbitr" ->
            List(
                    StructField("inn", StringType),
        StructField("case_id", StringType),
        StructField("case_number", StringType),
        StructField("case_reg_date", StringType),
        StructField("case_category", StringType),
        StructField("case_type_mcode", StringType),
        StructField("case_type_code", StringType),
        StructField("claim_sum", StringType),
        StructField("case_close_date", StringType),
        StructField("court_name", StringType),
        StructField("simple_justice_ind", StringType),
        StructField("last_document_id", StringType),
        StructField("last_document_date", StringType),
        StructField("last_document_descr", StringType),
        StructField("last_document_court", StringType),
        StructField("next_session_date", StringType),
        StructField("next_session_date_utc", StringType),
        StructField("next_session_description", StringType),
        StructField("next_session_court", StringType),
        StructField("case_result", StringType),
        StructField("case_result_code", StringType),
        StructField("bancruptcy_arbitr_manager", StringType),
        StructField("bancruptcy_case_stage", StringType),
        StructField("bancruptcy_registry_close_date", StringType),
        StructField("bancruptcy_debtor_address", StringType),
        StructField("bancruptcy_debtor_inn", StringType),
        StructField("bancruptcy_debtor_ogrn", StringType),
        StructField("bancruptcy_debtor_name", StringType),
        StructField("bancruptcy_debtor_category", StringType),
        StructField("bancruptcy_debtor_type", StringType),
        StructField("bancruptcy_debtor_id", StringType),
        StructField("bancruptcy_debtor_legal_status", StringType),
        StructField("bancruptcy_debtor_short_name", StringType),
        StructField("bancruptcy_debtor_opponent_type", StringType),
        StructField("bancruptcy_citizen_snils", StringType),
        StructField("bancruptcy_citizen_birthplace", StringType),
        StructField("bancruptcy_citizen_birthdate", StringType),
        StructField("bancruptcy_first_msg_date", StringType),
        StructField("case_active_ind", StringType),
        StructField("court_tag", StringType),
        StructField("is_approved", StringType),
        StructField("effective_date", StringType),
        StructField("expiration_date", StringType),
        StructField("comment", StringType)

  ),
        "ctl_task_code" ->
            List(
                StructField("task_code_type", StringType),
                StructField("task_condition", StringType),
                StructField("task_code", StringType),
                StructField("task_key", StringType),
                StructField("effective_date", StringType),
                StructField("expiration_date", StringType)
          ),
        "source_pravo_case_side" ->
            List(
              StructField("inn", StringType),
              StructField("case_id", StringType),
              StructField("side_address", StringType),
              StructField("side_inn", StringType),
              StructField("side_ogrn", StringType),
              StructField("side_name", StringType),
              StructField("side_category", StringType),
              StructField("side_type", StringType),
              StructField("side_id", StringType),
              StructField("side_legalstatus", StringType),
              StructField("side_shortname", StringType),
              StructField("side_opponenttype", StringType),
              StructField("effective_date", StringType),
              StructField("expiration_date", StringType)
          ),
        "target_pravo_bankrupt_stg_hist" ->
            List(
                StructField("actual_date_dt", StringType),
                StructField("client_id", StringType),
                StructField("bancruptcy_debtor_ogrn", StringType),
                StructField("bankrupt_stage_id", StringType),
                StructField("start_date", StringType),
                StructField("end_date", StringType),
                StructField("document_id", StringType),
                StructField("document_date", StringType),
                StructField("document_content_type", StringType),
                StructField("inn", StringType),
                StructField("case_id", StringType),
                StructField("sysdate_dttm", StringType)
          ),
        "target_pravo_case_arbitr" ->
            List(
                StructField("actual_date_dt", StringType),
                StructField("client_id", StringType),
                StructField("inn", StringType),
                StructField("case_id", StringType),
                StructField("case_number", StringType),
                StructField("case_reg_date", StringType),
                StructField("case_category", StringType),
                StructField("case_type_mcode", StringType),
                StructField("case_type_code", StringType),
                StructField("claim_sum", StringType),
                StructField("case_close_date", StringType),
                StructField("court_name", StringType),
                StructField("simple_justice_ind", StringType),
                StructField("last_document_id", StringType),
                StructField("last_document_date", StringType),
                StructField("last_document_descr", StringType),
                StructField("last_document_court", StringType),
                StructField("next_session_date", StringType),
                StructField("next_session_date_utc", StringType),
                StructField("next_session_description", StringType),
                StructField("next_session_court", StringType),
                StructField("case_result", StringType),
                StructField("case_result_code", StringType),
                StructField("bancruptcy_arbitr_manager", StringType),
                StructField("bancruptcy_case_stage", StringType),
                StructField("bancruptcy_registry_close_date", StringType),
                StructField("bancruptcy_debtor_address", StringType),
                StructField("bancruptcy_debtor_inn", StringType),
                StructField("bancruptcy_debtor_ogrn", StringType),
                StructField("bancruptcy_debtor_name", StringType),
                StructField("bancruptcy_debtor_category", StringType),
                StructField("bancruptcy_debtor_type", StringType),
                StructField("bancruptcy_debtor_id", StringType),
                StructField("bancruptcy_debtor_legal_status", StringType),
                StructField("bancruptcy_debtor_short_name", StringType),
                StructField("bancruptcy_debtor_opponent_type", StringType),
                StructField("bancruptcy_citizen_snils", StringType),
                StructField("bancruptcy_citizen_birthplace", StringType),
                StructField("bancruptcy_citizen_birthdate", StringType),
                StructField("bancruptcy_first_msg_date", StringType),
                StructField("case_active_ind", StringType),
                StructField("court_tag", StringType),
                StructField("is_approved", StringType),
                StructField("sysdate_dttm", StringType)

          ),
        "target_pravo_case_side_h" ->
            List(StructField("client_id", StringType),
                    StructField("inn", StringType),
        StructField("case_id", StringType),
        StructField("side_address", StringType),
        StructField("side_inn", StringType),
        StructField("side_ogrn", StringType),
        StructField("side_name", StringType),
        StructField("side_category", StringType),
        StructField("side_type", StringType),
        StructField("side_id", StringType),
        StructField("side_legalstatus", StringType),
        StructField("side_shortname", StringType),
        StructField("side_opponenttype", StringType),
        StructField("effective_date", StringType),
        StructField("expiration_date", StringType)
        )
    )
}
