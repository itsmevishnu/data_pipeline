from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator, BigQueryTableCheckOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

def read_sql_from_gcs(bucket_name, file_path):
    gcs_hook = GCSHook()
    sql_content = gcs_hook.download(bucket_name=bucket_name, object_name=file_path)
    return sql_content.decode("utf-8")


default_args = {
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    "learning-demo-dag",
    default_args=default_args,
    description="A simple learning DAG",
    tags=["demo", "learning"]
) as dag:
    start = DummyOperator(
        task_id="start"
    )
    
    load_hist_er = GCSToBigQueryOperator(
        task_id ="load_hist_er",
        description="Load historical exchange rates data from GCS to BigQuery",
        bucket="pdw_poc",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        source_objects=["franchise_hr_hist_er_US.csv"],
        destination_project_dataset_table="ggqqnfx-mcd-mged-kafka.vj_test_reports.stg_franchise_hr_hist_er_US"
    )

    load_hist_harri = GCSToBigQueryOperator(
        task_id ="load_hist_harri",
        bucket="pdw_poc",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        source_objects=["franchise_hr_hist_harri_US.csv"],
        destination_project_dataset_table="ggqqnfx-mcd-mged-kafka.vj_test_reports.stg_franchise_hr_hist_harri_US"
    )

    load_hist_lf = GCSToBigQueryOperator(
        task_id ="load_hist_lf",
        bucket="pdw_poc",
        create_disposition="CREATE_IF_NEEDED",        
        write_disposition="WRITE_TRUNCATE",
        source_objects=["franchise_hr_hist_lf_US.csv"],
        destination_project_dataset_table="ggqqnfx-mcd-mged-kafka.vj_test_reports.stg_franchise_hr_hist_lf_US"
    )

    check_demo_franchise_hr_hist_er_US_created = BigQueryTableCheckOperator(
        task_id="Check_demo_franchise_hr_hist_er_US_table",
        table="ggqqnfx-mcd-mged-kafka.vj_test_reports.stg_franchise_hr_hist_er_US",
        use_legacy_sql=False,
        checks={"row_count": {
            "check_statement":"COUNT(*) > 0"}
        },
    )
  
    check_demo_franchise_hr_hist_er_harri_created = BigQueryTableCheckOperator(
        task_id="Check_demo_franchise_hr_hist_er_harri_table",
        table="ggqqnfx-mcd-mged-kafka.vj_test_reports.stg_franchise_hr_hist_harri_US",
        use_legacy_sql=False,
        checks={"row_count": {
            "check_statement": "COUNT(*) > 0"}
        },
    )

    check_demo_franchise_hr_hist_lf_US_created = BigQueryTableCheckOperator(
        task_id="Check_demo_franchise_hr_hist_lf_US_table",
        use_legacy_sql=False,
        table="ggqqnfx-mcd-mged-kafka.vj_test_reports.stg_franchise_hr_hist_lf_US",
        checks={"row_count": {
            "check_statement": "COUNT(*) > 0"}
        },
    )

    clean_hist_er = BigQueryInsertJobOperator(
        task_id="clean_hist_er",
        configuration={
            "query": {
                "query": read_sql_from_gcs("vj_test_report", "sql/clean_hr_hist_er_us.sql"),
                "useLegacySql": False,
            }
        },
    )


    clean_hist_harri = BigQueryInsertJobOperator(
        task_id="clean_hist_harri",
        configuration={
            "query": {
                "query": read_sql_from_gcs("vj_test_report", "sql/clean_hr_hist_harri_US.sql"),
                "useLegacySql": False,
            }
        },
    )

    clean_hist_lf = BigQueryInsertJobOperator(
        task_id="clean_hist_lf",
        configuration={
            "query": {
                "query": read_sql_from_gcs("vj_test_report", "sql/clean_hr_hist_lf_US.sql"),
                "useLegacySql": False,
            }
        },
    )

    combine_hists = BigQueryInsertJobOperator(
        task_id="combine_hists",
        configuration={
            "query": {
                "query": read_sql_from_gcs("vj_test_report", "sql/combine_hr_data.sql"),
                "useLegacySql": False,
            }
        },
    )

    export_task = BigQueryToGCSOperator(
        task_id="export_data_to_file",
        source_project_dataset_table ="ggqqnfx-mcd-mged-kafka.vj_test_reports.dim_franchise_hr_hist_US",
        destination_cloud_storage_uris=[f"gs://vj_test_report/final_data_hist_us_{str(datetime.now())}.csv"],
        export_format="csv"
    )

    end = DummyOperator(
        task_id="end"
    )

    start >> [load_hist_er, load_hist_harri, load_hist_lf]
    load_hist_er >> check_demo_franchise_hr_hist_er_US_created
    load_hist_harri >> check_demo_franchise_hr_hist_er_harri_created
    load_hist_lf >> check_demo_franchise_hr_hist_lf_US_created
    check_demo_franchise_hr_hist_er_US_created >> clean_hist_er
    check_demo_franchise_hr_hist_er_harri_created >> clean_hist_harri
    check_demo_franchise_hr_hist_lf_US_created >> clean_hist_lf
    [clean_hist_er, clean_hist_lf, clean_hist_harri] >> combine_hists >> export_task >> end
