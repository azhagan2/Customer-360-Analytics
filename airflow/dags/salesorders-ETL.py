from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from utils.aws_utils import archive_and_clean_s3_folder, send_failure_sns_alert

# S3 configs
BUCKET = 'snowflake-august-2025'
DATA_PREFIX = 'ERP-Data/supply_sales/raw-data/'
ARCHIVE_BASE_PREFIX = 'ERP-Data/supply_sales/archive/'


def run_archive():
    archive_and_clean_s3_folder(BUCKET, DATA_PREFIX, ARCHIVE_BASE_PREFIX)


# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'on_failure_callback': send_failure_sns_alert,
}


with DAG(
    dag_id='salesorders_elt',
    default_args=default_args,
    description='ELT with validation, deduplication, and error handling',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Step 1: Raw View (Bronze)
    sales_raw = SnowflakeOperator(
        task_id='orders_raw_data',
        snowflake_conn_id='snowflakeid',
        retries=0,
        sql="""
            CREATE OR REPLACE VIEW ERP_DATA.RAW.VW_SALES_BRONZE AS
            SELECT 
                $1::TEXT                          AS SALES_ORDER_NUMBER,
                TRY_TO_NUMBER($2, 10, 0)          AS SALES_ORDER_LINENUMBER,
                TRY_TO_DATE($3, 'YYYY-MM-DD')     AS ORDER_DATE,
                $4::TEXT                          AS CUSTOMER_NAME,
                $5::TEXT                          AS EMAIL,
                $6::TEXT                          AS ITEM,
                TRY_TO_NUMBER($7, 10, 0)          AS QUANTITY,
                TRY_TO_NUMBER($8, 38, 4)          AS UNITPRICE,
                TRY_TO_NUMBER($9, 38, 4)          AS TAX,
                $10::TEXT                         AS STATUS,
                METADATA$FILE_ROW_NUMBER          AS FILE_ROW_NUMBER,
                METADATA$FILE_CONTENT_KEY         AS FILE_CONTENT_KEY,
                METADATA$FILENAME                 AS FILE_NAME,
                METADATA$FILE_LAST_MODIFIED       AS STG_MODIFIED_TS
            FROM  @ERP_DATA.RAW.AWS_STAGE/supply_sales/raw-data/
                (FILE_FORMAT => ERP_DATA.RAW.csv_with_header_sales,
                 PATTERN => '.*supply_sales.*\\.csv') t;
        """,
    )

    # Step 2: Insert Invalid Records → Error Table
    validate_sales = SnowflakeOperator(
        task_id='validate_orders_data',
        snowflake_conn_id='snowflakeid',
        retries=0,
        sql="""
            INSERT INTO ERP_DATA.RAW.SALES_DATA_ERRORS (
                SALES_ORDER_NUMBER,
                SALES_ORDER_LINENUMBER,
                ORDER_DATE,
                CUSTOMER_NAME,
                EMAIL,
                ITEM,
                QUANTITY,
                UNITPRICE,
                TAX,
                STATUS,
                FILE_ROW_NUMBER,
                FILE_CONTENT_KEY,
                FILE_NAME,
                ERROR_REASON,
                CREATED_TS
            )
            SELECT
                SALES_ORDER_NUMBER,
                SALES_ORDER_LINENUMBER,
                ORDER_DATE,
                CUSTOMER_NAME,
                EMAIL,
                ITEM,
                QUANTITY,
                UNITPRICE,
                TAX,
                STATUS,
                FILE_ROW_NUMBER,
                FILE_CONTENT_KEY,
                FILE_NAME,
                CASE
                    WHEN SALES_ORDER_NUMBER IS NULL THEN 'Missing Sales Order Number'
                    WHEN ORDER_DATE IS NULL THEN 'Invalid Order Date'
                    WHEN QUANTITY IS NULL OR QUANTITY < 0 THEN 'Invalid Quantity'
                    WHEN UNITPRICE IS NULL OR UNITPRICE < 0 THEN 'Invalid Unit Price'
                    ELSE 'Unknown Data Issue'
                END AS ERROR_REASON,
                CURRENT_TIMESTAMP()
            FROM ERP_DATA.RAW.VW_SALES_BRONZE
            WHERE SALES_ORDER_NUMBER IS NULL
               OR ORDER_DATE IS NULL
               OR QUANTITY IS NULL OR QUANTITY < 0
               OR UNITPRICE IS NULL OR UNITPRICE < 0;
        """,
    )

    # Step 3: Deduplicate Valid Records
    deduplicate_sales = SnowflakeOperator(
        task_id='deduplicate_orders_data',
        snowflake_conn_id='snowflakeid',
        retries=0,
        sql="""
            CREATE OR REPLACE view ERP_DATA.RAW.SALES_DEDUPED AS
            SELECT *
            FROM (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY SALES_ORDER_NUMBER, ITEM, EMAIL, ORDER_DATE
                        ORDER BY STG_MODIFIED_TS DESC, FILE_ROW_NUMBER DESC
                    ) AS rn
                FROM ERP_DATA.RAW.VW_SALES_BRONZE
                WHERE SALES_ORDER_NUMBER IS NOT NULL
                  AND ORDER_DATE IS NOT NULL
                  AND QUANTITY IS NOT NULL AND QUANTITY >= 0
                  AND UNITPRICE IS NOT NULL AND UNITPRICE >= 0
            )
            WHERE rn = 1;
        """,
    )

    # Step 4: Merge Deduplicated Valid Records → Processed Table
    sales_processed = SnowflakeOperator(
        task_id='salesorders_processed',
        snowflake_conn_id='snowflakeid',
        retries=0,
        sql="""
            MERGE INTO ERP_DATA.RAW.PROCESSED_SALES_DATA AS target
            USING (
                SELECT *
                FROM ERP_DATA.RAW.SALES_DEDUPED
            ) AS source
            ON  target.SALES_ORDER_NUMBER = source.SALES_ORDER_NUMBER
            AND target.ORDER_DATE          = source.ORDER_DATE
            AND target.EMAIL               = source.EMAIL
            AND target.ITEM                = source.ITEM

            WHEN MATCHED THEN
                UPDATE SET 
                    target.SALES_ORDER_LINENUMBER = source.SALES_ORDER_LINENUMBER,
                    target.CUSTOMER_NAME          = source.CUSTOMER_NAME,
                    target.EMAIL                  = source.EMAIL,
                    target.QUANTITY               = source.QUANTITY,
                    target.UNITPRICE              = source.UNITPRICE,
                    target.TAX                    = source.TAX,
                    target.STATUS                 = source.STATUS,
                    target.MODIFIED_TS            = CURRENT_TIMESTAMP(),
                    target.FILE_NAME  =   source.FILE_NAME

            WHEN NOT MATCHED THEN
                INSERT (
                    SALES_ORDER_NUMBER,
                    SALES_ORDER_LINENUMBER,
                    ORDER_DATE,
                    CUSTOMER_NAME,
                    EMAIL,
                    ITEM,
                    QUANTITY,
                    UNITPRICE,
                    TAX,
                    STATUS,
                    FILE_ROW_NUMBER,
                    FILE_CONTENT_KEY,
                    FILE_NAME,
                    CREATED_TS,
                    MODIFIED_TS,
                    STG_MODIFIED_TS
                )
                VALUES (
                    source.SALES_ORDER_NUMBER,
                    source.SALES_ORDER_LINENUMBER,
                    source.ORDER_DATE,
                    source.CUSTOMER_NAME,
                    source.EMAIL,
                    source.ITEM,
                    source.QUANTITY,
                    source.UNITPRICE,
                    source.TAX,
                    source.STATUS,
                    source.FILE_ROW_NUMBER,
                    source.FILE_CONTENT_KEY,
                    source.FILE_NAME,
                    CURRENT_TIMESTAMP(),
                    CURRENT_TIMESTAMP(),
                    source.STG_MODIFIED_TS
                );
        """,
    )

    # Step 5: Archive S3 files
    archive_task = PythonOperator(
        task_id='archive_s3_files',
        python_callable=run_archive,
    )

    # Task Dependencies
    sales_raw >> validate_sales >> deduplicate_sales >> sales_processed >> archive_task
