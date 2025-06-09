from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import fungsi-fungsi ETL dari modul yang terpisah
# Pastikan folder 'data' di-upload bersama DAG ke Composer
from data.extraction import extract_api_data_to_gcs
from data.transformation_db import create_operational_db_schema, transform_and_load_to_operational_db
from data.transformation_dw import create_bigquery_tables_for_data_mart, transform_and_load_to_bigquery_data_mart


with DAG(
    dag_id='tourism_finance_etl_daily',
    start_date=datetime(2025, 1, 1), # Tanggal mulai DAG, bisa disesuaikan
    schedule_interval=timedelta(days=1), # Jalankan setiap hari
    catchup=False, # Penting: Jangan jalankan untuk tanggal-tanggal yang terlewat
    tags=['etl', 'tourism', 'finance', 'daily'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': datetime(2025, 1, 1) # Juga definisikan di default_args
    },
) as dag:
    # --- Task Ekstraksi Data API ke GCS Staging Area ---
    extract_api_data = PythonOperator(
        task_id='extract_api_data_to_gcs_staging',
        python_callable=extract_api_data_to_gcs,
        op_kwargs={'query_lokasi_wisata': 'objek wisata populer di Malang'}
    )

    # --- Catatan: Data manual (keuangan) sudah diunggah ke GCS oleh proses terpisah (dengan skrip manual_finance_uploader pada folder manual_tools).

    # --- Task Pembentukan Skema Database Operasional ---
    create_operational_db_schema_task = PythonOperator(
        task_id='create_operational_db_schema',
        python_callable=create_operational_db_schema,
    )

    # --- Task Transformasi & Loading ke Database Operasional ---
    # Task ini sekarang akan membaca data API DAN data manual dari GCS,
    # serta menangani duplikat sebelum memasukkan ke DB operasional.
    transform_load_operational_db = PythonOperator(
        task_id='transform_load_to_operational_db',
        python_callable=transform_and_load_to_operational_db,
    )

    # --- Task Pembentukan Tabel BigQuery Data Mart ---
    create_bigquery_tables = PythonOperator(
        task_id='create_bigquery_tables_data_mart',
        python_callable=create_bigquery_tables_for_data_mart,
    )

    # --- Task Transformasi & Loading ke BigQuery Data Mart ---
    transform_load_bigquery_data_mart = PythonOperator(
        task_id='transform_load_to_bigquery_data_mart',
        python_callable=transform_and_load_to_bigquery_data_mart,
    )

    # --- Definisi Urutan (Dependensi) Tugas ---
    # Ekstraksi API dan notifikasi upload manual bisa berjalan paralel.
    # Keduanya harus selesai sebelum transformasi ke DB operasional.
    [extract_api_data, notify_manual_upload_expectation] >> create_operational_db_schema_task
    
    # Pembentukan skema DB operasional harus selesai sebelum transformasi & loading ke sana.
    create_operational_db_schema_task >> transform_load_operational_db

    # Setelah data di DB operasional, buat skema BigQuery dan kemudian muat data ke BigQuery.
    transform_load_operational_db >> create_bigquery_tables >> transform_load_bigquery_data_mart
