import os

# --- Konfigurasi API ---
GOOGLE_API_KEY = "YOUR_GOOGLE_API_KEY" # GANTI DENGAN API KEY Anda
TWITTER_BEARER_TOKEN = "YOUR_TWITTER_BEARER_TOKEN" # GANTI DENGAN BEARER TOKEN Anda

# --- Konfigurasi Google Cloud Storage (Staging Area) ---
GCS_BUCKET_NAME_API = "your-api-data-staging-bucket" # GANTI DENGAN NAMA BUCKET GCS Anda untuk data API
GCS_BUCKET_NAME_MANUAL = "your-manual-data-staging-bucket" # GANTI DENGAN NAMA BUCKET GCS Anda untuk data manual

GCS_PLACES_PREFIX = "source_data/places/"
GCS_REVIEWS_PREFIX = "source_data/reviews/"
GCS_TWEETS_PREFIX = "source_data/tweets/"
GCS_PEMASUKAN_PREFIX = "manual_input/pemasukan/"
GCS_PENGELUARAN_PREFIX = "manual_input/pengeluaran/"

# --- Konfigurasi Google BigQuery (Data Warehouse) ---
BIGQUERY_PROJECT_ID = "your-gcp-project-id" # GANTI DENGAN ID PROYEK GCP Anda
BIGQUERY_DATASET_ID = "your_data_warehouse_dataset" # GANTI DENGAN ID DATASET BigQuery Anda

# --- Konfigurasi Database Operasional ---
# Untuk simulasi: jalur file SQLite
# Untuk produksi: gunakan URL koneksi Cloud SQL, misal:
# SQL_ALCHEMY_DATABASE_URL = "mysql+pymysql://user:password@/instance_connection_name/database"
OPERATIONAL_DB_PATH = 'operational_data.db' # Path untuk SQLite DB
SQL_ALCHEMY_DATABASE_URL = f'sqlite:///{OPERATIONAL_DB_PATH}'
