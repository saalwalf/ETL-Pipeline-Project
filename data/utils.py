import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage
import io

def save_df_to_gcs(df: pd.DataFrame, gcs_bucket_name: str, gcs_prefix: str, base_file_name: str):
    """Menyimpan DataFrame ke GCS sebagai file CSV."""
    if df.empty:
        print(f"DataFrame untuk {base_file_name} kosong, tidak ada yang disimpan ke GCS.")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)

    file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = f"{gcs_prefix}{base_file_name}_{file_timestamp}.csv"

    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        blob = bucket.blob(file_name)
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        print(f"DataFrame '{base_file_name}' berhasil disimpan ke GCS: gs://{gcs_bucket_name}/{file_name}")
        csv_buffer.close()
    except Exception as e:
        print(f"Error menyimpan DataFrame '{base_file_name}' ke GCS: {e}")

def load_csv_from_gcs_to_df(gcs_bucket_name: str, gcs_prefix: str) -> pd.DataFrame:
    """Membaca semua file CSV dari prefix GCS tertentu dan mengembalikan DataFrame gabungan."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)
    blobs = bucket.list_blobs(prefix=gcs_prefix)
    all_dfs = []
    for blob in blobs:
        if blob.name.endswith('.csv'):
            try:
                csv_data = blob.download_as_text()
                df = pd.read_csv(io.StringIO(csv_data))
                all_dfs.append(df)
                print(f"Berhasil membaca {blob.name} dari GCS.")
            except Exception as e:
                print(f"Gagal membaca {blob.name} dari GCS: {e}")
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return pd.DataFrame()
