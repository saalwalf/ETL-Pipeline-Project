from datetime import datetime
import pandas as pd
from google.cloud import storage # Import library GCS
import io # Untuk menangani stream in-memory
import os # Untuk os.environ.get

# --- Konfigurasi ---
# Ganti dengan nama bucket GCS yang sama dengan yang di data/config.py
GCS_BUCKET_NAME = "your-manual-data-staging-bucket" 

# Path di dalam bucket untuk menyimpan file
GCS_PEMASUKAN_PREFIX = "manual_input/pemasukan/"
GCS_PENGELUARAN_PREFIX = "manual_input/pengeluaran/"

def save_df_to_gcs_single_record(df: pd.DataFrame, gcs_bucket_name: str, gcs_prefix: str, base_file_name: str):
    """Menyimpan DataFrame satu record ke GCS sebagai file CSV."""
    if df.empty:
        print(f"DataFrame untuk {base_file_name} kosong, tidak ada yang disimpan ke GCS.")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)

    file_name = f"{gcs_prefix}{base_file_name}.csv" # Nama file sudah unik dari pemanggil

    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        blob = bucket.blob(file_name)
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        print(f"Data transaksi '{base_file_name}' berhasil disimpan ke GCS: gs://{gcs_bucket_name}/{file_name}")
        csv_buffer.close()
    except Exception as e:
        print(f"Error menyimpan ke GCS untuk transaksi '{base_file_name}': {e}")


def run_manual_finance_uploader():
    """
    Fungsi interaktif untuk menginput transaksi keuangan manual
    dan langsung mengunggahnya ke GCS.
    """
    print("=== Input Transaksi Keuangan Manual (Upload Langsung ke GCS) ===")
    print("Pastikan Anda sudah mengkonfigurasi otentikasi Google Cloud.")
    print(f"File akan disimpan ke bucket: gs://{GCS_BUCKET_NAME}/")

    while True:
        jenis = input("Jenis transaksi (pemasukan/pengeluaran/selesai): ").strip().lower()
        if jenis == "selesai":
            break

        id_transaksi = input("ID Transaksi: ")
        timestamp_str = input("Tanggal dan waktu (YYYY-MM-DD HH:MM:SS) [kosongkan untuk sekarang]: ").strip()
        
        if not timestamp_str:
            dt_object = datetime.now() # Gunakan waktu sekarang jika kosong
            print(f"Menggunakan waktu sekarang: {dt_object.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            try:
                dt_object = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                print("Format tanggal salah. Gunakan YYYY-MM-DD HH:MM:SS. Transaksi ini dilewati.")
                continue

        # Format timestamp untuk nama file dan data
        file_timestamp_suffix = dt_object.strftime("%Y%m%d_%H%M%S")
        data_timestamp = dt_object.isoformat() # Simpan sebagai ISO format string

        id_proyek = input("ID Proyek: ")
        nama_proyek = input("Nama Proyek: ")
        sektor = input("Sektor Pariwisata: ")

        common_data = {
            "id_transaksi_original": id_transaksi,
            "timestamp": data_timestamp,
            "id_proyek": id_proyek,
            "nama_proyek": nama_proyek,
            "sektor_pariwisata": sektor
        }

        record = common_data.copy()
        base_file_name = ""
        gcs_prefix = ""

        if jenis == "pemasukan":
            record.update({
                "id_penyumbang": input("ID Penyumbang: "),
                "nama_penyumbang": input("Nama Penyumbang: "),
                "jenis_penyumbang": input("Jenis Penyumbang: "),
                "jenis_pemasukan": input("Jenis Pemasukan: "),
                "jumlah": int(input("Jumlah Pemasukan: ")),
                "bukti": input("Bukti Pemasukan (mis: URL/path): ")
            })
            base_file_name = f"pemasukan_{id_transaksi}_{file_timestamp_suffix}"
            gcs_prefix = GCS_PEMASUKAN_PREFIX

        elif jenis == "pengeluaran":
            record.update({
                "id_vendor": input("ID Vendor: "),
                "nama_vendor": input("Nama Vendor: "),
                "id_departemen": input("ID Departemen: "),
                "nama_departemen": input("Nama Departemen: "),
                "jenis_kebutuhan": input("Jenis Kebutuhan: "),
                "jumlah": int(input("Jumlah Pengeluaran: ")),
                "bukti": input("Bukti Pengeluaran (mis: URL/path): ")
            })
            base_file_name = f"pengeluaran_{id_transaksi}_{file_timestamp_suffix}"
            gcs_prefix = GCS_PENGELUARAN_PREFIX
        else:
            print("Jenis tidak dikenali.")
            continue

        df_single_record = pd.DataFrame([record])
        save_df_to_gcs_single_record(df_single_record, GCS_BUCKET_NAME, gcs_prefix, base_file_name)

    print("\nProses input transaksi manual selesai.")


if __name__ == '__main__':
    run_manual_finance_uploader()
