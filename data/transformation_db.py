import pandas as pd
from sqlalchemy import create_engine, text
from data.config import OPERATIONAL_DB_PATH, SQL_ALCHEMY_DATABASE_URL, \
    GCS_BUCKET_NAME_API, GCS_BUCKET_NAME_MANUAL, \
    GCS_PLACES_PREFIX, GCS_REVIEWS_PREFIX, GCS_TWEETS_PREFIX, \
    GCS_PEMASUKAN_PREFIX, GCS_PENGELUARAN_PREFIX
from data.utils import load_csv_from_gcs_to_df

from sqlalchemy import create_engine, text

def create_operational_db_schema():
    """Membuat skema tabel di database operasional (Cloud SQL) sesuai data ekstraksi API."""
    print("\n--- Membuat Skema Database Operasional ---")
    
    engine = create_engine(SQL_ALCHEMY_DATABASE_URL)
    
    with engine.connect() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS places (
                place_id TEXT PRIMARY KEY,
                name TEXT,
                phone_number TEXT,
                opening_hours_text TEXT,
                types TEXT,
                lat REAL,
                lng REAL,
                rating_search REAL
            );
        """))
        
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS reviews (
                id_review TEXT PRIMARY KEY,
                timestamp_review TIMESTAMP,
                place_id TEXT,
                author_url TEXT,
                review_text TEXT
            );
        """))
        
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS tweets (
                id_tweet TEXT PRIMARY KEY,
                place_id_source TEXT,
                keyword_search TEXT,
                created_at_tweet TIMESTAMP,
                text_tweet TEXT,
                id_author_twitter TEXT,
                author_location TEXT,
                tweet_geo_place_id TEXT
            );
        """))
        
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS pemasukan (
                id_transaksi_original TEXT PRIMARY KEY,
                timestamp TIMESTAMP,
                id_proyek TEXT,
                nama_proyek TEXT,
                sektor_pariwisata TEXT,
                id_penyumbang TEXT,
                nama_penyumbang TEXT,
                jenis_penyumbang TEXT,
                jenis_pemasukan TEXT,
                jumlah INTEGER,
                bukti TEXT
            );
        """))
        
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS pengeluaran (
                id_transaksi_original TEXT PRIMARY KEY,
                timestamp TIMESTAMP,
                id_proyek TEXT,
                nama_proyek TEXT,
                sektor_pariwisata TEXT,
                id_vendor TEXT,
                nama_vendor TEXT,
                id_departemen TEXT,
                nama_departemen TEXT,
                jenis_kebutuhan TEXT,
                jumlah INTEGER,
                bukti TEXT
            );
        """))
        
        connection.commit()
    
    print("Skema database operasional berhasil dibuat/diperbarui.")

def load_data_if_new(df, table_name, engine, id_column, column_mapping=None, select_columns=None):
    """Fungsi utilitas untuk membersihkan, seleksi kolom, dan menyimpan data baru ke tabel SQL."""
    if df.empty:
        print(f"Tidak ada data {table_name} dari GCS untuk diproses.")
        return

    df = df.drop_duplicates(subset=[id_column])

    # Rename kolom jika diperlukan
    if column_mapping:
        df = df.rename(columns=column_mapping)

    # Seleksi hanya kolom yang dibutuhkan
    if select_columns:
        df = df[[col for col in select_columns if col in df.columns]]

    try:
        # Ambil ID yang sudah ada di database
        existing_ids_df = pd.read_sql_query(f"SELECT {id_column} FROM {table_name}", engine)
        existing_ids = set(existing_ids_df[id_column]) if not existing_ids_df.empty else set()

        # Filter data baru yang belum ada di database
        df_new = df[~df[id_column].isin(existing_ids)]

        if not df_new.empty:
            df_new.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"Berhasil memuat {len(df_new)} record {table_name} baru ke database operasional.")
        else:
            print(f"Tidak ada record {table_name} baru untuk dimuat.")
    except Exception as e:
        print(f"Error memuat {table_name} ke database operasional: {e}")


def transform_and_load_to_operational_db():
    """Mengambil data dari GCS, transformasi dasar, dan loading ke database operasional."""
    print("\n--- Memulai Transformasi dan Loading ke Database Operasional ---")
    engine = create_engine(SQL_ALCHEMY_DATABASE_URL)

    # --- PLACES ---
    df_places = load_csv_from_gcs_to_df(GCS_BUCKET_NAME_API, GCS_PLACES_PREFIX)
    load_data_if_new(
        df_places, 'places', engine, 'place_id',
        column_mapping={
            'name_detail': 'name',
            'types_detail': 'types',
            'address_detail': 'address',
            'lat_detail': 'lat',
            'lng_detail': 'lng'
        },
        select_columns=[
            'place_id', 'name', 'phone_number', 'opening_hours_text', 'types',
            'lat', 'lng', 'rating_search'
        ]
    )

    # --- REVIEWS ---
    df_reviews = load_csv_from_gcs_to_df(GCS_BUCKET_NAME_API, GCS_REVIEWS_PREFIX)
    load_data_if_new(df_reviews, 'reviews', engine, 'id_review')

    # --- TWEETS ---
    df_tweets = load_csv_from_gcs_to_df(GCS_BUCKET_NAME_API, GCS_TWEETS_PREFIX)
    load_data_if_new(df_tweets, 'tweets', engine, 'id_tweet')

    # --- PEMASUKAN ---
    df_pemasukan = load_csv_from_gcs_to_df(GCS_BUCKET_NAME_MANUAL, GCS_PEMASUKAN_PREFIX)
    load_data_if_new(df_pemasukan, 'pemasukan', engine, 'id_transaksi_original')

    # --- PENGELUARAN ---
    df_pengeluaran = load_csv_from_gcs_to_df(GCS_BUCKET_NAME_MANUAL, GCS_PENGELUARAN_PREFIX)
    load_data_if_new(df_pengeluaran, 'pengeluaran', engine, 'id_transaksi_original')

    print("Transformasi dan loading ke database operasional selesai.")
