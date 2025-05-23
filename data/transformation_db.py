import pandas as pd
from sqlalchemy import create_engine, text
from data.config import OPERATIONAL_DB_PATH, SQL_ALCHEMY_DATABASE_URL, \
    GCS_BUCKET_NAME_API, GCS_BUCKET_NAME_MANUAL, \
    GCS_PLACES_PREFIX, GCS_REVIEWS_PREFIX, GCS_TWEETS_PREFIX, \
    GCS_PEMASUKAN_PREFIX, GCS_PENGELUARAN_PREFIX
from data.utils import load_csv_from_gcs_to_df

def create_operational_db_schema():
    """Membuat skema tabel di database operasional (Cloud SQL)."""
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
                address TEXT,
                lat REAL,
                lng REAL,
                rating_search REAL,
                user_ratings_total_search INTEGER
            );
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS reviews (
                id_review TEXT PRIMARY KEY,
                timestamp_review TEXT,
                place_id TEXT,
                author_name TEXT,
                author_url TEXT,
                review_text TEXT,
                rating INTEGER,
                language TEXT,
                relative_time_description TEXT
            );
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS tweets (
                id_tweet TEXT PRIMARY KEY,
                place_id_source TEXT,
                keyword_search TEXT,
                created_at_tweet TEXT,
                text_tweet TEXT,
                lang_tweet TEXT,
                retweet_count INTEGER,
                reply_count INTEGER,
                like_count INTEGER,
                quote_count INTEGER,
                impression_count INTEGER,
                id_author_twitter TEXT,
                author_username TEXT,
                author_name TEXT,
                author_location TEXT,
                author_verified BOOLEAN,
                tweet_geo_place_id TEXT
            );
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS pemasukan (
                id_transaksi_original TEXT PRIMARY KEY,
                timestamp TEXT,
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
                timestamp TEXT,
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

def transform_and_load_to_operational_db():
    """
    Mengambil data dari GCS (termasuk data manual), melakukan transformasi dasar,
    dan memuatnya ke database operasional (Cloud SQL/SQLite) dengan penanganan duplikat.
    """
    print("\n--- Memulai Transformasi dan Loading ke Database Operasional ---")

    engine = create_engine(SQL_ALCHEMY_DATABASE_URL)

    # --- Proses Data Places (API) ---
    df_places_raw = load_csv_from_gcs_to_df(GCS_BUCKET_NAME_API, GCS_PLACES_PREFIX)
    if not df_places_raw.empty:
        # Hapus duplikat dalam DataFrame yang baru dibaca
        df_places_raw = df_places_raw.drop_duplicates(subset=['place_id'])
        df_places_raw = df_places_raw.rename(columns={
            'name_detail': 'name', 'types_detail': 'types', 'address_detail': 'address',
            'lat_detail': 'lat', 'lng_detail': 'lng'
        })
        df_places_clean = df_places_raw[[
            'place_id', 'name', 'phone_number', 'opening_hours_text', 'types',
            'address', 'lat', 'lng', 'rating_search', 'user_ratings_total_search'
        ]].copy()
        try:
            # Ambil ID yang sudah ada di DB
            existing_place_ids_df = pd.read_sql_query("SELECT place_id FROM places", engine)
            existing_place_ids = set(existing_place_ids_df['place_id'].tolist()) if not existing_place_ids_df.empty else set()
            
            # Filter hanya record yang belum ada di DB
            df_new_places = df_places_clean[~df_places_clean['place_id'].isin(existing_place_ids)]

            if not df_new_places.empty:
                df_new_places.to_sql('places', engine, if_exists='append', index=False)
                print(f"Berhasil memuat {len(df_new_places)} record places baru ke database operasional.")
            else:
                print("Tidak ada record places baru untuk dimuat.")
        except Exception as e:
            print(f"Error memuat places ke database operasional: {e}")
    else:
        print("Tidak ada data places dari GCS untuk diproses.")

    # --- Proses Data Reviews (API) ---
    df_reviews_raw = load_csv_from_gcs_to_df(GCS_BUCKET_NAME_API, GCS_REVIEWS_PREFIX)
    if not df_reviews_raw.empty:
        df_reviews_raw = df_reviews_raw.drop_duplicates(subset=['id_review'])
        try:
            existing_review_ids_df = pd.read_sql_query("SELECT id_review FROM reviews", engine)
            existing_review_ids = set(existing_review_ids_df['id_review'].tolist()) if not existing_review_ids_df.empty else set()
            
            df_new_reviews = df_reviews_raw[~df_reviews_raw['id_review'].isin(existing_review_ids)]
            if not df_new_reviews.empty:
                df_new_reviews.to_sql('reviews', engine, if_exists='append', index=False)
                print(f"Berhasil memuat {len(df_new_reviews)} record reviews baru ke database operasional.")
            else:
                print("Tidak ada record reviews baru untuk dimuat.")
        except Exception as e:
            print(f"Error memuat reviews ke database operasional: {e}")
    else:
        print("Tidak ada data reviews dari GCS untuk diproses.")

    # --- Proses Data Tweets (API) ---
    df_tweets_raw = load_csv_from_gcs_to_df(GCS_BUCKET_NAME_API, GCS_TWEETS_PREFIX)
    if not df_tweets_raw.empty:
        df_tweets_raw = df_tweets_raw.drop_duplicates(subset=['id_tweet'])
        try:
            existing_tweet_ids_df = pd.read_sql_query("SELECT id_tweet FROM tweets", engine)
            existing_tweet_ids = set(existing_tweet_ids_df['id_tweet'].tolist()) if not existing_tweet_ids_df.empty else set()
            
            df_new_tweets = df_tweets_raw[~df_tweets_raw['id_tweet'].isin(existing_tweet_ids)]
            if not df_new_tweets.empty:
                df_new_tweets.to_sql('tweets', engine, if_exists='append', index=False)
                print(f"Berhasil memuat {len(df_new_tweets)} record tweets baru ke database operasional.")
            else:
                print("Tidak ada record tweets baru untuk dimuat.")
        except Exception as e:
            print(f"Error memuat tweets ke database operasional: {e}")
    else:
        print("Tidak ada data tweets dari GCS untuk diproses.")

    # --- Proses Data Pemasukan (Manual dari GCS) ---
    df_pemasukan_raw = load_csv_from_gcs_to_df(GCS_BUCKET_NAME_MANUAL, GCS_PEMASUKAN_PREFIX)
    if not df_pemasukan_raw.empty:
        # Penting: Hapus duplikat dalam DataFrame yang baru dibaca (jika satu file punya duplikat)
        df_pemasukan_raw = df_pemasukan_raw.drop_duplicates(subset=['id_transaksi_original'])
        try:
            # Ambil ID yang sudah ada di DB
            existing_pemasukan_ids_df = pd.read_sql_query("SELECT id_transaksi_original FROM pemasukan", engine)
            existing_pemasukan_ids = set(existing_pemasukan_ids_df['id_transaksi_original'].tolist()) if not existing_pemasukan_ids_df.empty else set()
            
            # Filter hanya record yang belum ada di DB
            df_new_pemasukan = df_pemasukan_raw[~df_pemasukan_raw['id_transaksi_original'].isin(existing_pemasukan_ids)]

            if not df_new_pemasukan.empty:
                df_new_pemasukan.to_sql('pemasukan', engine, if_exists='append', index=False)
                print(f"Berhasil memuat {len(df_new_pemasukan)} record pemasukan baru ke database operasional.")
            else:
                print("Tidak ada record pemasukan baru untuk dimuat.")
        except Exception as e:
            print(f"Error memuat pemasukan ke database operasional: {e}")
    else:
        print("Tidak ada data pemasukan dari GCS untuk diproses.")

    # --- Proses Data Pengeluaran (Manual dari GCS) ---
    df_pengeluaran_raw = load_csv_from_gcs_to_df(GCS_BUCKET_NAME_MANUAL, GCS_PENGELUARAN_PREFIX)
    if not df_pengeluaran_raw.empty:
        # Penting: Hapus duplikat dalam DataFrame yang baru dibaca (jika satu file punya duplikat)
        df_pengeluaran_raw = df_pengeluaran_raw.drop_duplicates(subset=['id_transaksi_original'])
        try:
            # Ambil ID yang sudah ada di DB
            existing_pengeluaran_ids_df = pd.read_sql_query("SELECT id_transaksi_original FROM pengeluaran", engine)
            existing_pengeluaran_ids = set(existing_pengeluaran_ids_df['id_transaksi_original'].tolist()) if not existing_pengeluaran_ids_df.empty else set()
            
            # Filter hanya record yang belum ada di DB
            df_new_pengeluaran = df_pengeluaran_raw[~df_pengeluaran_raw['id_transaksi_original'].isin(existing_pengeluaran_ids)]

            if not df_new_pengeluaran.empty:
                df_new_pengeluaran.to_sql('pengeluaran', engine, if_exists='append', index=False)
                print(f"Berhasil memuat {len(df_new_pengeluaran)} record pengeluaran baru ke database operasional.")
            else:
                print("Tidak ada record pengeluaran baru untuk dimuat.")
        except Exception as e:
            print(f"Error memuat pengeluaran ke database operasional: {e}")
    else:
        print("Tidak ada data pengeluaran dari GCS untuk diproses.")
    
    print("Transformasi dan loading ke database operasional selesai.")
