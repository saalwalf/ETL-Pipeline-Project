import pandas as pd
from google.cloud import bigquery
from sqlalchemy import create_engine
from data.config import BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID, SQL_ALCHEMY_DATABASE_URL

def create_bigquery_tables_for_data_mart():
    """Membuat tabel dimensi dan fakta di BigQuery sesuai skema data mart."""
    print("\n--- Membuat Tabel BigQuery untuk Data Mart ---")
    bigquery_client = bigquery.Client(project=BIGQUERY_PROJECT_ID)

    tables = {
        # Dimensi Waktu
        "dim_waktu": """
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.dim_waktu` (
                timestamp_datetime TIMESTAMP NOT NULL,
                jam TIME,
                hari STRING,
                tanggal DATE,
                bulan STRING,
                tahun INTEGER
            );
        """,
        # Dimensi Tempat
        "dim_place": """
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.dim_place` (
                place_id STRING NOT NULL,
                nama_tempat STRING,
                latitude FLOAT64,
                longitude FLOAT64,
                tipe_tempat STRING,
                kontak STRING,
                jam_operasional STRING
            );
        """,
        # Dimensi Vendor
        "dim_vendor": """
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.dim_vendor` (
                id_vendor STRING NOT NULL,
                nama_vendor STRING
            );
        """,
        # Dimensi Departemen
        "dim_departemen": """
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.dim_departemen` (
                id_departemen STRING NOT NULL,
                nama_departemen STRING
            );
        """,
        # Dimensi Penyumbang
        "dim_penyumbang": """
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.dim_penyumbang` (
                id_penyumbang STRING NOT NULL,
                nama_penyumbang STRING,
                jenis_penyumbang STRING
            );
        """,
        # Dimensi Proyek
        "dim_proyek": """
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.dim_proyek` (
                id_proyek STRING NOT NULL,
                nama_proyek STRING,
                sektor_pariwisata STRING
            );
        """,
        # Dimensi User (untuk Twitter)
        "dim_user": """
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.dim_user` (
                id_user STRING NOT NULL,
                lokasi_user STRING
            );
        """,
        # Fakta Ulasan (fact_maps)
        "fact_maps": """
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.fact_maps` (
                id_review STRING NOT NULL,
                timestamp_datetime TIMESTAMP,
                place_id STRING,
                author_url STRING,
                review_longtext STRING,
                rating FLOAT64
            );
        """,
        # Fakta Twitter (fact_twitter)
        "fact_twitter": """
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.fact_twitter` (
                id_tweet STRING NOT NULL,
                created_at_datetime TIMESTAMP,
                id_user STRING,
                nama_lokasi STRING,
                text_tweet STRING
            );
        """,
        # Fakta Pengeluaran
        "fact_pengeluaran": """
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.fact_pengeluaran` (
                id_transaksi STRING NOT NULL,
                timestamp_datetime TIMESTAMP,
                jenis_kebutuhan STRING,
                id_vendor STRING,
                id_departemen STRING,
                jumlah_pengeluaran BIGNUMERIC,
                bukti_pengeluaran STRING,
                id_proyek STRING
            );
        """,
        # Fakta Pemasukan
        "fact_pemasukan": """
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.fact_pemasukan` (
                id_transaksi_income STRING NOT NULL,
                timestamp_datetime TIMESTAMP,
                jenis_pemasukan STRING,
                id_penyumbang STRING,
                jumlah_pemasukan BIGNUMERIC,
                bukti_pemasukan STRING,
                id_proyek STRING
            );
        """
    }

    for table_name, schema_query in tables.items():
        try:
            query = schema_query.format(project_id=BIGQUERY_PROJECT_ID, dataset_id=BIGQUERY_DATASET_ID)
            bigquery_client.query(query).result()
            print(f"Tabel {table_name} berhasil dibuat/diperbarui di BigQuery.")
        except Exception as e:
            print(f"Error membuat tabel {table_name} di BigQuery: {e}")

def transform_and_load_to_bigquery_data_mart():
    """
    Mengambil data dari database operasional, melakukan transformasi kedua (denormalisasi/agregasi),
    dan memuatnya ke Google BigQuery sesuai skema data mart.
    """
    print("\n--- Memulai Transformasi dan Loading ke BigQuery Data Mart ---")
    
    engine = create_engine(SQL_ALCHEMY_DATABASE_URL)
    bigquery_client = bigquery.Client(project=BIGQUERY_PROJECT_ID)

    # --- 1. Muat Dimensi ---
    print("Memuat Dimensi...")

    # Dimensi Waktu
    try:
        df_reviews_op = pd.read_sql_table('reviews', engine)
        df_tweets_op = pd.read_sql_table('tweets', engine)
        df_pemasukan_op = pd.read_sql_table('pemasukan', engine)
        df_pengeluaran_op = pd.read_sql_table('pengeluaran', engine)

        all_timestamps = pd.Series(dtype='datetime64[ns]')
        if not df_reviews_op.empty:
            all_timestamps = pd.concat([all_timestamps, pd.to_datetime(df_reviews_op['timestamp_review'])])
        if not df_tweets_op.empty:
            all_timestamps = pd.concat([all_timestamps, pd.to_datetime(df_tweets_op['created_at_tweet'])])
        if not df_pemasukan_op.empty:
            all_timestamps = pd.concat([all_timestamps, pd.to_datetime(df_pemasukan_op['timestamp'])])
        if not df_pengeluaran_op.empty:
            all_timestamps = pd.concat([all_timestamps, pd.to_datetime(df_pengeluaran_op['timestamp'])])
        
        if not all_timestamps.empty:
            df_dim_waktu = pd.DataFrame({'timestamp_datetime': all_timestamps.unique()})
            df_dim_waktu['jam'] = df_dim_waktu['timestamp_datetime'].dt.time
            df_dim_waktu['hari'] = df_dim_waktu['timestamp_datetime'].dt.day_name()
            df_dim_waktu['tanggal'] = df_dim_waktu['timestamp_datetime'].dt.date
            df_dim_waktu['bulan'] = df_dim_waktu['timestamp_datetime'].dt.strftime('%Y-%m')
            df_dim_waktu['tahun'] = df_dim_waktu['timestamp_datetime'].dt.year
            
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE") # Timpa untuk memastikan unik
            table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_waktu"
            job = bigquery_client.load_table_from_dataframe(df_dim_waktu, table_id, job_config=job_config)
            job.result()
            print(f"Berhasil memuat {len(df_dim_waktu)} record ke dim_waktu di BigQuery.")
        else:
            print("Tidak ada data timestamp untuk dimuat ke dim_waktu.")
    except Exception as e:
        print(f"Error memuat dim_waktu ke BigQuery: {e}")

    # Dimensi Place
    try:
        df_places_op = pd.read_sql_table('places', engine)
        if not df_places_op.empty:
            df_dim_place = df_places_op[['place_id', 'name', 'lat', 'lng', 'types', 'phone_number', 'opening_hours_text']].copy()
            df_dim_place = df_dim_place.rename(columns={
                'name': 'nama_tempat',
                'lat': 'latitude',
                'lng': 'longitude',
                'types': 'tipe_tempat',
                'phone_number': 'kontak',
                'opening_hours_text': 'jam_operasional'
            })
            df_dim_place = df_dim_place.drop_duplicates(subset=['place_id'])
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_place"
            job = bigquery_client.load_table_from_dataframe(df_dim_place, table_id, job_config=job_config)
            job.result()
            print(f"Berhasil memuat {len(df_dim_place)} record ke dim_place di BigQuery.")
        else:
            print("Tidak ada data places untuk dimuat ke dim_place.")
    except Exception as e:
        print(f"Error memuat dim_place ke BigQuery: {e}")

    # Dimensi Vendor
    try:
        df_pengeluaran_op = pd.read_sql_table('pengeluaran', engine)
        if not df_pengeluaran_op.empty:
            df_dim_vendor = df_pengeluaran_op[['id_vendor', 'nama_vendor']].copy()
            df_dim_vendor = df_dim_vendor.drop_duplicates(subset=['id_vendor'])
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_vendor"
            job = bigquery_client.load_table_from_dataframe(df_dim_vendor, table_id, job_config=job_config)
            job.result()
            print(f"Berhasil memuat {len(df_dim_vendor)} record ke dim_vendor di BigQuery.")
        else:
            print("Tidak ada data vendor untuk dimuat ke dim_vendor.")
    except Exception as e:
        print(f"Error memuat dim_vendor ke BigQuery: {e}")

    # Dimensi Departemen
    try:
        df_pengeluaran_op = pd.read_sql_table('pengeluaran', engine)
        if not df_pengeluaran_op.empty:
            df_dim_departemen = df_pengeluaran_op[['id_departemen', 'nama_departemen']].copy()
            df_dim_departemen = df_dim_departemen.drop_duplicates(subset=['id_departemen'])
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_departemen"
            job = bigquery_client.load_table_from_dataframe(df_dim_departemen, table_id, job_config=job_config)
            job.result()
            print(f"Berhasil memuat {len(df_dim_departemen)} record ke dim_departemen di BigQuery.")
        else:
            print("Tidak ada data departemen untuk dimuat ke dim_departemen.")
    except Exception as e:
        print(f"Error memuat dim_departemen ke BigQuery: {e}")

    # Dimensi Penyumbang
    try:
        df_pemasukan_op = pd.read_sql_table('pemasukan', engine)
        if not df_pemasukan_op.empty:
            df_dim_penyumbang = df_pemasukan_op[['id_penyumbang', 'nama_penyumbang', 'jenis_penyumbang']].copy()
            df_dim_penyumbang = df_dim_penyumbang.drop_duplicates(subset=['id_penyumbang'])
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_penyumbang"
            job = bigquery_client.load_table_from_dataframe(df_dim_penyumbang, table_id, job_config=job_config)
            job.result()
            print(f"Berhasil memuat {len(df_dim_penyumbang)} record ke dim_penyumbang di BigQuery.")
        else:
            print("Tidak ada data penyumbang untuk dimuat ke dim_penyumbang.")
    except Exception as e:
        print(f"Error memuat dim_penyumbang ke BigQuery: {e}")

    # Dimensi Proyek
    try:
        df_pemasukan_op = pd.read_sql_table('pemasukan', engine)
        df_pengeluaran_op = pd.read_sql_table('pengeluaran', engine)
        
        df_proyek_pemasukan = df_pemasukan_op[['id_proyek', 'nama_proyek', 'sektor_pariwisata']].copy()
        df_proyek_pengeluaran = df_pengeluaran_op[['id_proyek', 'nama_proyek', 'sektor_pariwisata']].copy()
        
        df_dim_proyek = pd.concat([df_proyek_pemasukan, df_proyek_pengeluaran]).drop_duplicates(subset=['id_proyek'])
        
        if not df_dim_proyek.empty:
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_proyek"
            job = bigquery_client.load_table_from_dataframe(df_dim_proyek, table_id, job_config=job_config)
            job.result()
            print(f"Berhasil memuat {len(df_dim_proyek)} record ke dim_proyek di BigQuery.")
        else:
            print("Tidak ada data proyek untuk dimuat ke dim_proyek.")
    except Exception as e:
        print(f"Error memuat dim_proyek ke BigQuery: {e}")

    # Dimensi User (untuk Twitter)
    try:
        df_tweets_op = pd.read_sql_table('tweets', engine)
        if not df_tweets_op.empty:
            df_dim_user = df_tweets_op[['id_author_twitter', 'author_location']].copy()
            df_dim_user = df_dim_user.rename(columns={'id_author_twitter': 'id_user', 'author_location': 'lokasi_user'})
            df_dim_user = df_dim_user.drop_duplicates(subset=['id_user'])
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_user"
            job = bigquery_client.load_table_from_dataframe(df_dim_user, table_id, job_config=job_config)
            job.result()
            print(f"Berhasil memuat {len(df_dim_user)} record ke dim_user di BigQuery.")
        else:
            print("Tidak ada data user untuk dimuat ke dim_user.")
    except Exception as e:
        print(f"Error memuat dim_user ke BigQuery: {e}")


    # --- 2. Muat Fakta ---
    print("Memuat Fakta...")

    # Fakta Ulasan (fact_maps)
    try:
        df_reviews_op = pd.read_sql_table('reviews', engine)
        if not df_reviews_op.empty:
            df_fact_maps = df_reviews_op[['id_review', 'timestamp_review', 'place_id', 'author_url', 'review_text', 'rating']].copy()
            df_fact_maps = df_fact_maps.rename(columns={'review_text': 'review_longtext'})
            df_fact_maps['timestamp_datetime'] = pd.to_datetime(df_fact_maps['timestamp_review'])
            
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.fact_maps"
            job = bigquery_client.load_table_from_dataframe(df_fact_maps, table_id, job_config=job_config)
            job.result()
            print(f"Berhasil memuat {len(df_fact_maps)} record ke fact_maps di BigQuery.")
        else:
            print("Tidak ada data reviews dari DB operasional untuk dimuat ke fact_maps.")
    except Exception as e:
        print(f"Error memuat fact_maps ke BigQuery: {e}")

    # Fakta Twitter (fact_twitter)
    try:
        df_tweets_op = pd.read_sql_table('tweets', engine)
        df_places_op = pd.read_sql_table('places', engine) # Untuk mendapatkan nama tempat
        if not df_tweets_op.empty:
            df_fact_twitter = df_tweets_op.merge(
                df_places_op[['place_id', 'name']],
                left_on='place_id_source',
                right_on='place_id',
                how='left'
            )
            df_fact_twitter = df_fact_twitter.rename(columns={'name': 'nama_lokasi', 'created_at_tweet': 'created_at_datetime'})
            df_fact_twitter['created_at_datetime'] = pd.to_datetime(df_fact_twitter['created_at_datetime'])

            df_fact_twitter_final = df_fact_twitter[[
                'id_tweet', 'created_at_datetime', 'id_author_twitter', 'nama_lokasi', 'text_tweet'
            ]].copy()
            df_fact_twitter_final = df_fact_twitter_final.rename(columns={'id_author_twitter': 'id_user'})
            
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.fact_twitter"
            job = bigquery_client.load_table_from_dataframe(df_fact_twitter_final, table_id, job_config=job_config)
            job.result()
            print(f"Berhasil memuat {len(df_fact_twitter_final)} record ke fact_twitter di BigQuery.")
        else:
            print("Tidak ada data tweets dari DB operasional untuk dimuat ke fact_twitter.")
    except Exception as e:
        print(f"Error memuat fact_twitter ke BigQuery: {e}")

    # Fakta Pengeluaran (fact_pengeluaran)
    try:
        df_pengeluaran_op = pd.read_sql_table('pengeluaran', engine)
        if not df_pengeluaran_op.empty:
            df_fact_pengeluaran = df_pengeluaran_op[[
                'id_transaksi_original', 'timestamp', 'jenis_kebutuhan', 'id_vendor',
                'id_departemen', 'jumlah', 'bukti', 'id_proyek'
            ]].copy()
            df_fact_pengeluaran = df_fact_pengeluaran.rename(columns={
                'id_transaksi_original': 'id_transaksi',
                'timestamp': 'timestamp_datetime',
                'jumlah': 'jumlah_pengeluaran',
                'bukti': 'bukti_pengeluaran'
            })
            df_fact_pengeluaran['timestamp_datetime'] = pd.to_datetime(df_fact_pengeluaran['timestamp_datetime'])
            
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.fact_pengeluaran"
            job = bigquery_client.load_table_from_dataframe(df_fact_pengeluaran, table_id, job_config=job_config)
            job.result()
            print(f"Berhasil memuat {len(df_fact_pengeluaran)} record ke fact_pengeluaran di BigQuery.")
        else:
            print("Tidak ada data pengeluaran dari DB operasional untuk dimuat ke fact_pengeluaran.")
    except Exception as e:
        print(f"Error memuat fact_pengeluaran ke BigQuery: {e}")

    # Fakta Pemasukan (fact_pemasukan)
    try:
        df_pemasukan_op = pd.read_sql_table('pemasukan', engine)
        if not df_pemasukan_op.empty:
            df_fact_pemasukan = df_pemasukan_op[[
                'id_transaksi_original', 'timestamp', 'jenis_pemasukan', 'id_penyumbang',
                'jumlah', 'bukti', 'id_proyek'
            ]].copy()
            df_fact_pemasukan = df_fact_pemasukan.rename(columns={
                'id_transaksi_original': 'id_transaksi_income',
                'timestamp': 'timestamp_datetime',
                'jumlah': 'jumlah_pemasukan',
                'bukti': 'bukti_pemasukan'
            })
            df_fact_pemasukan['timestamp_datetime'] = pd.to_datetime(df_fact_pemasukan['timestamp_datetime'])
            
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.fact_pemasukan"
            job = bigquery_client.load_table_from_dataframe(df_fact_pemasukan, table_id, job_config=job_config)
            job.result()
            print(f"Berhasil memuat {len(df_fact_pemasukan)} record ke fact_pemasukan di BigQuery.")
        else:
            print("Tidak ada data pemasukan dari DB operasional untuk dimuat ke fact_pemasukan.")
    except Exception as e:
        print(f"Error memuat fact_pemasukan ke BigQuery: {e}")

    print("Transformasi dan loading ke BigQuery Data Mart selesai.")
