import pandas as pd
from google.cloud import bigquery
from sqlalchemy import create_engine
from data.config import BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID, SQL_ALCHEMY_DATABASE_URL

def create_bigquery_tables_for_data_mart():
    """Membuat tabel data mart sesuai desain fisik pada BigQuery."""
    bigquery_client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    tables = {
        # Dimensi
        "dim_waktu": f"""
            CREATE TABLE IF NOT EXISTS `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_waktu` (
                timestamp_datetime TIMESTAMP NOT NULL,
                jam TIME NOT NULL,
                hari STRING NOT NULL,
                tanggal DATE NOT NULL,
                bulan STRING NOT NULL,
                tahun INT64 NOT NULL
            );
        """,
        "dim_place": f"""
            CREATE TABLE IF NOT EXISTS `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_place` (
                place_id STRING NOT NULL,
                nama_tempat STRING NOT NULL,
                latitude FLOAT64 NOT NULL,
                longitude FLOAT64 NOT NULL,
                tipe_tempat STRING NOT NULL,
                kontak STRING,
                jam_operasional STRING,
                PRIMARY KEY(place_id)
            );
        """,
        "dim_user": f"""
            CREATE TABLE IF NOT EXISTS `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_user` (
                id_user STRING NOT NULL,
                lokasi_user STRING,
                PRIMARY KEY(id_user)
            );
        """,
        "dim_vendor": f"""
            CREATE TABLE IF NOT EXISTS `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_vendor` (
                id_vendor STRING NOT NULL,
                nama_vendor STRING NOT NULL,
                PRIMARY KEY(id_vendor)
            );
        """,
        "dim_departemen": f"""
            CREATE TABLE IF NOT EXISTS `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_departemen` (
                id_departemen STRING NOT NULL,
                nama_departemen STRING NOT NULL,
                PRIMARY KEY(id_departemen)
            );
        """,
        "dim_proyek": f"""
            CREATE TABLE IF NOT EXISTS `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_proyek` (
                id_proyek STRING NOT NULL,
                nama_proyek STRING NOT NULL,
                sektor_pariwisata STRING NOT NULL,
                PRIMARY KEY(id_proyek)
            );
        """,
        "dim_penyumbang": f"""
            CREATE TABLE IF NOT EXISTS `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_penyumbang` (
                id_penyumbang STRING NOT NULL,
                nama_penyumbang STRING NOT NULL,
                jenis_penyumbang STRING NOT NULL,
                PRIMARY KEY(id_penyumbang)
            );
        """,
        # Fakta
        "fact_maps": f"""
            CREATE TABLE IF NOT EXISTS `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.fact_maps` (
                id_review STRING NOT NULL,
                timestamp_datetime TIMESTAMP NOT NULL,
                place_id STRING NOT NULL,
                author_url STRING NOT NULL,
                review_longtext STRING NOT NULL,
                rating FLOAT64 NOT NULL,
                PRIMARY KEY(id_review)
            );
        """,
        "fact_twitter": f"""
            CREATE TABLE IF NOT EXISTS `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.fact_twitter` (
                id_tweet STRING NOT NULL,
                created_at_datetime TIMESTAMP NOT NULL,
                id_user STRING NOT NULL,
                nama_lokasi STRING NOT NULL,
                text_tweet STRING NOT NULL,
                PRIMARY KEY(id_tweet)
            );
        """,
        "fact_pengeluaran": f"""
            CREATE TABLE IF NOT EXISTS `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.fact_pengeluaran` (
                id_transaksi STRING NOT NULL,
                timestamp_datetime TIMESTAMP NOT NULL,
                jenis_kebutuhan STRING NOT NULL,
                id_vendor STRING NOT NULL,
                id_departemen STRING NOT NULL,
                jumlah_pengeluaran BIGNUMERIC NOT NULL,
                bukti_pengeluaran STRING,
                id_proyek STRING NOT NULL,
                PRIMARY KEY(id_transaksi)
            );
        """,
        "fact_pemasukan": f"""
            CREATE TABLE IF NOT EXISTS `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.fact_pemasukan` (
                id_transaksi_income STRING NOT NULL,
                timestamp_datetime TIMESTAMP NOT NULL,
                jenis_pemasukan STRING NOT NULL,
                id_penyumbang STRING NOT NULL,
                jumlah_pemasukan BIGNUMERIC NOT NULL,
                bukti_pemasukan STRING,
                id_proyek STRING NOT NULL,
                PRIMARY KEY(id_transaksi_income)
            );
        """
    }
    for name, sql in tables.items():
        bigquery_client.query(sql).result()
        print(f"Tabel {name} berhasil dicek/dibuat di BigQuery.")

def transform_and_load_to_bigquery_data_mart():
    print("--- Transformasi dan Load ke Data Mart BigQuery ---")
    engine = create_engine(SQL_ALCHEMY_DATABASE_URL)
    bigquery_client = bigquery.Client(project=BIGQUERY_PROJECT_ID)

    # --------- DIMENSI ---------
    # Dimensi Waktu (gabung semua timestamp dari tabel operasional)
    print("Memuat dim_waktu ...")
    df_reviews_op = pd.read_sql_table('reviews', engine)
    df_tweets_op = pd.read_sql_table('tweets', engine)
    df_pemasukan_op = pd.read_sql_table('pemasukan', engine)
    df_pengeluaran_op = pd.read_sql_table('pengeluaran', engine)

    all_timestamps = pd.Series(dtype='datetime64[ns]')
    for df, col in [
        (df_reviews_op, 'timestamp_review'),
        (df_tweets_op, 'created_at_tweet'),
        (df_pemasukan_op, 'timestamp'),
        (df_pengeluaran_op, 'timestamp'),
    ]:
        if not df.empty:
            all_timestamps = pd.concat([all_timestamps, pd.to_datetime(df[col])])
    all_timestamps = all_timestamps.dropna().unique()
    df_dim_waktu = pd.DataFrame({'timestamp_datetime': pd.to_datetime(all_timestamps)})
    if not df_dim_waktu.empty:
        df_dim_waktu['jam'] = df_dim_waktu['timestamp_datetime'].dt.time
        df_dim_waktu['hari'] = df_dim_waktu['timestamp_datetime'].dt.day_name()
        df_dim_waktu['tanggal'] = df_dim_waktu['timestamp_datetime'].dt.date
        df_dim_waktu['bulan'] = df_dim_waktu['timestamp_datetime'].dt.strftime('%Y-%m')
        df_dim_waktu['tahun'] = df_dim_waktu['timestamp_datetime'].dt.year
        # Semua NOT NULL
        df_dim_waktu = df_dim_waktu.dropna()
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_waktu"
        bigquery_client.load_table_from_dataframe(df_dim_waktu, table_id, job_config=job_config).result()
        print(f"Berhasil memuat {len(df_dim_waktu)} record ke dim_waktu.")

    # Dimensi Place
    print("Memuat dim_place ...")
    df_places_op = pd.read_sql_table('places', engine)
    if not df_places_op.empty:
        df_dim_place = df_places_op[[
            'place_id', 'name', 'lat', 'lng', 'types', 'phone_number', 'opening_hours_text'
        ]].copy()
        df_dim_place = df_dim_place.rename(columns={
            'name': 'nama_tempat',
            'lat': 'latitude',
            'lng': 'longitude',
            'types': 'tipe_tempat',
            'phone_number': 'kontak',
            'opening_hours_text': 'jam_operasional'
        })
        # Semua field NOT NULL kecuali kontak, jam_operasional
        df_dim_place = df_dim_place.dropna(subset=[
            'place_id', 'nama_tempat', 'latitude', 'longitude', 'tipe_tempat'
        ])
        df_dim_place = df_dim_place.drop_duplicates(subset=['place_id'])
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_place"
        bigquery_client.load_table_from_dataframe(df_dim_place, table_id, job_config=job_config).result()
        print(f"Berhasil memuat {len(df_dim_place)} record ke dim_place.")

    # Dimensi User (Twitter)
    print("Memuat dim_user ...")
    if not df_tweets_op.empty:
        df_dim_user = df_tweets_op[['id_author_twitter', 'author_location']].copy()
        df_dim_user = df_dim_user.rename(columns={
            'id_author_twitter': 'id_user',
            'author_location': 'lokasi_user'
        })
        df_dim_user = df_dim_user.drop_duplicates(subset=['id_user'])
        df_dim_user = df_dim_user.dropna(subset=['id_user'])
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_user"
        bigquery_client.load_table_from_dataframe(df_dim_user, table_id, job_config=job_config).result()
        print(f"Berhasil memuat {len(df_dim_user)} record ke dim_user.")

    # Dimensi Vendor
    print("Memuat dim_vendor ...")
    if not df_pengeluaran_op.empty:
        df_dim_vendor = df_pengeluaran_op[['id_vendor', 'nama_vendor']].copy()
        df_dim_vendor = df_dim_vendor.drop_duplicates(subset=['id_vendor'])
        df_dim_vendor = df_dim_vendor.dropna(subset=['id_vendor', 'nama_vendor'])
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_vendor"
        bigquery_client.load_table_from_dataframe(df_dim_vendor, table_id, job_config=job_config).result()
        print(f"Berhasil memuat {len(df_dim_vendor)} record ke dim_vendor.")

    # Dimensi Departemen
    print("Memuat dim_departemen ...")
    if not df_pengeluaran_op.empty:
        df_dim_departemen = df_pengeluaran_op[['id_departemen', 'nama_departemen']].copy()
        df_dim_departemen = df_dim_departemen.drop_duplicates(subset=['id_departemen'])
        df_dim_departemen = df_dim_departemen.dropna(subset=['id_departemen', 'nama_departemen'])
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_departemen"
        bigquery_client.load_table_from_dataframe(df_dim_departemen, table_id, job_config=job_config).result()
        print(f"Berhasil memuat {len(df_dim_departemen)} record ke dim_departemen.")

    # Dimensi Proyek
    print("Memuat dim_proyek ...")
    df_dim_proyek = pd.concat([
        df_pemasukan_op[['id_proyek', 'nama_proyek', 'sektor_pariwisata']],
        df_pengeluaran_op[['id_proyek', 'nama_proyek', 'sektor_pariwisata']]
    ]).drop_duplicates(subset=['id_proyek'])
    df_dim_proyek = df_dim_proyek.dropna(subset=['id_proyek', 'nama_proyek', 'sektor_pariwisata'])
    if not df_dim_proyek.empty:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_proyek"
        bigquery_client.load_table_from_dataframe(df_dim_proyek, table_id, job_config=job_config).result()
        print(f"Berhasil memuat {len(df_dim_proyek)} record ke dim_proyek.")

    # Dimensi Penyumbang
    print("Memuat dim_penyumbang ...")
    if not df_pemasukan_op.empty:
        df_dim_penyumbang = df_pemasukan_op[['id_penyumbang', 'nama_penyumbang', 'jenis_penyumbang']].copy()
        df_dim_penyumbang = df_dim_penyumbang.drop_duplicates(subset=['id_penyumbang'])
        df_dim_penyumbang = df_dim_penyumbang.dropna(subset=['id_penyumbang', 'nama_penyumbang', 'jenis_penyumbang'])
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dim_penyumbang"
        bigquery_client.load_table_from_dataframe(df_dim_penyumbang, table_id, job_config=job_config).result()
        print(f"Berhasil memuat {len(df_dim_penyumbang)} record ke dim_penyumbang.")

    # --------- FAKTA ---------
    # fact_maps
    print("Memuat fact_maps ...")
    if not df_reviews_op.empty:
        df_fact_maps = df_reviews_op[['id_review', 'timestamp_review', 'place_id', 'author_url', 'review_text', 'rating']].copy()
        df_fact_maps = df_fact_maps.rename(columns={
            'timestamp_review': 'timestamp_datetime',
            'review_text': 'review_longtext'
        })
        df_fact_maps = df_fact_maps.dropna(subset=[
            'id_review', 'timestamp_datetime', 'place_id', 'author_url', 'review_longtext', 'rating'
        ])
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.fact_maps"
        bigquery_client.load_table_from_dataframe(df_fact_maps, table_id, job_config=job_config).result()
        print(f"Berhasil memuat {len(df_fact_maps)} record ke fact_maps.")

    # fact_twitter
    print("Memuat fact_twitter ...")
    if not df_tweets_op.empty:
        df_places_op = pd.read_sql_table('places', engine)
        df_fact_twitter = df_tweets_op.merge(
            df_places_op[['place_id', 'name']],
            left_on='place_id_source',
            right_on='place_id',
            how='left'
        )
        df_fact_twitter = df_fact_twitter.rename(columns={
            'created_at_tweet': 'created_at_datetime',
            'name': 'nama_lokasi'
        })
        df_fact_twitter_final = df_fact_twitter[[
            'id_tweet', 'created_at_datetime', 'id_author_twitter', 'nama_lokasi', 'text_tweet'
        ]].copy()
        df_fact_twitter_final = df_fact_twitter_final.rename(columns={
            'id_author_twitter': 'id_user'
        })
        df_fact_twitter_final = df_fact_twitter_final.dropna(subset=[
            'id_tweet', 'created_at_datetime', 'id_user', 'nama_lokasi', 'text_tweet'
        ])
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.fact_twitter"
        bigquery_client.load_table_from_dataframe(df_fact_twitter_final, table_id, job_config=job_config).result()
        print(f"Berhasil memuat {len(df_fact_twitter_final)} record ke fact_twitter.")

    # fact_pengeluaran
    print("Memuat fact_pengeluaran ...")
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
        df_fact_pengeluaran = df_fact_pengeluaran.dropna(subset=[
            'id_transaksi', 'timestamp_datetime', 'jenis_kebutuhan',
            'id_vendor', 'id_departemen', 'jumlah_pengeluaran', 'id_proyek'
        ])
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.fact_pengeluaran"
        bigquery_client.load_table_from_dataframe(df_fact_pengeluaran, table_id, job_config=job_config).result()
        print(f"Berhasil memuat {len(df_fact_pengeluaran)} record ke fact_pengeluaran.")

    # fact_pemasukan
    print("Memuat fact_pemasukan ...")
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
        df_fact_pemasukan = df_fact_pemasukan.dropna(subset=[
            'id_transaksi_income', 'timestamp_datetime', 'jenis_pemasukan',
            'id_penyumbang', 'jumlah_pemasukan', 'id_proyek'
        ])
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.fact_pemasukan"
        bigquery_client.load_table_from_dataframe(df_fact_pemasukan, table_id, job_config=job_config).result()
        print(f"Berhasil memuat {len(df_fact_pemasukan)} record ke fact_pemasukan.")

    print("--- Selesai ---")
