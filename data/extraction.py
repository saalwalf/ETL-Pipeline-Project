import requests
import tweepy
import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage # Diperlukan untuk akses bucket di Cloud Composer Task
import io

from data.config import GOOGLE_API_KEY, TWITTER_BEARER_TOKEN, \
    GCS_BUCKET_NAME_API, GCS_BUCKET_NAME_MANUAL, \
    GCS_PLACES_PREFIX, GCS_REVIEWS_PREFIX, GCS_TWEETS_PREFIX, \
    GCS_PEMASUKAN_PREFIX, GCS_PENGELUARAN_PREFIX
from data.utils import save_df_to_gcs

def get_places(query: str) -> list:
    """Mengambil daftar tempat dasar menggunakan Text Search API."""
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={query}&key={GOOGLE_API_KEY}&language=id"
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get("results", [])

def get_place_details_and_reviews(place_id: str) -> tuple[dict, list]:
    """
    Mengambil detail lengkap tempat termasuk nama, ulasan, nomor telepon, dan jam operasional
    menggunakan Place Details API.
    """
    fields = "name,reviews,formatted_phone_number,opening_hours,place_id,types,geometry,formatted_address"
    url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields={fields}&key={GOOGLE_API_KEY}&language=id"
    response = requests.get(url)
    response.raise_for_status()
    result = response.json().get("result", {})

    reviews_data = result.get("reviews", [])
    formatted_reviews = []
    for r_idx, r in enumerate(reviews_data):
        if r.get('time'):
            author_id_part = r.get('author_name', str(r_idx)).replace(" ", "_")
            review_id = f"{place_id}_{author_id_part}_{r.get('time')}"

            formatted_reviews.append({
                "id_review": review_id,
                "timestamp_review": datetime.fromtimestamp(r["time"], timezone.utc).isoformat(),
                "place_id": place_id,
                "author_name": r.get("author_name"),
                "author_url": r.get("author_url"),
                "review_text": r.get("text"),
                "rating": r.get("rating"),
                "language": r.get("language"),
                "relative_time_description": r.get("relative_time_description")
            })

    opening_hours_text = None
    if "opening_hours" in result and "weekday_text" in result["opening_hours"]:
        opening_hours_text = " | ".join(result["opening_hours"]["weekday_text"])

    place_data_from_details = {
        "place_id": result.get("place_id"),
        "name_detail": result.get("name"),
        "phone_number": result.get("formatted_phone_number"),
        "opening_hours_text": opening_hours_text,
        "types_detail": ", ".join(result.get("types", [])),
        "address_detail": result.get("formatted_address"),
        "lat_detail": result.get("geometry", {}).get("location", {}).get("lat"),
        "lng_detail": result.get("geometry", {}).get("location", {}).get("lng"),
    }
    return place_data_from_details, formatted_reviews

def search_tweets(keyword: str, place_id: str, max_results: int = 10) -> list:
    """Mencari tweet berdasarkan kata kunci dan menyertakan place_id."""
    client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN)
    try:
        res = client.search_recent_tweets(
            query=f'"{keyword}" lang:id -is:retweet',
            max_results=max_results,
            tweet_fields=["created_at", "text", "author_id", "lang", "public_metrics", "geo"],
            user_fields=["username", "name", "location", "verified"],
            expansions=["author_id", "geo.place_id"]
        )
    except tweepy.TweepyException as e:
        print(f"Error saat mencari tweet untuk '{keyword}': {e}")
        return []

    tweets_output = []
    if not res.data:
        return tweets_output

    users_dict = {user["id"]: user for user in res.includes.get("users", [])} if res.includes else {}

    for t in res.data:
        user_info = users_dict.get(t.author_id)
        tweets_output.append({
            "id_tweet": str(t.id),
            "place_id_source": place_id,
            "keyword_search": keyword,
            "created_at_tweet": t.created_at.isoformat(),
            "text_tweet": t.text,
            "lang_tweet": t.lang,
            "retweet_count": t.public_metrics.get("retweet_count", 0) if t.public_metrics else 0,
            "reply_count": t.public_metrics.get("reply_count", 0) if t.public_metrics else 0,
            "like_count": t.public_metrics.get("like_count", 0) if t.public_metrics else 0,
            "quote_count": t.public_metrics.get("quote_count", 0) if t.public_metrics else 0,
            "impression_count": t.public_metrics.get("impression_count", 0) if t.public_metrics else 0,
            "id_author_twitter": str(t.author_id),
            "author_username": user_info.username if user_info else None,
            "author_name": user_info.name if user_info else None,
            "author_location": user_info.location if user_info and user_info.location else None,
            "author_verified": user_info.verified if user_info else None,
            "tweet_geo_place_id": t.geo.get("place_id") if t.geo else None
        })
    return tweets_output

def extract_api_data_to_gcs(query_lokasi_wisata: str = "wisata di Malang"):
    """Fungsi untuk ekstraksi data API dan penyimpanan ke GCS (staging area)."""
    print(f"\n--- Memulai Ekstraksi Data API ke GCS untuk query: '{query_lokasi_wisata}' ---")
    initial_places_from_search = get_places(query_lokasi_wisata)

    if not initial_places_from_search:
        print(f"Tidak ada tempat yang ditemukan untuk query: '{query_lokasi_wisata}'.")
        return

    all_places_records = []
    all_reviews_records = []
    all_tweets_records = []
    processed_place_ids = set()

    for i, p_basic_search in enumerate(initial_places_from_search):
        place_id = p_basic_search.get("place_id")
        
        if not place_id:
            print(f"Melewati kandidat tempat tanpa place_id: {p_basic_search.get('name')}")
            continue
        
        if place_id in processed_place_ids:
            print(f"Melewati place_id {place_id} karena sudah diproses.")
            continue

        nama_tempat_search = p_basic_search.get("name", "Nama Tidak Diketahui")
        print(f"Memproses {i+1}/{len(initial_places_from_search)}: {nama_tempat_search} (ID: {place_id})")

        try:
            place_details_data, reviews_for_place = get_place_details_and_reviews(place_id)
            
            merged_place_record = {
                "place_id": place_id,
                "name": place_details_data.get("name_detail") or nama_tempat_search,
                "phone_number": place_details_data.get("formatted_phone_number"),
                "opening_hours_text": place_details_data.get("opening_hours_text"),
                "types": place_details_data.get("types_detail") or ", ".join(p_basic_search.get("types", [])),
                "address": place_details_data.get("formatted_address") or p_basic_search.get("formatted_address"),
                "lat": place_details_data.get("lat_detail") or p_basic_search.get("geometry", {}).get("location", {}).get("lat"),
                "lng": place_details_data.get("lng_detail") or p_basic_search.get("geometry", {}).get("location", {}).get("lng"),
                "rating_search": p_basic_search.get("rating"),
                "user_ratings_total_search": p_basic_search.get("user_ratings_total"),
                "data_source": "merged_api_data"
            }
            all_places_records.append(merged_place_record)

            all_reviews_records.extend(reviews_for_place)

            nama_untuk_tweet = merged_place_record.get("name")
            if nama_untuk_tweet:
                tweets_for_place = search_tweets(nama_untuk_tweet, place_id, max_results=10)
                all_tweets_records.extend(tweets_for_place)
            
            processed_place_ids.add(place_id)

        except requests.exceptions.RequestException as e:
            print(f"Error HTTP saat mengambil detail untuk place_id {place_id} ({nama_tempat_search}): {e}")
        except Exception as e:
            print(f"Error tidak terduga saat memproses place_id {place_id} ({nama_tempat_search}): {e}")
            import traceback
            traceback.print_exc()

    df_places = pd.DataFrame(all_places_records)
    df_reviews = pd.DataFrame(all_reviews_records)
    df_tweets = pd.DataFrame(all_tweets_records)

    current_date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    save_df_to_gcs(df_places, GCS_BUCKET_NAME_API, GCS_PLACES_PREFIX, f"places_data_{current_date_str}")
    save_df_to_gcs(df_reviews, GCS_BUCKET_NAME_API, GCS_REVIEWS_PREFIX, f"reviews_data_{current_date_str}")
    save_df_to_gcs(df_tweets, GCS_BUCKET_NAME_API, GCS_TWEETS_PREFIX, f"tweets_data_{current_date_str}")

    print("\nEkstraksi data API dan penyimpanan ke GCS selesai.")
    print(f"Total tempat unik diproses: {len(processed_place_ids)}")
    print(f"Total record tempat: {len(df_places)}")
    print(f"Total record review: {len(df_reviews)}")
    print(f"Total record tweet: {len(df_tweets)}")
