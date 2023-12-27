import pandas as pd
import json
import random
from kafka import KafkaProducer
from kafka.errors import KafkaError
import myconfig

# 5000개의 영화 데이터를 로드
rawdf = pd.read_csv('kafka1/dfs/tmdb_5000_movies.csv')

# 1. 랜덤하게 100개 행을 뽑아서 고객 A의 시청 목록 A.csv로 생성
customer_a_watchlist = rawdf.sample(n=100)
watchlist_filename = f'{myconfig.customer_name}.csv'
customer_a_watchlist.to_csv(f'C:/Users/CHK/Desktop/kafka/kafka/kafka1/dfs/{watchlist_filename}', index=False)

# 2. A.csv에서 가장 많이 본 장르와 그 장르에 맞는 영화 목록을 A_favorites.csv로 생성
# 장르 컬럼을 JSON 파싱하여 각 장르를 리스트로 추출
customer_a_watchlist['genres'] = customer_a_watchlist['genres'].apply(lambda x: json.loads(x.replace("'", "\"")))

# 모든 장르 리스트를 하나의 리스트로 펼침
all_genres = [genre['name'] for genres in customer_a_watchlist['genres'] for genre in genres]

# 가장 많이 등장한 장르를 찾음
most_watched_genre = max(set(all_genres), key=all_genres.count)

# 해당 장르에 맞는 영화 목록을 추출
customer_a_favorites = rawdf[rawdf['genres'].apply(lambda x: most_watched_genre in [genre['name'] for genre in json.loads(x.replace("'", "\""))])].head(10)
favorites_filename = f'{myconfig.customer_name}_favorites.csv'
customer_a_favorites.to_csv(f'C:/Users/CHK/Desktop/kafka/kafka/kafka1/fav_dfs/{favorites_filename}', index=False)

# Kafka producer 설정
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda v: str(v).encode('utf-8'),  # key에 대한 serializer 추가
    value_serializer=lambda v: str(v).encode('utf-8')
)

# 3. Topic은 A, key는 장르, value는 영화 제목 형태의 메시지를 브로커에 저장
try:
    # Send most-watched genre as the key and favorites as the value
    producer.send(myconfig.customer_name, key=most_watched_genre, value=favorites_filename)
    print(f"{myconfig.customer_name}님이 가장 좋아하시는 영화를 추출했습니다.")
    print(f"Produced message for customer A: Most-watched genre - {most_watched_genre}, Favorites - {favorites_filename}")
except KafkaError as e:
    print(f"Error while producing message for customer A: {e}")
finally:
    producer.close()
