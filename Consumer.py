from kafka import KafkaConsumer
from myconfig import customer_name
import pandas as pd

rawdf = pd.read_csv('C:/Users/CHK/Desktop/kafka/kafka/kafka1/dfs/tmdb_5000_movies.csv')

# Kafka consumer 설정
consumer = KafkaConsumer(
    'customer_movies',
    group_id='customer_group',
    bootstrap_servers=['localhost:9092'],  # Kafka 서버 주소로 변경
    value_deserializer=lambda x: x.decode('utf-8')
)

# Kafka 토픽에서 메시지 수신
for message in consumer:
    if message.value == customer_name:
        # 메시지가 해당 고객의 이름과 일치할 경우, A.csv를 처리하는 로직을 추가

        # A.csv 파일 읽기
        try:
            df = pd.read_csv(f'C:/Users/CHK/Desktop/kafka/kafka/kafka1/dfs/{customer_name}.csv')

            # 30개의 영화 중에서 가장 많이 등장한 장르 찾기
            top_genre = df['genres'].str.split(', ', expand=True).stack().value_counts().idxmax()
            print(top_genre)
            
            # rawdf에서 해당 장르의 영화 중 A.csv에 없는 영화 찾기
            genre_movies = rawdf[rawdf['genres'].str.contains(top_genre, case=False, na=False)]
            unseen_genre_movies = genre_movies[~genre_movies['original_title'].isin(df['original_title'])]

            print(f"Recommended movies for customer {customer_name} in the most watched genre '{top_genre}':")
            print(unseen_genre_movies[['original_title', 'genres']].head())

            # 추천 결과를 CSV 파일로 저장
            recommended_csv_filename = f'C:/Users/CHK/Desktop/kafka/kafka/kafka1/results/{customer_name}_recommended.csv'
            unseen_genre_movies.to_csv(recommended_csv_filename, index=False)

        except FileNotFoundError:
            print(f"{customer_name}.csv not found.")