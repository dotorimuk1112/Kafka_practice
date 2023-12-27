import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from myconfig import customer_name

# 5000개의 영화 데이터를 로드
rawdf = pd.read_csv('C:/Users/CHK/Desktop/kafka/kafka/kafka1/dfs/tmdb_5000_movies.csv')

# 무작위로 30개 행을 뽑아서 {이름}.csv로 저장
sample_df = rawdf.sample(n=30)
csv_filename = f'C:/Users/CHK/Desktop/kafka/kafka/kafka1/dfs/{customer_name}.csv'
sample_df.to_csv(csv_filename, index=False)  # 결과 CSV 파일로 저장

# Kafka producer 설정
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Kafka 서버 주소로 변경
    value_serializer=lambda v: str(v).encode('utf-8')
)

# Kafka 토픽에 메시지 전송
try:
    print('producer start')
    producer.send(customer_name, customer_name)
except KafkaError as e:
    print(f"Error while producing message: {e}")
finally:
    producer.close()

