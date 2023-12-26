import pandas as pd
from collections import Counter
from datetime import datetime

customer_name= 'C'

# def parse_genres(genres_str):
#     try:
#         return eval(genres_str)
#     except:
#         return []

# def get_top_watched_genres(movies_data, top_n=3):
#     all_genres = []

#     for movie in movies_data:
#         genres = parse_genres(movie.get("genres", ""))
#         all_genres.extend(genres)

#     genre_names = [genre["name"] for genre in all_genres if isinstance(genre, dict)]

#     if genre_names:
#         top_watched_genres = Counter(genre_names).most_common(top_n)
#         return [genre for genre, _ in top_watched_genres]
#     else:
#         return None