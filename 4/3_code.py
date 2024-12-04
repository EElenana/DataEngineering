import sqlite3
import msgpack
import json
import pandas as pd
from sqlalchemy import create_engine
import pickle

VAR = 81


# ---------------------------------------------------------Задание 3-----------------------------------------------------------
print("\n\nЗадание 3\n")

conn = sqlite3.connect('3_music.db')  
cursor = conn.cursor()

data_part_1 = pd.read_json('3/_part_1.json')

data_part_2 = pd.read_pickle('3/_part_2.pkl')

data_part_2 = pd.DataFrame(data_part_2)

data_part_2['duration_ms'] = data_part_2['duration_ms'].astype(int)
data_part_2['year'] = data_part_2['year'].astype(int)
data_part_2['popularity'] = data_part_2['popularity'].astype(int)
data_part_2['tempo'] = data_part_2['tempo'].astype(float)

combined_data = pd.merge(data_part_1, data_part_2, on=['artist', 'song', 'duration_ms', 'year', 'tempo', 'genre', 'popularity'], how='outer')
combined_data = combined_data.drop_duplicates(keep='first')

combined_data.to_sql('music_data', conn, if_exists='replace', index=False)

# Сортировка
sorted_data = pd.read_sql_query("SELECT * FROM music_data ORDER BY popularity LIMIT ?", conn, params=(VAR + 10,))
sorted_data.to_json('3_sorted.json', orient='records', lines=True)

# Статистики
stats = pd.read_sql_query("""
SELECT
    SUM(tempo) AS total_popularity,
    MIN(tempo) AS min_popularity,
    MAX(tempo) AS max_popularity,
    AVG(tempo) AS avg_popularity
FROM music_data
""", conn)

print("Статистика по tempo:")
print('Сумма tempo:', stats.iloc[0, 0])
print('Минимум tempo:', stats.iloc[0, 1])
print('Максимум tempo:', stats.iloc[0, 2])
print('Среднее tempo:', stats.iloc[0, 3])

# Частота
frequency = pd.read_sql_query("""
SELECT artist, COUNT(*) AS frequency
FROM music_data
GROUP BY artist
""", conn)
print("\nЧастота встречаемости по авторам:")
print(frequency)

# Фильтрация
filtered_data = pd.read_sql_query("""
SELECT * FROM music_data
WHERE duration_ms > 20000
ORDER BY duration_ms
LIMIT ?
""", conn, params=(VAR + 15,))
filtered_data.to_json('3_filtered.json', orient='records', lines=True)

conn.commit()
conn.close()
