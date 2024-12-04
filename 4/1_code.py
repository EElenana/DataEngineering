import sqlite3
import msgpack
import json
import pandas as pd
from sqlalchemy import create_engine
import pickle

VAR = 81

# ---------------------------------------------------------Задание 1-----------------------------------------------------------
print("Задание 1")

with open('1-2/item.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

conn = sqlite3.connect('1_books.db')  
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS books (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    author TEXT NOT NULL,
    genre TEXT NOT NULL,
    pages INTEGER NOT NULL,
    published_year INTEGER NOT NULL,
    isbn TEXT NOT NULL,
    rating REAL NOT NULL,
    views INTEGER NOT NULL
)
''')

for item in data:
    cursor.execute('''
    INSERT INTO books (title, author, genre, pages, published_year, isbn, rating, views)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (item['title'], item['author'], item['genre'], item['pages'], item['published_year'], item['isbn'], item['rating'], item['views']))

conn.commit()

# Сортировка
cursor.execute('SELECT * FROM books ORDER BY rating LIMIT ?', (VAR + 10,))
rows = cursor.fetchall()
columns = [column[0] for column in cursor.description]
result = [dict(zip(columns, row)) for row in rows]
with open('1_sorted.json', 'w', encoding='utf-8') as json_file:
    json.dump(result, json_file, ensure_ascii=False, indent=4)

#Статистики
cursor.execute('SELECT SUM(pages), MIN(pages), MAX(pages), AVG(pages) FROM books')
agg_results = cursor.fetchone()
print('\nСтатистика: ')
print('Сумма страниц:', agg_results[0])
print('Минимум страниц:', agg_results[1])
print('Максимум страниц:', agg_results[2])
print('Среднее страниц:', agg_results[3])

# Частота
cursor.execute('SELECT genre, COUNT(*) FROM books GROUP BY genre')
frequency_results = cursor.fetchall()
print("\nЧастота встречаемости жанров:")
for genre, count in frequency_results:
    print(f"{genre}: {count}")

# Фильтрация
cursor.execute('SELECT * FROM books WHERE genre = ? ORDER BY published_year LIMIT ?', ('приключения', VAR + 10))
filtered_rows = cursor.fetchall()
result = [dict(zip(columns, row)) for row in filtered_rows]

with open('1_filtered.json', 'w', encoding='utf-8') as json_file:
    json.dump(result, json_file, ensure_ascii=False, indent=4)

conn.close()
