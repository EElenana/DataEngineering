import sqlite3
import msgpack
import json
import pandas as pd
from sqlalchemy import create_engine
import pickle

VAR = 81


# ---------------------------------------------------------Задание 2-----------------------------------------------------------
print("\n\nЗадание 2")
with open('1-2/subitem.msgpack', 'rb') as f:
    sales_data = msgpack.unpack(f)

conn = sqlite3.connect('1_books.db')  
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS book_sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    price INTEGER NOT NULL,
    place TEXT NOT NULL,
    date TEXT NOT NULL,
    FOREIGN KEY (title) REFERENCES books (title)
)
''')

for item in sales_data:
    cursor.execute('''
    INSERT INTO book_sales (title, price, place, date)
    VALUES (?, ?, ?, ?)
    ''', (item['title'], item['price'], item['place'], item['date']))

conn.commit()

# Количество проданных книг автора Оскар Уайльд
author = 'Оскар Уайльд'
cursor.execute('''
SELECT bs.title, COUNT(*) AS sales_count 
FROM book_sales bs
JOIN books b ON bs.title = b.title
WHERE b.author = ?
GROUP BY bs.title
ORDER BY sales_count DESC
''', (author,))

author_books = cursor.fetchall()
print(f"\nПроданные книги автора {author}:")
for book, count in author_books:
    print(f"{book}: {count}")

# Средний рейтинг книг, проданных 7.1.2023
cursor.execute('''
SELECT AVG(b.rating) AS avg_price
FROM books b
JOIN book_sales bs ON b.title = bs.title
WHERE bs.date = ?
''', ('7.1.2023', ))
rating_books = cursor.fetchall()
print("\nСредний рейтинг книг, проданных 7.1.2023:")
for record in rating_books:
    print(*record)

# Количество продаж для каждого жанра
cursor.execute('''
SELECT b.genre, COUNT(*) AS sales_count 
FROM book_sales bs
JOIN books b ON bs.title = b.title
GROUP BY b.genre
''')
sales_count = cursor.fetchall()
print("\nКоличество продаж для каждого жанра:")
for genre, count in sales_count:
    print(f"{genre}: {count}")

conn.close()
