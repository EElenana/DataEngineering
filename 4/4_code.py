import sqlite3
import msgpack
import json
import pandas as pd
from sqlalchemy import create_engine
import pickle

VAR = 81

# ---------------------------------------------------------Задание 4-----------------------------------------------------------

print('\n\nЗадание 4')
conn = sqlite3.connect('4_products.db', timeout=10)
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    price FLOAT,
    quantity INTEGER,
    category TEXT,
    fromCity TEXT,
    isAvailable BOOLEAN,
    views INTEGER,
    update_counter INTEGER DEFAULT 0
)
''')

def load_product_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        product_data = file.read().strip().split('=====')
        for product in product_data:
            product_info = {}
            for line in product.strip().split('\n'):
                if '::' in line:
                    key, value = line.split('::', 1)
                    product_info[key.strip()] = value.strip()
                    
            if all(key in product_info for key in ['name', 'price', 'quantity', 'fromCity', 'isAvailable', 'views']):
                name = product_info['name']
                price = float(product_info['price'])
                quantity = int(product_info['quantity'])
                category = product_info.get('category', 'Без категории')  
                fromCity = product_info['fromCity']
                isAvailable = product_info['isAvailable'] == 'True'
                views = int(product_info['views'])

                cursor.execute('''
                INSERT INTO products (name, price, quantity, category, fromCity, isAvailable, views)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (name, price, quantity, category, fromCity, isAvailable, views))

    conn.commit()

def update_product_data(file_path):
    with open(file_path, 'rb') as file:
        updates = pickle.load(file)

        for update in updates:
            name = update['name']
            method = update['method']
            param = update['param']

            if method == 'quantity_add' or method == 'quantity_sub':
                cursor.execute('SELECT quantity FROM products WHERE name = ?', (name,))
                current_quantity = cursor.fetchone()
                if current_quantity:
                    new_quantity = current_quantity[0] + param
                    if new_quantity >= 0:
                        cursor.execute('''
                        UPDATE products
                        SET quantity = ?, update_counter = update_counter + 1
                        WHERE name = ?
                        ''', (new_quantity, name))

            elif method == 'remove':
                cursor.execute('''
                DELETE FROM products
                WHERE name = ?
                ''', (name,))

            elif method == 'price_abs':
                cursor.execute('SELECT price FROM products WHERE name = ?', (name,))
                current_price = cursor.fetchone()
                if current_price:
                    new_price = current_price[0] + param
                    if new_price >= 0:
                        cursor.execute('''
                        UPDATE products
                        SET price = ?, update_counter = update_counter + 1
                        WHERE name = ?
                        ''', (new_price, name))

            elif method == 'price_percent':
                cursor.execute('SELECT price FROM products WHERE name = ?', (name,))
                current_price = cursor.fetchone()
                if current_price:
                    new_price = current_price[0] * (1 + param / 100)
                    cursor.execute('''
                    UPDATE products
                    SET price = ?, update_counter = update_counter + 1
                    WHERE name = ?
                    ''', (new_price, name))

            elif method == 'available':
                cursor.execute('''
                UPDATE products
                SET isAvailable = ?, update_counter = update_counter + 1
                WHERE name = ?
                ''', (param, name))

    conn.commit()
    
def get_top_10_updated_products():
    cursor.execute('''
    SELECT name, fromCity, update_counter
    FROM products
    ORDER BY update_counter DESC
    LIMIT 10
    ''')
    top_products = cursor.fetchall()
    print("Топ-10 самых обновляемых товаров:")
    for product in top_products:
        print(f"Название: {product[0]}, Город: {product[1]}, Количество обновлений: {product[2]}")
        
def analyze_prices():
    cursor.execute('''
    SELECT category, 
           COUNT(*) AS count, 
           SUM(price) AS total_price, 
           MIN(price) AS min_price, 
           MAX(price) AS max_price, 
           AVG(price) AS avg_price
    FROM products
    GROUP BY category
    ''')
    price_analysis = cursor.fetchall()
    print("\nАнализ цен товаров по категориям:")
    for row in price_analysis:
        print(f"Категория: {row[0]}, Количество: {row[1]}, Сумма: {row[2]}, Минимум: {row[3]}, Максимум: {row[4]}, Среднее: {row[5]}")
        
def analyze_quantities():
    cursor.execute('''
    SELECT category, 
           COUNT(*) AS count, 
           SUM(quantity) AS total_quantity, 
           MIN(quantity) AS min_quantity, 
           MAX(quantity) AS max_quantity, 
           AVG(quantity) AS avg_quantity
    FROM products
    GROUP BY category
    ''')
    quantity_analysis = cursor.fetchall()
    print("\nАнализ остатков товаров по категориям:")
    for row in quantity_analysis:
        print(f"Категория: {row[0]}, Количество: {row[1]}, Сумма: {row[2]}, Минимум: {row[3]}, Максимум: {row[4]}, Среднее: {row[5]}")
        
def get_available_products():
    cursor.execute('''
    SELECT name, price, quantity, fromCity
    FROM products
    WHERE isAvailable = 1
    ''')
    available_products = cursor.fetchall()
    print("\nДоступные товары:")
    for product in available_products:
        print(f"Название: {product[0]}, Цена: {product[1]}, Количество: {product[2]}, Город: {product[3]}")

try:
    load_product_data('4/_product_data.text')
    update_product_data('4/_update_data.pkl')
    get_top_10_updated_products()
    analyze_prices()
    analyze_quantities()
    get_available_products()
except Exception as e:
    print(f'Error: {e}')
finally:
    conn.close()
