import numpy as np
import json
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# Задание 1
data = np.load('first_task.npy')

res = {}
res['sum'] = int(np.sum(data))
res['avr'] = float(np.mean(data))

main_diagonal = np.diagonal(data)
res['sumMD'] = int(np.sum(main_diagonal))
res['avrMD'] = float(np.mean(main_diagonal))

secondary_diagonal = np.diagonal(np.fliplr(data))
res['sumSD'] = int(np.sum(secondary_diagonal))
res['avrSD'] = float(np.mean(secondary_diagonal))

res['max'] = int(np.max(data))
res['min'] = int(np.min(data))

with open('1.1.json', 'w') as json_file:
    json.dump(res, json_file, indent=4)

normalized_data = (data - np.min(data)) / (np.max(data) - np.min(data))

np.save('1.2.npy', normalized_data)


# Задание 2
data = np.load('second_task.npy')

x = np.argwhere(data > 581)[:, 0]
y = np.argwhere(data > 581)[:, 1]
z = data[data > 581]  

np.savez('2.npz', row_indices=x, col_indices=y, values=z)
np.savez_compressed('2_compressed.npz', row_indices=x, col_indices=y, values=z)

size_npz = os.path.getsize('2.npz')
size_compressed_npz = os.path.getsize('2_compressed.npz')

print("Задача 2:")
print(f"Размер файла data.npz: {size_npz} байт")
print(f"Размер файла data_compressed.npz: {size_compressed_npz} байт")
print(f"Размер сжатого файла меньше в: {round(size_npz/size_compressed_npz, 2)} раз")


# Задача 3 
with open('third_task.json', 'r', encoding='utf-8') as json_file:
    data = json.load(json_file)
    
df = pd.DataFrame(data)

gruped_df = df.groupby('name').agg(
    average_price=('price', 'mean'),
    max_price=('price', 'max'),
    min_price=('price', 'min')
).reset_index()

gruped_df.to_json('3.json', orient='records', lines=True)

table = pa.Table.from_pandas(gruped_df)
with pa.OSFile('3.msgpack', 'wb') as f:
    pq.write_table(table, f)

size_json = os.path.getsize('3.json')
size_msgpack = os.path.getsize('3.msgpack')

print("\nЗадача 3:")
print(f"Размер файла aggregated_data.json: {size_json} байт")
print(f"Размер файла aggregated_data.msgpack: {size_msgpack} байт")
print(f"Размер msgpack файла меньше в: {round(size_json/size_msgpack, 2)} раз")


# Задание 4
products = pd.read_pickle('fourth_task_products.json')
products_df = pd.DataFrame(products)

with open('fourth_task_updates.json', 'r', encoding='utf-8') as json_file:
    updates = json.load(json_file)
    
for update in updates:
    product_name = update['name']
    method = update['method']
    param = update['param']
    
    if product_name in products_df['name'].values:
        if method == "add":
            products_df.loc[products_df['name'] == product_name, 'price'] += param
        elif method == "sub":
            products_df.loc[products_df['name'] == product_name, 'price'] -= param
        elif method == "percent+":
            products_df.loc[products_df['name'] == product_name, 'price'] *= (1 + param)
        elif method == "percent-":
            products_df.loc[products_df['name'] == product_name, 'price'] *= (1 - param)

products_df.to_pickle('4.pkl')


# Задание 5
df = pd.read_csv('kaggle_london_house_price_data.csv')
df = df[df.columns[:10]]
stats = {}

numerical_fields = df.select_dtypes(include=['float64', 'int64']).columns
for field in numerical_fields:
    stats[field] = {
        'max': df[field].max(),
        'min': df[field].min(),
        'mean': df[field].mean(),
        'sum': df[field].sum(),
        'std': df[field].std()
    }
    
categorical_fields = df.select_dtypes(include=['object']).columns
for field in categorical_fields:
    stats[field] = df[field].value_counts().to_dict()

with open('5_stat.json', 'w', encoding='utf-8') as json_file:
    json.dump(stats, json_file, ensure_ascii=False, indent=4)

df.to_csv('5.csv', index=False)
df.to_json('5.json', orient='records', lines=True)

table = pa.Table.from_pandas(df)
with pa.OSFile('5.msgpack', 'wb') as f:
    pq.write_table(table, f)

df.to_pickle('5.pkl')

file_sizes = {
    'csv': os.path.getsize('5.csv'),
    'json': os.path.getsize('5.json'),
    'msgpack': os.path.getsize('5.msgpack'),
    'pkl': os.path.getsize('5.pkl')
}

print("\nЗадача 5:")
for format_type, size in file_sizes.items():
    print(f"{format_type}: {size} байт")
    
min_size_format = min(file_sizes, key=file_sizes.get)
min_size_value = file_sizes[min_size_format]

print(f"Файл минимального размера: {min_size_format}, Размер: {min_size_value} байт")
