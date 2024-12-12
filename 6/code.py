# датасет: https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets?select=transactions_data.csv
import pandas as pd
import json
import os
import matplotlib.pyplot as plt
import seaborn as sns

file_path = 'transactions_data.csv'
data = pd.read_csv(file_path)

# 2. Провести анализ набора данных
# a. Объем памяти, который занимает файл на диске
file_size = os.path.getsize(file_path) / (1024 * 1024)

# b. Объем памяти, который занимает набор данных при загрузке в память
memory_usage = (data.memory_usage(deep=True) / (1024 * 1024)).sum()

# c. Вычислить для каждой колонки занимаемый объем памяти, долю от общего объема, а также выяснить тип данных
memory_info = data.memory_usage(deep=True)/ (1024 * 1024)
memory_info_df = pd.DataFrame({
    'Memory Usage (MB)': memory_info,
    'Percentage of Total Memory (%)': (memory_info / memory_usage) * 100,
    'Data Type': data.dtypes
})

# 3. Отсортировать по занимаемому объему памяти и сохранить в файл
memory_info_df = memory_info_df.sort_values(by='Memory Usage (MB)', ascending=False)
memory_info_df['Data Type'] = memory_info_df['Data Type'].astype(str)
memory_info_df.to_json('memory_usage_stats.json', orient='index')

# 4. Преобразовать все колонки с типом данных «object» в категориальные
for col in data.select_dtypes(include=['object']).columns:
    if data[col].nunique() < 0.5 * len(data):
        data[col] = data[col].astype('category')

# 5. Понижающее преобразование типов «int» колонок
int_cols = data.select_dtypes(include=['int']).columns
for col in int_cols:
    data[col] = pd.to_numeric(data[col], downcast='integer')

# 6. Понижающее преобразование типов «float» колонок
float_cols = data.select_dtypes(include=['float']).columns
for col in float_cols:
    data[col] = pd.to_numeric(data[col], downcast='float')

# 7. Повторный анализ набора данных
new_memory_usage = data.memory_usage(deep=True).sum() / (1024 * 1024)  # в МБ
new_memory_info = data.memory_usage(deep=True)
new_memory_info_df = pd.DataFrame({
    'Memory Usage (MB)': new_memory_info / (1024 * 1024),
    'Percentage of Total Memory (%)': (new_memory_info / new_memory_usage) * 100,
    'Data Type': data.dtypes
})

# Сравнение старого и нового объема памяти
print(f"Original Memory Usage: {memory_usage:.2f} MB")
print(f"New Memory Usage: {new_memory_usage:.2f} MB")

# 8. Выбор 10 колонок для дальнейшей работы и сохранение поднабора
selected_columns = ['id', 'date', 'client_id', 'amount', 'use_chip', 'merchant_id', 'merchant_city', 'merchant_state', 'zip', 'mcc']
data_subset = data[selected_columns].iloc[:5000]
data_subset[['amount', 'use_chip', 'merchant_city', 'merchant_state']] = data_subset[['amount', 'use_chip', 'merchant_city', 'merchant_state']].astype(str)
data_subset['date'] = pd.to_datetime(data_subset['date'], errors='coerce')
data_subset = data_subset.dropna(subset=['date', 'amount'])
data_subset.to_csv('optimized_subset.csv', index=False)

# 9. Построение графиков
plt.figure(figsize=(12, 6))
sns.countplot(data=data_subset, x='merchant_city')
plt.title('Количество транзакций по городам')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

plt.figure(figsize=(12, 6))
data_subset['amount'] = data_subset['amount'].replace({'\$': '', '': ''}, regex=True).astype(float)
sns.histplot(data_subset['amount'], bins=30, kde=True)
plt.title('Распределение сумм транзакций')
plt.xlabel('Сумма транзакции')
plt.ylabel('Частота')
plt.tight_layout()
plt.show()

daily_transactions = data_subset.resample('H', on='date').size()
plt.figure(figsize=(12, 6))
plt.plot(daily_transactions.index, daily_transactions.values, marker='o', color='skyblue', linestyle='-')
plt.title('Количество транзакций по часам')
plt.xlabel('Час')
plt.ylabel('Количество транзакций')
plt.xticks(rotation=45)
plt.grid()
plt.tight_layout()
plt.show()

chip_usage = data_subset['use_chip'].value_counts()
plt.figure(figsize=(8, 8))
plt.pie(chip_usage, labels=chip_usage.index, autopct='%1.1f%%', startangle=90, colors=['lightblue', 'lightcoral'])
plt.title('Распределение использования чипов')
plt.axis('equal') 
plt.show()

monthly_avg_amount = data_subset.resample('H', on='date')['amount'].mean()
plt.figure(figsize=(12, 6))
plt.plot(monthly_avg_amount.index, monthly_avg_amount.values, marker='o', color='orange', linestyle='-')
plt.title('Средняя сумма транзакций по часам')
plt.xlabel('Час')
plt.ylabel('Средняя сумма транзакции')
plt.xticks(rotation=45)
plt.grid()
plt.tight_layout()
plt.show()
