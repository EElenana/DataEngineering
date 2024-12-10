# ---------------------------------------------------------Задание 1-----------------------------------------------------------
# Запуск сервера mongodb осуществлялся через Docker
import pandas as pd
from pymongo import MongoClient
import json
from bson import ObjectId

def convert_objectid_to_str(data):
    if isinstance(data, list):
        return [convert_objectid_to_str(item) for item in data]
    elif isinstance(data, dict):
        return {key: convert_objectid_to_str(value) for key, value in data.items()}
    elif isinstance(data, ObjectId):
        return str(data)
    return data


data = pd.read_csv('task_1_item.csv', sep=';')

client = MongoClient('mongodb://localhost:27017/')
db = client['db'] 
collection = db['1_db'] 

#collection.insert_many(data.to_dict('records'))

# 1. Вывод первых 10 записей, отсортированных по убыванию по полю salary
result_1 = list(collection.find().sort('salary', -1).limit(10))
result_1 = convert_objectid_to_str(result_1)
with open('1_1.json', 'w', encoding='utf-8') as f:
    json.dump(result_1, f, ensure_ascii=False, indent=4)

# 2. Вывод первых 15 записей, отфильтрованных по предикату age < 30, отсортированных по убыванию по полю salary
result_2 = list(collection.find({'age': {'$lt': 30}}).sort('salary', -1).limit(15))
result_2 = convert_objectid_to_str(result_2)
with open('1_2.json', 'w', encoding='utf-8') as f:
    json.dump(result_2, f, ensure_ascii=False, indent=4)

# 3. Вывод первых 10 записей, отфильтрованных по сложному предикату
result_3 = list(collection.find({
    'city': 'Хихон', 
    'job': {'$in': ['Программист', 'Учитель', 'Менеджер']} 
}).sort('age', 1).limit(10))
result_3 = convert_objectid_to_str(result_3)
with open('1_3.json', 'w', encoding='utf-8') as f:
    json.dump(result_3, f, ensure_ascii=False, indent=4)

# 4. Вывод количества записей, получаемых в результате фильтрации
result_4 = collection.count_documents({
    'age': {'$gte': 20, '$lte': 40}, 
    'year': {'$gte': 2019, '$lte': 2022},
    '$or': [
        {'salary': {'$gt': 50000, '$lte': 75000}},
        {'salary': {'$gt': 125000, '$lt': 150000}}
    ]
})
result_4 = convert_objectid_to_str(result_4)
with open('1_4.json', 'w', encoding='utf-8') as f:
    json.dump(result_4, f, ensure_ascii=False, indent=4)
    
    
# ---------------------------------------------------------Задание 2-----------------------------------------------------------
with open('task_2_item.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

#collection.insert_many(data)

# 1. Вывод минимальной, средней, максимальной salary
salary_stats = list(collection.aggregate([
    {
        '$group': {
            '_id': None,
            'min_salary': {'$min': '$salary'},
            'avg_salary': {'$avg': '$salary'},
            'max_salary': {'$max': '$salary'}
        }
    }
]))

# 2. Вывод количества данных по представленным профессиям
job_counts = list(collection.aggregate([
    {
        '$group': {
            '_id': '$job',
            'count': {'$sum': 1}
        }
    }
]))

# 3. Вывод минимальной, средней, максимальной salary по городу
salary_by_city = list(collection.aggregate([
    {
        '$group': {
            '_id': '$city',
            'min_salary': {'$min': '$salary'},
            'avg_salary': {'$avg': '$salary'},
            'max_salary': {'$max': '$salary'}
        }
    }
]))

# 4. Вывод минимальной, средней, максимальной salary по профессии
salary_by_job = list(collection.aggregate([
    {
        '$group': {
            '_id': '$job',
            'min_salary': {'$min': '$salary'},
            'avg_salary': {'$avg': '$salary'},
            'max_salary': {'$max': '$salary'}
        }
    }
]))

# 5. Вывод минимального, среднего, максимального возраста по городу
age_by_city = list(collection.aggregate([
    {
        '$group': {
            '_id': '$city',
            'min_age': {'$min': '$age'},
            'avg_age': {'$avg': '$age'},
            'max_age': {'$max': '$age'}
        }
    }
]))

# 6. Вывод минимального, среднего, максимального возраста по профессии
age_by_job = list(collection.aggregate([
    {
        '$group': {
            '_id': '$job',
            'min_age': {'$min': '$age'},
            'avg_age': {'$avg': '$age'},
            'max_age': {'$max': '$age'}
        }
    }
]))

# 7. Вывод максимальной заработной платы при минимальном возрасте
min_age_result = collection.aggregate([
    {
        '$group': {
            '_id': None,
            'min_age': {'$min': '$age'}
        }
    }
])

min_age = list(min_age_result)[0]['min_age']

max_salary_min_age = list(collection.aggregate([
    {
        '$match': {
            'age': min_age  
        }
    },
    {
        '$group': {
            '_id': None,
            'max_salary': {'$max': '$salary'}
        }
    }
]))

# 8. Вывод минимальной заработной платы при максимальном возрасте
max_age_result = collection.aggregate([
    {
        '$group': {
            '_id': None,
            'max_age': {'$max': '$age'}
        }
    }
])

max_age = list(max_age_result)[0]['max_age']

min_salary_max_age = list(collection.aggregate([
    {
        '$match': {
            'age': max_age  
        }
    },
    {
        '$group': {
            '_id': None,
            'min_salary': {'$min': '$salary'}
        }
    }
]))

# 9. Вывод минимального, среднего, максимального возраста по городу, при условии, что заработная плата больше 50 000, отсортировать вывод по убыванию по полю avg
age_by_city_high_salary = list(collection.aggregate([
    {
        '$match': {
            'salary': {'$gt': 50000}
        }
    },
    {
        '$group': {
            '_id': '$city',
            'min_age': {'$min': '$age'},
            'avg_age': {'$avg': '$age'},
            'max_age': {'$max': '$age'}
        }
    },
    {
        '$sort': {
            'avg_age': -1 
        }
    }
]))

# 10. Вывод минимальной, средней, максимальной salary в произвольно заданных диапазонах по городу, профессии, и возрасту: 18<age<25 & 50<age<65
salary_in_age_ranges = list(collection.aggregate([
    {
        '$match': {
            '$or': [
                {'age': {'$gt': 18, '$lt': 25}},
                {'age': {'$gt': 50, '$lt': 65}}
            ]
        }
    },
    {
        '$group': {
            '_id': {
                'city': '$city',
                'job': '$job'
            },
            'min_salary': {'$min': '$salary'},
            'avg_salary': {'$avg': '$salary'},
            'max_salary': {'$max': '$salary'}
        }
    }
]))

# 11. Вывод количества сотрудников по профессиям с зарплатой больше 30 000, отсортированный по убыванию
employee_count_by_job = list(collection.aggregate([
    {
        '$match': {
            'salary': {'$gt': 30000}  
        }
    },
    {
        '$group': {
            '_id': '$job',
            'count': {'$sum': 1}
        }
    },
    {
        '$sort': {
            'count': -1  
        }
    }
]))

output = {
    "salary_stats": convert_objectid_to_str(salary_stats),
    "job_counts": convert_objectid_to_str(job_counts),
    "salary_by_city": convert_objectid_to_str(salary_by_city),
    "salary_by_job": convert_objectid_to_str(salary_by_job),
    "age_by_city": convert_objectid_to_str(age_by_city),
    "age_by_job": convert_objectid_to_str(age_by_job),
    "max_salary_min_age": convert_objectid_to_str(max_salary_min_age),
    "min_salary_max_age": convert_objectid_to_str(min_salary_max_age),
    "age_by_city_high_salary": convert_objectid_to_str(age_by_city_high_salary),
    "salary_in_age_ranges": convert_objectid_to_str(salary_in_age_ranges),
    "employee_count_by_job": convert_objectid_to_str(employee_count_by_job),
}

with open('2.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=4)
    
    
# ---------------------------------------------------------Задание 3-----------------------------------------------------------
def read_data_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = f.read().strip().split('=====')
    
    records = []
    for entry in data:
        if entry.strip():  
            record = {}
            for line in entry.strip().split('\n'):
                key, value = line.split('::')
                record[key.strip()] = value.strip()
            records.append(record)
    
    return records

data = read_data_from_file('task_3_item.text')

for record in data:
    if 'age' in record:
        record['age'] = int(record['age'])
    if 'salary' in record:
        record['salary'] = int(record['salary'])
    if 'year' in record:
        record['year'] = int(record['year'])

#collection.insert_many(data)

# 1. Удалить из коллекции документы по предикату: salary < 25 000 || salary > 175000
collection.delete_many({'$or': [{'salary': {'$lt': 25000}}, {'salary': {'$gt': 175000}}]})

# 2. Увеличить возраст (age) всех документов на 1
collection.update_many({}, {'$inc': {'age': 1}})

# 3. Поднять заработную плату на 5% для произвольно выбранных профессий
collection.update_many({'job': 'Водитель'}, {'$mul': {'salary': 1.05}})

# 4. Поднять заработную плату на 7% для произвольно выбранных городов
collection.update_many({'city': 'Санкт-Петербург'}, {'$mul': {'salary': 1.07}})

# 5. Поднять заработную плату на 10% для выборки по сложному предикату
collection.update_many(
    {
        'city': 'Краков',
        'job': {'$in': ['Бухгалтер', 'Учитель']},
        'age': {'$gt': 30, '$lt': 50}
    },
    {'$mul': {'salary': 1.10}}
)

# 6. Удалить из коллекции записи по произвольному предикату
collection.delete_many({'job': 'Менеджер'})



# ---------------------------------------------------------Задание 5-----------------------------------------------------------

# Подключение к MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['db2'] 
collection = db['properties'] 

csv_data = pd.read_csv('task_5_1.csv')
json_data = pd.read_json('task_5_2.json', lines=True)

collection.insert_many(json_data.to_dict('records'))
collection.insert_many(csv_data.to_dict('records'))

# Запросы на выборку
results = {}

# 1. Первые 10 записей, отсортированных по убыванию по полю 'bedrooms'
results['first_10_sorted_by_bedrooms'] = convert_objectid_to_str(list(collection.find().sort('bedrooms', -1).limit(10)))

# 2. Первые 15 записей, отфильтрованных по 'bathrooms' > 1, отсортированных по убыванию по полю 'bedrooms'
results['first_15_filtered_by_bathrooms'] = convert_objectid_to_str(list(collection.find({'bathrooms': {'$gt': 1}}).sort('bedrooms', -1).limit(15)))

# 3. Первые 10 записей, отфильтрованных по сложному предикату, отсортированных по возрастанию по полю 'livingRooms'
results['first_10_filtered_complex'] = convert_objectid_to_str(list(collection.find({'bedrooms': {'$gt': 1}, 'bathrooms': {'$lt': 3}}).sort('livingRooms', 1).limit(10)))

# 4. Количество записей, получаемых в результате фильтрации (bathrooms > 1)
results['count_filtered_bathrooms'] = collection.count_documents({'bathrooms': {'$gt': 1}})

# 5. Первые 5 записей, отсортированных по 'postcode'
results['first_5_sorted_by_postcode'] = convert_objectid_to_str(list(collection.find().sort('postcode', 1).limit(5)))

# Запросы на агрегацию
results['aggregation'] = {}

# 1. Минимальное количество 'bedrooms'
results['aggregation']['min_bedrooms'] = convert_objectid_to_str(list(collection.aggregate([{'$group': {'_id': None, 'min_bedrooms': {'$min': '$bedrooms'}}}]))[0])

# 2. Минимальное, среднее и максимальное значение 'sale'
results['aggregation']['sale_stats'] = convert_objectid_to_str(list(collection.aggregate([
        {
        '$group': {
            '_id': None,
            'min_sale': {'$min': '$sale'},
            'avg_sale': {'$avg': '$sale'},
            'max_sale': {'$max': '$sale'}
        }
    }
])))

# 3. Минимальное, среднее и максимальное количество 'bedrooms' с фильтрацией по 'tenure'
results['aggregation']['bedrooms_stats'] = convert_objectid_to_str(list(collection.aggregate([
    {'$match': {'tenure': 'Leasehold'}},
    {'$group': {
        '_id': None,
        'min_bedrooms': {'$min': '$bedrooms'},
        'avg_bedrooms': {'$avg': '$bedrooms'},
        'max_bedrooms': {'$max': '$bedrooms'}
    }}
]))[0])

# 4. Минимальное, среднее и максимальное количество 'livingRooms' по 'tenure' с условием, что 'sale' > 700000, сортированные по убыванию среднего 'livingRooms'
results['aggregation']['livingRooms_by_tenure'] = convert_objectid_to_str(list(collection.aggregate([
    {'$match': {'sale': {'$gt': 2000000}}}, 
    {'$group': {
        '_id': '$tenure',
        'min_livingRooms': {'$min': '$livingRooms'},
        'avg_livingRooms': {'$avg': '$livingRooms'},
        'max_livingRooms': {'$max': '$livingRooms'}
    }},
    {'$sort': {'avg_livingRooms': -1}}  
])))

# 5. Количество записей с 'postcode' = 'E1 6GS'
results['count_postcode_E1_6GS'] = collection.count_documents({'postcode': 'E1 6GS'})

# Запросы на изменение данных

# 1. Обновление 'sale' на 5% для всех помещений, где tenure = 'Feudal'
collection.update_many(
    {'tenure': 'Feudal'},  
    [{'$set': {'sale': {'$multiply': ['$sale', 1.05]}}}] 
)

# 2. Изменение 'tenure' для 'postcode' = 'E1 6GS'
collection.update_many({'postcode': 'E1 6GS'}, {'$set': {'tenure': 'Freehold'}})

# 3. Удаление помещений с количеством комнат = 2
collection.delete_many({'bedrooms': {'$lt': 2}})

# 4. Удаление документов, где sale < 300000 или sale > 5000000
collection.delete_many({
    '$or': [
        {'sale': {'$lt': 300000}},
        {'sale': {'$gt': 5000000}}
    ]
})

# 5. Увеличение 'sale' на 50000 для всех документов, где postcode = 'E1 8GE'
collection.update_many(
    {'postcode': 'E1 8GE'},  
    {'$inc': {'sale': 50000}}  
)

with open('5.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=4)
