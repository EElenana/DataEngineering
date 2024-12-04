import sqlite3
import msgpack
import json
import pandas as pd
from sqlalchemy import create_engine
import pickle
    
# ---------------------------------------------------------Задание 5-----------------------------------------------------------

print("\n\nЗадание 5\n")
print('Описание предметной области:')
print('В датасете films.json представлена информация о наиболее популярных фильмах Кинопоиска')
print('В датасете awards.pk представлена информация о номинациях на различные награды различных фильмов')

conn = sqlite3.connect('5_movies.db')  
cursor = conn.cursor()

data_films = pd.read_json('5/films.json')
data_awards = pd.read_pickle('5/awards.pkl')

data_films.to_sql('films', conn, if_exists='replace', index=False)
data_awards.to_sql('awards', conn, if_exists='replace', index=False)

cursor.execute('''
CREATE TABLE IF NOT EXISTS films_awards AS
SELECT * FROM films
INNER JOIN awards ON films.id = awards.movieId
''')

conn.commit()



cursor.execute('''
SELECT name, COUNT(id) AS count_awards FROM films_awards 
GROUP BY id
ORDER BY COUNT(id) DESC
LIMIT 10
''')

most_awards_films = cursor.fetchall()
results = [{'name': row[0], 'count_awards': row[1]} for row in most_awards_films]

with open('5_most_awards_films.json', 'w', encoding ='utf8') as json_file:
    json.dump(results, json_file, ensure_ascii=False, indent=4)
    
    
    
cursor.execute('''
SELECT name, nomination_award_year, nomination_title FROM films_awards 
WHERE nomination_award_title=(?) AND nomination_title = (?)
''', ['Оскар', 'Лучшая мужская роль'])

oscar_films = cursor.fetchall()
results = [{'name': row[0], 'nomination_award_year': row[1], 'nomination_title': row[2]} for row in oscar_films]

with open('5_oscar_films.json', 'w', encoding ='utf8') as json_file:
    json.dump(results, json_file, ensure_ascii=False, indent=4)
    
    
    
cursor.execute('''
SELECT AVG(rating_kp), 
AVG(rating_imdb), 
AVG(rating_filmCritics),
AVG(ageRating),
AVG(budget_value)
FROM films 
''')

average_stat = cursor.fetchall()
results = [{'av_rating_kp': row[0], 
            'av_rating_imdb': row[1], 
            'av_rating_filmCritics': row[2],
            'av_ageRating': row[3],
            'av_budget_value': row[4]} for row in average_stat]

with open('5_average_stat.json', 'w', encoding ='utf8') as json_file:
    json.dump(results, json_file, ensure_ascii=False, indent=4)
    
    
    
cursor.execute('''
SELECT ageRating, COUNT(*) AS count_films 
FROM films 
GROUP BY ageRating 
ORDER BY count_films DESC
''')

count_age_rating = cursor.fetchall()
results = [{'ageRating': row[0], 'count': row[1]} for row in count_age_rating]

with open('5_count_age_rating.json', 'w', encoding ='utf8') as json_file:
    json.dump(results, json_file, ensure_ascii=False, indent=4)
    
    
cursor.execute('''
SELECT name, budget_value, budget_currency 
FROM films 
WHERE budget_value > 100000000 
ORDER BY budget_value DESC
''')

big_budget_films = cursor.fetchall()
results = [{'name': row[0], 'budget_value': row[1], 'budget_currency': row[2]} for row in big_budget_films]

with open('5_big_budget_films.json', 'w', encoding ='utf8') as json_file:
    json.dump(results, json_file, ensure_ascii=False, indent=4)
    
    
cursor.execute('''
SELECT nomination_title, COUNT(*) AS count_nomination
FROM awards 
GROUP BY nomination_title
ORDER BY count_nomination DESC
''')

count_nomination = cursor.fetchall()
results = [{'name': row[0], 'count': row[1]} for row in count_nomination]

with open('5_count_nomination.json', 'w', encoding ='utf8') as json_file:
    json.dump(results, json_file, ensure_ascii=False, indent=4)
    
    
cursor.execute('''
SELECT name, year, MAX(rating_kp)
FROM films 
GROUP BY year
ORDER BY year
''')

best_films_by_year = cursor.fetchall()
results = [{'name': row[0], 'year': row[1], 'max_rating': row[2]} for row in best_films_by_year]

with open('5_best_films_by_year.json', 'w', encoding ='utf8') as json_file:
    json.dump(results, json_file, ensure_ascii=False, indent=4)

conn.close()
