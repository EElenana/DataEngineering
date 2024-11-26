import pandas as pd
from bs4 import BeautifulSoup
import re
import json
from statistics import mode, median, variance
import requests
import numpy as np

def save_json(n, data):
    with open(f'{n}.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        
def statistics(data):
    sum_data = sum(data)
    min_data = min(data)
    max_data = max(data)
    mean_data = sum(data)/len(data)
    mode_data = mode(data)
    median_data = median(data)
    variance_data = variance(data)
    print(f'Статистика:')
    print(f'Сумма: {sum_data}\nМинимум: {min_data}\nМаксимум: {max_data}\nСреднее: {mean_data}\nМода: {mode_data}\nМедиана: {median_data}\nДисперсия: {variance_data}')

def convert_values(data):
    for entry in data:
        for key in entry:
            if key in ['Year', 'id']:
                continue
            value = entry[key]
            if value in ['–*)', '–', '-']:
                entry[key] = np.nan 
            else:
                value = value.replace(',', '.').replace(' ', '')
                try:
                    entry[key] = float(value)
                except ValueError:
                    entry[key] = ''

# ---------------------------------------------------------Задание 1-----------------------------------------------------------
data = []
street_pattern = r"Улица:\s*([^\d]+)\s*(\d+)"
index_pattern = r"Индекс:\s*(\d+)"

for i in range(2, 68):
    with open(f'1/{i}.html', 'r', encoding='utf-8') as f:
        data2 = {}
        data2['id'] = i
        contents = f.read()
        soup = BeautifulSoup(contents)
        for child in soup.select('span'):
            key = child.text.split(":")[0].strip() if ':' in child.text else child.text.split("в")[0].strip()
            value = child.text.split(":")[1].strip() if ':' in child.text else child.text.split("в")[1].strip()

            value = value.strip()

            if value.isdigit(): 
                data2[key] = int(value)
            else:
                try:
                    data2[key] = float(value)
                except ValueError:
                    data2[key] = value
                
        name = soup.select('h1')[0].text
        data2['Строение'] = name.split(":")[1].strip()
        
        adress = soup.select('p')[0].text
        street_match = re.search(street_pattern, adress)
        data2['Улица'] = street_match.group(1).strip()
        data2['Дом'] = int(street_match.group(2).strip())
        data2['Индекс'] = re.search(index_pattern, adress).group(1).strip()
        
        img = soup.select('img')[0]['src']
        data2['Изображение'] = img
        data.append(data2)
                
save_json('1', data)
    
sorted_data = sorted(data, key=lambda x: x['Просмотры'])
save_json('1_sorted', sorted_data)
    
filtered_data = [item for item in data if item['Рейтинг'] > 4]
save_json('1_filtered', filtered_data)

print('Задание 1\n')
floors = [item['Этажи'] for item in data]
statistics(floors)

parking_counts = {}
for item in data:
    parking = item['Парковка']
    if parking in parking_counts:
        parking_counts[parking] += 1
    else:
        parking_counts[parking] = 1

print('\nЧастота меток по улицам:')
for parking, count in parking_counts.items():
    print(f'{parking}: {count}')
    
    
    
# ---------------------------------------------------------Задание 2-----------------------------------------------------------
for i in range(1, 47):
    with open(f'2/{i}.html', 'r', encoding='utf-8') as f:
        data = []
        contents = f.read() 
        soup = BeautifulSoup(contents)
        for child in soup.select('.pad img'):
            item_data = {}
            item_data['img'] = child['src']

            span = child.find_next('span')  
            item_data['name'] = span.get_text(strip=True)


            price = child.find_next('price')  
            item_data['price'] = int(re.sub(r'[^а-яА-ЯёЁa-zA-Z0-9\s]', '', price.get_text(strip=True)).replace(' ', ''))

            bonus = child.find_next('strong')  
            item_data['bonus'] = int(re.sub(r'\D', '', bonus.get_text(strip=True)))

            ul = child.find_next('ul') 
            specifications = {}

            for li in ul.select('li'):
                text = li.text
                cleaned_text = ' '.join(text.split())
                specifications[li['type']] = cleaned_text

            item_data['specifications'] = specifications
            data.append(item_data)
        
save_json('2', data)
    
sorted_data = sorted(data, key=lambda x: x['price'])
save_json('2_sorted', sorted_data)
    
filtered_data = [item for item in data if 'matrix' in item['specifications'] and item['specifications']['matrix'] == 'AMOLED']
save_json('2_filtered', filtered_data)

print('\n\nЗадание 2\n')
bonuses = [item['bonus'] for item in data]
statistics(bonuses)
    
sim_counts = {}
for item in data:
    if 'sim' in item['specifications']:
        sim = item['specifications']['sim']
        if sim in sim_counts:
            sim_counts[sim] += 1
        else:
            sim_counts[sim] = 1

print('\nЧастота меток по симкартам:')
for sim, count in sim_counts.items():
    print(f'{sim}: {count}')
    
    

# ---------------------------------------------------------Задание 3-----------------------------------------------------------

data = []

for i in range(1, 201):
    with open(f'3/{i}.xml', 'r', encoding='utf-8') as file:
        xml_content = file.read()
    soup = BeautifulSoup(xml_content, 'xml')

    item_info = {
        'name': soup.find('name').text.strip(),
        'constellation': soup.find('constellation').text.strip(),
        'spectral-class': soup.find('spectral-class').text.strip(),
        'radius': int(soup.find('radius').text.strip()),
        'rotation': float(re.sub(r'[^0-9.]', '', soup.find('rotation').text.strip())),
        'age': float(re.sub(r'[^0-9.]', '', soup.find('age').text.strip())),
        'distance': float(re.sub(r'[^0-9.]', '', soup.find('distance').text.strip())),
        'absolute-magnitude': float(re.sub(r'[^0-9.]', '', soup.find('absolute-magnitude').text.strip()))
    }
    
    data.append(item_info)

save_json('3', data)

sorted_data = sorted(data, key=lambda x: x['name'])
save_json('3_sorted', sorted_data)

filtered_data = [item for item in data if item['age'] > 5]
save_json('3_filtered', filtered_data)

print('\n\nЗадание 3\n')
magnitude = [item['absolute-magnitude'] for item in data]
statistics(magnitude)

constellation_counts = {}
for item in data:
    constellation = item['constellation']
    if constellation in constellation_counts:
        constellation_counts[constellation] += 1
    else:
        constellation_counts[constellation] = 1

print('\nЧастота меток по constellation:')
for constellation, count in constellation_counts.items():
    print(f'{constellation}: {count}')
    
    

# ---------------------------------------------------------Задание 4-----------------------------------------------------------

unique_tags = set()
for i in range(1, 188):
    with open(f'4/{i}.xml', 'r', encoding='utf-8') as file:
        xml_content = file.read()

    soup = BeautifulSoup(xml_content, 'xml')
    for element in soup.find_all(True):
        unique_tags.add(element.name)
        
unique_tags.remove('clothing-items')
unique_tags.remove('clothing')

data = []
for i in range(1, 188):
    with open(f'4/{i}.xml', 'r', encoding='utf-8') as file:
        xml_content = file.read()

    soup = BeautifulSoup(xml_content, 'xml')

    for clothing in soup.find_all('clothing'):
        item_info = {}
        for tag in unique_tags:
            item_info[tag] = clothing.find(tag).text.strip() if clothing.find(tag) else None
            
        data.append(item_info)

for item in data:
    item['price'] = int(item['price']) 
    item['reviews'] = int(item['reviews']) 
    item['rating'] = float(item['rating'])
        
save_json('4', data)

sorted_data = sorted(data, key=lambda x: x['name'])
save_json('4_sorted', sorted_data)

filtered_data = [item for item in data if item['reviews'] > 950000]
save_json('4_filtered', filtered_data)

print('\n\nЗадание 4\n')
price = [item['price'] for item in data]
statistics(price)

color_counts = {}
for item in data:
    color = item['color']
    if color in color_counts:
        color_counts[color] += 1
    else:
        color_counts[color] = 1

print('\nЧастота меток по цвету:')
for color, count in color_counts.items():
    print(f'{color}: {count}')
    

# ---------------------------------------------------------Задание 5-----------------------------------------------------------

univers_data = []
years = [2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016]
univer_ids = ['1519', '313']

for j in range(len(univer_ids)):
    for i in range(len(years)):
        univ_resp = requests.get('https://monitoring.miccedu.ru/iam/'+str(years[i])+'/_vpo/inst.php?id='+univer_ids[j])
        if univ_resp.status_code == 200:
            univ_soup = BeautifulSoup(univ_resp.text, 'html.parser')
            uni = {}
            uni['id'] = univer_ids[j]
            uni['Year'] = years[i] 
            tr = univ_soup.select('.napr_head tr')[0]
            tds = tr.find_all('td')
            uni[tds[1].text] = float(tds[3].text.replace(',', '.'))
            univers_data.append(uni)

save_json('5_one_object', univers_data)

univers_data = []

for j in range(len(univer_ids)):
    for i in range(len(years)):
        univ_resp = requests.get('https://monitoring.miccedu.ru/iam/'+str(years[i])+'/_vpo/inst.php?id='+str(univer_ids[j]))
        if univ_resp.status_code == 200:
            univ_soup = BeautifulSoup(univ_resp.text, 'html.parser')
            uni = {}
            uni['id'] = univer_ids[j]
            uni['Year'] = years[i] 
            for tr in univ_soup.select('.napr_head tr'):
                tds = tr.find_all('td')
                uni[tds[1].text] = tds[3].text
                
            univers_data.append(uni)
            
convert_values(univers_data)               
save_json('5_several_objects', univers_data)

sorted_data = sorted(univers_data, key=lambda x: x['Year'])
save_json('5_sorted', sorted_data)

filtered_data = [item for item in univers_data if item['Средний балл ЕГЭ студентов, принятых по результатам ЕГЭ на обучение по очной форме по программам бакалавриата и специалитета за счет средств соответствующих бюджетов бюджетной системы РФ'] > 80]
save_json('5_filtered', filtered_data)

print('\n\nЗадание 5\n')
price = [item['Удельный вес доходов от НИОКР в общих доходах образовательной организации'] for item in univers_data]
statistics(price)

ids_counts = {}
for item in univers_data:
    ids = item['id']
    if ids in ids_counts:
        ids_counts[ids] += 1
    else:
        ids_counts[ids] = 1

print('\nЧастота меток по id:')
for ids, count in ids_counts.items():
    print(f'{ids}: {count}')
