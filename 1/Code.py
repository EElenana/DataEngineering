# Задание 1.1
from collections import Counter
import re
import math
import pandas as pd
import requests

with open('first_task.txt', encoding='utf-8') as file:
    text = file.read().lower()  
       
words = re.findall(r'\b\w+\b', text)
word_count = Counter(words)
sorted_word_count = sorted(word_count.items(), key=lambda x: x[1], reverse=True)
    
with open('1.1.txt', 'w', encoding='utf-8') as file:
    for word, freq in sorted_word_count:
        file.write(f"{word}:{freq}\n")
        
        
        
# Задание 1.2
def starts_with_vowel(word):
    vowels = 'aeiou'  
    return word[0] in vowels

count = 0
for word in words:
    if starts_with_vowel(word):
        count+=1
        
with open('1.2.txt', 'w', encoding='utf-8') as file:
    file.write(f"Количество: {count}\nДоля: {round(count/len(words)*100, 2)}")
    
    
    
# Задание 2
with open('second_task.txt', encoding='utf-8') as file:
    lines = file.readlines()

counter = []
for line in lines:
    count = 0
    for i in line.split():
        if int(i) > 0:
            count += int(i)
    counter.append(count)

counter.append(sum(counter))
counter = [str(i) for i in counter]

with open('2.txt', 'w', encoding='utf-8') as file:
    file.write('\n'.join(counter))
    
    
# Задание 3
with open('third_task.txt', encoding='utf-8') as file:
    lines = file.readlines()

lines_modified = []
for line in lines:
    line_lst = line.split()
    line_lst = [float(num) if num != 'N/A' else 'N/A' for num in line_lst]
    for i in range(len(line_lst)):
        if line_lst[i] == 'N/A':
            line_lst[i] = (int(line_lst[i-1])+int(line_lst[i+1]))/2
    lines_modified.append(line_lst)
 
results = []
for line in lines_modified:
    filtered_numbers = [num for num in line if num > 0 and math.sqrt(num) < 200]
    total_sum = sum(filtered_numbers)
    results.append(total_sum)

results = [str(i) for i in results]
with open('3.txt', 'w', encoding='utf-8') as file:
    file.write('\n'.join(results))
    
    
# Задание 4
df_4 = pd.read_csv('fourth_task.txt')
df_modified = df_4.drop('rating', axis=1)

with open('4.1.txt', 'w', encoding='utf-8') as file:
    file.write(f"Среднее price: {df_modified['price'].mean()}\nМаксимум quantity: {df_modified['quantity'].max()}\nМинимум price: {df_modified['price'].min()}")

df_modified[df_modified['category'] != 'Бакалея'].to_csv('4.2.txt', encoding='utf-8', index=False)


# Задание 5
df_5 = pd.read_html('fifth_task.html', encoding='utf-8')[0]
df_5.to_csv('5.txt', encoding='utf-8', index=False)


# Задание 6
resp = requests.get('https://dog-api.kinduff.com/api/facts')

if resp.status_code == 200:
    data = resp.json()
    facts = data['facts']
    html_content = '<html><head><title>Факты о собаках</title></head><body>'
    html_content += '<h1>Факты о собаках на сегодня:</h1>'
    html_content += '<ul>'
    
    # Добавляем факты в HTML
    for fact in facts:
        html_content += f'<li>{fact}</li>'
    
    html_content += '</ul>'
    html_content += '</body></html>'
    
    # Сохраняем HTML в файл
    with open('6.html', 'w') as file:
        file.write(html_content)
    
else:
    print("Ошибка в задании 6:", response.status_code)
