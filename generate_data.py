"""
Генератор синтетичних даних для аналізу продажів.
Створює реалістичні дані для порівняння схем "зірка" та "сніжинка".
"""

import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

# Налаштування
fake = Faker('uk_UA')  # Українська локалізація
np.random.seed(42)
random.seed(42)

# Кількість записів
NUM_REGIONS = 5
NUM_CITIES = 20
NUM_COUNTRIES = 3
NUM_CATEGORIES = 8
NUM_SUBCATEGORIES = 25
NUM_PRODUCTS = 500
NUM_CUSTOMERS = 2000
NUM_SALES_PEOPLE = 50
NUM_SALES_RECORDS = 50000

def generate_geography_data():
    """Генерує географічні дані для схеми сніжинка"""
    
    # Країни
    countries = []
    country_names = ['Україна', 'Польща', 'Німеччина']
    for i, name in enumerate(country_names, 1):
        countries.append({'country_id': i, 'country_name': name})
    
    # Регіони
    regions = []
    ukrainian_regions = ['Київська', 'Львівська', 'Одеська', 'Харківська', 'Дніпропетровська']
    for i, region in enumerate(ukrainian_regions, 1):
        regions.append({
            'region_id': i,
            'region_name': region + ' область',
            'country_id': 1  # Україна
        })
    
    # Міста
    cities = []
    ukrainian_cities = ['Київ', 'Львів', 'Одеса', 'Харків', 'Дніпро', 'Запоріжжя', 
                       'Вінниця', 'Полтава', 'Чернівці', 'Івано-Франківськ',
                       'Тернопіль', 'Житомир', 'Черкаси', 'Суми', 'Рівне',
                       'Хмельницький', 'Чернігів', 'Кропивницький', 'Миколаїв', 'Ужгород']
    
    for i, city in enumerate(ukrainian_cities, 1):
        cities.append({
            'city_id': i,
            'city_name': city,
            'region_id': random.randint(1, NUM_REGIONS)
        })
    
    return pd.DataFrame(countries), pd.DataFrame(regions), pd.DataFrame(cities)

def generate_product_data():
    """Генерує дані про продукти для схеми сніжинка"""
    
    # Категорії
    categories = []
    category_names = ['Електроніка', 'Одяг', 'Книги', 'Дім і сад', 'Спорт', 'Авто', 'Краса', 'Їжа']
    for i, name in enumerate(category_names, 1):
        categories.append({'category_id': i, 'category_name': name})
    
    # Підкategорії
    subcategories = []
    subcategory_data = {
        1: ['Смартфони', 'Ноутбуки', 'Телевізори'],
        2: ['Сорочки', 'Джинси', 'Взуття'],
        3: ['Фантастика', 'Детективи', 'Навчальна література'],
        4: ['Меблі', 'Інструменти', 'Декор'],
        5: ['Фітнес', 'Туризм', 'Командні види спорту'],
        6: ['Шини', 'Масла', 'Аксесуари'],
        7: ['Косметика', 'Парфуми', 'Догляд'],
        8: ['М\'ясо', 'Овочі', 'Солодощі']
    }
    
    subcategory_id = 1
    for category_id, subcats in subcategory_data.items():
        for subcat_name in subcats:
            subcategories.append({
                'subcategory_id': subcategory_id,
                'subcategory_name': subcat_name,
                'category_id': category_id
            })
            subcategory_id += 1
    
    # Продукти
    products = []
    for i in range(1, NUM_PRODUCTS + 1):
        subcategory_id = random.randint(1, len(subcategories))
        products.append({
            'product_id': i,
            'product_name': fake.word().capitalize() + ' ' + fake.word().capitalize(),
            'product_code': f'PRD{i:05d}',
            'subcategory_id': subcategory_id,
            'unit_price': round(random.uniform(10, 5000), 2)
        })
    
    return pd.DataFrame(categories), pd.DataFrame(subcategories), pd.DataFrame(products)

def generate_customer_data():
    """Генерує дані про клієнтів"""
    customers = []
    for i in range(1, NUM_CUSTOMERS + 1):
        customers.append({
            'customer_id': i,
            'customer_name': fake.name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'city_id': random.randint(1, NUM_CITIES),
            'registration_date': fake.date_between(start_date='-3y', end_date='today')
        })
    
    return pd.DataFrame(customers)

def generate_sales_people_data():
    """Генерує дані про продавців"""
    sales_people = []
    for i in range(1, NUM_SALES_PEOPLE + 1):
        sales_people.append({
            'salesperson_id': i,
            'salesperson_name': fake.name(),
            'hire_date': fake.date_between(start_date='-5y', end_date='-1y'),
            'region_id': random.randint(1, NUM_REGIONS)
        })
    
    return pd.DataFrame(sales_people)

def generate_time_data():
    """Генерує часові дані"""
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    time_data = []
    current_date = start_date
    time_id = 1
    
    while current_date <= end_date:
        time_data.append({
            'time_id': time_id,
            'date': current_date.date(),
            'year': current_date.year,
            'quarter': f'Q{(current_date.month - 1) // 3 + 1}',
            'month': current_date.month,
            'month_name': current_date.strftime('%B'),
            'day': current_date.day,
            'weekday': current_date.weekday() + 1,
            'weekday_name': current_date.strftime('%A')
        })
        current_date += timedelta(days=1)
        time_id += 1
    
    return pd.DataFrame(time_data)

def generate_sales_data(time_df, products_df, customers_df, sales_people_df):
    """Генерує дані про продажі (факти)"""
    sales_data = []
    
    for i in range(1, NUM_SALES_RECORDS + 1):
        time_record = time_df.sample(1).iloc[0]
        product_record = products_df.sample(1).iloc[0]
        customer_record = customers_df.sample(1).iloc[0]
        salesperson_record = sales_people_df.sample(1).iloc[0]
        
        quantity = random.randint(1, 10)
        unit_price = product_record['unit_price']
        discount = round(random.uniform(0, 0.2), 3)  # 0-20% знижка
        total_amount = round(quantity * unit_price * (1 - discount), 2)
        
        sales_data.append({
            'sale_id': i,
            'time_id': time_record['time_id'],
            'product_id': product_record['product_id'],
            'customer_id': customer_record['customer_id'],
            'salesperson_id': salesperson_record['salesperson_id'],
            'quantity': quantity,
            'unit_price': unit_price,
            'discount': discount,
            'total_amount': total_amount
        })
    
    return pd.DataFrame(sales_data)

def save_data_to_csv():
    """Генерує та зберігає всі дані у CSV файли"""
    print("Генерація даних...")
    
    # Створення папки для даних
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True)
    
    # Генерація даних
    countries_df, regions_df, cities_df = generate_geography_data()
    categories_df, subcategories_df, products_df = generate_product_data()
    customers_df = generate_customer_data()
    sales_people_df = generate_sales_people_data()
    time_df = generate_time_data()
    sales_df = generate_sales_data(time_df, products_df, customers_df, sales_people_df)
    
    # Збереження у CSV
    countries_df.to_csv(f'{data_dir}/countries.csv', index=False)
    regions_df.to_csv(f'{data_dir}/regions.csv', index=False)
    cities_df.to_csv(f'{data_dir}/cities.csv', index=False)
    categories_df.to_csv(f'{data_dir}/categories.csv', index=False)
    subcategories_df.to_csv(f'{data_dir}/subcategories.csv', index=False)
    products_df.to_csv(f'{data_dir}/products.csv', index=False)
    customers_df.to_csv(f'{data_dir}/customers.csv', index=False)
    sales_people_df.to_csv(f'{data_dir}/sales_people.csv', index=False)
    time_df.to_csv(f'{data_dir}/time_dimension.csv', index=False)
    sales_df.to_csv(f'{data_dir}/sales_facts.csv', index=False)
    
    print(f"Дані згенеровано та збережено у папці '{data_dir}':")
    print(f"- Країн: {len(countries_df)}")
    print(f"- Регіонів: {len(regions_df)}")
    print(f"- Міст: {len(cities_df)}")
    print(f"- Категорій: {len(categories_df)}")
    print(f"- Підкategорій: {len(subcategories_df)}")
    print(f"- Продуктів: {len(products_df)}")
    print(f"- Клієнтів: {len(customers_df)}")
    print(f"- Продавців: {len(sales_people_df)}")
    print(f"- Часових записів: {len(time_df)}")
    print(f"- Записів продажів: {len(sales_df)}")

if __name__ == "__main__":
    save_data_to_csv()