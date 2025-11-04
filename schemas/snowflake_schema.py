"""
Створення схеми "сніжинка" (Snowflake Schema) для аналізу продажів.
Нормалізована структура з центральною таблицею фактів та повністю нормалізованими вимірюваннями.
"""

import sqlite3
import pandas as pd
import os
from pathlib import Path

class SnowflakeSchemaBuilder:
    def __init__(self, db_path='data/snowflake_schema.db'):
        self.db_path = db_path
        self.conn = None
        
    def connect(self):
        """Підключення до бази даних"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        print(f"Підключено до бази даних: {self.db_path}")
        
    def disconnect(self):
        """Відключення від бази даних"""
        if self.conn:
            self.conn.close()
            print("Відключено від бази даних")
    
    def create_snowflake_schema(self):
        """Створює схему сніжинка з повністю нормалізованими таблицями"""
        
        # Видалення існуючих таблиць
        tables_to_drop = [
            'sales_facts', 'dim_product', 'dim_customer', 'dim_salesperson', 'dim_time',
            'dim_category', 'dim_subcategory', 'dim_city', 'dim_region', 'dim_country'
        ]
        
        for table in tables_to_drop:
            self.conn.execute(f"DROP TABLE IF EXISTS {table}")
        
        # ГЕОГРАФІЧНІ ВИМІРЮВАННЯ (нормалізовані)
        
        # Таблиця країн
        create_dim_country = """
        CREATE TABLE dim_country (
            country_id INTEGER PRIMARY KEY,
            country_name TEXT NOT NULL UNIQUE
        )
        """
        
        # Таблиця регіонів
        create_dim_region = """
        CREATE TABLE dim_region (
            region_id INTEGER PRIMARY KEY,
            region_name TEXT NOT NULL,
            country_id INTEGER NOT NULL,
            FOREIGN KEY (country_id) REFERENCES dim_country(country_id)
        )
        """
        
        # Таблиця міст
        create_dim_city = """
        CREATE TABLE dim_city (
            city_id INTEGER PRIMARY KEY,
            city_name TEXT NOT NULL,
            region_id INTEGER NOT NULL,
            FOREIGN KEY (region_id) REFERENCES dim_region(region_id)
        )
        """
        
        # ПРОДУКТОВІ ВИМІРЮВАННЯ (нормалізовані)
        
        # Таблиця категорій
        create_dim_category = """
        CREATE TABLE dim_category (
            category_id INTEGER PRIMARY KEY,
            category_name TEXT NOT NULL UNIQUE
        )
        """
        
        # Таблиця підкатегорій
        create_dim_subcategory = """
        CREATE TABLE dim_subcategory (
            subcategory_id INTEGER PRIMARY KEY,
            subcategory_name TEXT NOT NULL,
            category_id INTEGER NOT NULL,
            FOREIGN KEY (category_id) REFERENCES dim_category(category_id)
        )
        """
        
        # Таблиця продуктів
        create_dim_product = """
        CREATE TABLE dim_product (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            product_code TEXT NOT NULL,
            unit_price REAL NOT NULL,
            subcategory_id INTEGER NOT NULL,
            FOREIGN KEY (subcategory_id) REFERENCES dim_subcategory(subcategory_id)
        )
        """
        
        # ОСНОВНІ ВИМІРЮВАННЯ
        
        # Таблиця клієнтів
        create_dim_customer = """
        CREATE TABLE dim_customer (
            customer_id INTEGER PRIMARY KEY,
            customer_name TEXT NOT NULL,
            email TEXT,
            phone TEXT,
            registration_date DATE,
            city_id INTEGER NOT NULL,
            FOREIGN KEY (city_id) REFERENCES dim_city(city_id)
        )
        """
        
        # Таблиця продавців
        create_dim_salesperson = """
        CREATE TABLE dim_salesperson (
            salesperson_id INTEGER PRIMARY KEY,
            salesperson_name TEXT NOT NULL,
            hire_date DATE,
            region_id INTEGER NOT NULL,
            FOREIGN KEY (region_id) REFERENCES dim_region(region_id)
        )
        """
        
        # Таблиця часу
        create_dim_time = """
        CREATE TABLE dim_time (
            time_id INTEGER PRIMARY KEY,
            date DATE NOT NULL,
            year INTEGER NOT NULL,
            quarter TEXT NOT NULL,
            month INTEGER NOT NULL,
            month_name TEXT NOT NULL,
            day INTEGER NOT NULL,
            weekday INTEGER NOT NULL,
            weekday_name TEXT NOT NULL
        )
        """
        
        # ТАБЛИЦЯ ФАКТІВ
        
        # Таблиця фактів продажів
        create_sales_facts = """
        CREATE TABLE sales_facts (
            sale_id INTEGER PRIMARY KEY,
            time_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            salesperson_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            discount REAL NOT NULL,
            total_amount REAL NOT NULL,
            FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
            FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
            FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
            FOREIGN KEY (salesperson_id) REFERENCES dim_salesperson(salesperson_id)
        )
        """
        
        # Виконання створення таблиць в правильному порядку (від батьківських до дочірніх)
        table_creation_order = [
            create_dim_country,
            create_dim_region,
            create_dim_city,
            create_dim_category,
            create_dim_subcategory,
            create_dim_product,
            create_dim_customer,
            create_dim_salesperson,
            create_dim_time,
            create_sales_facts
        ]
        
        for create_statement in table_creation_order:
            self.conn.execute(create_statement)
        
        # Створення індексів для оптимізації запитів
        indexes = [
            # Індекси для таблиці фактів
            "CREATE INDEX idx_sf_sales_time ON sales_facts(time_id)",
            "CREATE INDEX idx_sf_sales_product ON sales_facts(product_id)",
            "CREATE INDEX idx_sf_sales_customer ON sales_facts(customer_id)",
            "CREATE INDEX idx_sf_sales_salesperson ON sales_facts(salesperson_id)",
            
            # Індекси для вимірювань
            "CREATE INDEX idx_sf_time_date ON dim_time(date)",
            "CREATE INDEX idx_sf_time_year ON dim_time(year)",
            "CREATE INDEX idx_sf_product_subcategory ON dim_product(subcategory_id)",
            "CREATE INDEX idx_sf_subcategory_category ON dim_subcategory(category_id)",
            "CREATE INDEX idx_sf_customer_city ON dim_customer(city_id)",
            "CREATE INDEX idx_sf_city_region ON dim_city(region_id)",
            "CREATE INDEX idx_sf_region_country ON dim_region(country_id)",
            "CREATE INDEX idx_sf_salesperson_region ON dim_salesperson(region_id)"
        ]
        
        for index in indexes:
            self.conn.execute(index)
        
        self.conn.commit()
        print("Схему 'сніжинка' створено успішно!")
    
    def load_data(self):
        """Завантажує дані у схему сніжинка"""
        data_dir = Path('data')
        
        # Завантаження базових даних
        countries_df = pd.read_csv(data_dir / 'countries.csv')
        regions_df = pd.read_csv(data_dir / 'regions.csv')
        cities_df = pd.read_csv(data_dir / 'cities.csv')
        categories_df = pd.read_csv(data_dir / 'categories.csv')
        subcategories_df = pd.read_csv(data_dir / 'subcategories.csv')
        products_df = pd.read_csv(data_dir / 'products.csv')
        customers_df = pd.read_csv(data_dir / 'customers.csv')
        sales_people_df = pd.read_csv(data_dir / 'sales_people.csv')
        time_df = pd.read_csv(data_dir / 'time_dimension.csv')
        sales_df = pd.read_csv(data_dir / 'sales_facts.csv')
        
        # Завантаження у базу даних в правильному порядку
        countries_df.to_sql('dim_country', self.conn, if_exists='replace', index=False)
        regions_df.to_sql('dim_region', self.conn, if_exists='replace', index=False)
        cities_df.to_sql('dim_city', self.conn, if_exists='replace', index=False)
        categories_df.to_sql('dim_category', self.conn, if_exists='replace', index=False)
        subcategories_df.to_sql('dim_subcategory', self.conn, if_exists='replace', index=False)
        products_df.to_sql('dim_product', self.conn, if_exists='replace', index=False)
        customers_df.to_sql('dim_customer', self.conn, if_exists='replace', index=False)
        sales_people_df.to_sql('dim_salesperson', self.conn, if_exists='replace', index=False)
        time_df.to_sql('dim_time', self.conn, if_exists='replace', index=False)
        sales_df.to_sql('sales_facts', self.conn, if_exists='replace', index=False)
        
        self.conn.commit()
        
        # Статистика завантажених даних
        stats = {
            'dim_country': len(countries_df),
            'dim_region': len(regions_df),
            'dim_city': len(cities_df),
            'dim_category': len(categories_df),
            'dim_subcategory': len(subcategories_df),
            'dim_product': len(products_df),
            'dim_customer': len(customers_df),
            'dim_salesperson': len(sales_people_df),
            'dim_time': len(time_df),
            'sales_facts': len(sales_df)
        }
        
        print("Дані завантажено у схему 'сніжинка':")
        for table, count in stats.items():
            print(f"- {table}: {count} записів")
        
        return stats
    
    def get_schema_info(self):
        """Повертає інформацію про схему"""
        cursor = self.conn.cursor()
        
        # Список таблиць
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        schema_info = {}
        for table in tables:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            schema_info[table] = columns
            
        return schema_info
    
    def print_schema_info(self):
        """Виводить інформацію про схему"""
        schema_info = self.get_schema_info()
        
        print("\n=== СХЕМА 'СНІЖИНКА' ===")
        for table, columns in schema_info.items():
            print(f"\nТаблиця: {table}")
            print("-" * (len(table) + 10))
            for col in columns:
                print(f"  {col[1]} ({col[2]})")

def main():
    """Основна функція для створення схеми сніжинка"""
    builder = SnowflakeSchemaBuilder()
    
    try:
        builder.connect()
        builder.create_snowflake_schema()
        builder.load_data()
        builder.print_schema_info()
        
    except Exception as e:
        print(f"Помилка: {e}")
        
    finally:
        builder.disconnect()

if __name__ == "__main__":
    main()