"""
Створення схеми "зірка" (Star Schema) для аналізу продажів.
Денормалізована структура з центральною таблицею фактів та спрощеними вимірюваннями.
"""

import sqlite3
import pandas as pd
import os
from pathlib import Path

class StarSchemaBuilder:
    def __init__(self, db_path='data/star_schema.db'):
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
    
    def create_star_schema(self):
        """Створює схему зірка з денормалізованими таблицями вимірювань"""
        
        # Видалення існуючих таблиць
        tables_to_drop = ['sales_facts', 'dim_product', 'dim_customer', 
                         'dim_salesperson', 'dim_time']
        
        for table in tables_to_drop:
            self.conn.execute(f"DROP TABLE IF EXISTS {table}")
        
        # Створення таблиці вимірювання продуктів (денормалізована)
        create_dim_product = """
        CREATE TABLE dim_product (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            product_code TEXT NOT NULL,
            unit_price REAL NOT NULL,
            subcategory_name TEXT NOT NULL,
            category_name TEXT NOT NULL
        )
        """
        
        # Створення таблиці вимірювання клієнтів (денормалізована)
        create_dim_customer = """
        CREATE TABLE dim_customer (
            customer_id INTEGER PRIMARY KEY,
            customer_name TEXT NOT NULL,
            email TEXT,
            phone TEXT,
            registration_date DATE,
            city_name TEXT NOT NULL,
            region_name TEXT NOT NULL,
            country_name TEXT NOT NULL
        )
        """
        
        # Створення таблиці вимірювання продавців (денормалізована)
        create_dim_salesperson = """
        CREATE TABLE dim_salesperson (
            salesperson_id INTEGER PRIMARY KEY,
            salesperson_name TEXT NOT NULL,
            hire_date DATE,
            region_name TEXT NOT NULL,
            country_name TEXT NOT NULL
        )
        """
        
        # Створення таблиці вимірювання часу
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
        
        # Створення таблиці фактів продажів
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
        
        # Виконання створення таблиць
        self.conn.execute(create_dim_product)
        self.conn.execute(create_dim_customer)
        self.conn.execute(create_dim_salesperson)
        self.conn.execute(create_dim_time)
        self.conn.execute(create_sales_facts)
        
        # Створення індексів для оптимізації запитів
        indexes = [
            "CREATE INDEX idx_sales_time ON sales_facts(time_id)",
            "CREATE INDEX idx_sales_product ON sales_facts(product_id)",
            "CREATE INDEX idx_sales_customer ON sales_facts(customer_id)",
            "CREATE INDEX idx_sales_salesperson ON sales_facts(salesperson_id)",
            "CREATE INDEX idx_time_date ON dim_time(date)",
            "CREATE INDEX idx_time_year ON dim_time(year)",
            "CREATE INDEX idx_product_category ON dim_product(category_name)",
            "CREATE INDEX idx_customer_city ON dim_customer(city_name)",
            "CREATE INDEX idx_customer_region ON dim_customer(region_name)"
        ]
        
        for index in indexes:
            self.conn.execute(index)
        
        self.conn.commit()
        print("Схему 'зірка' створено успішно!")
    
    def load_data(self):
        """Завантажує дані у схему зірка"""
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
        
        # Створення денормалізованої таблиці продуктів
        dim_product = products_df.merge(subcategories_df, on='subcategory_id') \
                                .merge(categories_df, on='category_id') \
                                [['product_id', 'product_name', 'product_code', 'unit_price', 
                                  'subcategory_name', 'category_name']]
        
        # Створення денормалізованої таблиці клієнтів
        dim_customer = customers_df.merge(cities_df, on='city_id') \
                                  .merge(regions_df, on='region_id') \
                                  .merge(countries_df, on='country_id') \
                                  [['customer_id', 'customer_name', 'email', 'phone', 
                                    'registration_date', 'city_name', 'region_name', 'country_name']]
        
        # Створення денормалізованої таблиці продавців
        dim_salesperson = sales_people_df.merge(regions_df, on='region_id') \
                                        .merge(countries_df, on='country_id') \
                                        [['salesperson_id', 'salesperson_name', 'hire_date', 
                                          'region_name', 'country_name']]
        
        # Завантаження у базу даних
        dim_product.to_sql('dim_product', self.conn, if_exists='replace', index=False)
        dim_customer.to_sql('dim_customer', self.conn, if_exists='replace', index=False)
        dim_salesperson.to_sql('dim_salesperson', self.conn, if_exists='replace', index=False)
        time_df.to_sql('dim_time', self.conn, if_exists='replace', index=False)
        sales_df.to_sql('sales_facts', self.conn, if_exists='replace', index=False)
        
        self.conn.commit()
        
        # Статистика завантажених даних
        stats = {
            'dim_product': len(dim_product),
            'dim_customer': len(dim_customer),
            'dim_salesperson': len(dim_salesperson),
            'dim_time': len(time_df),
            'sales_facts': len(sales_df)
        }
        
        print("Дані завантажено у схему 'зірка':")
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
        
        print("\n=== СХЕМА 'ЗІРКА' ===")
        for table, columns in schema_info.items():
            print(f"\nТаблиця: {table}")
            print("-" * (len(table) + 10))
            for col in columns:
                print(f"  {col[1]} ({col[2]})")

def main():
    """Основна функція для створення схеми зірка"""
    builder = StarSchemaBuilder()
    
    try:
        builder.connect()
        builder.create_star_schema()
        builder.load_data()
        builder.print_schema_info()
        
    except Exception as e:
        print(f"Помилка: {e}")
        
    finally:
        builder.disconnect()

if __name__ == "__main__":
    main()