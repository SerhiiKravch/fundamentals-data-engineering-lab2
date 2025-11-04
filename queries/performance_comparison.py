"""
Порівняння продуктивності запитів між схемами "зірка" та "сніжинка".
Виконує ідентичні аналітичні запити в обох схемах та вимірює час виконання.
"""

import sqlite3
import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path

class PerformanceComparator:
    def __init__(self):
        self.star_db = 'data/star_schema.db'
        self.snowflake_db = 'data/snowflake_schema.db'
        self.results = []
        
    def connect_to_db(self, db_path):
        """Підключення до бази даних"""
        return sqlite3.connect(db_path)
    
    def execute_query_with_timing(self, conn, query, query_name):
        """Виконує запит та вимірює час виконання"""
        start_time = time.time()
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        end_time = time.time()
        
        execution_time = (end_time - start_time) * 1000  # в мілісекундах
        
        return {
            'query_name': query_name,
            'execution_time_ms': execution_time,
            'result_count': len(results),
            'results': results[:5]  # Перші 5 результатів для верифікації
        }
    
    def run_performance_tests(self):
        """Запускає набір тестів продуктивності"""
        
        # Набір аналітичних запитів
        queries = self.get_analytical_queries()
        
        star_conn = self.connect_to_db(self.star_db)
        snowflake_conn = self.connect_to_db(self.snowflake_db)
        
        print("Запуск тестів продуктивності...")
        print("=" * 60)
        
        for query_name, star_query, snowflake_query in queries:
            print(f"\\nВиконання запиту: {query_name}")
            print("-" * 40)
            
            # Виконання запиту в схемі зірка
            star_result = self.execute_query_with_timing(star_conn, star_query, query_name)
            print(f"Зірка: {star_result['execution_time_ms']:.2f} мс")
            
            # Виконання запиту в схемі сніжинка
            snowflake_result = self.execute_query_with_timing(snowflake_conn, snowflake_query, query_name)
            print(f"Сніжинка: {snowflake_result['execution_time_ms']:.2f} мс")
            
            # Розрахунок співвідношення
            if snowflake_result['execution_time_ms'] > 0:
                ratio = star_result['execution_time_ms'] / snowflake_result['execution_time_ms']
                if ratio < 1:
                    print(f"Зірка швидше в {1/ratio:.2f} рази")
                else:
                    print(f"Сніжинка швидше в {ratio:.2f} рази")
            
            # Збереження результатів
            self.results.append({
                'query_name': query_name,
                'star_time_ms': star_result['execution_time_ms'],
                'snowflake_time_ms': snowflake_result['execution_time_ms'],
                'star_results': star_result['results'],
                'snowflake_results': snowflake_result['results']
            })
        
        star_conn.close()
        snowflake_conn.close()
        
        return self.results
    
    def get_analytical_queries(self):
        """Повертає набір аналітичних запитів для обох схем"""
        
        queries = [
            # 1. Загальні продажі по регіонах
            (
                "Продажі по регіонах",
                """
                SELECT 
                    dc.region_name,
                    SUM(sf.total_amount) as total_sales,
                    COUNT(*) as transaction_count,
                    AVG(sf.total_amount) as avg_transaction
                FROM sales_facts sf
                JOIN dim_customer dc ON sf.customer_id = dc.customer_id
                GROUP BY dc.region_name
                ORDER BY total_sales DESC
                """,
                """
                SELECT 
                    dr.region_name,
                    SUM(sf.total_amount) as total_sales,
                    COUNT(*) as transaction_count,
                    AVG(sf.total_amount) as avg_transaction
                FROM sales_facts sf
                JOIN dim_customer dc ON sf.customer_id = dc.customer_id
                JOIN dim_city dci ON dc.city_id = dci.city_id
                JOIN dim_region dr ON dci.region_id = dr.region_id
                GROUP BY dr.region_name
                ORDER BY total_sales DESC
                """
            ),
            
            # 2. Топ-10 продуктів по продажам
            (
                "Топ-10 продуктів",
                """
                SELECT 
                    dp.product_name,
                    dp.category_name,
                    SUM(sf.total_amount) as total_sales,
                    SUM(sf.quantity) as total_quantity
                FROM sales_facts sf
                JOIN dim_product dp ON sf.product_id = dp.product_id
                GROUP BY dp.product_id, dp.product_name, dp.category_name
                ORDER BY total_sales DESC
                LIMIT 10
                """,
                """
                SELECT 
                    dp.product_name,
                    dcat.category_name,
                    SUM(sf.total_amount) as total_sales,
                    SUM(sf.quantity) as total_quantity
                FROM sales_facts sf
                JOIN dim_product dp ON sf.product_id = dp.product_id
                JOIN dim_subcategory dsub ON dp.subcategory_id = dsub.subcategory_id
                JOIN dim_category dcat ON dsub.category_id = dcat.category_id
                GROUP BY dp.product_id, dp.product_name, dcat.category_name
                ORDER BY total_sales DESC
                LIMIT 10
                """
            ),
            
            # 3. Продажі по місяцях у 2023 році
            (
                "Тренд продажів по місяцях 2023",
                """
                SELECT 
                    dt.month,
                    dt.month_name,
                    SUM(sf.total_amount) as total_sales,
                    COUNT(*) as transaction_count
                FROM sales_facts sf
                JOIN dim_time dt ON sf.time_id = dt.time_id
                WHERE dt.year = 2023
                GROUP BY dt.month, dt.month_name
                ORDER BY dt.month
                """,
                """
                SELECT 
                    dt.month,
                    dt.month_name,
                    SUM(sf.total_amount) as total_sales,
                    COUNT(*) as transaction_count
                FROM sales_facts sf
                JOIN dim_time dt ON sf.time_id = dt.time_id
                WHERE dt.year = 2023
                GROUP BY dt.month, dt.month_name
                ORDER BY dt.month
                """
            ),
            
            # 4. Продуктивність продавців по регіонах
            (
                "Продуктивність продавців",
                """
                SELECT 
                    ds.salesperson_name,
                    ds.region_name,
                    SUM(sf.total_amount) as total_sales,
                    COUNT(*) as deals_count,
                    AVG(sf.total_amount) as avg_deal_size
                FROM sales_facts sf
                JOIN dim_salesperson ds ON sf.salesperson_id = ds.salesperson_id
                GROUP BY ds.salesperson_id, ds.salesperson_name, ds.region_name
                ORDER BY total_sales DESC
                LIMIT 20
                """,
                """
                SELECT 
                    ds.salesperson_name,
                    dr.region_name,
                    SUM(sf.total_amount) as total_sales,
                    COUNT(*) as deals_count,
                    AVG(sf.total_amount) as avg_deal_size
                FROM sales_facts sf
                JOIN dim_salesperson ds ON sf.salesperson_id = ds.salesperson_id
                JOIN dim_region dr ON ds.region_id = dr.region_id
                GROUP BY ds.salesperson_id, ds.salesperson_name, dr.region_name
                ORDER BY total_sales DESC
                LIMIT 20
                """
            ),
            
            # 5. Складний запит: Аналіз по категоріях, регіонах та квартал
            (
                "Комплексний аналіз продажів",
                """
                SELECT 
                    dp.category_name,
                    dc.region_name,
                    dt.quarter,
                    dt.year,
                    SUM(sf.total_amount) as total_sales,
                    SUM(sf.quantity) as total_quantity,
                    AVG(sf.discount) as avg_discount,
                    COUNT(DISTINCT sf.customer_id) as unique_customers
                FROM sales_facts sf
                JOIN dim_product dp ON sf.product_id = dp.product_id
                JOIN dim_customer dc ON sf.customer_id = dc.customer_id
                JOIN dim_time dt ON sf.time_id = dt.time_id
                WHERE dt.year IN (2023, 2024)
                GROUP BY dp.category_name, dc.region_name, dt.quarter, dt.year
                HAVING SUM(sf.total_amount) > 10000
                ORDER BY total_sales DESC
                """,
                """
                SELECT 
                    dcat.category_name,
                    dr.region_name,
                    dt.quarter,
                    dt.year,
                    SUM(sf.total_amount) as total_sales,
                    SUM(sf.quantity) as total_quantity,
                    AVG(sf.discount) as avg_discount,
                    COUNT(DISTINCT sf.customer_id) as unique_customers
                FROM sales_facts sf
                JOIN dim_product dp ON sf.product_id = dp.product_id
                JOIN dim_subcategory dsub ON dp.subcategory_id = dsub.subcategory_id
                JOIN dim_category dcat ON dsub.category_id = dcat.category_id
                JOIN dim_customer dc ON sf.customer_id = dc.customer_id
                JOIN dim_city dci ON dc.city_id = dci.city_id
                JOIN dim_region dr ON dci.region_id = dr.region_id
                JOIN dim_time dt ON sf.time_id = dt.time_id
                WHERE dt.year IN (2023, 2024)
                GROUP BY dcat.category_name, dr.region_name, dt.quarter, dt.year
                HAVING SUM(sf.total_amount) > 10000
                ORDER BY total_sales DESC
                """
            )
        ]
        
        return queries
    
    def create_performance_visualization(self):
        """Створює візуалізацію результатів продуктивності"""
        
        if not self.results:
            print("Немає результатів для візуалізації")
            return
        
        # Підготовка даних для візуалізації
        df_results = pd.DataFrame(self.results)
        
        # Налаштування стилю
        plt.style.use('default')
        sns.set_palette("husl")
        
        # Створення фігури з подграфіками
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Порівняння продуктивності схем "Зірка" vs "Сніжинка"', fontsize=16, fontweight='bold')
        
        # 1. Порівняння часу виконання
        ax1 = axes[0, 0]
        x_pos = np.arange(len(df_results))
        width = 0.35
        
        bars1 = ax1.bar(x_pos - width/2, df_results['star_time_ms'], width, 
                       label='Star Schema', alpha=0.8, color='skyblue')
        bars2 = ax1.bar(x_pos + width/2, df_results['snowflake_time_ms'], width,
                       label='Snowflake Schema', alpha=0.8, color='lightcoral')
        
        ax1.set_xlabel('Запити')
        ax1.set_ylabel('Час виконання (мс)')
        ax1.set_title('Час виконання запитів')
        ax1.set_xticks(x_pos)
        ax1.set_xticklabels([f'Q{i+1}' for i in range(len(df_results))], rotation=45)
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Додавання значень на стовпці
        for bar in bars1:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'{height:.1f}', ha='center', va='bottom', fontsize=8)
        
        for bar in bars2:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'{height:.1f}', ha='center', va='bottom', fontsize=8)
        
        # 2. Співвідношення швидкості
        ax2 = axes[0, 1]
        ratios = df_results['snowflake_time_ms'] / df_results['star_time_ms']
        colors = ['green' if r > 1 else 'red' for r in ratios]
        
        bars = ax2.bar(range(len(ratios)), ratios, color=colors, alpha=0.7)
        ax2.axhline(y=1, color='black', linestyle='--', alpha=0.5)
        ax2.set_xlabel('Запити')
        ax2.set_ylabel('Співвідношення (Сніжинка/Зірка)')
        ax2.set_title('Співвідношення швидкості\\n(>1 = Зірка швидше)')
        ax2.set_xticks(range(len(ratios)))
        ax2.set_xticklabels([f'Q{i+1}' for i in range(len(ratios))], rotation=45)
        ax2.grid(True, alpha=0.3)
        
        # Додавання значень
        for i, bar in enumerate(bars):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                    f'{height:.2f}x', ha='center', va='bottom', fontsize=8)
        
        # 3. Середній час виконання
        ax3 = axes[1, 0]
        avg_star = df_results['star_time_ms'].mean()
        avg_snowflake = df_results['snowflake_time_ms'].mean()
        
        schemas = ['Зірка', 'Сніжинка']
        avg_times = [avg_star, avg_snowflake]
        colors_avg = ['skyblue', 'lightcoral']
        
        bars = ax3.bar(schemas, avg_times, color=colors_avg, alpha=0.8)
        ax3.set_ylabel('Середній час (мс)')
        ax3.set_title('Середня продуктивність')
        ax3.grid(True, alpha=0.3)
        
        for bar in bars:
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{height:.1f} мс', ha='center', va='bottom', fontweight='bold')
        
        # 4. Таблиця зі статистикою
        ax4 = axes[1, 1]
        ax4.axis('tight')
        ax4.axis('off')
        
        # Розрахунок статистики
        star_wins = sum(1 for r in ratios if r > 1)
        snowflake_wins = len(ratios) - star_wins
        avg_improvement = ratios.mean()
        
        stats_data = [
            ['Метрика', 'Значення'],
            ['Перемог Зірки', f'{star_wins}/{len(ratios)}'],
            ['Перемог Сніжинки', f'{snowflake_wins}/{len(ratios)}'],
            ['Середнє співвідношення', f'{avg_improvement:.2f}x'],
            ['Середній час Зірки', f'{avg_star:.1f} мс'],
            ['Середній час Сніжинки', f'{avg_snowflake:.1f} мс'],
            ['Загальне покращення', f'{((avg_improvement - 1) * 100):.1f}%']
        ]
        
        table = ax4.table(cellText=stats_data[1:], colLabels=stats_data[0], 
                         cellLoc='center', loc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.5)
        ax4.set_title('Статистика продуктивності', y=0.95)
        
        plt.tight_layout()
        
        # Збереження графіка
        output_path = Path('data/performance_comparison.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Графік збережено: {output_path}")
        
        plt.show()
    
    def print_detailed_results(self):
        """Виводить детальні результати"""
        print("\\n" + "="*80)
        print("ДЕТАЛЬНІ РЕЗУЛЬТАТИ ПОРІВНЯННЯ ПРОДУКТИВНОСТІ")
        print("="*80)
        
        for i, result in enumerate(self.results, 1):
            query_name = result['query_name']
            star_time = result['star_time_ms']
            snowflake_time = result['snowflake_time_ms']
            ratio = snowflake_time / star_time if star_time > 0 else 0
            
            print(f"\\n{i}. {query_name}")
            print("-" * (len(query_name) + 4))
            print(f"Зірка:     {star_time:.2f} мс")
            print(f"Сніжинка:  {snowflake_time:.2f} мс")
            
            if ratio > 1:
                print(f"Результат: Зірка швидше в {ratio:.2f} рази ✓")
            else:
                print(f"Результат: Сніжинка швидше в {1/ratio:.2f} рази ✓")
        
        # Загальна статистика
        star_times = [r['star_time_ms'] for r in self.results]
        snowflake_times = [r['snowflake_time_ms'] for r in self.results]
        
        print("\\n" + "="*80)
        print("ЗАГАЛЬНА СТАТИСТИКА")
        print("="*80)
        print(f"Середній час Зірки:     {np.mean(star_times):.2f} мс")
        print(f"Середній час Сніжинки:  {np.mean(snowflake_times):.2f} мс")
        print(f"Медіанний час Зірки:    {np.median(star_times):.2f} мс")
        print(f"Медіанний час Сніжинки: {np.median(snowflake_times):.2f} мс")
        
        ratios = [s/n if n > 0 else 0 for s, n in zip(snowflake_times, star_times)]
        star_wins = sum(1 for r in ratios if r > 1)
        
        print(f"\\nПеремог схеми 'Зірка': {star_wins}/{len(self.results)}")
        print(f"Переваг схеми 'Сніжинка': {len(self.results) - star_wins}/{len(self.results)}")

def main():
    """Основна функція для запуску порівняння продуктивності"""
    comparator = PerformanceComparator()
    
    # Запуск тестів
    results = comparator.run_performance_tests()
    
    # Виведення детальних результатів
    comparator.print_detailed_results()
    
    # Створення візуалізації
    comparator.create_performance_visualization()
    
    return results

if __name__ == "__main__":
    main()