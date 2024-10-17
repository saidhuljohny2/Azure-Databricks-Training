# Databricks notebook source
pip install pandas faker

# COMMAND ----------

customers = "dbfs:/FileStore/tables/customers/"
orders = "dbfs:/FileStore/tables/orders/"
sales = "dbfs:/FileStore/tables/sales/"
products = "dbfs:/FileStore/tables/products/"
countries = "dbfs:/FileStore/tables/countries/"

# COMMAND ----------

import random
from faker import Faker

# Initialize Faker
fake = Faker()

def generate_customers_data(num_customers=100):
    customers_data = []
    for _ in range(num_customers):
        customers_data.append({
            'CustomerId': fake.random_int(min=1, max=num_customers),
            'Email': fake.email(),
            'Name': fake.name(),
            'Active': fake.boolean(),
            'Address': fake.address(),
            'City': fake.city(),
            'Country': fake.country()
        })
    return spark.createDataFrame(customers_data)

def generate_orders_data(num_orders=100, num_customers=100):
    orders_data = []
    for _ in range(num_orders):
        orders_data.append({
            'OrderId': fake.random_int(min=1, max=num_orders),
            'CustomerId': fake.random_int(min=1, max=num_customers),
            'Date': fake.date_this_decade().isoformat()
        })
    return spark.createDataFrame(orders_data)

def generate_sales_data(num_sales=200, num_orders=100, num_products=50):
    sales_data = []
    for _ in range(num_sales):
        sales_data.append({
            'SaleId': fake.random_int(min=1, max=num_sales),
            'OrderId': fake.random_int(min=1, max=num_orders),
            'ProductId': fake.random_int(min=1, max=num_products),
            'Quantity': fake.random_int(min=1, max=20)
        })
    return spark.createDataFrame(sales_data)

def generate_products_data(num_products=50):
    products_data = []
    for _ in range(num_products):
        products_data.append({
            'ProductId': fake.random_int(min=1, max=num_products),
            'Name': fake.word(),
            'ManufacturedCountry': fake.country(),
            'WeightGrams': fake.random_int(min=100, max=10000)
        })
    return spark.createDataFrame(products_data)

def generate_countries_data(num_countries=100):
    countries_data = []
    for _ in range(num_countries):
        country_name = fake.country()
        countries_data.append({
            'Country': country_name,
            'Currency': fake.currency_code(),
            'Name': country_name,
            'Region': fake.random_element(elements=('Africa', 'Asia', 'Europe', 'North America', 'South America', 'Oceania')),
            'Population': fake.random_int(min=100000, max=1000000000),
            'Area_sq_mi': fake.random_int(min=500, max=17000000),
            'Pop_Density_per_sq_mi': round(random.uniform(1.0, 500.0), 2),
            'Coastline_coast_per_area_ratio': round(random.uniform(0.0, 50.0), 2),
            'Net_migration': round(random.uniform(-10.0, 10.0), 2),
            'Infant_mortality_per_1000_births': round(random.uniform(1.0, 100.0), 2),
            'GDP_per_capita': fake.random_int(min=500, max=100000),
            'Literacy_percent': round(random.uniform(30.0, 100.0), 2),
            'Phones_per_1000': round(random.uniform(50.0, 1000.0), 2),
            'Arable_percent': round(random.uniform(0.0, 50.0), 2),
            'Crops_percent': round(random.uniform(0.0, 10.0), 2),
            'Other_percent': round(100.0 - random.uniform(0.0, 60.0), 2),  # Ensuring the total is approximately 100%
            'Climate': round(random.uniform(1.0, 5.0), 2),
            'Birthrate': round(random.uniform(5.0, 40.0), 2),
            'Deathrate': round(random.uniform(1.0, 20.0), 2),
            'Agriculture': f"{round(random.uniform(0.0, 20.0), 2)}%",
            'Industry': f"{round(random.uniform(0.0, 40.0), 2)}%",
            'Service': f"{round(random.uniform(40.0, 100.0), 2)}%"  # Making sure service is the largest sector
        })
    return spark.createDataFrame(countries_data)

# Example usage:
customers_df = generate_customers_data()
orders_df = generate_orders_data()
sales_df = generate_sales_data()
products_df = generate_products_data()
countries_df = generate_countries_data()

# Display the generated data
customers_df.show(5)
orders_df.show(5)
sales_df.show(5)
products_df.show(5)
countries_df.show(5)

# write to dbfs
customers_df.coalesce(1).write.format("json").mode("append").save(customers)
orders_df.coalesce(1).write.format("json").mode("append").save(orders)
sales_df.coalesce(1).write.format("json").mode("append").save(sales)
products_df.coalesce(1).write.format("json").mode("append").save(products)
countries_df.coalesce(1).write.format("json").mode("append").save(countries)
