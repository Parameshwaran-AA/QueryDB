import pandas as pd
import psycopg2
from psycopg2 import Error
import os
import psycopg2.extras

DB_CONNECTION_STRING = "postgresql://db_test_yl7j_user:al0VBcLZP5EQt4FQgGLqsXxGmAKVZiUJ@dpg-d4mfa2ali9vc73epf9h0-a.oregon-postgres.render.com/db_test_yl7j"

def create_connection(db_file, delete_db=False):
    conn = None
    try:
        conn = psycopg2.connect(DB_CONNECTION_STRING)
    except Error as e:
        print(e)
    return conn

def create_table(conn, create_table_sql, drop_table_name=None):
    if drop_table_name:
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s CASCADE""" % (drop_table_name))
            conn.commit()
        except Error as e:
            print(e)
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        conn.commit()
    except Error as e:
        print(e)

def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)
    rows = cur.fetchall()
    return rows

def step1_create_region_table(data_filename, normalized_database_filename):
    regions = set()
    with open(data_filename, "r", encoding="utf-8") as f:
        header = f.readline()
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) > 4:
                region = parts[4].strip()
                if region:
                    regions.add(region)
    sorted_regions = sorted(list(regions))
    conn = create_connection(normalized_database_filename)
    create_region_sql = """
        CREATE TABLE IF NOT EXISTS Region (
            RegionID SERIAL PRIMARY KEY,
            Region TEXT NOT NULL
        );
    """
    create_table(conn, create_region_sql, drop_table_name="Region")
    insert_sql = "INSERT INTO Region (Region) VALUES (%s);"
    values = [(r,) for r in sorted_regions]
    with conn:
        cur = conn.cursor()
        cur.executemany(insert_sql, values)
    conn.close()

def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    sql = "SELECT RegionID, Region FROM Region;"
    rows = execute_sql_statement(sql, conn)
    region_dict = {}
    for region_id, region_name in rows:
        region_dict[region_name] = region_id
    conn.close()
    return region_dict

def step3_create_country_table(data_filename, normalized_database_filename):
    region_dict = step2_create_region_to_regionid_dictionary(normalized_database_filename)
    country_region_pairs = set()
    with open(data_filename, "r", encoding="utf-8") as f:
        header = f.readline()
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) > 4:
                country = parts[3].strip()
                region = parts[4].strip()
                if country and region:
                    region_id = region_dict[region]
                    country_region_pairs.add((country, region_id))
    sorted_countries = sorted(list(country_region_pairs), key=lambda x: x[0])
    conn = create_connection(normalized_database_filename)
    create_country_sql = """
        CREATE TABLE IF NOT EXISTS Country (
            CountryID SERIAL PRIMARY KEY,
            Country TEXT NOT NULL,
            RegionID INTEGER NOT NULL,
            FOREIGN KEY (RegionID) REFERENCES Region(RegionID)
        );
    """
    create_table(conn, create_country_sql, drop_table_name="Country")
    insert_sql = "INSERT INTO Country (Country, RegionID) VALUES (%s, %s);"
    values = [(country, region_id) for country, region_id in sorted_countries]
    with conn:
        cur = conn.cursor()
        cur.executemany(insert_sql, values)
    conn.close()

def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    sql = "SELECT CountryID, Country FROM Country;"
    rows = execute_sql_statement(sql, conn)
    country_dict = {}
    for country_id, country_name in rows:
        country_dict[country_name] = country_id
    conn.close()
    return country_dict

def step5_create_customer_table(data_filename, normalized_database_filename):
    country_dict = step4_create_country_to_countryid_dictionary(normalized_database_filename)
    customers = set()
    with open(data_filename, "r", encoding="utf-8") as f:
        header = f.readline()
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) > 3:
                name = parts[0].strip()
                address = parts[1].strip()
                city = parts[2].strip()
                country = parts[3].strip()
                if country not in country_dict:
                    continue
                country_id = country_dict[country]
                try:
                    first, last = name.split(" ", 1)
                except ValueError:
                    first = name
                    last = ""
                customers.add((first, last, address, city, country_id))
    sorted_customers = sorted(list(customers), key=lambda x: (x[0], x[1]))
    conn = create_connection(normalized_database_filename)
    create_customer_sql = """
        CREATE TABLE IF NOT EXISTS Customer (
            CustomerID SERIAL PRIMARY KEY,
            FirstName TEXT NOT NULL,
            LastName TEXT NOT NULL,
            Address TEXT NOT NULL,
            City TEXT NOT NULL,
            CountryID INTEGER NOT NULL,
            FOREIGN KEY (CountryID) REFERENCES Country(CountryID)
        );
    """
    create_table(conn, create_customer_sql, drop_table_name="Customer")
    insert_sql = """
        INSERT INTO Customer (FirstName, LastName, Address, City, CountryID)
        VALUES (%s, %s, %s, %s, %s);
    """
    with conn:
        cur = conn.cursor()
        cur.executemany(insert_sql, sorted_customers)
    conn.close()

def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    sql = "SELECT CustomerID, FirstName, LastName FROM Customer;"
    rows = execute_sql_statement(sql, conn)
    customer_dict = {}
    for customer_id, first, last in rows:
        full_name = f"{first} {last}".strip()
        customer_dict[full_name] = customer_id
    conn.close()
    return customer_dict

def step7_create_productcategory_table(data_filename, normalized_database_filename):
    categories = set()
    with open(data_filename, "r", encoding="utf-8") as f:
        header = f.readline()
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) > 7:
                product_categories = parts[6].split(";")
                product_descriptions = parts[7].split(";")
                for cat, desc in zip(product_categories, product_descriptions):
                    cat = cat.strip()
                    desc = desc.strip()
                    if cat and desc:
                        categories.add((cat, desc))
    sorted_categories = sorted(list(categories), key=lambda x: x[0])
    conn = create_connection(normalized_database_filename)
    create_pc_sql = """
        CREATE TABLE IF NOT EXISTS ProductCategory (
            ProductCategoryID SERIAL PRIMARY KEY,
            ProductCategory TEXT NOT NULL,
            ProductCategoryDescription TEXT NOT NULL
        );
    """
    create_table(conn, create_pc_sql, drop_table_name="ProductCategory")
    insert_sql = """
        INSERT INTO ProductCategory (ProductCategory, ProductCategoryDescription)
        VALUES (%s, %s);
    """
    with conn:
        cur = conn.cursor()
        cur.executemany(insert_sql, sorted_categories)
    conn.close()

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    sql = "SELECT ProductCategoryID, ProductCategory FROM ProductCategory;"
    rows = execute_sql_statement(sql, conn)
    pc_dict = {}
    for pc_id, pc_name in rows:
        pc_dict[pc_name] = pc_id
    conn.close()
    return pc_dict

def step9_create_product_table(data_filename, normalized_database_filename):
    pc_dict = step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
    products = set()
    with open(data_filename, "r", encoding="utf-8") as f:
        header = f.readline()
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) > 8:
                names = parts[5].split(";")
                categories = parts[6].split(";")
                prices = parts[8].split(";")
                for name, cat, price in zip(names, categories, prices):
                    name = name.strip()
                    cat = cat.strip()
                    try:
                        price = float(price.strip())
                    except ValueError:
                        continue
                    if name and cat and cat in pc_dict:
                        pc_id = pc_dict[cat]
                        products.add((name, price, pc_id))
    sorted_products = sorted(list(products), key=lambda x: x[0])
    conn = create_connection(normalized_database_filename)
    create_product_sql = """
        CREATE TABLE IF NOT EXISTS Product (
            ProductID SERIAL PRIMARY KEY,
            ProductName TEXT NOT NULL,
            ProductUnitPrice REAL NOT NULL,
            ProductCategoryID INTEGER NOT NULL,
            FOREIGN KEY (ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID)
        );
    """
    create_table(conn, create_product_sql, drop_table_name="Product")
    insert_sql = """
        INSERT INTO Product (ProductName, ProductUnitPrice, ProductCategoryID)
        VALUES (%s, %s, %s);
    """
    with conn:
        cur = conn.cursor()
        cur.executemany(insert_sql, sorted_products)
    conn.close()

def step10_create_product_to_productid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    sql = "SELECT ProductID, ProductName FROM Product;"
    rows = execute_sql_statement(sql, conn)
    product_dict = {}
    for product_id, product_name in rows:
        product_dict[product_name] = product_id
    conn.close()
    return product_dict

def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    import datetime
    product_dict = step10_create_product_to_productid_dictionary(normalized_database_filename)
    customer_dict = step6_create_customer_to_customerid_dictionary(normalized_database_filename)
    conn = create_connection(normalized_database_filename)
    create_order_sql = """
        CREATE TABLE IF NOT EXISTS OrderDetail (
            OrderID SERIAL PRIMARY KEY,
            CustomerID INTEGER NOT NULL,
            ProductID INTEGER NOT NULL,
            OrderDate DATE NOT NULL,
            QuantityOrdered INTEGER NOT NULL,
            FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
            FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
        );
    """
    create_table(conn, create_order_sql, drop_table_name="OrderDetail")
    batch_size = 5000
    orders_batch = []
    insert_sql = "INSERT INTO OrderDetail (CustomerID, ProductID, OrderDate, QuantityOrdered) VALUES %s"
    cur = conn.cursor()
    with open(data_filename, "r", encoding="utf-8") as f:
        header = f.readline()
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) <= 10:
                continue
            name = parts[0]
            try:
                sp = name.split(" ", 1)
                first = sp[0]
                last = sp[1] if len(sp) > 1 else ""
                customer_fullname = f"{first} {last}".strip()
            except:
                continue
            customer_id = customer_dict.get(customer_fullname)
            if customer_id is None:
                continue
            product_names = parts[5].split(";")
            quantities = parts[9].split(";")
            dates = parts[10].split(";")
            for i in range(len(product_names)):
                pname = product_names[i].strip()
                if not pname:
                    continue
                product_id = product_dict.get(pname)
                if product_id is None:
                    continue
                try:
                    q_str = quantities[i].strip()
                    if '.' in q_str:
                        qty = int(float(q_str))
                    else:
                        qty = int(q_str)
                except:
                    qty = 0
                try:
                    d_str = dates[i].strip()
                    if len(d_str) == 8:
                        orderdate = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
                    else:
                        import datetime
                        orderdate = datetime.datetime.strptime(d_str, "%Y%m%d").strftime("%Y-%m-%d")
                except:
                    continue
                orders_batch.append((customer_id, product_id, orderdate, qty))
            if len(orders_batch) >= batch_size:
                try:
                    psycopg2.extras.execute_values(cur, insert_sql, orders_batch)
                    conn.commit()
                    orders_batch = []
                except Error as e:
                    print(e)
    if orders_batch:
        try:
            psycopg2.extras.execute_values(cur, insert_sql, orders_batch)
            conn.commit()
        except Error as e:
            print(e)
    conn.close()

if __name__ == "__main__":
    data_file = "data.csv"
    db_name = "postgres_db"
    step1_create_region_table(data_file, db_name)
    step3_create_country_table(data_file, db_name)
    step5_create_customer_table(data_file, db_name)
    step7_create_productcategory_table(data_file, db_name)
    step9_create_product_table(data_file, db_name)
    step11_create_orderdetail_table(data_file, db_name)
    print("Normalization Complete!")
