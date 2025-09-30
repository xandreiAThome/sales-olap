from util.db_source import products
from models.Dim_Products import dim_products
import pandas as pd
from util.db_source import Session_db_source
from util.db_warehouse import Session_db_warehouse


def extract_products():
    session = Session_db_source()
    result = session.execute(products.select())
    products_data = result.fetchall()
    session.close()
    return products_data


def clean_products_data(data):
    df = pd.DataFrame([dict(row._mapping) for row in data])
    df.dropna(inplace=True)

    df["productCode"] = df["productCode"].str.strip()
    df["category"] = df["category"].str.lower().str.strip()
    df["description"] = df["description"].str.strip()
    df["name"] = df["name"].str.title().str.strip()

    cleaned_data = df.to_dict(orient="records")

    return cleaned_data


def transform_and_load_products():
    products_data = extract_products()
    cleaned_products = clean_products_data(products_data)

    session = Session_db_warehouse()

    for product in cleaned_products:
        product_record = {
            "Product_Code": product["productCode"],
            "Product_ID": product["id"],
            "Name": product["name"],
            "Category": product["category"],
            "Description": product["description"],
            "Price": product["price"],
        }
        insert_stmt = dim_products.insert().values(**product_record)
        session.execute(insert_stmt)
    session.commit()
    session.close()
