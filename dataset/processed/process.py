import pandas as pd

df = pd.read_csv("../raw/Superstore.csv", encoding="latin1")

df = df[["Order ID", "Category", "Sub-Category", "Quantity", "Sales"]].head(100)


df.to_csv("sales_orders_100.csv", index=False)