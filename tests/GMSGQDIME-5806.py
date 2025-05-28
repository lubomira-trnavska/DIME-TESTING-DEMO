

table_name = "detst_eda.eda_de_raw.raw_product_materialmaster_eda"
column_name = "ROW_KEY"

null_percent = null_percentage(table_name, column_name)
display(null_percent)

#print(f"Null percentage in '{column_name}' column: {null_pct:.2f}%")
