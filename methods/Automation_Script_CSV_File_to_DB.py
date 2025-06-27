# Databricks notebook source
# DBTITLE 1,DBFS_File_Location
# MAGIC  %fs ls dbfs:/FileStore/tables/FileStore/

# COMMAND ----------

# DBTITLE 1,Reading_Src_Trgt_Files
# Cell 1: Configuration and DataFrame loading

# Change only these 3 values per validation run
csv_path = "dbfs:/FileStore/tables/CMO_Tables/MICROSIZE_POWDERSIZE_INV_REC_20250618_105100.csv"
table_name ="gms_us_lake.gmsgq_cmo_microsize_powdersize_inv_rec"
# db_name = "gms_us_lake"

from pyspark.sql import functions as F

# Load CSV file into source DataFrame
try:
    df_source = spark.read.option("header", "true").csv(csv_path)
    source_loaded = True
except Exception as e:
    print(f"Error reading source file: {e}")
    source_loaded = False

# Step 2: Extract filename from path
file_name = csv_path.split("/")[-1]

# Load target table with dynamic filtering
try:
    df_target = spark.table(table_name) \
                     .filter((F.col("active_flag") == "Y") & (F.col("aud_file_nm") == file_name))
    target_loaded = True
except Exception as e:
    print(f"Error reading target table: {e}")
    target_loaded = False

# Display sample records
if source_loaded:
    df_source.show(1)
if target_loaded:
    df_target.show(1)

# Final success message
if source_loaded and target_loaded:
    print(f"✅ Source file and target table loaded successfully for file: {file_name}")
else:
    print("❌ One or both datasets failed to load. Please check the logs above.")



# COMMAND ----------

# DBTITLE 1,Count_check
from pyspark.sql.functions import lit

# Count rows with error handling
try:
    df_source_count = df_source.count()
except Exception as e:
    df_source_count = -1
    print(f"Error counting source DataFrame: {e}")

try:
    df_target_count = df_target.count()
except Exception as e:
    df_target_count = -1
    print(f"Error counting target DataFrame: {e}")

# Determine status
status = "Passed" if df_source_count == df_target_count and df_source_count >= 0 else "Failed"

# Create summary DataFrame with status
summary_df = spark.createDataFrame([
    (csv_path, 'source', df_source_count, status),
    (table_name, 'target', df_target_count, status)
], ['table_name', 'source_or_target', 'row_count', 'status'])

# Show result
summary_df.show(truncate=False)

# Optional print message
print(f"✅ Validation {status}: Source count = {df_source_count}, Target count = {df_target_count}")


# COMMAND ----------

# DBTITLE 1,Duplicate_check
# Step 1: Read the merge key columns string from BatchConfig for the given table
try:
    key_cols_str = spark.sql(f"""
        SELECT merge_key_columns 
        FROM gms_us_lake.gmsgq_srctolake_batch_config  
        WHERE target_table = '{table_name}'
    """).collect()[0][0]
except IndexError:
    print(f"❌ No config found for table: {table_name}")
    key_cols_str = ""

# Step 2: Split the string into a list of key columns (strip spaces)
key_cols = [col.strip() for col in key_cols_str.split(',') if col.strip()]

if key_cols:
    # Step 3: Prepare the GROUP BY and SELECT column lists as strings
    group_by_cols = ", ".join(key_cols)
    select_cols = ", ".join(key_cols)

    # Step 4: Build the dynamic SQL query to find duplicates with filters
    duplicate_query = f"""
    SELECT {select_cols}, COUNT(*) AS duplicate_count
    FROM {table_name}
    WHERE active_flag = 'Y' AND aud_file_nm = '{file_name}'
    GROUP BY {group_by_cols}
    HAVING COUNT(*) > 1
    """

    print("\n▶️ Running duplicate check query:")
    print(duplicate_query)

    # Step 5: Execute the query and show the duplicate records
    duplicates_df = spark.sql(duplicate_query)
    duplicates_count = duplicates_df.count()

    # Step 6: Display result
    print(f"\n🔍 Duplicates Found: {duplicates_count}")

    if duplicates_count == 0:
        print(f"✅ Duplicate Check Passed: No duplicates found for table '{table_name}' based on keys: {key_cols_str}")
        validation_status = "Passed"
    else:
        print(f"❌ Duplicate Check Failed: {duplicates_count} duplicates found for table '{table_name}' based on keys: {key_cols_str}")
        duplicates_df.show(truncate=False)
        validation_status = "Failed"
else:
    validation_status = "Failed"
    print(f"❌ Merge key columns are not defined for table: {table_name}")

# Optional: Store the result in a summary variable for logging/reporting if needed
duplicate_check_summary = {
    "table_name": table_name,
    "file_name": file_name,
    "key_columns": key_cols_str,
    "duplicates_found": duplicates_count if key_cols else "N/A",
    "status": validation_status
}




# COMMAND ----------

# DBTITLE 1,Audit_column_Null_Check
# Define audit columns
audit_cols = ["AUD_HASH_VAL","AUD_SRC_SYS_ID", "AUD_FILE_NM", "AUD_LD_DTS",
              "AUD_UPD_DTS", "AUD_CRT_DTS", "AUD_LD_BTCH_ID"]

# Build filter condition to check for NULLs
null_condition = " OR ".join([f"{col} IS NULL" for col in audit_cols])

# Perform the null check
null_count = df_target.filter(null_condition).count()

# Display result
if null_count == 0:
    print(f"✅ Audit Column Null Check Passed: No NULL values found in audit columns for table '{table_name}'.")
    audit_status = "Passed"
else:
    print(f"❌ Audit Column Null Check Failed: Found {null_count} rows with NULL values in audit columns for table '{table_name}'.")
    audit_status = "Failed"
# Optional: Store result in a dictionary for reporting
audit_null_check_summary = {
    "table_name": table_name,
    "file_name": file_name,
    "null_rows_found": null_count,
    "status": audit_status
}

# COMMAND ----------

# DBTITLE 1,SourceMinusTarget
# Step 1: Drop audit columns
audit_cols = ["AUD_HASH_VAL", "AUD_SRC_SYS_ID", "AUD_FILE_NM", "AUD_LD_DTS",
              "AUD_UPD_DTS", "AUD_CRT_DTS", "AUD_LD_BTCH_ID"]
df_target_clean = df_target.drop(*audit_cols)

# Step 2: Normalize column names to lowercase
df_source = df_source.toDF(*[c.lower() for c in df_source.columns])
df_target_clean = df_target_clean.toDF(*[c.lower() for c in df_target_clean.columns])

# Step 3: Register as temp views
df_source.createOrReplaceTempView("source_view")
df_target_clean.createOrReplaceTempView("target_view")

# Step 4: Get common columns only
common_columns = sorted(set(df_source.columns).intersection(set(df_target_clean.columns)))

# Step 5: Create standardized SELECT expressions
select_list_source = []
select_list_target = []

for col in common_columns:
    if col == "vendor_invt_qty":
        select_list_source.append(f"COALESCE(CAST(TRIM({col}) AS DECIMAL(20,6)), 0) AS {col}")
        select_list_target.append(f"COALESCE({col}, 0) AS {col}")
    else:
        select_list_source.append(f"COALESCE(TRIM(CAST({col} AS STRING)), '') AS {col}")
        select_list_target.append(f"COALESCE(TRIM(CAST({col} AS STRING)), '') AS {col}")

select_source_sql = ", ".join(select_list_source)
select_target_sql = ", ".join(select_list_target)

# Step 6: Source minus Target
print("▶️ Running Source Minus Target comparison...")
source_minus_query = f"""
SELECT {select_source_sql} FROM source_view
EXCEPT
SELECT {select_target_sql} FROM target_view
"""
df_source_minus_target = spark.sql(source_minus_query)
source_minus_count = df_source_minus_target.count()
print(f"🔍 Source records NOT in target: {source_minus_count}")

# Step 7: Target minus Source (optional, for bidirectional check)
print("▶️ Running Target Minus Source comparison...")
target_minus_query = f"""
SELECT {select_target_sql} FROM target_view
EXCEPT
SELECT {select_source_sql} FROM source_view
"""
df_target_minus_source = spark.sql(target_minus_query)
target_minus_count = df_target_minus_source.count()
print(f"🔍 Target records NOT in source: {target_minus_count}")

# Step 8: Show mismatched records (optional)
if source_minus_count > 0:
    print("\n🔹 Example mismatches from Source - Target:")
    df_source_minus_target.show(truncate=False)

if target_minus_count > 0:
    print("\n🔹 Example mismatches from Target - Source:")
    df_target_minus_source.show(truncate=False)

# Step 9: Validation status
if source_minus_count == 0 and target_minus_count == 0:
    print("✅ Data Match Validation Passed: Source and Target data match.")
    data_match_status = "Passed"
else:
    print("❌ Data Match Validation Failed: Source and Target data mismatch found.")
    data_match_status = "Failed"

# Step 10: Create a summary dict for reporting
data_match_summary = {
    "table_name": table_name,
    "file_name": file_name,
    "columns_compared": common_columns,
    "source_minus_target_count": source_minus_count,
    "target_minus_source_count": target_minus_count,
    "status": data_match_status
}


# COMMAND ----------

# DBTITLE 1,TargetMinusSource
# 🔁 Step: Run Target MINUS Source
sql_query_tgt_minus_src = f"""
SELECT {select_target_sql} FROM target_view
EXCEPT
SELECT {select_source_sql} FROM source_view
"""

print("▶️ Running Target Minus Source query...")

# Execute query
df_target_minus_source = spark.sql(sql_query_tgt_minus_src)
target_minus_count = df_target_minus_source.count()

# Show result count
print(f"🔍 Target records NOT in Source: {target_minus_count}")

# Show mismatched rows only if present
if target_minus_count > 0:
    print("\n🔹 Example mismatches from Target - Source:")
    df_target_minus_source.show(truncate=False)
    tgt_minus_src_status = "Failed"
else:
    print("✅ Target Minus Source Check Passed: No extra records in target.")
    tgt_minus_src_status = "Passed"

# Optional summary dictionary
target_minus_source_summary = {
    "table_name": table_name,
    "file_name": file_name,
    "direction": "Target Minus Source",
    "mismatch_count": target_minus_count,
    "status": tgt_minus_src_status
}
