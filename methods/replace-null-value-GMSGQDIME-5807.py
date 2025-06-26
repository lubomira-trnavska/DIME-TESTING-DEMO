%python
##############
# UPLOAD CONFIG
##############
def upload_config(path:str, config:str) -> None:
    import boto3
    workspace = spark.conf.get("spark.databricks.workspaceUrl").split('.')[0].split('-')[1]
    if workspace == "usprd":
        bucket_name = "tpc-aws-ted-prd-edpp-raw-gms-us-east-1"
    elif workspace == "ustst":
        bucket_name = "tpc-aws-ted-tst-edpp-raw-gms-us-east-1"
    elif workspace == "usdev":
        bucket_name = "tpc-aws-ted-dev-edpp-raw-gms-us-east-1"
    s3 = boto3.client('s3')
    s3.put_object(Body=config,
                  Bucket=bucket_name,
                  Key=path)

    return None

###############
# CONFIG EXAMPLE
###############
config = """{
    "framework_type": "NRT",
    "tables_to_test": [
        [
            "WERUM_PASX - VIENNA",
            "gms_us_lake.gmsgq_werum_pasx_vienna_manufacturingorder",
            "gms_us_hub.ref_mfg_ord_mat_equip"
        ]
    ]
}"""
upload_config(path='gms_us_raw/GMSGQ_DDM/TESTING/CONFIG_FILES/SIT/release7_VIENNA_gms_us_hub.ref_mfg_ord_mat_equip_paragTest1.json',
              config=config)





%python
from pyspark.sql import DataFrame
from typing import Dict
from pyspark.sql.functions import col
def fill_missing_values(df: DataFrame, values_dict: Dict[str, str]) -> DataFrame:
  return df.fillna(values_dict)




%python
 
        from pyspark.sql.types import StructType, StructField, StringType
        data = [
            ("abc", None, None),
            (None, "262kah", "971")
        ]

        schema = StructType([
            StructField("col1", StringType(), True),
            StructField("col2", StringType(), True),
            StructField("col3", StringType(), True)
        ])

        df = spark.createDataFrame(data, schema)
        expected_data = [
            ("abc", None, "REPLACED"),
            ("notNULLnow", "262kah", "971")
        ]

        expected_df = spark.createDataFrame(expected_data, schema)
        
        result_df = fill_missing_values(df, {"col1": "notNULLnow", "col3": "REPLACED"})
        result_df.display()
        

      
    
    