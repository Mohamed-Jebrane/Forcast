import subprocess
import rioxarray
import xarray as xa
import sqlalchemy as sa
import pandas as pd
import geopandas as gpd
from itertools import product
import seaborn as sns
import matplotlib.pyplot as plt
import os 
import boto3
import io
import json

WEAHTER_TABLE_NAME = "dnexr-structured-nws-ags-usa-weather_fcst_gefs_0Z-daily"

def get_secret(secrets_client, secret_arn):
    """
    Fetch secrets from AWS Secrets Manager.
    Args:
        secrets_client (botocore.client.BaseClient)
        : boto3 secrets manager client.
        secret_arn (str): The ARN of the secret to retrieve.
    Returns:
        dict: The retrieved secret values.
    """
    response = secrets_client.get_secret_value(SecretId=secret_arn)
    if 'SecretString' in response:
        secret = response['SecretString']
    else:
        secret = json.loads(response['SecretBinary'])
    return json.loads(secret)


def get_credentials():
    secrets_client = boto3.client('secretsmanager', region_name='eu-west-1')
    secret_arn = os.getenv('SNOWFLAKE_CREDENTIALS_SECRET_ARN')
    return get_secret(secrets_client, secret_arn)
"""
mkdir(string) -> void

create a directory if the path doesn't exist

"""


CLIENT_CODE=get_credentials()["database"]
def mkdir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def snowflake_credentials():
    dic=get_credentials()

    return sa.create_engine
    (
        "snowflake://{user}:{password}@{account}/{database}?warehouse={warehouse}&role={role}".format(
            user=dic["username"],
            password=dic["password"],
            account=dic["account"],
            database=dic["database"],
            warehouse=dic["warehouse"],
            role=dic["role"]

        )
    )

def get_gefs(forecast_run, min_date, max_date):
    engine =  snowflake_credentials()

    sql_gefs = f"""
    SELECT 
    "latitude" as "lat", 
    "longitude" as "lon", 
    SUM("precipitation") as "precip_gefs" 
    FROM "DATASET"."{WEAHTER_TABLE_NAME}" 
    WHERE "run_datetime" = '{forecast_run}' 
    and "forecast_date" >= '{min_date}' 
    and "forecast_date" <= '{max_date}' 
    GROUP BY "latitude", "longitude" """

    da_gefs = (
        pd.read_sql(sql_gefs, engine)
        .set_index(["lat", "lon"])
        .to_xarray()["precip_gefs"]
    )
