import pandas as pd
from sqlalchemy import create_engine
import yaml


with open('config.yml', 'r') as f:
    config_co = yaml.safe_load(f)['CO_SA']
    config_etl = yaml.safe_load(f)['ETL_PRO']

# Construct the database URL
url_co = (f"{config_co['drivername']}://{config_co['user']}:{config_co['password']}@{config_co['host']}:"
          f"{config_co['port']}/{config_co['dbname']}")
url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@{config_etl['host']}:"
           f"{config_etl['port']}/{config_etl['dbname']}")
# Create the SQLAlchemy Engine
co_sa = create_engine(url_co)
etl_conn = create_engine(url_etl)


dim_fecha = pd.DataFrame({
    "date": pd.date_range(start='1/1/2005', end='1/1/2009', freq='D')
})
dim_fecha.head()

dim_fecha["year"] = dim_fecha["date"].dt.year
dim_fecha["month"] = dim_fecha["date"].dt.month
dim_fecha["day"] = dim_fecha["date"].dt.day
dim_fecha["weekday"] = dim_fecha["date"].dt.weekday
dim_fecha["quarter"] = dim_fecha["date"].dt.quarter

dim_fecha.head()

dim_fecha["day_of_year"] = dim_fecha["date"].dt.day_of_year
dim_fecha["day_of_month"] = dim_fecha["date"].dt.days_in_month
dim_fecha["month_str"] = dim_fecha["date"].dt.month_name() # run locale -a en unix
dim_fecha["day_str"] = dim_fecha["date"].dt.day_name() # locale = 'es_CO.UTF8'
dim_fecha["date_str"] = dim_fecha["date"].dt.strftime("%d/%m/%Y")
dim_fecha.head()

co_holidays = holidays.CO(language="es")
dim_fecha["is_Holiday"] = dim_fecha["date"].apply(lambda x:  x in co_holidays)
dim_fecha["holiday"] = dim_fecha["date"].apply(lambda x: co_holidays.get(x))

dim_fecha["weekend"] = dim_fecha["weekday"].apply(lambda x: x>4)
dim_fecha.head()

with open("config.yml", 'r') as f:
    config = yaml.safe_load(f)
