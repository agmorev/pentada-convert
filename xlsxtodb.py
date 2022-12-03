import pandas as pd
from bs4 import BeautifulSoup
import requests
import sqlite3


# CREATE BROKERS DATABASE

# Get xls file with the information about all brokers registered in customs
q = requests.get("https://data.gov.ua/dataset/scsu-register-customs-brokers")
soup = BeautifulSoup(q.text, 'lxml')
link = soup.find("a", {"class": "resource-url-analytics"})['href']
df = pd.read_excel(link, sheet_name=0, skiprows=[0, 2], dtype={3: str}, parse_dates=[2,7], na_filter=False)

# Rename columns
df.columns.values[0] = "SEQ"
df.columns.values[1] = "NUMBER"
df.columns.values[2] = "DATE"
df.columns.values[3] = "CODE"
df.columns.values[4] = "NAME"
df.columns.values[5] = "ADDRESS"
df.columns.values[6] = "PROT_NUM"
df.columns.values[7] = "PROT_DATE"
df.columns.values[8] = "NOTE"

# Change columns format of data
df['NAME'] = df['NAME'].str.upper()
df['ADDRESS'] = df['ADDRESS'].str.upper()
print(df)

# Save dataframe to sqlite database
conn = sqlite3.connect('/home/agmorev/pentadabot_v2/data/brokers.db')
# conn = sqlite3.connect('D:\PYTHON\PROJECTS\Bots\pentadabot_v2\data\/brokers.db')
df.to_sql('brokers', conn, if_exists='replace', index=False)


# CREATE ZED DATABASE

# Get xls file with the information about all zed companies registered in customs
q = requests.get("https://data.gov.ua/dataset/scsu-register-trade-entity")
soup = BeautifulSoup(q.text, 'lxml')
link = soup.find("a", {"class": "resource-url-analytics"})['href']
df = pd.read_excel(link, sheet_name=0, dtype={0: str}, parse_dates=[4,5], na_filter=False, engine="openpyxl")

# Rename columns
df.columns.values[0] = "CODE"
df.columns.values[1] = "ZED_CODE"
df.columns.values[2] = "NAME"
df.columns.values[3] = "ADDRESS"
df.columns.values[4] = "REG_DATE"
df.columns.values[5] = "DEL_DATE"

# Change columns format of data
df['NAME'] = df['NAME'].str.upper()
df['ADDRESS'] = df['ADDRESS'].str.upper()
print(df)

# Save dataframe to sqlite database
conn = sqlite3.connect('/home/agmorev/pentadabot_v2/data/companies.db')
# conn = sqlite3.connect('D:\PYTHON\PROJECTS\Bots\pentadabot_v2\data\companies.db')
df.to_sql('companies', conn, if_exists='replace', index=False)