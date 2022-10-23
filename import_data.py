# -*- coding: utf-8 -*-
"""
Created on Tue Sep 27 14:06:58 2022

@author: ioannis.thanos
"""

import pandas as pd
# from sodapy import Socrata
import os
import psycopg2
import logging
from sqlalchemy import create_engine

################## Import Data ########################################################
path = "C:\\Users\\ioannis.thanos\\OneDrive - Accenture\\Documents\\\MScBigData\\"\
"Dissertation\\Data\\Final\\"

#### calendar
calendar = pd.read_csv(path + 'calendar.csv', sep=';')
#### cancer_mortality
cancer_mortality = pd.read_csv(path + 'cancer_mortality.csv', sep=';')
#### education
education = pd.read_csv(path + 'education.csv', sep=';')
#### poverty
poverty = pd.read_csv(path + 'poverty.csv', sep=';')
#### unemployment
unemployment = pd.read_csv(path + 'unemployment.csv', sep=';')
#### us_hiv_2019
us_hiv_2019 = pd.read_csv(path + 'us_hiv_2019.csv', sep=';')
#### us_population
us_population = pd.read_csv(path + 'us_population.csv', sep=';')
#### uscities
uscities = pd.read_csv(path + 'uscities.csv', sep=';')
#### Covid
cases_urlfile = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
cases = pd.read_csv(cases_urlfile, sep = ",")
cases['Type'] = 'cases'

deaths_urlfile = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"
deaths = pd.read_csv(deaths_urlfile, sep = ",")
deaths['Type'] = 'deaths'
deaths = deaths.drop(['Population'], axis=1)

covid = cases.append(deaths)
covid = covid.drop(['UID','iso2','iso3','code3'], axis=1)

#client = Socrata("healthdata.gov", None)
## dictionaries by sodapy.
# results = client.get("a6za-z3xi")
## Convert to pandas DataFrame
# covid = pd.DataFrame.from_records(results)
# covid.to_csv(r'C:\\Users\\ioannis.thanos\\OneDrive - Accenture\\Documents\\\MScBigData\\Dissertation\\Data\\Final\\covid.csv')
# covid_csv_drop = pd.read_csv(path + 'covid.csv').iloc[:, 1:]
#### Temperatures
# Folder Path
txtfolder = "C:\\Users\\ioannis.thanos\\OneDrive - Accenture\\Documents\\\MScBigData\\"\
"Dissertation\\Data\\Final\\Temperatures"

#Find the textfiles
textfiles = []
for root, folder, files in os.walk(txtfolder):
    for file in files:
        if file.endswith('.txt'):
            fullname = os.path.join(root, file)
            textfiles.append(fullname)
textfiles.sort() #Sort the filesnames

#Read each of them to a dataframe
for filenum, file in enumerate(textfiles, 1):
    if filenum==1:
        temper = pd.read_csv(file, names=['Month','Day','Year','Temperature'], delim_whitespace=True)
        temper['File']=os.path.basename(file)
    else:
        tempdf = pd.read_csv(file, names=['Month','Day','Year','Temperature'], delim_whitespace=True)
        tempdf['File']=os.path.basename(file)
        temper = pd.concat([temper, tempdf], ignore_index=True)
        
temper = temper[['File','Month','Day','Year','Temperature']] #Reorder columns

#######################################################################################

#######################################################################################
############################  Connection ##############################################
#######################################################################################

try:
    connection = psycopg2.connect(
            database="Dissertation", user="postgres", password="Password123", host="localhost",
            options="-c search_path=Raw"
       # database="courseworkbi", user="test", password="test", host="localhost"
    )
    logging.info("Connected to db")

# this catches everything
except Exception as e:
    logging.critical(str(e))


#cursor = connection.cursor()

engine = create_engine('postgresql://postgres:Password123@localhost:5432/Dissertation')
con = engine.connect()

#######################################################################################
############################  Export to PostgreSQL ####################################
#######################################################################################
calendar.to_sql("calendar", engine, schema='Raw')
cancer_mortality.to_sql("cancer_mortality", engine, schema='Raw')
education.to_sql("education", engine, schema='Raw')
poverty.to_sql("poverty", engine, schema='Raw')
unemployment.to_sql("unemployment", engine, schema='Raw')
us_hiv_2019.to_sql("us_hiv_2019", engine, schema='Raw')
us_population.to_sql("us_population", engine, schema='Raw')
uscities.to_sql("uscities", engine, schema='Raw')
covid.to_sql("covid", engine, schema='Raw')
temper.to_sql("temper", engine, schema='Raw')

#Closing the connection
connection.close()
#######################################################################################






