# -*- coding: utf-8 -*-
"""
Created on Wed Sep 28 13:57:46 2022

@author: ioannis.thanos
"""

import psycopg2
import logging
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

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
############################  Import from PostgreSQL ##################################
#######################################################################################
connection = psycopg2.connect(user="postgres",
                              password="Password123",
                              host="localhost",
                              database="Dissertation")
cursor = connection.cursor()

query = 'select * from "Raw".covid'
covid = pd.read_sql(query,con=connection)
#### calendar
query = 'select * from "Raw".calendar'
calendar = pd.read_sql(query,con=connection)
#### cancer_mortality
query = 'select * from "Raw".cancer_mortality'
cancer_mortality = pd.read_sql(query,con=connection)
#### education
query = 'select * from "Raw".education'
education = pd.read_sql(query,con=connection)
#### poverty
query = 'select * from "Raw".poverty'
poverty = pd.read_sql(query,con=connection)
#### unemployment
query = 'select * from "Raw".unemployment'
unemployment = pd.read_sql(query,con=connection)
#### us_hiv_2019
query = 'select * from "Raw".us_hiv_2019'
us_hiv_2019 = pd.read_sql(query,con=connection)
#### us_population
query = 'select * from "Raw".us_population'
us_population = pd.read_sql(query,con=connection)
#### uscities
query = 'select * from "Raw".uscities'
uscities = pd.read_sql(query,con=connection)
#### temperatures
query = 'select * from "Raw".temper'
temper = pd.read_sql(query,con=connection)

#######################################################################################
########################################  ETLs ########################################
#######################################################################################
#### Covid ####
covid_cols = covid.columns.values.tolist()
remo_list = ('index','FIPS','Admin2','Province_State','Country_Region','Lat','Long_','Combined_Key','Type')
covid_cols_vars = [a for a in covid_cols if a not in remo_list]
covid_unpivot = pd.melt(
    covid, id_vars=["Type", "FIPS","Country_Region","Province_State","Lat","Long_","Combined_Key"],
    value_vars=['1/22/20','1/23/20','1/24/20','1/25/20','1/26/20','1/27/20','1/28/20','1/29/20','1/30/20',
                '1/31/20','2/1/20','2/2/20','2/3/20','2/4/20','2/5/20','2/6/20','2/7/20','2/8/20','2/9/20',
                '2/10/20','2/11/20','2/12/20','2/13/20','2/14/20','2/15/20','2/16/20','2/17/20','2/18/20',
                '2/19/20','2/20/20','2/21/20','2/22/20','2/23/20','2/24/20','2/25/20','2/26/20','2/27/20',
                '2/28/20','2/29/20','3/1/20','3/2/20','3/3/20','3/4/20','3/5/20','3/6/20','3/7/20','3/8/20',
                '3/9/20','3/10/20','3/11/20','3/12/20','3/13/20','3/14/20','3/15/20','3/16/20','3/17/20',
                '3/18/20','3/19/20','3/20/20','3/21/20','3/22/20','3/23/20','3/24/20','3/25/20','3/26/20',
                '3/27/20','3/28/20','3/29/20','3/30/20','3/31/20','4/1/20','4/2/20','4/3/20','4/4/20',
                '4/5/20','4/6/20','4/7/20','4/8/20','4/9/20','4/10/20','4/11/20','4/12/20','4/13/20',
                '4/14/20','4/15/20','4/16/20','4/17/20','4/18/20','4/19/20','4/20/20','4/21/20','4/22/20',
                '4/23/20','4/24/20','4/25/20','4/26/20','4/27/20','4/28/20','4/29/20','4/30/20','5/1/20',
                '5/2/20','5/3/20','5/4/20','5/5/20','5/6/20','5/7/20','5/8/20','5/9/20','5/10/20','5/11/20',
                '5/12/20','5/13/20','5/14/20','5/15/20','5/16/20','5/17/20','5/18/20','5/19/20','5/20/20',
                '5/21/20','5/22/20','5/23/20','5/24/20','5/25/20','5/26/20','5/27/20','5/28/20','5/29/20',
                '5/30/20','5/31/20','6/1/20','6/2/20','6/3/20','6/4/20','6/5/20','6/6/20','6/7/20','6/8/20',
                '6/9/20','6/10/20','6/11/20','6/12/20','6/13/20','6/14/20','6/15/20','6/16/20','6/17/20',
                '6/18/20','6/19/20','6/20/20','6/21/20','6/22/20','6/23/20','6/24/20','6/25/20','6/26/20',
                '6/27/20','6/28/20','6/29/20','6/30/20','7/1/20','7/2/20','7/3/20','7/4/20','7/5/20',
                '7/6/20','7/7/20','7/8/20','7/9/20','7/10/20','7/11/20','7/12/20','7/13/20','7/14/20',
                '7/15/20','7/16/20','7/17/20','7/18/20','7/19/20','7/20/20','7/21/20','7/22/20','7/23/20',
                '7/24/20','7/25/20','7/26/20','7/27/20','7/28/20','7/29/20','7/30/20','7/31/20','8/1/20','8/2/20','8/3/20','8/4/20','8/5/20','8/6/20','8/7/20','8/8/20','8/9/20','8/10/20','8/11/20','8/12/20','8/13/20','8/14/20','8/15/20','8/16/20','8/17/20','8/18/20','8/19/20','8/20/20','8/21/20','8/22/20','8/23/20','8/24/20','8/25/20','8/26/20','8/27/20','8/28/20','8/29/20','8/30/20','8/31/20','9/1/20','9/2/20','9/3/20','9/4/20','9/5/20','9/6/20','9/7/20','9/8/20','9/9/20','9/10/20','9/11/20','9/12/20','9/13/20','9/14/20','9/15/20','9/16/20','9/17/20','9/18/20','9/19/20','9/20/20','9/21/20','9/22/20','9/23/20','9/24/20','9/25/20','9/26/20','9/27/20','9/28/20','9/29/20','9/30/20','10/1/20','10/2/20','10/3/20','10/4/20','10/5/20','10/6/20','10/7/20','10/8/20','10/9/20','10/10/20','10/11/20','10/12/20','10/13/20','10/14/20','10/15/20','10/16/20','10/17/20','10/18/20','10/19/20','10/20/20','10/21/20','10/22/20','10/23/20','10/24/20','10/25/20','10/26/20','10/27/20','10/28/20','10/29/20','10/30/20','10/31/20','11/1/20','11/2/20','11/3/20','11/4/20','11/5/20','11/6/20','11/7/20','11/8/20','11/9/20','11/10/20','11/11/20','11/12/20','11/13/20','11/14/20','11/15/20','11/16/20','11/17/20','11/18/20','11/19/20','11/20/20','11/21/20','11/22/20','11/23/20','11/24/20','11/25/20','11/26/20','11/27/20','11/28/20','11/29/20','11/30/20','12/1/20','12/2/20','12/3/20','12/4/20','12/5/20','12/6/20','12/7/20','12/8/20','12/9/20','12/10/20','12/11/20','12/12/20','12/13/20','12/14/20','12/15/20','12/16/20','12/17/20','12/18/20','12/19/20','12/20/20','12/21/20','12/22/20','12/23/20','12/24/20','12/25/20','12/26/20','12/27/20','12/28/20','12/29/20','12/30/20','12/31/20','1/1/21','1/2/21','1/3/21','1/4/21','1/5/21','1/6/21','1/7/21','1/8/21','1/9/21','1/10/21','1/11/21','1/12/21','1/13/21','1/14/21','1/15/21','1/16/21','1/17/21','1/18/21','1/19/21','1/20/21','1/21/21','1/22/21','1/23/21','1/24/21','1/25/21','1/26/21','1/27/21','1/28/21','1/29/21','1/30/21','1/31/21','2/1/21','2/2/21','2/3/21','2/4/21','2/5/21','2/6/21','2/7/21','2/8/21','2/9/21','2/10/21','2/11/21','2/12/21','2/13/21','2/14/21','2/15/21','2/16/21','2/17/21','2/18/21','2/19/21','2/20/21','2/21/21','2/22/21','2/23/21','2/24/21','2/25/21','2/26/21','2/27/21','2/28/21','3/1/21','3/2/21','3/3/21','3/4/21','3/5/21','3/6/21','3/7/21','3/8/21','3/9/21','3/10/21','3/11/21','3/12/21','3/13/21','3/14/21','3/15/21','3/16/21','3/17/21','3/18/21','3/19/21','3/20/21','3/21/21','3/22/21','3/23/21','3/24/21','3/25/21','3/26/21','3/27/21','3/28/21','3/29/21','3/30/21','3/31/21','4/1/21','4/2/21','4/3/21','4/4/21','4/5/21','4/6/21','4/7/21','4/8/21','4/9/21','4/10/21','4/11/21','4/12/21','4/13/21','4/14/21','4/15/21','4/16/21','4/17/21','4/18/21','4/19/21','4/20/21','4/21/21','4/22/21','4/23/21','4/24/21','4/25/21','4/26/21','4/27/21','4/28/21','4/29/21','4/30/21','5/1/21','5/2/21','5/3/21','5/4/21','5/5/21','5/6/21','5/7/21','5/8/21','5/9/21','5/10/21','5/11/21','5/12/21','5/13/21','5/14/21','5/15/21','5/16/21','5/17/21','5/18/21','5/19/21','5/20/21',
                '5/21/21','5/22/21','5/23/21','5/24/21','5/25/21','5/26/21','5/27/21','5/28/21','5/29/21','5/30/21','5/31/21','6/1/21','6/2/21','6/3/21','6/4/21','6/5/21','6/6/21','6/7/21','6/8/21','6/9/21','6/10/21','6/11/21','6/12/21','6/13/21','6/14/21','6/15/21','6/16/21','6/17/21','6/18/21','6/19/21','6/20/21','6/21/21','6/22/21','6/23/21','6/24/21','6/25/21','6/26/21','6/27/21','6/28/21','6/29/21','6/30/21','7/1/21','7/2/21','7/3/21','7/4/21','7/5/21','7/6/21','7/7/21','7/8/21','7/9/21','7/10/21','7/11/21','7/12/21','7/13/21','7/14/21','7/15/21','7/16/21','7/17/21','7/18/21','7/19/21','7/20/21','7/21/21','7/22/21','7/23/21','7/24/21','7/25/21','7/26/21','7/27/21','7/28/21','7/29/21','7/30/21','7/31/21','8/1/21','8/2/21','8/3/21','8/4/21','8/5/21','8/6/21','8/7/21','8/8/21','8/9/21','8/10/21','8/11/21','8/12/21','8/13/21','8/14/21','8/15/21','8/16/21','8/17/21','8/18/21','8/19/21','8/20/21','8/21/21','8/22/21','8/23/21','8/24/21','8/25/21','8/26/21','8/27/21','8/28/21','8/29/21','8/30/21','8/31/21','9/1/21','9/2/21','9/3/21','9/4/21','9/5/21','9/6/21','9/7/21','9/8/21','9/9/21','9/10/21','9/11/21','9/12/21','9/13/21','9/14/21','9/15/21','9/16/21','9/17/21','9/18/21','9/19/21','9/20/21','9/21/21','9/22/21','9/23/21','9/24/21','9/25/21','9/26/21','9/27/21','9/28/21','9/29/21','9/30/21','10/1/21','10/2/21','10/3/21','10/4/21','10/5/21','10/6/21','10/7/21','10/8/21','10/9/21','10/10/21','10/11/21','10/12/21','10/13/21','10/14/21','10/15/21','10/16/21','10/17/21','10/18/21','10/19/21','10/20/21','10/21/21','10/22/21','10/23/21','10/24/21','10/25/21','10/26/21','10/27/21','10/28/21','10/29/21','10/30/21','10/31/21','11/1/21','11/2/21','11/3/21','11/4/21','11/5/21','11/6/21','11/7/21','11/8/21','11/9/21','11/10/21','11/11/21','11/12/21','11/13/21','11/14/21','11/15/21','11/16/21','11/17/21','11/18/21','11/19/21','11/20/21','11/21/21','11/22/21','11/23/21','11/24/21','11/25/21','11/26/21','11/27/21','11/28/21','11/29/21','11/30/21','12/1/21','12/2/21','12/3/21','12/4/21','12/5/21','12/6/21','12/7/21','12/8/21','12/9/21','12/10/21','12/11/21','12/12/21','12/13/21','12/14/21','12/15/21','12/16/21','12/17/21','12/18/21','12/19/21','12/20/21','12/21/21','12/22/21','12/23/21','12/24/21','12/25/21','12/26/21','12/27/21','12/28/21','12/29/21','12/30/21','12/31/21','1/1/22','1/2/22','1/3/22','1/4/22','1/5/22','1/6/22','1/7/22','1/8/22','1/9/22','1/10/22','1/11/22','1/12/22','1/13/22','1/14/22','1/15/22','1/16/22','1/17/22','1/18/22','1/19/22','1/20/22','1/21/22','1/22/22','1/23/22','1/24/22','1/25/22','1/26/22','1/27/22','1/28/22','1/29/22','1/30/22','1/31/22','2/1/22','2/2/22','2/3/22','2/4/22','2/5/22','2/6/22','2/7/22','2/8/22','2/9/22','2/10/22','2/11/22','2/12/22','2/13/22','2/14/22','2/15/22','2/16/22','2/17/22','2/18/22','2/19/22','2/20/22','2/21/22','2/22/22','2/23/22','2/24/22','2/25/22','2/26/22','2/27/22','2/28/22','3/1/22','3/2/22','3/3/22','3/4/22','3/5/22','3/6/22','3/7/22','3/8/22','3/9/22','3/10/22','3/11/22','3/12/22','3/13/22','3/14/22','3/15/22','3/16/22','3/17/22','3/18/22','3/19/22','3/20/22','3/21/22','3/22/22','3/23/22','3/24/22','3/25/22','3/26/22','3/27/22','3/28/22','3/29/22','3/30/22','3/31/22','4/1/22','4/2/22','4/3/22','4/4/22','4/5/22','4/6/22','4/7/22','4/8/22','4/9/22','4/10/22','4/11/22','4/12/22','4/13/22','4/14/22','4/15/22','4/16/22','4/17/22','4/18/22','4/19/22','4/20/22','4/21/22','4/22/22','4/23/22','4/24/22','4/25/22','4/26/22','4/27/22','4/28/22','4/29/22','4/30/22','5/1/22','5/2/22','5/3/22','5/4/22','5/5/22','5/6/22','5/7/22','5/8/22','5/9/22','5/10/22','5/11/22','5/12/22','5/13/22','5/14/22','5/15/22','5/16/22','5/17/22','5/18/22','5/19/22','5/20/22','5/21/22','5/22/22','5/23/22','5/24/22','5/25/22','5/26/22','5/27/22','5/28/22','5/29/22','5/30/22','5/31/22','6/1/22','6/2/22','6/3/22','6/4/22','6/5/22','6/6/22','6/7/22','6/8/22','6/9/22','6/10/22','6/11/22','6/12/22','6/13/22','6/14/22','6/15/22','6/16/22','6/17/22','6/18/22','6/19/22','6/20/22','6/21/22','6/22/22','6/23/22','6/24/22','6/25/22','6/26/22','6/27/22','6/28/22','6/29/22','6/30/22','7/1/22','7/2/22','7/3/22','7/4/22','7/5/22','7/6/22','7/7/22','7/8/22','7/9/22','7/10/22','7/11/22','7/12/22','7/13/22','7/14/22','7/15/22','7/16/22','7/17/22','7/18/22','7/19/22','7/20/22','7/21/22','7/22/22','7/23/22','7/24/22','7/25/22','7/26/22','7/27/22','7/28/22','7/29/22','7/30/22','7/31/22','8/1/22','8/2/22','8/3/22','8/4/22','8/5/22','8/6/22','8/7/22','8/8/22','8/9/22','8/10/22','8/11/22','8/12/22','8/13/22','8/14/22','8/15/22','8/16/22','8/17/22','8/18/22','8/19/22','8/20/22','8/21/22','8/22/22','8/23/22','8/24/22','8/25/22',
                '8/26/22','8/27/22','8/28/22','8/29/22','8/30/22','8/31/22','9/1/22','9/2/22','9/3/22','9/4/22','9/5/22','9/6/22','9/7/22','9/8/22','9/9/22','9/10/22','9/11/22','9/12/22','9/13/22','9/14/22','9/15/22','9/16/22','9/17/22','9/18/22','9/19/22','9/20/22','9/21/22','9/22/22','9/23/22','9/24/22','9/25/22','9/26/22','9/27/22','9/28/22','9/29/22','9/30/22','10/1/22','10/2/22','10/3/22','10/4/22','10/5/22',
                '10/6/22'
],
var_name='date', value_name='Value')

covid_unpivot = covid_unpivot.rename(
    columns={'FIPS': 'county_fips'})

covid_unpivot['date'] = pd.to_datetime(covid_unpivot.date).dt.date

#### calendar ####
calendar = calendar.drop(['index'], axis=1)

#### cancer_mortality
cancer_mortality = cancer_mortality.drop(['index'], axis=1)
cancer_mortality[["mort_rate_1980", "string"]] = cancer_mortality['Mortality Rate, 1980*'].str.split('(', expand=True)
cancer_mortality = cancer_mortality.drop(['string','Mortality Rate, 1980*'], axis=1)
cancer_mortality[["mort_rate_1985", "string"]] = cancer_mortality['Mortality Rate, 1985*'].str.split('(', expand=True)
cancer_mortality = cancer_mortality.drop(['string','Mortality Rate, 1985*'], axis=1)
cancer_mortality[["mort_rate_1990", "string"]] = cancer_mortality['Mortality Rate, 1990*'].str.split('(', expand=True)
cancer_mortality = cancer_mortality.drop(['string','Mortality Rate, 1990*'], axis=1)
cancer_mortality[["mort_rate_1995", "string"]] = cancer_mortality['Mortality Rate, 1995*'].str.split('(', expand=True)
cancer_mortality = cancer_mortality.drop(['string','Mortality Rate, 1995*'], axis=1)
cancer_mortality[["mort_rate_2000", "string"]] = cancer_mortality['Mortality Rate, 2000*'].str.split('(', expand=True)
cancer_mortality = cancer_mortality.drop(['string','Mortality Rate, 2000*'], axis=1)
cancer_mortality[["mort_rate_2005", "string"]] = cancer_mortality['Mortality Rate, 2005*'].str.split('(', expand=True)
cancer_mortality = cancer_mortality.drop(['string','Mortality Rate, 2005*'], axis=1)
cancer_mortality[["mort_rate_2010", "string"]] = cancer_mortality['Mortality Rate, 2010*'].str.split('(', expand=True)
cancer_mortality = cancer_mortality.drop(['string','Mortality Rate, 2010*'], axis=1)
cancer_mortality[["mort_rate_2014", "string"]] = cancer_mortality['Mortality Rate, 2014*'].str.split('(', expand=True)
cancer_mortality = cancer_mortality.drop(['string','Mortality Rate, 2014*'], axis=1)
cancer_mortality[["perc_mort_rate_1980_2014", "string"]] = cancer_mortality['% Change in Mortality Rate, 1980-2014'].str.split('(', expand=True)
cancer_mortality = cancer_mortality.drop(['string','% Change in Mortality Rate, 1980-2014'], axis=1)
cancer_mortality[["City", "State"]] = cancer_mortality['Location'].str.split(',', expand=True)
cancer_mortality = cancer_mortality.drop(['Location'], axis=1)

#### education
education = education.drop(['index'], axis=1)

#### poverty
poverty = poverty.drop(['index'], axis=1)

#### unemployment
unemployment = unemployment.drop(['index'], axis=1)
unemployment[["County", "string"]] = unemployment['Area_name'].str.split(',', expand=True)
unemployment = unemployment.drop(['string','Area_name'], axis=1)

#### us_hiv_2019
us_hiv_2019 = us_hiv_2019.drop(['index'], axis=1)
us_hiv_2019 = us_hiv_2019.rename(
    columns={"White": "White_2019", "Black/African American": "Black/African American_2019",
             "Hispanic/Latino":"Hispanic/Latino_2019","American Indian/Alaska Native":"American Indian/Alaska Native_2019",
             "Asian": "Asian_2019","Native Hawaiian/Other Pacific Islander": "Native Hawaiian/Other Pacific Islander_2019",
             "Multiple races": "Multiple races_2019", "Total": "Total_2019"})

## Create New 2020 columns
# White_2020
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['White_2020'] = us_hiv_2019["randNumCol"] * us_hiv_2019["White_2019"]
us_hiv_2019['White_2020'] = us_hiv_2019['White_2020'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Black/African American_2020
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['Black/African American_2020'] = us_hiv_2019["randNumCol"] * us_hiv_2019["Black/African American_2019"]
us_hiv_2019['Black/African American_2020'] = us_hiv_2019['Black/African American_2020'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Hispanic/Latino_2020
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['Hispanic/Latino_2020'] = us_hiv_2019["randNumCol"] * us_hiv_2019["Hispanic/Latino_2019"]
us_hiv_2019['Hispanic/Latino_2020'] = us_hiv_2019['Hispanic/Latino_2020'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# American Indian/Alaska Native_2020
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['American Indian/Alaska Native_2020'] = us_hiv_2019["randNumCol"] * us_hiv_2019["American Indian/Alaska Native_2019"]
us_hiv_2019['American Indian/Alaska Native_2020'] = us_hiv_2019['American Indian/Alaska Native_2020'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Asian_2020
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['Asian_2020'] = us_hiv_2019["randNumCol"] * us_hiv_2019["Asian_2019"]
us_hiv_2019['Asian_2020'] = us_hiv_2019['Asian_2020'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Native Hawaiian/Other Pacific Islander_2020
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['Native Hawaiian/Other Pacific Islander_2020'] = us_hiv_2019["randNumCol"] * us_hiv_2019["Native Hawaiian/Other Pacific Islander_2019"]
us_hiv_2019['Native Hawaiian/Other Pacific Islander_2020'] = us_hiv_2019['Native Hawaiian/Other Pacific Islander_2020'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Multiple races_2020
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['Multiple races_2020'] = us_hiv_2019["randNumCol"] * us_hiv_2019["Multiple races_2019"]
us_hiv_2019['Multiple races_2020'] = us_hiv_2019['Multiple races_2020'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Total_2020
us_hiv_2019['Total_2020'] = us_hiv_2019["White_2020"] + us_hiv_2019["Black/African American_2020"] + \
                            us_hiv_2019["Hispanic/Latino_2020"] + us_hiv_2019["American Indian/Alaska Native_2020"] + \
                            us_hiv_2019["Asian_2020"] + us_hiv_2019["Native Hawaiian/Other Pacific Islander_2020"] + \
                            us_hiv_2019["Multiple races_2020"]    

##########################

## Create New 2021 columns
# White_2021
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['White_2021'] = us_hiv_2019["randNumCol"] * us_hiv_2019["White_2019"]
us_hiv_2019['White_2021'] = us_hiv_2019['White_2021'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Black/African American_2021
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['Black/African American_2021'] = us_hiv_2019["randNumCol"] * us_hiv_2019["Black/African American_2019"]
us_hiv_2019['Black/African American_2021'] = us_hiv_2019['Black/African American_2021'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Hispanic/Latino_2021
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['Hispanic/Latino_2021'] = us_hiv_2019["randNumCol"] * us_hiv_2019["Hispanic/Latino_2019"]
us_hiv_2019['Hispanic/Latino_2021'] = us_hiv_2019['Hispanic/Latino_2021'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# American Indian/Alaska Native_2021
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['American Indian/Alaska Native_2021'] = us_hiv_2019["randNumCol"] * us_hiv_2019["American Indian/Alaska Native_2019"]
us_hiv_2019['American Indian/Alaska Native_2021'] = us_hiv_2019['American Indian/Alaska Native_2021'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Asian_2021
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['Asian_2021'] = us_hiv_2019["randNumCol"] * us_hiv_2019["Asian_2019"]
us_hiv_2019['Asian_2021'] = us_hiv_2019['Asian_2021'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Native Hawaiian/Other Pacific Islander_2021
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['Native Hawaiian/Other Pacific Islander_2021'] = us_hiv_2019["randNumCol"] * us_hiv_2019["Native Hawaiian/Other Pacific Islander_2019"]
us_hiv_2019['Native Hawaiian/Other Pacific Islander_2021'] = us_hiv_2019['Native Hawaiian/Other Pacific Islander_2021'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Multiple races_2021
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['Multiple races_2021'] = us_hiv_2019["randNumCol"] * us_hiv_2019["Multiple races_2019"]
us_hiv_2019['Multiple races_2021'] = us_hiv_2019['Multiple races_2021'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Total_2021
us_hiv_2019['Total_2021'] = us_hiv_2019["White_2021"] + us_hiv_2019["Black/African American_2021"] + \
                            us_hiv_2019["Hispanic/Latino_2021"] + us_hiv_2019["American Indian/Alaska Native_2021"] + \
                            us_hiv_2019["Asian_2021"] + us_hiv_2019["Native Hawaiian/Other Pacific Islander_2021"] + \
                            us_hiv_2019["Multiple races_2021"]    

##########################

## Create New 2021 columns
# White_2022
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['White_2022'] = us_hiv_2019["randNumCol"] * us_hiv_2019["White_2019"]
us_hiv_2019['White_2022'] = us_hiv_2019['White_2022'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Black/African American_2022
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['Black/African American_2022'] = us_hiv_2019["randNumCol"] * us_hiv_2019["Black/African American_2019"]
us_hiv_2019['Black/African American_2022'] = us_hiv_2019['Black/African American_2022'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Hispanic/Latino_2022
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['Hispanic/Latino_2022'] = us_hiv_2019["randNumCol"] * us_hiv_2019["Hispanic/Latino_2019"]
us_hiv_2019['Hispanic/Latino_2022'] = us_hiv_2019['Hispanic/Latino_2022'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# American Indian/Alaska Native_2022
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['American Indian/Alaska Native_2022'] = us_hiv_2019["randNumCol"] * us_hiv_2019["American Indian/Alaska Native_2019"]
us_hiv_2019['American Indian/Alaska Native_2022'] = us_hiv_2019['American Indian/Alaska Native_2022'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Asian_2022
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['Asian_2022'] = us_hiv_2019["randNumCol"] * us_hiv_2019["Asian_2019"]
us_hiv_2019['Asian_2022'] = us_hiv_2019['Asian_2022'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Native Hawaiian/Other Pacific Islander_2022
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['Native Hawaiian/Other Pacific Islander_2022'] = us_hiv_2019["randNumCol"] * us_hiv_2019["Native Hawaiian/Other Pacific Islander_2019"]
us_hiv_2019['Native Hawaiian/Other Pacific Islander_2022'] = us_hiv_2019['Native Hawaiian/Other Pacific Islander_2022'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Multiple races_2022
us_hiv_2019['randNumCol'] = np.random.randint(-10, 10, us_hiv_2019.shape[0])/100 + 1
us_hiv_2019['Multiple races_2022'] = us_hiv_2019["randNumCol"] * us_hiv_2019["Multiple races_2019"]
us_hiv_2019['Multiple races_2022'] = us_hiv_2019['Multiple races_2022'].round(0)
us_hiv_2019 = us_hiv_2019.drop(['randNumCol'], axis=1)

# Total_2022
us_hiv_2019['Total_2022'] = us_hiv_2019["White_2022"] + us_hiv_2019["Black/African American_2022"] + \
                            us_hiv_2019["Hispanic/Latino_2022"] + us_hiv_2019["American Indian/Alaska Native_2022"] + \
                            us_hiv_2019["Asian_2022"] + us_hiv_2019["Native Hawaiian/Other Pacific Islander_2022"] + \
                            us_hiv_2019["Multiple races_2022"]  
##########################

#### us_population
us_population = us_population.drop(['index'], axis=1)

#### uscities
uscities = uscities.drop(['index'], axis=1)

#### temper
temper = temper.drop(['index'], axis=1)

#######################################################################################
############################  Export to PostgreSQL ####################################
#######################################################################################
calendar.to_sql("calendar", engine, schema='Curated')
cancer_mortality.to_sql("cancer_mortality", engine, schema='Curated')
education.to_sql("education", engine, schema='Curated')
poverty.to_sql("poverty", engine, schema='Curated')
unemployment.to_sql("unemployment", engine, schema='Curated')
us_hiv_2019.to_sql("us_hiv_2019", engine, schema='Curated')
us_population.to_sql("us_population", engine, schema='Curated')
uscities.to_sql("uscities", engine, schema='Curated')
covid_unpivot.to_sql("covid", engine, schema='Curated')
temper.to_sql("temper", engine, schema='Curated')

#Closing the connection
connection.close()
#######################################################################################