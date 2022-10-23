# -*- coding: utf-8 -*-
"""
Created on Thu Oct  6 11:44:31 2022

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
            options="-c search_path=Curated"
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

#### dim_diseas
query = 'select * from "Curated".star_dim_diseas'
dim_diseas = pd.read_sql(query,con=connection).drop(['index'], axis=1)

#### dim_geogr
query = 'select * from "Curated".star_dim_geogr'
dim_geogr = pd.read_sql(query,con=connection).drop(['index'], axis=1)

#### dim_calendar
query = 'select * from "Curated".star_dim_calendar'
dim_calendar = pd.read_sql(query,con=connection).drop(['index'], axis=1)

#### dim_holidays
query = 'select * from "Curated".star_dim_holidays'
dim_holidays = pd.read_sql(query,con=connection).drop(['index'], axis=1)

#### fact table
query = 'select * from "Curated".star_fact_table'
fact_table = pd.read_sql(query,con=connection).drop(['index'], axis=1)

#### fact_poverty
fact_poverty1 = fact_table[fact_table.Source == 'Poverty']
fact_poverty = fact_poverty1.drop_duplicates(
    ['state_id','county_fips','Year','KPI','Value'])[['state_id','county_fips','Year','KPI','Value']]

#### fact_covid
fact_covid1 = fact_table[fact_table.Source == 'Covid']
fact_covid = fact_covid1.drop_duplicates(
    ['Type','county_fips','Country_Region','Province_State','Lat','Long_','Combined_Key',
     'date','Value','Year','month','day','Disease']
    )[['Type','county_fips','Country_Region','Province_State','Lat','Long_','Combined_Key',
     'date','Value','Year','month','day','Disease']]

#### fact_cancer
fact_cancer1 = fact_table[fact_table.Source == 'Cancer']
fact_cancer = fact_cancer1.drop_duplicates(
    ['Disease','county_fips','Year','KPI','Value'])[['Disease','county_fips','Year','KPI','Value']]

#### fact_hiv
fact_hiv1 = fact_table[fact_table.Source == 'HIV']
fact_hiv = fact_hiv1.drop_duplicates(
    ['Disease','state_id','Year','KPI','Value'])[['Disease','state_id','Year','KPI','Value']]

#### fact_demogr
fact_demogr1 = fact_table[fact_table.Source == 'Demographics']
fact_demogr = fact_demogr1.drop_duplicates(
    ['state_id','county_fips','Year','KPI','Value'])[['state_id','county_fips','Year','KPI','Value']]

#### fact_unempl
fact_unempl1 = fact_table[fact_table.Source == 'Unemployment']
fact_unempl = fact_unempl1.drop_duplicates(
    ['state_id','county_fips','Year','KPI','Value'])[['state_id','county_fips','Year','KPI','Value']]

#### fact_temps
fact_temps1 = fact_table[fact_table.Source == 'Temperatures']
fact_temps = fact_temps1.drop_duplicates(
    ['county_fips','date','KPI','Value'])[['county_fips','date','KPI','Value']]

#### fact_educ
fact_educ1 = fact_table[fact_table.Source == 'Education']
fact_educ = fact_educ1.drop_duplicates(
    ['state_id','county_fips','Year','KPI','Value'])[['state_id','county_fips','Year','KPI','Value']]

#######################################################################################
############################  Denormalize Data Model ##################################
#######################################################################################
#### cancer
fact_cancer['Value'] = fact_cancer['Value'].astype(float)
fact_cancer['Year'] = np.where(
     fact_cancer['Year'] == '1980_2014', 
    19802014, fact_cancer['Year']
     )

cancer_df = fact_cancer.pivot_table(
     'Value',index=['Disease', 'county_fips', 'Year'],columns = 'KPI')

cancer_df.reset_index(inplace=True)
cancer_df_1 = cancer_df
dim_geogr_dupl = dim_geogr.drop_duplicates(['county_fips','county_name'])[['county_fips','county_name']]

cancer_df = cancer_df_1.merge(
    dim_geogr_dupl, on=['county_fips'], how='left'
    )

assert len(cancer_df) == len(cancer_df_1)

#### covid
fact_covid['Value'] = fact_covid['Value'].astype(int)
covid_df = fact_covid.pivot_table(
      'Value',index=['county_fips', 'Country_Region', 'Province_State','Lat','Long_',
                     'Combined_Key','Year','month','day','Disease'],columns = 'Type')

covid_df.reset_index(inplace=True)
covid_df['date'] = pd.to_datetime(covid_df[['Year', 'month', 'day']])
covid_df_1 = covid_df

covid_df = covid_df_1.merge(
    dim_geogr_dupl, on=['county_fips'], how='left'
    )

assert len(covid_df_1) == len(covid_df)

#### demogr
fact_demogr['Value'] = fact_demogr['Value'].str.replace(',','')
fact_demogr['Value'] = fact_demogr['Value'].astype(float)

demogr_df = fact_demogr.pivot_table(
     'Value',index=['state_id','county_fips'],columns = 'KPI')

demogr_df.reset_index(inplace=True)
demogr_df_1 = demogr_df

demogr_df = demogr_df_1.merge(
    dim_geogr_dupl, on=['county_fips'], how='left'
    )

assert len(demogr_df_1) == len(demogr_df)

#### educ
fact_educ['Value'] = fact_educ['Value'].str.replace(',','')
fact_educ['Value'] = fact_educ['Value'].astype(float)

educ_df = fact_educ.pivot_table(
     'Value',index=['state_id','county_fips','Year'],columns = 'KPI')

educ_df.reset_index(inplace=True)

#### hiv
# fact_hiv["KPI_Year_state"] = fact_hiv['state_id'] +"-"+ fact_hiv["KPI"]+ fact_hiv["Year"]
fact_hiv['Value'] = fact_hiv['Value'].astype(float)

hiv_df = fact_hiv.pivot_table(
     'Value',index=['Disease','state_id','Year'],columns = 'KPI')

hiv_df.reset_index(inplace=True)

#### poverty
fact_poverty['Value'] = fact_poverty['Value'].str.replace(',','')
fact_poverty['Value'] = fact_poverty['Value'].astype(float)

poverty_df = fact_poverty.pivot_table(
     'Value',index=['state_id','county_fips','Year'],columns = 'KPI')

poverty_df.reset_index(inplace=True)

#### temps
fact_temps['Value'] = fact_temps['Value'].astype(float)

temps_df = fact_temps.pivot_table(
     'Value',index=['county_fips','date'],columns = 'KPI')

temps_df.reset_index(inplace=True)
temps_df_1 = temps_df

temps_df = temps_df_1.merge(
    dim_geogr_dupl, on=['county_fips'], how='left'
    )

assert len(temps_df_1) == len(temps_df)

#### unemployment
fact_unempl['Value'] = fact_unempl['Value'].str.replace(',','')
fact_unempl['Value'] = fact_unempl['Value'].astype(float)

fact_unempl['KPI'] = np.where(
    fact_unempl['KPI'].str.contains('Civilian_labor_force', na=False),
    'Civilian_labor_force', np.where(
        fact_unempl['KPI'].str.contains('Employed', na=False),
        'Employed', np.where(
            fact_unempl['KPI'].str.contains('Unemployed', na=False),
            'Unemployed', np.where(
                fact_unempl['KPI'].str.contains('Unemployment_rate', na=False),
                'Unemployment_rate', fact_unempl['KPI']))))

unempl_df = fact_unempl.pivot_table(
     'Value',index=['state_id','county_fips','Year'],columns = 'KPI')

unempl_df.reset_index(inplace=True)

#######################################################################################
############################  Export to PostgreSQL ####################################
#######################################################################################
dim_diseas.to_sql("dim_diseas", engine, schema='Exploration')
dim_geogr.to_sql("dim_geogr", engine, schema='Exploration')
dim_calendar.to_sql("dim_calendar", engine, schema='Exploration')
dim_holidays.to_sql("dim_holidays", engine, schema='Exploration')
poverty_df.to_sql("poverty", engine, schema='Exploration')
covid_df.to_sql("covid", engine, schema='Exploration')
cancer_df.to_sql("cancer", engine, schema='Exploration')
hiv_df.to_sql("hiv", engine, schema='Exploration')
demogr_df.to_sql("demographics", engine, schema='Exploration')
unempl_df.to_sql("unemployment", engine, schema='Exploration')
temps_df.to_sql("temperatures", engine, schema='Exploration')
educ_df.to_sql("education", engine, schema='Exploration')

#Closing the connection
connection.close()
#######################################################################################

