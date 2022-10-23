# -*- coding: utf-8 -*-
"""
Created on Mon Oct  3 15:33:28 2022

@author: ioannis.thanos
"""
import psycopg2
import logging
from sqlalchemy import create_engine
import pandas as pd
import datetime as dt
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

#########################  Import helpful file for Temperature ########################
path = "C:\\Users\\ioannis.thanos\\OneDrive - Accenture\\Documents\\\MScBigData\\"\
"Dissertation\\Data\\Final\\"

#### helpful_temper
helpful_temper = pd.read_csv(path + 'helpful_temper.csv', sep=';')
#######################################################################################

#######################################################################################
############################  Import from PostgreSQL ##################################
#######################################################################################
connection = psycopg2.connect(user="postgres",
                              password="Password123",
                              host="localhost",
                              database="Dissertation")
cursor = connection.cursor()

query = 'select * from "Curated".covid'
covid = pd.read_sql(query,con=connection).drop(['index'], axis=1)
#### calendar
query = 'select * from "Curated".calendar'
calendar = pd.read_sql(query,con=connection).drop(['index'], axis=1)
#### cancer_mortality
query = 'select * from "Curated".cancer_mortality'
cancer_mortality = pd.read_sql(query,con=connection).drop(['index'], axis=1)
#### education
query = 'select * from "Curated".education'
education = pd.read_sql(query,con=connection).drop(['index'], axis=1)
#### poverty
query = 'select * from "Curated".poverty'
poverty = pd.read_sql(query,con=connection).drop(['index'], axis=1)
#### unemployment
query = 'select * from "Curated".unemployment'
unemployment = pd.read_sql(query,con=connection).drop(['index'], axis=1)
#### us_hiv_2019
query = 'select * from "Curated".us_hiv_2019'
us_hiv_2019 = pd.read_sql(query,con=connection).drop(['index'], axis=1)
#### us_population
query = 'select * from "Curated".us_population'
us_population = pd.read_sql(query,con=connection).drop(['index'], axis=1)
#### uscities
query = 'select * from "Curated".uscities'
uscities = pd.read_sql(query,con=connection).drop(['index'], axis=1)
#### temperatures
query = 'select * from "Curated".temper'
temper = pd.read_sql(query,con=connection).drop(['index'], axis=1)

#######################################################################################
#############################  Data Vault Creation ####################################
#######################################################################################

################# Ref Tables #################

#### Ref Calendar
ref_calendar = calendar.drop(['word_date','holiday','month_and_year'], axis=1)
ref_calendar['date'] = pd.to_datetime(ref_calendar.date).dt.date
#################

#### Ref Holidays
ref_holidays = calendar[['date', 'holiday']]
ref_holidays['date'] = pd.to_datetime(ref_holidays.date).dt.date
#################

##############################################

################# Hub Tables #################
#### Hub State
states = uscities['state_id'].unique()
states_df = pd.DataFrame(states)
states_df = states_df.rename(columns={0: 'state_id'})

add_states = pd.DataFrame(columns=['state_id'])
add_states = add_states.append({'state_id': 'US'}, ignore_index=True)
states_df = states_df.append(add_states, ignore_index=True)

states_df['h_state_sid'] = states_df.state_id.map(hash)

now = dt.datetime.now()
states_df['Timestamp'] = now

#### Hub County
counties = uscities['county_fips'].unique()
counties_df = pd.DataFrame(counties)
counties_df = counties_df.rename(columns={0: 'county_fips'})

add_counties = pd.DataFrame(columns=['county_fips'])
add_counties = add_counties.append({'county_fips': 1000}, ignore_index=True)
add_counties = add_counties.append({'county_fips': 0}, ignore_index=True)
counties_df = counties_df.append(add_counties, ignore_index=True)

counties_df['h_county_sid'] = counties_df.county_fips.map(hash)

counties_df['Timestamp'] = now

#### Hub City
cities = uscities['id'].unique()
cities_df = pd.DataFrame(cities)
cities_df = cities_df.rename(columns={0: 'city_id'})
cities_df['h_city_sid'] = cities_df.city_id.map(hash)

cities_df['Timestamp'] = now

#### Hub Diseases
cancer_types = cancer_mortality['Cancer Type'].unique()
dim_diseas = pd.DataFrame(cancer_types)
dim_diseas = dim_diseas.rename(columns={0: 'disease_type'})

add_diseas = pd.DataFrame(columns=['disease_type'])
add_diseas = add_diseas.append({'disease_type': 'Covid'}, ignore_index=True)
add_diseas = add_diseas.append({'disease_type': 'HIV'}, ignore_index=True)

dim_diseas = dim_diseas.append(add_diseas, ignore_index=True)
dim_diseas['disease_id'] = range(len(dim_diseas))
dim_diseas['h_diseas_sid'] = dim_diseas.disease_id.map(hash)
h_diseas = dim_diseas.drop(['disease_type'], axis=1)

h_diseas['Timestamp'] = now
##############################################

################# Link Table #################
#### Link_state_county_city
uscities = uscities.rename(columns={'id': 'city_id'})
sids_uscities = uscities.merge(
    states_df, on='state_id', how='left'
    ).drop(
        ['Timestamp'], axis=1
        ).merge(
    counties_df, on='county_fips', how='left'
    ).drop(
        ['Timestamp'], axis=1
        ).merge(
    cities_df, on='city_id', how='left'
    ).drop(
        ['Timestamp'], axis=1
        )

assert len(sids_uscities) == len(uscities)
        
#sids_uscities_dup = sids_uscities.drop_duplicates(
#    ['h_state_sid','h_county_sid','h_city_sid'])[['h_state_sid','h_county_sid','h_city_sid']]
#sids_uscities_dup['link_scc_sid'] = sids_uscities_dup.h_city_sid.map(hash)

l_state_county = sids_uscities.drop_duplicates(
    ['h_state_sid','h_county_sid'])[['h_state_sid','h_county_sid']]

add_sc = pd.DataFrame(columns=['h_state_sid','h_county_sid'])
add_sc = add_sc.append({'h_state_sid':-4837297267247506480,'h_county_sid': 0}, ignore_index=True)
add_sc = add_sc.append({'h_state_sid':6515686848865320023,'h_county_sid': 1000}, ignore_index=True)
l_state_county = l_state_county.append(add_sc, ignore_index=True)

l_state_county['l_sc_sid_1'] = l_state_county['h_state_sid'] * l_state_county['h_county_sid']
l_state_county['l_sc_sid'] = l_state_county.l_sc_sid_1.map(hash)
l_state_county = l_state_county.drop(['l_sc_sid_1'], axis=1)
l_state_county['Timestamp'] = now
#######################################

########### Satellite Table ###########
#### Sat state main
sat_state_main = sids_uscities.drop_duplicates(
    ['h_state_sid','state_name'])[['h_state_sid','state_name']]
sat_state_main['Timestamp'] = now

#### Sat county main
sat_county_main = sids_uscities.drop_duplicates(
    ['h_county_sid','county_name'])[['h_county_sid','county_name']]
sat_county_main['Timestamp'] = now

#### Sat city main
sat_city_main = sids_uscities.drop_duplicates(
    ['h_city_sid','city'])[['h_city_sid','city']]
sat_city_main['Timestamp'] = now

#### Sat disease main
sat_diseas_main = dim_diseas.drop_duplicates(
    ['h_diseas_sid','disease_type'])[['h_diseas_sid','disease_type']]
sat_diseas_main['Timestamp'] = now

#### Sat education
education = education.rename(columns={'State': 'state_id', ' (FIPS) Code': 'county_fips'})
education = education.drop(['Area name'], axis=1)
education = education.rename(
    columns={"Some college or assoc's degree, 1990": 'Some college or assoc degree, 1990',
             "Bachelor's degree or higher, 1990": 'Bachelor degree or higher, 1990',
             "Prct of adults compl some college or assoc's degree, 1990": 'Prct of adults compl some college or assoc degree, 1990',
             "Prct of adults with a bachelor's degree or higher, 1990": "Prct of adults with a bachelor degree or higher, 1990",
             "Some college or assoc's degree, 2000": 'Some college or assoc degree, 2000',
             "Bachelor's degree or higher, 2000": 'Bachelor degree or higher, 2000',
             "Prct of adults compl some college or assoc's degree, 2000":'Prct of adults compl some college or assoc degree, 2000',
             "Prct of adults with a bachelor's degree or higher, 2000": 'Prct of adults with a bachelor degree or higher, 2000',
             "Some college or assoc's degree, 2007-11":'Some college or assoc degree, 2007-11',
             "Bachelor's degree or higher, 2007-11":'Bachelor degree or higher, 2007-11',
             "Prct of adults compl some college or assoc's degree, 2007-11":'Prct of adults compl some college or assoc degree, 2007-11',
             "Prct of adults with a bachelor's degree or higher 2007-11":'Prct of adults with a bachelor degree or higher 2007-11',
             "Some college or assoc's degree, 2016-20":'Some college or assoc degree, 2016-20',
             "Bachelor's degree or higher, 2016-20":'Bachelor degree or higher, 2016-20',
             "Prct of adults compl some college or assoc's degree, 2016-20":'Prct of adults compl some college or assoc degree, 2016-20',
             "Prct of adults with a bachelor's degree or higher 2016-20":'Prct of adults with a bachelor degree or higher 2016-20'})

education_unpivot = pd.melt(
    education, id_vars=["state_id", "county_fips"],
    value_vars=[
    '2003 Rural-Uban Continuum Code','2003 Urban Influence Code','2013 Rural-Urban Continuum Code',
    '2013 Urban Influence Code','< a high school diploma, 1970','High school diploma only, 1970',
    'Some college (1-3 years), 1970','4 years of college or higher, 1970',
    'Prct of adults with < a high school diploma, 1970','Prct of adults with a high school diploma only, 1970',
    'Prct of adults compl some college (1-3 years), 1970',
    'Prct of adults compl 4 years of college or higher, 1970','< a high school diploma, 1980',
    'High school diploma only, 1980','Some college (1-3 years), 1980','4 years of college or higher, 1980',
    'Prct of adults with < a high school diploma, 1980','Prct of adults with a high school diploma only, 1980',
    'Prct of adults compl some college (1-3 years), 1980',
    'Prct of adults compl 4 years of college or higher, 1980','< a high school diploma, 1990',
    'High school diploma only, 1990','Some college or assoc degree, 1990','Bachelor degree or higher, 1990',
    'Prct of adults with < a high school diploma, 1990','Prct of adults with a high school diploma only, 1990',
    'Prct of adults compl some college or assoc degree, 1990',
    'Prct of adults with a bachelor degree or higher, 1990','< a high school diploma, 2000',
    'High school diploma only, 2000','Some college or assoc degree, 2000','Bachelor degree or higher, 2000',
    'Prct of adults with < a high school diploma, 2000','Prct of adults with a high school diploma only, 2000',
    'Prct of adults compl some college or assoc degree, 2000','Prct of adults with a bachelor degree or higher, 2000',
    '< a high school diploma, 2007-11','High school diploma only, 2007-11','Some college or assoc degree, 2007-11',
    'Bachelor degree or higher, 2007-11','Prct of adults with < a high school diploma, 2007-11',
    'Prct of adults with a high school diploma only, 2007-11',
    'Prct of adults compl some college or assoc degree, 2007-11','Prct of adults with a bachelor degree or higher 2007-11',
    '< a high school diploma, 2016-20','High school diploma only, 2016-20','Some college or assoc degree, 2016-20',
    'Bachelor degree or higher, 2016-20','Prct of adults with < a high school diploma, 2016-20',
    'Prct of adults with a high school diploma only, 2016-20','Prct of adults compl some college or assoc degree, 2016-20',
    'Prct of adults with a bachelor degree or higher 2016-20'
],
var_name='KPI', value_name='Value')

education_unpivot[["KPI", "Year_1"]] = education_unpivot['KPI'].str.split(',', expand=True)

education_unpivot['Year'] = np.where(
     education_unpivot['KPI'] == '2003 Rural-Uban Continuum Code', 2003,
     np.where(
         education_unpivot['KPI'] == '2003 Urban Influence Code', 2003,
         np.where(
             education_unpivot['KPI'] == '2013 Rural-Urban Continuum Code', 2013,
             np.where(
                 education_unpivot['KPI'] == '2013 Urban Influence Code', 2013,
                 np.where(
                     education_unpivot['KPI'] == 'Prct of adults with a bachelor degree or higher 2007-11', '2007-2011',
                     np.where(
                         education_unpivot['KPI'] == 'Prct of adults with a bachelor degree or higher 2016-20', '2016-2020',
                         np.where(
                             education_unpivot['KPI'] == 'Prct of adults with a bachelor degree or higher 2016-20', '2016-2020',
                             education_unpivot['Year_1']
                             )
                         )
                     )
                 )
             )
     )
)

education_unpivot = education_unpivot.drop(['Year_1'], axis=1)
education_unpivot = education_unpivot[['state_id', 'county_fips', 'Year', 'KPI', 'Value']]

sids_education = education_unpivot.merge(
    states_df, on='state_id', how='left'
    ).drop(
        ['Timestamp'], axis=1
        ).merge(
    counties_df, on='county_fips', how='left'
    ).drop(
        ['Timestamp'], axis=1
        ).merge(
    l_state_county, on=['h_state_sid', 'h_county_sid'], how='left'
    ).drop(
        ['Timestamp'], axis=1
        )

assert len(sids_education) == len(education_unpivot)
        
sat_education = sids_education[['l_sc_sid', 'Year', 'KPI', 'Value']]

#### Sat demographics
us_population = us_population.rename(columns={'State': 'state_id', 'Federal Information Processing Standards (FIPS) Code': 'county_fips'})
us_population = us_population.drop(['Area name'], axis=1)

us_pop_unpivot = pd.melt(
    us_population, id_vars=["state_id", "county_fips"],
    value_vars=[
    "Rural-Urban Continuum Code 2013","Population 1990", "Population 2000", "Population 2010",
    "Population 2020", "Population 2021"
],
var_name='KPI', value_name='Value')

us_pop_unpivot['Year'] = us_pop_unpivot['KPI'].astype('str').str.extractall('(\d+)').unstack().fillna('').sum(axis=1).astype(int)
us_pop_unpivot1 = us_pop_unpivot[['state_id', 'county_fips', 'Year', 'KPI', 'Value']]

us_pop_unpivot = us_pop_unpivot1.merge(
    states_df, on='state_id', how='left'
    ).drop(
        ['Timestamp'], axis=1
        ).merge(
    counties_df, on='county_fips', how='left'
    ).drop(
        ['Timestamp'], axis=1
        ).merge(
    l_state_county, on=['h_state_sid', 'h_county_sid'], how='left'
    ).drop(
        ['Timestamp'], axis=1
        )

assert len(us_pop_unpivot) == len(us_pop_unpivot1)        
        
sat_demogr = us_pop_unpivot[['l_sc_sid', 'Year', 'KPI', 'Value']]

#### Sat diseas metrics ---> Covid, Cancer Types, Hiv
# Unpivot covid table
cities_counties = sids_uscities.drop_duplicates(
    ['state_id','city','county_fips'])[['state_id','city','county_fips']]

covid_unpivot2= covid.drop_duplicates(
    ['Type','county_fips','date','Value'])[['Type','county_fips','date','Value']]

covid_unpivot2['disease_type'] = 'Covid'
covid_unpivot3 = covid_unpivot2

covid_unpivot2 = covid_unpivot3.merge(
    sat_diseas_main, on='disease_type', how='left'
    ).drop(
        ['Timestamp','disease_type'], axis=1
        )

assert len(covid_unpivot2) == len(covid_unpivot3)        
        
sat_covid = covid_unpivot2[['h_diseas_sid','county_fips','Type','date', 'Value']]

# Unpivot cancer table
cancer_unpivot = pd.melt(
    cancer_mortality, id_vars=["Cancer Type", "FIPS"],
    value_vars=[
    "mort_rate_1980","mort_rate_1985", "mort_rate_1990", "mort_rate_1995", "mort_rate_2000", "mort_rate_2005",
    "mort_rate_2010", "mort_rate_2014", 'perc_mort_rate_1980_2014'
],
var_name='KPI', value_name='Value')

cancer_unpivot[["KPI", "Year"]] = cancer_unpivot['KPI'].str.split('mort_rate', expand=True)
cancer_unpivot['KPI'] = np.where(
     cancer_unpivot['KPI'] == '', 
    'mort_rate', 'perc_mort_rate'
     )

cancer_unpivot['Year'] = np.where(
     cancer_unpivot['Year'] == '_1980', 1980,
     np.where(
         cancer_unpivot['Year'] == '_1985', 1985,
         np.where(
             cancer_unpivot['Year'] == '_1990', 1990,
             np.where(
                 cancer_unpivot['Year'] == '_1995', 1995,
                 np.where(
                     cancer_unpivot['Year'] == '_2000', 2000,
                     np.where(
                         cancer_unpivot['Year'] == '_2005', 2005,
                         np.where(
                             cancer_unpivot['Year'] == '_2010', 2010,
                             np.where(
                                 cancer_unpivot['Year'] == '_2014', 2014, '1980_2014'
                                 )
                             )
                         )
                     )
                 )
             )
     )
)
cancer_unpivot = cancer_unpivot.rename(columns={'Cancer Type': 'Disease','FIPS': 'Location'})
cancer_unpivot['Location_type'] = 'county_fips'
cancer_unpivot = cancer_unpivot[['Disease','Location_type', 'Location', 'Year', 'KPI', 'Value']]
cancer_unpivot2 = cancer_unpivot.rename(columns={'Location': 'county_fips','Disease': 'disease_type'})
cancer_unpivot3 = cancer_unpivot2

cancer_unpivot2 = cancer_unpivot3.merge(
    counties_df, on='county_fips', how='left'
    ).drop(
        ['Timestamp'], axis=1
        ).merge(
    sat_diseas_main, on='disease_type', how='left'
    ).drop(
        ['Timestamp'], axis=1
        )

assert len(cancer_unpivot2) == len(cancer_unpivot3)         
        
cancer_unpivot2 = cancer_unpivot2[['h_diseas_sid','h_county_sid','Location_type', 'Year', 'KPI', 'Value']]

# Unpivot HIV table
hiv_unpivot = pd.melt(
    us_hiv_2019, id_vars=["Location"],
    value_vars=[
    'White_2019','Black/African American_2019','Hispanic/Latino_2019','American Indian/Alaska Native_2019',
    'Asian_2019','Native Hawaiian/Other Pacific Islander_2019','Multiple races_2019','Total_2019','Footnotes',
    'White_2020','Black/African American_2020','Hispanic/Latino_2020','American Indian/Alaska Native_2020',
    'Asian_2020','Native Hawaiian/Other Pacific Islander_2020','Multiple races_2020','Total_2020','White_2021',
    'Black/African American_2021','Hispanic/Latino_2021','American Indian/Alaska Native_2021','Asian_2021',
    'Native Hawaiian/Other Pacific Islander_2021','Multiple races_2021','Total_2021','White_2022',
    'Black/African American_2022','Hispanic/Latino_2022','American Indian/Alaska Native_2022','Asian_2022',
    'Native Hawaiian/Other Pacific Islander_2022','Multiple races_2022','Total_2022'
],
var_name='KPI', value_name='Value')

hiv_unpivot[["KPI", "Year"]] = hiv_unpivot['KPI'].str.split('_', expand=True)
hiv_unpivot['Disease'] = 'HIV'
dim_geogr = uscities.drop(
    ['population', 'density', 'source', 'military', 'incorporated', 'timezone', 'ranking', 'zips'], axis=1)
dim_geogr_states = dim_geogr.drop_duplicates(['state_name','state_id'])[['state_name','state_id']]
hiv_unpivot = hiv_unpivot.rename(columns={'Location': 'state_name'})
hiv_unpivot_1 = hiv_unpivot
hiv_unpivot = hiv_unpivot_1.merge(dim_geogr_states, on='state_name', how='left').drop(['state_name'], axis=1)

assert len(hiv_unpivot_1) == len(hiv_unpivot)

hiv_unpivot = hiv_unpivot.rename(columns={'state_id': 'Location'})
hiv_unpivot['Location_type'] = 'state_id'
hiv_unpivot = hiv_unpivot[['Disease', 'Location_type', 'Location', 'Year', 'KPI', 'Value']]
hiv_unpivot = hiv_unpivot.rename(columns={'Location': 'state_id','Disease': 'disease_type'})

hiv_unpivot2 = hiv_unpivot.merge(
    states_df, on='state_id', how='left'
    ).drop(
        ['Timestamp'], axis=1
        ).merge(
    sat_diseas_main, on='disease_type', how='left'
    ).drop(
        ['Timestamp'], axis=1
        )

assert len(hiv_unpivot2) == len(hiv_unpivot)        
        
hiv_unpivot2 = hiv_unpivot2[['h_diseas_sid','h_state_sid','Location_type', 'Year', 'KPI', 'Value']]

#### Sat temperatures
helpful_temper = helpful_temper.drop(['City File'], axis=1)
f_temper = temper.merge(helpful_temper, on='File', how='left')

assert len(f_temper) == len(temper)

fact_temper = f_temper[f_temper.State.notnull()].drop(['File'], axis=1)
dim_geogr_count = dim_geogr.drop_duplicates(subset=['city','state_name'], keep='first')
dim_geogr_counties = dim_geogr.drop_duplicates(['city','state_name','county_fips'])[['city','state_name','county_fips']]
fact_temper = fact_temper.rename(columns={'State': 'state_name', 'City': 'city','Temperature': 'Value'})
fact_temper['KPI'] = 'Temperature'
fact_temper_f = fact_temper.merge(dim_geogr_counties, on=['city','state_name'], how='left').drop(['city','state_name'], axis=1)

assert len(fact_temper_f) == len(fact_temper)

fact_temper_f['date'] = pd.to_datetime(fact_temper_f[['Year', 'Month', 'Day']])
fact_temper_f = fact_temper_f.drop(['Year','Month','Day'], axis=1)
fact_temper_f = fact_temper_f[['county_fips', 'date', 'KPI', 'Value']]
fact_temper_f_1 = fact_temper_f

fact_temper_f = fact_temper_f_1.merge(
    counties_df, on='county_fips', how='left'
    ).drop(
        ['Timestamp'], axis=1
        )

assert len(fact_temper_f) == len(fact_temper_f_1)        
        
fact_temper_f = fact_temper_f.drop(['county_fips'], axis=1)

#### Sat Poverty
poverty = poverty.rename(columns={'Stabr': 'state_id', 'FIPS_code': 'county_fips'})
poverty = poverty.drop(['Area_name'], axis=1)

poverty_unpivot = pd.melt(
    poverty, id_vars=["state_id","county_fips"],
    value_vars=[
    'Rural-urban_Continuum_Code_2003','Urban_Influence_Code_2003',
    'Rural-urban_Continuum_Code_2013','Urban_Influence_Code_2013','people_est of all in pov 2020',
    '90_prc low_bound of people_est of all in pov 2020','90_prc up_bound of people_est of all in pov 2020',
    'estd prc of people of all in pov 2020','90_prc low_bound of est of prc of people of all in pov 2020',
    '90_prc up_bound of est of prc of people of all in pov 2020','people_est age 0-17 in pov 2020',
    '90_prc low_bound of people_est age 0-17 in pov 2020','90_prc up_bound of people_est age 0-17 in pov 2020',
    'estd prc of people age 0-17 in pov 2020','90_prc low_bound of est of prc of people age 0-17 in pov 2020',
    '90_prc up_bound of est of prc of people age 0-17 in pov 2020','est of child 5-17 in fam in pov 2020',
    '90_prc low_bound of est of child 5-17 in fam in pov 2020',
    '90_prc up_bound of est of child 5-17 in fam in pov 2020','estd prc of child 5-17 in fam in pov 2020',
    '90_prc low_bound of est_prc_child 5-17 in fam in pov 2020',
    '90_prc up_bound of est_prc_child 5-17 in fam in pov 2020','est of median household income 2020',
    '90_prc low_bound of est of median household income 2020',
    '90_prc up_bound of est of median household income 2020','est of children ages 0 to 4 in pov 2020',
    '90_prc low_bound est_child ages 0 to 4 in pov 2020','90_prc up_bound est_child ages 0 to 4 in pov 2020',
    'estd prc of children ages 0 to 4 in pov 2020','90_prc low_bound of est prc_child ages 0 to 4 in pov 2020',
    '90_prc up_bound of est prc_child ages 0 to 4 in pov 2020'
],
var_name='KPI', value_name='Value')
poverty_unpivot['Year'] = poverty_unpivot['KPI'].str[-4:]
poverty_unpivot = poverty_unpivot[['state_id', 'county_fips', 'Year', 'KPI', 'Value']]

sids_poverty = poverty_unpivot.merge(
    states_df, on='state_id', how='left'
    ).drop(
        ['Timestamp'], axis=1
        ).merge(
    counties_df, on='county_fips', how='left'
    ).drop(
        ['Timestamp'], axis=1
        ).merge(
    l_state_county, on=['h_state_sid', 'h_county_sid'], how='left'
    ).drop(
        ['Timestamp'], axis=1
        )

assert len(sids_poverty) == len(poverty_unpivot)          
        
sat_poverty = sids_poverty[['l_sc_sid', 'Year', 'KPI', 'Value']]

#### Sat Unemployment
unemployment = unemployment.rename(columns={'State': 'state_id', 'FIPS_code': 'county_fips'})
unemployment = unemployment.drop(['County'], axis=1)

unemployment_unpivot = pd.melt(
    unemployment, id_vars=["state_id", "county_fips"],
    value_vars=[
    'Rural_urban_continuum_code_2013','Urban_influence_code_2013','Metro_2013','Civilian_labor_force_2000',
    'Employed_2000','Unemployed_2000','Unemployment_rate_2000','Civilian_labor_force_2001','Employed_2001',
    'Unemployed_2001','Unemployment_rate_2001','Civilian_labor_force_2002','Employed_2002','Unemployed_2002',
    'Unemployment_rate_2002','Civilian_labor_force_2003','Employed_2003','Unemployed_2003',
    'Unemployment_rate_2003','Civilian_labor_force_2004','Employed_2004','Unemployed_2004',
    'Unemployment_rate_2004','Civilian_labor_force_2005','Employed_2005','Unemployed_2005',
    'Unemployment_rate_2005','Civilian_labor_force_2006','Employed_2006','Unemployed_2006',
    'Unemployment_rate_2006','Civilian_labor_force_2007','Employed_2007','Unemployed_2007',
    'Unemployment_rate_2007','Civilian_labor_force_2008','Employed_2008','Unemployed_2008',
    'Unemployment_rate_2008','Civilian_labor_force_2009','Employed_2009','Unemployed_2009',
    'Unemployment_rate_2009','Civilian_labor_force_2010','Employed_2010','Unemployed_2010',
    'Unemployment_rate_2010','Civilian_labor_force_2011','Employed_2011','Unemployed_2011',
    'Unemployment_rate_2011','Civilian_labor_force_2012','Employed_2012','Unemployed_2012',
    'Unemployment_rate_2012','Civilian_labor_force_2013','Employed_2013','Unemployed_2013',
    'Unemployment_rate_2013','Civilian_labor_force_2014','Employed_2014','Unemployed_2014',
    'Unemployment_rate_2014','Civilian_labor_force_2015','Employed_2015','Unemployed_2015',
    'Unemployment_rate_2015','Civilian_labor_force_2016','Employed_2016','Unemployed_2016',
    'Unemployment_rate_2016','Civilian_labor_force_2017','Employed_2017','Unemployed_2017',
    'Unemployment_rate_2017','Civilian_labor_force_2018','Employed_2018','Unemployed_2018',
    'Unemployment_rate_2018','Civilian_labor_force_2019','Employed_2019','Unemployed_2019',
    'Unemployment_rate_2019','Civilian_labor_force_2020','Employed_2020','Unemployed_2020',
    'Unemployment_rate_2020','Civilian_labor_force_2021','Employed_2021','Unemployed_2021',
    'Unemployment_rate_2021','Median_Household_Income_2020','Med_HH_Income_Percent_of_State_Total_2020'
],
var_name='KPI', value_name='Value')

unemployment_unpivot['Year'] = unemployment_unpivot['KPI'].astype('str').str.extractall('(\d+)').unstack().fillna('').sum(axis=1).astype(int)
unemployment_unpivot = unemployment_unpivot[['state_id', 'county_fips', 'Year', 'KPI', 'Value']]

sids_unemployment = unemployment_unpivot.merge(
    states_df, on='state_id', how='left'
    ).drop(
        ['Timestamp'], axis=1
        ).merge(
    counties_df, on='county_fips', how='left'
    ).drop(
        ['Timestamp'], axis=1
        ).merge(
    l_state_county, on=['h_state_sid', 'h_county_sid'], how='left'
    ).drop(
        ['Timestamp'], axis=1
        )

assert len(sids_unemployment) == len(unemployment_unpivot)         
        
sat_unemployment = sids_unemployment[['l_sc_sid', 'Year', 'KPI', 'Value']]
#######################################

#######################################################################################
############################  Export to PostgreSQL ####################################
#######################################################################################
# HUBs
states_df.to_sql("DV_hub_states", engine, schema='Curated')
counties_df.to_sql("DV_hub_counties", engine, schema='Curated')
cities_df.to_sql("DV_hub_cities", engine, schema='Curated')
h_diseas.to_sql("DV_hub_diseas", engine, schema='Curated')

# LINKs
l_state_county.to_sql("DV_link_state_county", engine, schema='Curated')

# References
ref_calendar.to_sql("DV_ref_calendar", engine, schema='Curated')
ref_holidays.to_sql("DV_ref_holidays", engine, schema='Curated')

# Satellites
sat_state_main.to_sql("DV_sat_state_main", engine, schema='Curated')
sat_county_main.to_sql("DV_sat_county_main", engine, schema='Curated')
sat_city_main.to_sql("DV_sat_city_main", engine, schema='Curated')
sat_diseas_main.to_sql("DV_sat_diseas_main", engine, schema='Curated')
sat_education.to_sql("DV_sat_education", engine, schema='Curated')
sat_demogr.to_sql("DV_sat_demogr", engine, schema='Curated')
sat_covid.to_sql("DV_sat_covid", engine, schema='Curated')
cancer_unpivot2.to_sql("DV_sat_cancer", engine, schema='Curated')
hiv_unpivot2.to_sql("DV_sat_hiv", engine, schema='Curated')
fact_temper_f.to_sql("DV_sat_temperat", engine, schema='Curated')
sat_poverty.to_sql("DV_sat_poverty", engine, schema='Curated')
sat_unemployment.to_sql("DV_sat_unemployment", engine, schema='Curated')

#Closing the connection
connection.close()
#######################################################################################





