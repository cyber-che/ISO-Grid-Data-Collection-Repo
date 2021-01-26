# -*- coding: utf-8 -*-
"""
Created on Sat Aug 15 13:50:23 2020
@author: CJ

Gets Real time Load Data

'The Real-Time Total Load chart displays
 the hour ending values for integrated
 Medium Term Load Forecast (MTLF) and 
 Cleared Demand for the MISO market. 
 The Actual Load updates every 5 minutes 
 and represents the load served in the MISO market'
 --MISO website
"""

#week 6 lmp consolidated data  (How to get url data from online source in json format)
import requests  
import pandas as pd
import json
from sqlalchemy import create_engine
import time_function
from datetime import time
import datetime as dt


    
url='https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=gettotalload&returnType=json'
data= requests.get(url).json()                  #converts file into usable python dictionary


timestamp=data['LoadInfo']['RefId']
Datetime=time_function.get_date(timestamp)


#FiveMinTotalLoad Call this once, maybe somewhere around 11:56 pm each day
def get_FiveMinTotalLoad(Datetime, data):
    listdata=data['LoadInfo']['FiveMinTotalLoad']
    LoadData = {'date':[],'hour':[],'Load':[]}
   
    for load_vals in listdata:
        LoadData['hour'].append(load_vals['Load']['Time'])
        LoadData['Load'].append(load_vals['Load']['Value'])
        LoadData['date'].append(Datetime)

    FiveMin_load_Dataframe= pd.DataFrame(LoadData) #creates dataframe

    #writes FiveMin_load_Data to database
    engine = create_engine('postgresql://postgres:pserc_data@localhost:5433/miso_db')
    FiveMin_load_Dataframe.to_sql('fivemin_load_data',engine, if_exists='append', index=False )
   
    
#MTL_FORCAST: Medium Term Load Forcast
def get_MTL_forecast(Datetime, data):
    listdata=data['LoadInfo']['MediumTermLoadForecast']
    LoadData = {'date':[],'hour':[],'mtl_forecast':[]}

    #agenerates hour/load column data. Parses time in hours
    for forcast_vals in listdata:

        hour_val= int(forcast_vals['Forecast']['HourEnding'])
        hour_val=time_function.hour_parsing(hour_val)

        LoadData['hour'].append(hour_val)
        LoadData['mtl_forecast'].append(forcast_vals['Forecast']['LoadForecast'])
        LoadData['date'].append(Datetime)

    MTL_Forecast_Dataframe= pd.DataFrame(LoadData)   #creates dataframe

    #assigns datatypes to columns.
    MTL_Forecast_Dataframe['mtl_forecast']=pd.to_numeric(MTL_Forecast_Dataframe['mtl_forecast'])  

    #writes MTL Forcast data for to table in database
    engine = create_engine('postgresql://postgres:pserc_data@localhost:5433/miso_db')
    MTL_Forecast_Dataframe.to_sql('mtl_forecast_data',engine, if_exists='append', index=False )

#GET CLEARED MW
def get_clearedMW(Datetime, data):
    listdata=data['LoadInfo']['ClearedMW']
    
    LoadData = {'date':[],'Hour':[],'cleared_mw':[]}
    for clearedMWHourlylist in listdata:
        hour_val= int(clearedMWHourlylist['ClearedMWHourly']['Hour'])
        hour_val=time_function.hour_parsing(hour_val)

        LoadData['Hour'].append(hour_val)
        LoadData['cleared_mw'].append(clearedMWHourlylist['ClearedMWHourly']['Value'])
        LoadData['date'].append(Datetime)
    

    ClearedLoadDataframe= pd.DataFrame(LoadData)    #most recent dataframe

    #writes load data for all hubs dataframe to table in database
    engine = create_engine('postgresql://postgres:pserc_data@localhost:5433/miso_db')
    ClearedLoadDataframe.to_sql('cleared_load_data',engine, if_exists='append', index=False )
   
 #run the whole program   
def get_data():
    get_FiveMinTotalLoad(Datetime, data)
    get_clearedMW(Datetime, data)
    get_MTL_forecast(Datetime, data)

get_data()
