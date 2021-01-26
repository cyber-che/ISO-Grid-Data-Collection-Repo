# -*- coding: utf-8 -*-
"""
Created on Sat Aug 15 13:50:23 2020
@author: CJ

Extracts LMP Consolidated data from MISO using provided API.
Collected data is stored on the "pserc-d.ad.engr.wisc.edu" remote server database.
"""
import time_function
import requests  
import pandas as pd
import json
from sqlalchemy import create_engine



def get_data():

    url='https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=getlmpconsolidatedtable&returnType=json'
    data= requests.get(url).json()#converts file into usable python dictionary

    timestamp=data['LMPData']['RefId'] #call time_function to get current time
    time=time_function.get_datetime(timestamp)

    listnodes=data['LMPData'][ 'FiveMinLMP']['PricingNode']
    
    for listd in listnodes: # adds time to data
        listd['time']=time

    consol_dataframe= pd.DataFrame(listnodes)#most recent dataframe
    

    #assigns convenient datatypes to given columns
    consol_dataframe["LMP"]=pd.to_numeric(consol_dataframe["LMP"])
    consol_dataframe["MCC"]=pd.to_numeric(consol_dataframe["MCC"])
    consol_dataframe["MLC"]=pd.to_numeric(consol_dataframe["MLC"])   

    #writes lmp for all hubs dataframe to table in database
    engine = create_engine('postgresql://postgres:pserc_data@localhost:5433/miso_db')
    consol_dataframe.to_sql('full_lmp_consolidated_data',engine, if_exists='append', index=False )
  
get_data()
    
