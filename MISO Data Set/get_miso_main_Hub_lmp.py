# -*- coding: utf-8 -*-
"""
Created on Wed Aug  5 15:32:21 2020
@author: CJ
gets LMP prices from the MISO website and uploads data to remote server
(More elaborate commenting later once commenting style is fully established)
"""
def get_data():
    import time_function
    import requests  
    import pandas as pd
    import json
    from sqlalchemy import create_engine
    
    data= requests.get('https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=getexantelmp&returnType=json')
    data= data.json()

    # time delivered with data: 
    timestamp =data["LMPData"]["RefId"] #call time_function to get current time
    dt=time_function.get_datetime(timestamp)

    LMPData=data["LMPData"]['ExAnteLMP']['Hub']
    for listd in LMPData: # adds time formatting to data
        listd.update({'time':dt})
    
    main_hub_dataframe=pd.DataFrame(LMPData)

    #assigns convenient datatypes to given columns
    main_hub_dataframe["LMP"]=pd.to_numeric(main_hub_dataframe["LMP"])
    main_hub_dataframe["loss"]=pd.to_numeric(main_hub_dataframe["loss"])
    main_hub_dataframe["congestion"]=pd.to_numeric(main_hub_dataframe["congestion"])
   
    #writes Main Hub dataframe to table in database
    engine = create_engine('postgresql://postgres:pserc_data@localhost:5433/miso_db')
    main_hub_dataframe.to_sql('lmp_data_main_hubs',engine, if_exists='append', index=False )

get_data()  
