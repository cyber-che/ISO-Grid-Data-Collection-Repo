
def get_data():
    # -*- coding: utf-8 -*-
    """
    Created on Fri Sep 18 22:41:44 2020
    
    @author: CJ
    """
    
    
    """Parser for the MISO area of the United States."""
    import csv
    import time
    import logging
    import requests
    from dateutil import parser, tz
    import pandas as pd
    from sqlalchemy import create_engine
    import time_function
    
    mix_url = 'https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType' \
              '=getfuelmix&returnType=json'
    
    mapping = {'Coal': 'coal',
               'Natural Gas': 'gas',
               'Nuclear': 'nuclear',
               'Wind': 'wind',
               'Solar': 'solar',
               'Other': 'unknown'}
    
    wind_forecast_url = 'https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=getWindForecast&returnType=json'
    
    # To quote the MISO data source;
    # "The category listed as “Other” is the combination of Hydro, Pumped Storage Hydro, Diesel, Demand Response Resources,
    # External Asynchronous Resources and a varied assortment of solid waste, garbage and wood pulp burners".
    
    
    def get_json_data(logger, session=None):
        """Returns 5 minute generation data in json format."""
    
        s = session or requests.session()
        json_data = s.get(mix_url).json()
    
        return json_data
    
    
    def data_processer(json_data, logger):
        """
        Identifies any unknown fuel types and logs a warning.
        Returns a tuple containing datetime object and production dictionary.
        """
    
        generation = json_data['Fuel']['Type']
    
        production = {}
        for fuel in generation:
            try:
                k = mapping[fuel['CATEGORY']]
            except KeyError as e:
                logger.warning("Key '{}' is missing from the MISO fuel mapping.".format(
                    fuel['CATEGORY']))
                k = 'unknown'
            v = float(fuel['ACT'])
            production[k] = production.get(k, 0.0) + v
    
        # Remove unneeded parts of timestamp to allow datetime parsing.
        timestamp = json_data['RefId']   #call time_function to get current time
        dt=time_function.get_datetime(timestamp)

        return dt, production
    
    
    def fetch_production(zone_key='US-MISO', session=None, target_datetime=None, logger=logging.getLogger(__name__)):
        """
        Requests the last known production mix (in MW) of a given country
        Arguments:
        zone_key (optional) -- used in case a parser is able to fetch multiple countries
        session (optional)      -- request session passed in order to re-use an existing session
        target_datetime (optional) -- used if parser can fetch data for a specific day
        logger (optional) -- handles logging when parser is run as main
        Return:
        A dictionary in the form:
        {
          'zoneKey': 'FR',
          'datetime': '2017-01-01T00:00:00Z',
          'production': {
              'biomass': 0.0,
              'coal': 0.0,
              'gas': 0.0,
              'hydro': 0.0,
              'nuclear': null,
              'oil': 0.0,
              'solar': 0.0,
              'wind': 0.0,
              'geothermal': 0.0,
              'unknown': 0.0
          },
          'storage': {
              'hydro': -10.0,
          },
          'source': 'mysource.com'
        }
        """
    
        if target_datetime:
            raise NotImplementedError('This parser is not yet able to parse past dates')
    
        json_data = get_json_data(logger, session=session)
        processed_data = data_processer(json_data, logger)

        data = {
            'zoneKey': zone_key,
            'datetime': processed_data[0],  
            'production': processed_data[1].values(),
            'production_type': processed_data[1].keys(),            
        }
                        
        return data
    
    
    def fetch_wind_forecast(zone_key='US-MISO', session=None, target_datetime=None, logger=None):
        """
        Requests the day ahead wind forecast (in MW) of a given zone
        Arguments:
        zone_key (optional) -- used in case a parser is able to fetch multiple countries
        session (optional)  -- request session passed in order to re-use an existing session
        target_datetime (optional) -- used if parser can fetch data for a specific day
        logger (optional) -- handles logging when parser is run as main
        Return:
        A list of dictionaries in the form:
        {
        'source': 'misoenergy.org',
        'production': {'wind': 12932.0},
        'datetime': '2019-01-01T00:00:00Z',
        'zoneKey': 'US-MISO'
        }
        """
    
        if target_datetime:
            raise NotImplementedError('This parser is not yet able to parse past dates')
    
        s = session or requests.Session()
        req = s.get(wind_forecast_url)
        raw_json = req.json()
        raw_data = raw_json['Forecast']
    
        data = []
        for item in raw_data:
            dt = parser.parse(item['DateTimeEST']).replace(tzinfo=tz.gettz('America/New_York'))
            value = float(item['Value'])
    
            datapoint = {'datetime': dt,
                         'production': {'wind': value},                        
                         'zoneKey': zone_key}
            data.append(datapoint)
    
        return data

    #creating handles for fetch_production and fetch_wind_forecast data
    fp = fetch_production() 
    fwf = fetch_wind_forecast()

    fetch_prod_dataframe=pd.DataFrame.from_dict(fp)   
    
    for i in range(len(fwf)):        
        fwf[i]['production']=fwf[i]['production']['wind']
        fwf[i]['Wind production']= fwf[i]['production']
        fwf[i].pop('production')        
    
    fetch_wind_forcast_dataframe=pd.DataFrame.from_dict(fwf)
    
    #writes fetch production dataframe to table in database
    engine = create_engine('postgresql://postgres:pserc_data@localhost:5433/miso_db')
    fetch_prod_dataframe.to_sql('fetch_production', engine, if_exists='append', index=False )
    
    #writes fetch production dataframe to table in database
    engine = create_engine('postgresql://postgres:pserc_data@localhost:5433/miso_db')
    fetch_wind_forcast_dataframe.to_sql('fetch_wind_forcast', engine, if_exists='append', index=False )
get_data()
