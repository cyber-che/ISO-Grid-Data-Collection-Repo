

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
    
    # Timestamp reported by data source is in format 23-Jan-2018 - Interval 11:45 EST
    # Unsure exactly why EST is used, possibly due to operational connections with PJM.
    
    
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
        timestamp = json_data['RefId']
        split_time = timestamp.split(" ")
        time_junk = {1, 2}  # set literal
        useful_time_parts = [v for i, v in enumerate(split_time) if i not in time_junk]
    
        if useful_time_parts[-1] != 'EST':
            raise ValueError('Timezone reported for US-MISO has changed.')
    
        time_data = " ".join(useful_time_parts)
        tzinfos = {"EST": tz.gettz('America/New_York')}
        dt = parser.parse(time_data, tzinfos=tzinfos)
    
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
            'production': processed_data[1],
            'storage': {},
            'source': 'misoenergy.org'
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
                         #'source': 'misoenergy.org',
                         'zoneKey': zone_key}
            data.append(datapoint)
    
        return data
    
    
        
   
    
    fp = fetch_production()
    print(fp)
    fwf = fetch_wind_forecast()
   # print(fp)
    
    fp["coal"]  = fp["production"]["coal"]
    fp["wind"]  = fp["production"]["wind"]
    fp["gas"]  = fp["production"]["gas"]
    fp["nuclear"]  = fp["production"]["nuclear"]
    fp["solar"]  = fp["production"]["solar"]
    fp.pop('production', None)
   
    #df=pd.DataFrame()
    fp.pop('zoneKey')
    fp.pop('datetime')  
    fp.pop('storage')
    print( fp)
   

    '''ndf= pd.DataFrame(fp)#most recent dataframe
    print(ndf)
    df=df.append(ndf)
    print('\n')'''
    keys=[]
    values=[]
    for header in fp:
        keys.append(header)
        values.append(fp[header])
    print(keys)
    print(values)
    with open('beta1.csv', 'a') as output_file:
        for key in keys:
            output_file.write("xxy")
        print( '\n' +'\n')   
        for value in values:            
            output_file.write(',' + str(value) )            
            
        

        #df.to_csv('New_fetch_production_consolidated_data.csv')

    
    
        
    ''' keys = fwf[0].keys()
    with open('fetch_new1_wind_forcast.csv', 'a') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()       
        dict_writer.writerows(fwf) 
        
    f = open('fetch_neww_production.csv','a')
    w = csv.DictWriter(f,fp.keys())        
    w.writerow(fp)
    f.close()
    
    print("This prints once every 5 minutes.")
    print('fetch_production() ->')
    print(fp)
    print('fetch_wind_forecast() ->')
    print(fwf)'''
    
get_data()