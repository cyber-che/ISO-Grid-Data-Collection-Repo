from datetime import datetime
import pytz
from dateutil import parser, tz
from datetime import time


'''
Returns 3 different time formats depending:
    get_datetime(timestamp)  returns date and time 
    get_date(timestamp) returns date
    get_hour(timestamp) returns hour
'''

#returns the date and time in one format
def get_datetime(timestamp):

    split_time = timestamp.split(" ")
    time_junk = {1, 2}  # set literal
    
    ''' time data comes with a lot of junk data. useful_time_parts refers to etracts
        the Date, time and timezone portions of the time data
        '''
    useful_time_parts = [v for i, v in enumerate(split_time) if i not in time_junk]

    if useful_time_parts[-1] != 'EST':
        raise ValueError('Timezone reported for US-MISO has changed.')

    time_data = " ".join(useful_time_parts)
    tzinfos = {"EST": tz.gettz('America/New_York')}
    dt = parser.parse(time_data, tzinfos=tzinfos)  #gets time in 'year-month-day Hour:Min:Sec-timezone' format

    return dt

#gerates and returns the date separately.
def get_date(timestamp):
    dt=get_datetime(timestamp)
    dt=dt.strftime("%m-%d-%Y")
    return dt

#gets the hours
def hour_parsing(hour_val):
     if hour_val==24:
            parsed_hour_val=time(0,0,0)
     else: 
        parsed_hour_val=time(hour_val,0,0)
     return parsed_hour_val