'''
This is the main function which runs all the data to be collected.
Runs every 5 minutes
+++++ 
----- jtt
'''

import time
import get_miso_wind_forecast_fuel_energy_mix
import get_miso_main_Hub_lmp
import get_MISO_LMP_Consolidated_Data
import real_time_load


def main():
    # while True:
        get_miso_wind_forecast_fuel_energy_mix.get_data()
        get_miso_main_Hub_lmp.get_data()
        get_MISO_LMP_Consolidated_Data.get_data()
        real_time_load.get_data()
        #time.sleep(30000)
      
main()
        
    