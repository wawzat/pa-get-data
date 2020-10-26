# PA Get Data  
Get PurpleAir sensor data from PurpleAir and Thingspeak API's for bounding box and year-month.
Data are stored in csv files per sensor similarly to the PurpleAir map download tool.

## Operation  
1. Rename config_template.py to config.py and enter PurpleAir keys, directory bath and bounding box variables as follows:
    READ_KEY = 'READ_KEY'  
    WRITE_KEY = 'WRITE_KEY'

    path_variable = r'd:\Users\UserName\foo\bar'

    bbox = ['-118.6298', '33.9180', '-118.4166', '33.7188'] 
2. Run the program for example: python pa_get_data.pu -y 2020 -m 9


# get_weather
Stand alone program that gets weather data from DarkSky

1. Add the Darsky API Key to config.py
2. Change the HOME variable to your home coordinates in config.py
3. Run the program for example: python get_weather.py -y 2020 -m 9