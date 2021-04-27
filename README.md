# PA Get Data  
Get PurpleAir sensor data from PurpleAir and Thingspeak API's for bounding box and year-month.
Data are stored in csv files per sensor similarly to the PurpleAir map download tool.

## Operation  
1. Rename config_template.py to config.py and enter PurpleAir keys, directory path and bounding box variables as follows:  
    PURPLEAIR_READ_KEY = 'READ_KEY'  
    PURPLEAIR_WRITE_KEY = 'WRITE_KEY'  

    path_variable = r'd:\Users\UserName\foo\bar'  

    bbox_dict = {  
        'TV': [['-117.5298', '33.7180', '-117.4166', '33.8188'], ''] ,  
        'RS': [['-117.448311', '33.869845', '-117.282829', '34.002297'], 'r'],  
        'OC': [['-117.936172', '33.425138', '-117.566071', '33.782001'], 'o'],  
        'CEP': [['-117.811203', '33.932821', '-117.573280', '34.084228'], 'c']  
        }  

    bbox = ['-118.6298', '33.9180', '-118.4166', '33.7188']   

    DARKSKY_API_KEY = 'INSERT DARKSKY API KEY HERE'  

2. Run the program for example: python pa_get_data.py -y 2020 -m 9  
3. Parameters:  
  * -y, --year   year to get data for.  
  * -m  --month  month to get data for.  
  * -c  --channel  channel to get.  
  * -g  --group    group to get (primary or secondary).  
  * -a  --all      get all channels and groups.  
  * -r  --regional get data for regions defined in config.py bbox_dict.  



# get_weather
Stand alone program that gets weather data from DarkSky  

1. Add the Darsky API Key to config.py
2. Change the HOME variable to your home coordinates in config.py
3. Run the program for example: python get_weather.py -y 2020 -m 9