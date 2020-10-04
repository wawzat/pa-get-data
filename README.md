# PA Get Data  
Get PurpleAir sensor data from PurpleAir and Thingspeak API's for bounding box and year-month.
Data are stored in csv files per sensor similarly to the PurpleAir map download tool.

## Operation  
1. Change hard coded coordinates in bbox variable in get_sensor_indexes()
2. Change hard coded yr and mnth in Main
3. Edit the directory paths as appropriate for your setup
4. Create a file called config.py in the same folder as the code, with your PurpleAir keys entered as follows:
    READ_KEY = 'READ_KEY'
    WRITE_KEY = 'WRITE_KEY'

## Potential Future Enhancements
1. change to class or function accepting input for bbox and date range and returning data as DF or other.
2. convert sensor_ids tuple to dictionary
3. rename variables consistently with names that are more appropriate to function
4. allow partial month date range
5. exception handling
6. directories and paths from config file vs hardcoded. Prompt for path on first run and save as config file.
7. Include sensor name and coordinates with the data, not just in the filename.
