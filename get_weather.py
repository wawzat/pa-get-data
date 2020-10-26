#get_weather.py
# Gets weather data from DarkSky for the provided year and month.
# Author: James S. Lucas, Temescal Valley CA - 20201025
# Requires darkskylib by lukaskubis (sudo python -m pip install darkskylib)


import os
import sys
import argparse
from datetime import datetime, timedelta, date
#from pytz import timezone, FixedOffset
import calendar
import pandas as pd
from darksky import forecast  #https://github.com/lukaskubis/darkskylib (pip3 install darkskylib)
import config


# Change this variable to point to the desired directory in config.py. 
data_directory = config.matrix5


API_KEY = config.API_KEY
HOME = config.HOME
timestamp = []
sensor = []
wind_direction = []
wind_speed = []
lat = []
lon = []
wind = []
#timezone = []


def get_arguments():
    parser = argparse.ArgumentParser(
    description='Dowload PurpleAir csv files for sensors in bounding box during year and month provided.',
    prog='pa_get_data',
    usage='%(prog)s [-y <year>], [-m <month>]',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    g=parser.add_argument_group(title='arguments',
          description='''    -y, --year   year to get data for.
    -m  --month  month to get data for.                                        ''')
    g.add_argument('-y', '--year',
                    type=int,
                    dest='yr',
                    help=argparse.SUPPRESS)
    g.add_argument('-m', '--month',
                    type=int,
                    dest='mnth',
                    help=argparse.SUPPRESS)
    args = parser.parse_args()
    return(args)

args = get_arguments()

yr = args.yr
mnth = args.mnth

# returns a tuple (first_day_of_month, last_day_of_month)
mnth_range = calendar.monthrange(yr, mnth)

data_date = date(yr, mnth, 1)
start_date = date(yr, mnth-1, mnth_range[1])
end_date = date(yr, mnth+1, 1)

data_time_template = '{year}-{month}-1 00:00:00'
params = {'year': str(yr),
          'month': str(mnth)
         }
data_time_str = data_time_template.format(**params)
data_time = datetime.strptime(data_time_str, '%Y-%m-%d %H:%M:%S')

file_name = "DSKY_station_merged.csv"
output_folder = data_date.strftime('%Y-%m')
output_path = data_directory + os.path.sep + output_folder
if not os.path.isdir(output_path):
    os.mkdir(output_path)
output_pathname = output_path + os.path.sep + file_name


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)


def get_darksky(start_date, end_date, data_time, HOME, output_pathname):
    num_days = int ((end_date - start_date).days)
    print(str(num_days) + " days of hourly weather data will be collected.")
    if num_days > 200:
        print("error! number of days exceeded. exiting.")
        sys.exit(1)

    for single_date in daterange(start_date, end_date):
        t = datetime(
                single_date.year, single_date.month, single_date.day, 12
                ).isoformat()

        home = forecast(API_KEY, *HOME, time=t)

        for hour in home.hourly:
            try:
                timestamp.append(
                    datetime.fromtimestamp(hour.time).strftime('%Y-%m-%d %H:%M:%S')
                    )
                sensor.append('DARKSKY_REF')
                wind_direction.append(hour.windBearing)
                wind_speed.append(hour.windSpeed)
                lat.append(HOME[0])
                lon.append(HOME[1])
            except AttributeError as e:
                print(e)
                print(datetime.fromtimestamp(hour.time).strftime('%Y-%m-%d %H:%M:%S'))
                continue

    wind = [
        list(a) for a in zip(
            timestamp, sensor, wind_direction, wind_speed, lat, lon
            )
        ]
    df_dsky = (
        pd.DataFrame(
            wind,
            columns=[
                'DateTime_UTC',
                'Sensor',
                'WindDirection',
                'WindSpeed',
                'Lat',
                'Lon'
                ]
            )
        )
    df_dsky['DateTime_UTC'] = pd.to_datetime(df_dsky['DateTime_UTC'])
    df_dsky = df_dsky[(df_dsky['DateTime_UTC'] >= data_time)]
    df_dsky.to_csv(
        output_pathname, index=False, date_format='%Y-%m-%d %H:%M:%S'
        )

get_darksky(start_date, end_date, data_time_str, HOME, output_pathname)