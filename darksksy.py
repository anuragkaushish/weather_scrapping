# -*- coding: utf-8 -*-
"""
@author: Anurag Sharma
"""

import os
import requests
import pandas as pd
from datetime import datetime
import datetime as dt
from dateutil import tz
import time
import pymysql
from sqlalchemy import create_engine

###DB ---read info
host='abc'
user='******'
password='********'
dbname='******'

###DB---write info
###EDEN
host1='******'
user1='root'
password1='*********'
dbname1='******'

string1='mysql+pymysql://'+user1+':'+password1+'@'+host1+':3306/'+dbname1
engine = create_engine(string1, echo=False)

###SET UTC as base timezone
os.environ["TZ"] = "UTC"
#os.environ["TZ"] = "Asia/Kolkata"

###Plant Info
def plants():
    db_connection = pymysql.connect(host=host,user=user,password=password,db=dbname)
    SQL = "SELECT plant_id,latitude,longitude FROM plants WHERE plant_id=2"
    df = pd.read_sql(SQL, con=db_connection)
    db_connection.close()
    return(df)
    
plant=plants()
plant_series= plant['plant_id']

###Forecast Type
forecasts=['hourly']
#if dt.datetime.now().hour==4:
#    forecasts= ['hourly','hourly10day']
#else:
#    forecasts= ['hourly']

###API KEYs
keys=['7a10976377647a9e9bb4d3741935db2a']
###Initialise Counters
calls=0
keyno=0

###Start  and End Dates for forecast
start= dt.date(2017,9,30)
end=dt.date(2018,12,29)
interval=pd.Series(pd.date_range(start, end)).apply(lambda x: x.date())

###Date to Unix converter
def date_unix(t_date):
    t_unix = t_date.strftime("%Y-%m-%d %H:%M:%S")
    t_unix = datetime.strptime(t_unix, "%Y-%m-%d %H:%M:%S")
    t_unix = time.mktime(t_unix.timetuple())
    t_unix=round(t_unix)
    return(t_unix)


for i in range(0,len(interval)):
    dt=interval[i]
    print(dt)
    dt_unix=date_unix(dt)
    
    for forecast in forecasts:
#        print(forecast)
        for p in range(0,len(plant_series)):
            plantID=plant_series[p]
#            print(plantID)
            latitude=round(plant.loc[plant['plant_id']==plantID,'latitude'][p],5)
            longitude=round(plant.loc[plant['plant_id']==plantID,'longitude'][p],5)
            
            url='https://api.darksky.net/forecast/'+keys[keyno]+'/'+str(latitude)+','+str(longitude)+','+str(dt_unix)+'?units=auto'
            r= requests.get(url)
            a = r.json()
            
            ##Updation of counters
            print(calls)
            calls=calls+1
            if (calls%971)==0:
                keyno=keyno+1
                calls=0
                print(keyno)
                print(keys[keyno])
             
            
            ###location from the coordinates used for api call
            local_tz= a['timezone']  
            local_tz= tz.gettz(local_tz)
            sd_tz=tz.gettz('UTC')
            
            df1= pd.DataFrame(a['daily']['data'])  ###for sunrise and sunset times
            df = pd.DataFrame(pd.DataFrame(a['hourly']['data']))  ### for daily hour-wise weather values
            
            df['sunrise']=df1['sunriseTime'].apply(lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S"))[0]
            df['sunset']=df1['sunsetTime'].apply(lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S"))[0]            
            df['datetime'] = df['time'].apply(lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S"))
            df['update'] = datetime.now()
            df['update']=df['update'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
            
            df['datetime'] = pd.to_datetime(df['datetime'])
            df['sunrise'] = pd.to_datetime(df['sunrise'])
            df['sunset'] = pd.to_datetime(df['sunset'])
            df['update'] = pd.to_datetime(df['update'])
            df['timestamp']=df['datetime'].apply(lambda x: int(time.mktime(x.timetuple())))
            df['datetime'] = df['datetime'].apply(lambda x:x.replace(tzinfo=sd_tz))
            df['local_time']=df['datetime'].apply(lambda x:x.tz_localize(sd_tz).tz_convert(local_tz).tz_localize(None))
            df['datetime']  = df['datetime'].apply(lambda x:x.tz_localize(None))
            #df['local_time']  = df['local_time'].apply(lambda x:x.tz_localize(None))
            #df['date']=df['datetime'].apply(lambda x: x.date())
            #df['time']=df['datetime'].apply(lambda x: x.time())
            df['plant_id']= plantID
            nlist= ['apparentTemperature', 'cloudCover','dewPoint', 'humidity', 'icon', 'precipType',
                    'pressure', 'summary', 'temperature', 'time', 'visibility',
                    'windBearing', 'windSpeed', 'sunrise', 'sunset', 'datetime', 'update',
                    'timestamp', 'plant_id']
            df['apparent_temperature']= df['apparentTemperature']
            df['cloud_cover']= df['cloudCover']
            df['dew_point']= df['dewPoint']
            df['precip_type']= df['precipType']
            df['wind_bearing']= df['windBearing']
            df['wind_speed']= df['windSpeed']
            mlist= ['plant_id','datetime','apparent_temperature','cloud_cover','dew_point','humidity','icon',
                   'precip_type','pressure','summary','temperature','wind_bearing',
                   'wind_speed','sunrise','sunset','update','local_time','timestamp']
            mlist=[val for val in df.columns.tolist() if val in mlist]
            df=df[mlist]
                   
            df.to_sql(name='darksky', con=engine, if_exists = 'append', index=False)

