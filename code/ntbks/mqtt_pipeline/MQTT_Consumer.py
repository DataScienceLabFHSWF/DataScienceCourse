import paho.mqtt.client as mqtt
import time
import json
from pprint import pprint
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import pandas as pd

org = "FHSWF"
bucket = "weather"
token="clmwcDxm_39ML529Q5lbtT4XRLChNqqGull5VJt_zrFkAw4rTTPcMyzkhPo2sAlDhr6mIJW66en1PqRGmybRXQ=="


def precip_prob(i):
    try:
        return weather_data['properties']['timeseries'][i]['data']['next_1_hours']['details']['probability_of_precipitation']
    except:
        None
def precip_amount(i):
    try:
        return weather_data['properties']['timeseries'][i]['data']['next_1_hours']['details']['precipitation_amount']
    except:
        None

# define the preprocessing function returning a pandas df
def format_data(weather_data):
    data = [weather_data['properties']['timeseries'][i]['data']['instant']['details'] for i in range(len(weather_data['properties']['timeseries']))]
    times = [weather_data['properties']['timeseries'][i]['time'] for i in range(len(weather_data['properties']['timeseries']))]
    precip_proba = [precip_prob(i) for i in range(len(weather_data['properties']['timeseries']))]
    precip = [precip_amount(i) for i in range(len(weather_data['properties']['timeseries']))]
    df = pd.DataFrame(data, index=times)
    df.index = pd.to_datetime(df.index)
    df.index.name = 'forecast_time'
    df['lat'] = weather_data['geometry']['coordinates'][1]
    df['lon'] = weather_data['geometry']['coordinates'][0]
    df['height'] = weather_data['geometry']['coordinates'][2]
    df['timestamp'] = weather_data['properties']['meta']['updated_at']
    df['precipitation_prob'] = precip_proba
    df['precipitation_amount'] = precip

    return df


#define a callback that processes messages when they come in from the broker
def on_message(client, userdata, message):
    msg = json.loads(message.payload)
    df = format_data(msg)
    print(df)
    #insert preprocessing for the data here from json dump to lines
    with InfluxDBClient("http://localhost:8086", token=token, org=org) as dbclient:
        write_api = dbclient.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket, record=df, data_frame_measurement_name=df['timestamp'][0], data_frame_tag_columns='forecast')
    print('successfully wrote to DB')

#connect to broker
mqttBroker ="localhost"

client = mqtt.Client("Subscriber")
client.connect(mqttBroker) 

client.subscribe("CurrentWeather")
client.on_message=on_message 
client.loop_forever()
