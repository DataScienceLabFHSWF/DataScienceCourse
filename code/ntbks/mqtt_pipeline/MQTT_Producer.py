import paho.mqtt.client as mqtt 
from random import randrange, uniform
import time
import requests
import json

weather_url = 'https://api.met.no/weatherapi/locationforecast/2.0/complete?lat=69.64&lon=18.95'

#initialize client and connect to the broker
mqttBroker ="localhost"
client = mqtt.Client("Weather Publisher")
client.connect(mqttBroker) 
#infinity loop
while True:
    weather_data = requests.get(weather_url).json()
    
    client.publish("CurrentWeather", json.dumps(weather_data))
    print("Just published weather data from Tromso to CurrentWeather")
    time.sleep(60*60) #60min