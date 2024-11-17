import json
import threading
import time

from indicators import fetch_indicators
from s1_evaluation import instrument_key ,entry_evaluation, exit_evaluation


# Initialize dictionaries
with open("credentials.json", "r") as file:     credentials = json.load(file)
user_config = json.load(open("user_data.json"))     #   To Store User Configurations
live_data = {}      #   To Store Live Market Data and Option Chain Data
user_data = {}      # To Update Necessary flags
order_details = {}      #   To update necssary flags related to order and Insturment key details
indicators = {}     # To store all the live Indicators Data


# Determine broker -> import respective modules -> Connect and Fetch Access Token
broker = user_config['broker']
if broker == 'upstox':
    from brokers_api.upstox_api import websocket, place_order, authorize
    authorize(credentials['upstox'])
elif broker == 'zerodha':
    from brokers_api.zerodha_api import websocket, place_order, authorize
    authorize(credentials['zerodha'])
elif broker == 'kotak':
    from brokers_api.kotak_api import websocket, place_order, authorize
    authorize(credentials['kotak'])
elif broker == 'fyers':
    from brokers_api.fyers_api import websocket, place_order, authorize
    authorize(credentials['fyers'])
else:
    raise ValueError(f"Unsupported broker: {broker}")


# Create and start Live Data Thread to Connect Websocket 
live_data_thread = threading.Thread(target=websocket, args=(broker,live_data,))
live_data_thread.start()

# Create and start Indicators Thread to fetch and updates Indicators
indicator_thread = threading.Thread(target=fetch_indicators,args=(live_data,user_config))
indicator_thread.start()


while True : 
    
    time.sleep(float(user_config['sleep_interval']))
    
    if entry_evaluation(indicators, user_data) == True :
        instrument_key(live_data, user_data, order_details)
        place_order(user_data,order_details)
    if exit_evaluation(indicators ,user_data ) :
        place_order(user_data,order_details)    

