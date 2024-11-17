import asyncio
import json
import ssl
import upstox_client
import websockets
import pandas as pd
import threading
import requests as rq
import json
from google.protobuf.json_format import MessageToDict
import brokers_api.MarketDataFeed_pb2 as pb
from urllib.parse import quote, urlparse, parse_qs



def authorize(credentials):
    auth_url = f'https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={credentials["API_KEY"]}&redirect_uri={quote(credentials["RURL"], safe="")}'
    print("Generated authentication URL:", auth_url)
    auth_code = input("Paste the authentication URL obtained after manual login: ")
    
    parsed_url = urlparse(auth_code)
    auth_code = parse_qs(parsed_url.query).get('code', [None])[0]
    
    if auth_code:
        url = 'https://api-v2.upstox.com/login/authorization/token'
        headers = {
            'accept': 'application/json',
            'Api-Version': '2.0',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        data = {
            'code': auth_code,
            'client_id': credentials["API_KEY"],
            'client_secret': credentials["SECRET_KEY"],
            'redirect_uri': credentials["RURL"],
            'grant_type': 'authorization_code'
        }
        response = rq.post(url, headers=headers, data=data)
        if response.status_code == 200:
            access_token = response.json()['access_token']
            with open("access_token.txt", "w") as file:
                file.write(access_token)
            print("Access token saved successfully.")
        else:
            print("Failed to fetch access token.")
    else:
        print("Authentication code not found in URL.")



def place_order(order_details, user_data):

    with open('access_token.txt', 'r') as file: access_token = file.read().strip()

    payload = {
        "quantity": user_data['quantity'],
        "product": "I",
        "validity": "DAY",
        "price": user_data['limit_price'],
        "tag": "string",
        "instrument_token": user_data['instrument_key'],
        "order_type": user_data['order_type'],
        "transaction_type": user_data['transaction_type'],
        "disclosed_quantity": 0,
        "trigger_price": 0,
        "is_amo": False }

    headers = {
        'accept': 'application/json',
        'Api-Version': '2.0',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'}

    response = rq.post("https://api.upstox.com/v2/order/place", headers=headers, data=json.dumps(payload))
    user_data['position_active'] = False
    return response.json()

    

def websocket(live_data):
    
    # Global live data dictionary
    live_data_lock = threading.Lock()

    def get_open_value(access_token, symbol="NSE_INDEX|Nifty Bank"):
        url = "https://api.upstox.com/v2/market-quote/quotes"
        headers = {'accept': 'application/json', 'Api-Version': '2.0', 'Authorization': f'Bearer {access_token}'}
        response = rq.get(url, headers=headers, params={'symbol': symbol})
        return response.json()['data'][symbol]['ohlc']['open']

    def BN_DF(open_value, strike_price_cap):
        rounded_open = round(open_value / 100) * 100
        upper_limit_ikey, lower_limit_ikey = rounded_open + strike_price_cap, rounded_open - strike_price_cap
        df = pd.read_csv("https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz")
        BNDF = df[(df['exchange'] == 'NSE_FO') & (df['instrument_type'] == 'OPTIDX') & (df['lot_size'] == 15) & 
                  (df['option_type'].isin(['CE', 'PE']))]
        BNDF = BNDF.sort_values(by='expiry').query(f'strike >= {lower_limit_ikey} & strike <= {upper_limit_ikey}')
        BNDF['strike'] = BNDF['strike'].round(-2)
        return BNDF[['instrument_key', 'strike', 'option_type']]

    def ikey_string(BNDF):
        return BNDF['instrument_key'].tolist()

    def get_market_data_feed_authorize(api_version, configuration):
        api_instance = upstox_client.WebsocketApi(upstox_client.ApiClient(configuration))
        return api_instance.get_market_data_feed_authorize(api_version)

    def decode_protobuf(buffer):
        feed_response = pb.FeedResponse()
        feed_response.ParseFromString(buffer)
        return feed_response

    async def fetch_market_data(instrument_keys_list, access_token, BNDF, live_data, index_type):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        configuration = upstox_client.Configuration()
        configuration.access_token = access_token
        api_version = '2.0'
        response = get_market_data_feed_authorize(api_version, configuration)

        async with websockets.connect(response.data.authorized_redirect_uri, ssl=ssl_context) as websocket:
            data = {"guid": "someguid", "method": "sub", "data": {"mode": "full", "instrumentKeys": instrument_keys_list}}
            await websocket.send(json.dumps(data).encode('utf-8'))
            while True:
                message = await websocket.recv()
                decoded_data = decode_protobuf(message)
                data_dict = MessageToDict(decoded_data)
                websocket_df = process_instruments_data(data_dict, instrument_keys_list, BNDF, index_type)
                with live_data_lock:
                    live_data[index_type] = websocket_df.to_dict('records')

    def process_instruments_data(data_dict, instrument_keys_list, BNDF, index_type):
        instrument_data = []
        for instrument_key in instrument_keys_list:
            instrument_info = data_dict.get("feeds", {}).get(instrument_key, {})
            ltp = instrument_info.get("ff", {}).get("marketFF", {}).get("ltpc", {}).get("ltp", "NA")
            instrument_data.append({"Instrument Key": instrument_key, "LTP": ltp})
        return pd.DataFrame(instrument_data).merge(BNDF, on="Instrument Key", how="right")

    access_token = live_data["access_token"]
    strike_price_cap = 300
    nifty_open_value = get_open_value(access_token, "NSE_INDEX|Nifty")
    bank_nifty_open_value = get_open_value(access_token, "NSE_INDEX|Nifty Bank")

    BNDF_nifty = BN_DF(nifty_open_value, strike_price_cap)
    BNDF_bank_nifty = BN_DF(bank_nifty_open_value, strike_price_cap)

    instrument_keys_list_nifty = ikey_string(BNDF_nifty)
    instrument_keys_list_bank_nifty = ikey_string(BNDF_bank_nifty)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(fetch_market_data(instrument_keys_list_nifty, access_token, BNDF_nifty, live_data, "nifty"))
    loop.create_task(fetch_market_data(instrument_keys_list_bank_nifty, access_token, BNDF_bank_nifty, live_data, "bank_nifty"))
    loop.run_forever()




