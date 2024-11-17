from datetime import datetime

def entry_evaluation(indicators, user_data):

    # Check if current time is within the specified range (9:15 AM to 3:20 PM)
    current_time = datetime.now().time()
    if current_time >= datetime.strptime("09:15", "%H:%M").time() and \
       current_time <= datetime.strptime("15:20", "%H:%M").time() and \
       not user_data['position_active']:
        
        # Check Indicators Data for Entry Signal
        if indicators['MBB_direction'] == 'UP' and \
           indicators['MBB_threshold'] >= 0.1 and \
           indicators['RSI'] >= indicators['Smooth_MA'] and \
           indicators['RSI_crossover'] == True and \
           indicators['Stoch_RSI_K'] >= indicators['Stoch_RSI_D'] and \
           indicators['Stoch_RSI_crossover'] == True:
            
            # Update user data to reflect new order and active position
            user_data['order_flag'] = True
            user_data['position_active'] = True
            return True


def exit_evaluation(indicators, user_data):
    broker = user_data['broker']
    current_pnl = fetch_current_pnl(broker)  # Fetch current PnL (this function should be defined elsewhere)
    current_time = datetime.now()

    # Check conditions for exiting
    if current_pnl >= 500:  # Stop Loss
        user_data['position_active'] = False
        return

    if current_time >= datetime.strptime('15:20', '%H:%M'):  # Close position at 3:20 PM
        user_data['position_active'] = False
        return

    if (indicators['RSI'] >= indicators['Smooth_MA'] + 5 and 
        indicators['Stoch_RSI_K'] >= indicators['Stoch_RSI_D'] and 
        indicators['Stoch_RSI_crossover'] is True):
        user_data['position_active'] = False
        return


def instrument_key(live_data, user_data, order_details):
    # Populate order_details with required data
    order_details['index'] = user_data['user_index']
    order_details['quantity'] = 5  # In Lots
    order_details['order_type'] = 'BUY'
    order_details['option_type'] = live_data['current_option_type']  # CE or PE
    order_details['ATM'] = live_data['current_atm']
    order_details['strike'] = fetch_strike_from_atm(order_details)


def fetch_strike_from_atm(order_details):
    index = order_details['index']
    atm = order_details['ATM']
    option_type = order_details['option_type']
    step = 100 if index.lower() == "bank nifty" else 50
    nearest_strike = round(atm / step) * step
    if option_type == "CE" and nearest_strike < atm:
        nearest_strike += step
    elif option_type == "PE" and nearest_strike > atm:
        nearest_strike -= step
    return nearest_strike
