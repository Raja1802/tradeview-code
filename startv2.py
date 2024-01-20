from flask import Flask, request, jsonify
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.daily import DailyTrigger
import re
from datetime import datetime
import pytz
from dateutil import parser

app = Flask(__name__)

mongo_uri = "mongodb://ajar:Raja1802@ac-ujpend1-shard-00-00.gcd17y2.mongodb.net:27017,ac-ujpend1-shard-00-01.gcd17y2.mongodb.net:27017,ac-ujpend1-shard-00-02.gcd17y2.mongodb.net:27017/?ssl=true&replicaSet=atlas-4q8exw-shard-0&authSource=admin&retryWrites=true&w=majority"

client = MongoClient(mongo_uri)
db = client['trade_data']
us_stocks_collection = db['us_stocks']
indian_stocks_collection = db['indian_stocks']
raw_collection = db['raw_trades']
processed_collection = db['processed_trades']
open_queue_positions = db['open_queue_positions']
BATS_queue_positions = db['BATS_queue_positions']
NSE_queue_positions = db['NSE_queue_positions']
trades_not_processed = db['trades_not_processed']
us_unprocessed_table = db['us_unprocessed_table']
india_unprocessed_table = db['india_unprocessed_table']
trades_data = db['trades_data']


def move_open_positions_us_to_unprocessed_tables():
    # Move open positions to us_unprocessed_table
    us_open_positions = BATS_queue_positions.find({})
    for position in us_open_positions:
        us_unprocessed_table.insert_one(position)
        BATS_queue_positions.delete_one({'_id': position['_id']})
def move_open_positions_india_to_unprocessed_tables():
    # Move open positions to india_unprocessed_table
    india_open_positions = NSE_queue_positions.find({})
    for position in india_open_positions:
        india_unprocessed_table.insert_one(position)
        NSE_queue_positions.delete_one({'_id': position['_id']})
# Create a scheduler for US stocks (3:00 AM UTC)
us_scheduler = BackgroundScheduler()
us_scheduler.add_job(
    move_open_positions_us_to_unprocessed_tables,
    trigger='cron',  # Use the cron trigger
    hour=3, minute=0, second=0,
    # trigger=DailyTrigger(hour=3, minute=0, second=0),
    timezone=pytz.utc,
)
us_scheduler.start()

# Create a scheduler for Indian stocks (3:00 AM IST)
india_scheduler = BackgroundScheduler()
india_scheduler.add_job(
    move_open_positions_india_to_unprocessed_tables,
    trigger='cron',  # Use the cron trigger
    hour=3, minute=0, second=0,
    # trigger=DailyTrigger(hour=3, minute=0, second=0),
    timezone=pytz.timezone("Asia/Kolkata"),
)
india_scheduler.start()

def calculate_profit(sell_price, buy_price):
    if buy_price is None or sell_price is None:
        return None  # Unable to calculate profit without both prices

    return round(sell_price - buy_price, 2)

def convert_to_ist(time_str):
    utc_timezone = pytz.timezone("UTC")
    ist_timezone = pytz.timezone("Asia/Kolkata")

    time_utc = parser.parse(time_str)
    time_utc = utc_timezone.localize(time_utc)
    time_ist = time_utc.astimezone(ist_timezone)
    return time_ist.strftime("%H:%M")

def parse_strategy_text(strategy_text):
    match = re.search(r'Strategy', strategy_text)
    if not match:
        raise ValueError("Invalid input format. Unable to find 'Strategy'.")
    
    index = match.start()
    
    first_part = strategy_text[:index]
    second_part = strategy_text[index:]
    
    lines = second_part.split(',')
    strategy_dict = {}
    for line in lines:
        if '=' in line:
            key, value = map(str.strip, line.split('=', 1))
            value = value.rstrip('\\n')
            key = key.replace('{{', '').replace('}}', '').replace('\\n', '').strip()
            value = value.replace('{{', '').replace('}}', '').replace('\\n', '').strip()

            if key == 'Time':
                try:
                    value = datetime.fromisoformat(value)
                except ValueError:
                    pass
            
            strategy_dict[key] = value

    raw_collection.insert_one({'raw_data': strategy_text, 'first_part': first_part})
    trades_data.insert_one(strategy_dict)
    currency = strategy_dict.get('Currency', '').upper()

    if currency:
        if currency == 'USD':
            exchange = "BATS"
            print(exchange)
            stock = strategy_dict.get('BATS')
            process_and_save_stock(strategy_dict, us_stocks_collection, exchange)
        elif currency == 'INR':
            exchange = "NSE"
            stock = strategy_dict.get('NSE')
            process_and_save_stock(strategy_dict, indian_stocks_collection, exchange)

    return jsonify({'message': 'Trade data saved successfully'})

def process_and_save_stock(stock_data, stock_collection, exchange):
    exchange_symbol = stock_data.get(exchange)
    if exchange == "BATS":
        open_queue_ = BATS_queue_positions
    else:
        open_queue_ = NSE_queue_positions
    existing_position = open_queue_.find_one({exchange: exchange_symbol})
    print(existing_position)
    if existing_position and (existing_position['Order'] != stock_data['Order']) and existing_position != None:
        sell_date_object = datetime.fromisoformat(str(existing_position['Time'])[:-1])
        formatted_date = sell_date_object.strftime("%d %b %Y")
        sell_time = sell_date_object.strftime("%I:%M")
        buy_date_object = datetime.fromisoformat(str(stock_data['Time'])[:-1])
        buy_time = buy_date_object.strftime("%I:%M")

        existing_position['Stock'] = exchange_symbol
        existing_position['Date'] = formatted_date
        existing_position['Buy_time'] = buy_time
        existing_position['Sell_time'] = sell_time

        if 'Buy_time' in existing_position:
            existing_position['Buy_time'] = convert_to_ist(existing_position['Buy_time'])
        if 'Sell_time' in existing_position:
            existing_position['Sell_time'] = convert_to_ist(existing_position['Sell_time'])

        existing_position['Sell_Price'] = float(existing_position["Price"])
        existing_position['Buy_Price'] = float(stock_data['Price'])

        profit = calculate_profit(existing_position['Sell_Price'], existing_position['Buy_Price'])
        existing_position['P/L'] = profit
        existing_position['PL_Percentage'] = round(
            ((existing_position['Sell_Price'] - existing_position['Buy_Price']) / existing_position['Buy_Price']) * 100, 2
        )

        for unwanted_field in ['Price', 'Strategy', 'Order', 'Comment', 'Time', exchange]:
            existing_position.pop(unwanted_field, None)

        stock_collection.insert_one(existing_position)
        open_queue_.delete_one({exchange: exchange_symbol})
    elif stock_data['Order'] == 'buy':
        trades_not_processed.insert_one(stock_data)
    elif stock_data['Order'] != 'buy':
        open_queue_.insert_one(stock_data)

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.data.decode('utf-8')

    if data:
        try:
            parse_strategy_text(data)
            return jsonify({'message': 'Trade data saved successfully'})
        except ValueError as e:
            return jsonify({'message': str(e)}), 400
    
    return jsonify({'message': 'Invalid or unsupported payload'}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
