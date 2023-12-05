from flask import Flask, request, jsonify
from pymongo import MongoClient
import re
from datetime import datetime

app = Flask(__name__)

# Replace the connection string with your MongoDB Atlas connection string
mongo_uri = "mongodb://ajar:Raja1802@ac-ujpend1-shard-00-00.gcd17y2.mongodb.net:27017,ac-ujpend1-shard-00-01.gcd17y2.mongodb.net:27017,ac-ujpend1-shard-00-02.gcd17y2.mongodb.net:27017/?ssl=true&replicaSet=atlas-4q8exw-shard-0&authSource=admin&retryWrites=true&w=majority"

client = MongoClient(mongo_uri)
db = client['trade_data']
raw_collection = db['raw_trades']
processed_collection = db['processed_trades']
open_queue_positions = db['open_queue_positions']

def calculate_profit(buy_price, sell_price):
    if buy_price is None or sell_price is None:
        return None  # Unable to calculate profit without both prices

    return round(sell_price - buy_price, 4)

def parse_strategy_text(strategy_text):
    # Check if "Strategy" is present in the input string
    match = re.search(r'Strategy', strategy_text)
    if not match:
        # Handle the case where the pattern is not found
        raise ValueError("Invalid input format. Unable to find 'Strategy'.")
    
    # Get the index where the pattern was found
    index = match.start()
    
    # Extract the remaining text
    first_part = strategy_text[:index]
    second_part = strategy_text[index:]
    
    # Split the remaining text by '\\n'
    lines = second_part.split(',')
    # Create a dictionary from the key-value pairs
    strategy_dict = {}
    for line in lines:
        if '=' in line:
            key, value = map(str.strip, line.split('=', 1))
            
            # Remove ending comma if present
            value = value.rstrip('\\n')
            
            # Convert date and time strings to datetime objects
            if key == 'Time':
                try:
                    value = datetime.fromisoformat(value)
                except ValueError:
                    # Handle the case where the string is not a valid ISO format
                    pass
            
            # Remove {{ }} brackets from values
            value = value.replace('{{', '').replace('}}', '').replace('\\n', '').strip()
            key = key.replace('{{', '').replace('}}', '').replace('\\n', '').strip()
            # Insert each key-value pair as a separate field in processed_collection
            strategy_dict[key] = value

    # Save raw data and first_part in raw_collection
    raw_collection.insert_one({'raw_data': strategy_text, 'first_part': first_part})
    
    bats = strategy_dict.get('NSE')
    if bats:
        existing_position = open_queue_positions.find_one({'NSE': bats})
        print(existing_position)
        if existing_position and (existing_position["Order"] !=  strategy_dict["Order"]) and existing_position != None:
            # If position exists, close it and save to processed_collection
            if existing_position["Order"] == 'sell':
                sell_date_object = datetime.fromisoformat(str(existing_position['Time'])[:-1])
                formatted_date = sell_date_object.strftime("%d %B %Y")
                sell_time = sell_date_object.strftime("%I:%M")
                buy_date_object = datetime.fromisoformat(str(strategy_dict['Time'])[:-1])
                buy_time = buy_date_object.strftime("%I:%M")
                existing_position['Date'] = formatted_date
                existing_position['Buy_time'] = buy_time
                existing_position['Sell_time'] = sell_time
                existing_position['Sell_Price'] =  float(existing_position["Price"])
                existing_position['Buy_Price'] = float(strategy_dict['Price'])
                profit = calculate_profit(existing_position['Sell_Price'], existing_position['Buy_Price'])
                existing_position['P/L'] = profit
                existing_position['closed_time'] = datetime.now()
                for unwanted_field in ['Price', 'Strategy', 'Order', 'Comment', 'Time']:
                    existing_position.pop(unwanted_field, None)
                processed_collection.insert_one(existing_position)
                # Remove the closed position from the open_queue_positions
                open_queue_positions.delete_one({'NSE': bats})
        else:
            open_queue_positions.insert_one(strategy_dict)
    return jsonify({'message': 'Trade data saved successfully'})

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.data.decode('utf-8')

    if data:
        try:
            # Parse and save processed data to MongoDB
            parse_strategy_text(data)
            return jsonify({'message': 'Trade data saved successfully'})
        except ValueError as e:
            return jsonify({'message': str(e)}), 400
    
    return jsonify({'message': 'Invalid or unsupported payload'}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
