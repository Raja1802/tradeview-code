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
    
    # Split the remaining text by '\n'
    lines = second_part.split('\n')
    
    # Create a dictionary from the key-value pairs
    strategy_dict = {}
    for line in lines:
        if '=' in line:
            key, value = map(str.strip, line.split('=', 1))
            
            # Remove ending comma if present
            value = value.rstrip(',')
            
            # Convert date and time strings to datetime objects
            if key == 'Time':
                try:
                    value = datetime.fromisoformat(value)
                except ValueError:
                    # Handle the case where the string is not a valid ISO format
                    pass
            
            strategy_dict[key] = value
    
    return first_part, strategy_dict

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.data.decode('utf-8')

    if data:
        try:
            # Save raw data to MongoDB
            raw_collection.insert_one({'raw_data': data})

            # Parse and save processed data to MongoDB
            first_part, processed_data = parse_strategy_text(data)
            processed_collection.insert_one({'first_part': first_part, 'processed_data': processed_data})

            return jsonify({'message': 'Trade data saved successfully'})
        except ValueError as e:
            return jsonify({'message': str(e)}), 400
    
    return jsonify({'message': 'Invalid or unsupported payload'}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
