from flask import Flask, request, jsonify
from pymongo import MongoClient

app = Flask(__name__)

# Replace the connection string with your MongoDB Atlas connection string
mongo_uri = "mongodb://ajar:Raja1802@ac-ujpend1-shard-00-00.gcd17y2.mongodb.net:27017,ac-ujpend1-shard-00-01.gcd17y2.mongodb.net:27017,ac-ujpend1-shard-00-02.gcd17y2.mongodb.net:27017/?ssl=true&replicaSet=atlas-4q8exw-shard-0&authSource=admin&retryWrites=true&w=majority"

client = MongoClient(mongo_uri)
db = client['trade_data']
collection = db['trades']

@app.route('/webhook', methods=['POST'])
def webhook():

    data = request.data.decode('utf-8')

    if data:
        # Save entire payload to MongoDB
        collection.insert_one({'data': data})

        return jsonify({'message': 'Trade data saved successfully'})
    return jsonify({'message': 'Invalid or unsupported payload'}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
