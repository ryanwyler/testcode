from flask import Flask, jsonify, request

app = Flask(__name__)

@app.route('/example', methods=['GET'])
def example():
    return jsonify({"message": "Hello, World!"})

if __name__ == '__main__':
    app.run()

