from flask import Flask
from main import run_all

app = Flask(__name__)

@app.route('/')
def home():
    return "Connector is live."

@app.route('/run', methods=['GET'])
def manual_run():
    run_all()
    return "Manual sync complete."

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
