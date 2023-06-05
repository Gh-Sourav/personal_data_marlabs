from flask import Flask


app = Flask(__name__)

@app.route('/home')
def home():
    return "Hello Saurabh"

app.run(debug=True)