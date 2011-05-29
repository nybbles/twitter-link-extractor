from flask import Flask
app = Flask(__name__)

@app.route('/')
def top_links():
    return "Top links for term!"

app.run()
