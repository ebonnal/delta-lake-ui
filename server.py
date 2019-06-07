from flask import Flask
from flask import request
import subprocess
import os
import query_runner
import plotter

app = Flask(__name__)


"""
SELECT AVG(price) FROM delta.products  p JOIN delta.colors c ON c.name=p.name WHERE color='Red'"""

@app.route('/', methods=['GET'])
def login():
    statement = request.args.get('sql')
    if statement is not None:
        data = query_runner.run(statement)
        fig = plotter.plot(data, statement, append=int(request.args.get('append')))
        return "<p>" + fig + "</p>"
    else:
        with open("index.html") as f:
            return f.read()

if __name__ == "__main__":
    os.environ["FLASK_APP"] = "server.py"
    subprocess.call(["python3", "-m", "flask", "run"])