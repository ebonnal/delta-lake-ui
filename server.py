from flask import Flask
from flask import request
import subprocess
import os
import query_runner
import plotter
import dl_generator

dl_generator.generate_data_if_needed()
app = Flask(__name__)



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