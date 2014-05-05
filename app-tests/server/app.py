import sys, os

cwd = u"".join(reversed(os.getcwd()))
test_dir = "stset-ppa"
try:
    base_dir = "".join(reversed(cwd[cwd.index(test_dir) + len(test_dir):]))
except ValueError:
    base_dir = os.getcwd()

if base_dir not in sys.path:
    sys.path.append(base_dir)

import json
from flask import Flask, request
import models

from dbsync import models as synchmodels, server


app = Flask(__name__)


def enc(string):
    table = {"<": "&lt;",
             ">": "&gt;"}
    return u"".join(table.get(c, c) for c in string)


@app.route("/")
def root():
    return 'Ping: any method <a href="/ping">/ping</a><br />'\
        'Repair: GET <a href="/repair">/repair</a><br />'\
        'Register: POST /register<br />'\
        'Pull: GET <a href="/pull">/pull</a><br />'\
        'Push: POST /push<br />'\
        'Query: GET <a href="/query">/query</a><br />'\
        'Inspect: GET <a href="/inspect">/inspect</a><br />'\
        'Synch query: GET <a href="/synch">/synch</a>'


@app.route("/ping")
def ping():
    return ""


@app.route("/repair", methods=["GET"])
def repair():
    return (json.dumps(server.handle_repair()),
            200,
            {"Content-Type": "application/json"})


@app.route("/register", methods=["POST"])
def register():
    return (json.dumps(server.handle_register()),
            200,
            {"Content-Type": "application/json"})


@app.route("/pull", methods=["GET", "POST"])
def pull():
    if request.method == "POST":
        print json.dumps(request.json, indent=2)
        try:
            return (json.dumps(server.handle_pull_request(request.json)),
                    200,
                    {"Content-Type": "application/json"})
        except server.handlers.PullRejected as e:
            return (json.dumps({'error': [repr(arg) for arg in e.args]}),
                    400,
                    {"Content-Type": "application/json"})
    print json.dumps(request.args, indent=2)
    return (json.dumps(server.handle_pull(request.args)),
            200,
            {"Content-Type": "application/json"})


@app.route("/push", methods=["POST"])
def push():
    print json.dumps(request.json, indent=2)
    try:
        return (json.dumps(server.handle_push(request.json)),
                200,
                {"Content-Type": "application/json"})
    except server.handlers.PushRejected as e:
        return (json.dumps({'error': [repr(arg) for arg in e.args]}),
                400,
                {"Content-Type": "application/json"})


@app.route("/query", methods=["GET"])
def query():
    return (json.dumps(server.handle_query(request.args)),
            200,
            {"Content-Type": "application/json"})


@app.route("/inspect", methods=["GET"])
def inspect():
    session = models.Session()
    return u"<strong>Cities:</strong><pre>{0}</pre><hr />"\
        u"<strong>Houses:</strong><pre>{1}</pre><hr />"\
        u"<strong>Persons:</strong><pre>{2}</pre><hr />".format(
        u"\n".join(enc(repr(x)) for x in session.query(models.City)),
        u"\n".join(enc(repr(x)) for x in session.query(models.House)),
        u"\n".join(enc(repr(x)) for x in session.query(models.Person)))


@app.route("/synch", methods=["GET"])
def synch():
    session = models.Session()
    return u"<strong>Content Types:</strong><pre>{0}</pre><hr />"\
        u"<strong>Nodes:</strong><pre>{1}</pre><hr />"\
        u"<strong>Versions:</strong><pre>{2}</pre><hr />"\
        u"<strong>Operations:</strong><pre>{3}</pre><hr />".format(
        u"\n".join(enc(repr(x)) for x in session.query(synchmodels.ContentType)),
        u"\n".join(enc(repr(x)) for x in session.query(synchmodels.Node)),
        u"\n".join(enc(repr(x)) for x in session.query(synchmodels.Version)),
        u"\n".join(enc(repr(x)) for x in session.query(synchmodels.Operation)))


if __name__ == "__main__":
    app.run(debug=True)
